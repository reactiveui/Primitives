// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates delayed notification delivery with a single serialized timer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="dueTime">The normalized delay applied to each notification.</param>
/// <param name="sequencer">The sequencer used to schedule delayed notifications.</param>
/// <param name="observer">The downstream observer.</param>
/// <remarks>
/// The gate only guards the queue and the flags. Due notifications are queued in order under the gate and delivered by a
/// <see cref="SerializedDelivery{T}"/> after it is released, so no lock is held while the observer runs.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("ShiftCoordinator: Source = {_source}")]
public sealed class ShiftCoordinator<T>(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, IObserver<T> observer) : IDisposable
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The normalized delay applied to each notification.</summary>
    private readonly TimeSpan _dueTime = dueTime;

    /// <summary>The sequencer used to schedule delayed notifications.</summary>
    private readonly ISequencer _sequencer = sequencer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>Guards the queue and the flags; never held while the observer runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Active source and timer resources.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>The single active timer slot.</summary>
    private readonly SingleReplaceableDisposable _timer = new();

    /// <summary>Queued delayed notifications in source order.</summary>
    private readonly Queue<DelayedNotification> _queue = [];

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>A value indicating whether a timer or drain is active.</summary>
    private bool _timerActive;

    /// <summary>A value indicating whether the source has signaled a terminal notification.</summary>
    private bool _sourceStopped;

    /// <summary>A value indicating whether a terminal notification has been queued for delivery.</summary>
    private bool _done;

    /// <summary>Tracks disposal.</summary>
    private int _disposed;

    /// <summary>The queued notification kind.</summary>
    private enum NotificationKind
    {
        /// <summary>A value notification.</summary>
        Next = 0,

        /// <summary>An error notification.</summary>
        Error = 1,

        /// <summary>A completion notification.</summary>
        Completed = 2,
    }

    /// <summary>Gets a value indicating whether the coordinator is disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!TryBeginDispose())
        {
            return;
        }

        ReleaseSubscriptions();
    }

    /// <summary>Claims disposal before waiting for an in-flight notification.</summary>
    /// <returns>Whether this call owns resource cleanup.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryBeginDispose() => Interlocked.Exchange(ref _disposed, 1) == 0;

    /// <summary>Releases resources after the active notification leaves the gate.</summary>
    internal void ReleaseSubscriptions()
    {
        lock (_gate)
        {
            _timer.Dispose();
            _subscriptions.Dispose();
        }
    }

    /// <summary>Starts the delayed notification coordinator.</summary>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    internal ShiftCoordinator<T> Run()
    {
        _subscriptions.Add(_timer);
        _subscriptions.Add(_source.Subscribe(new EmptyWitness<T>(OnNext, OnError, OnCompleted)));
        return this;
    }

    /// <summary>Queues a source value for delayed delivery.</summary>
    /// <param name="value">The source value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnNext(T value) => Enqueue(DelayedNotification.Next(value, DueAt()), false);

    /// <summary>Queues a source error for delayed delivery behind earlier values.</summary>
    /// <param name="error">The source error.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnError(Exception error) => Enqueue(DelayedNotification.Failure(error, DueAt()), true);

    /// <summary>Queues source completion for delayed delivery behind earlier values.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnCompleted() => Enqueue(DelayedNotification.Completed(DueAt()), true);

    /// <summary>Computes the due time for the current source notification.</summary>
    /// <returns>The absolute due time.</returns>
    private DateTimeOffset DueAt() => _sequencer.Now + _dueTime;

    /// <summary>Queues a notification and starts the timer when this item owns the drain.</summary>
    /// <param name="notification">The delayed notification.</param>
    /// <param name="isTerminal">A value indicating whether the notification stops the source.</param>
    private void Enqueue(DelayedNotification notification, bool isTerminal)
    {
        TimeSpan delay = default;
        var shouldSchedule = false;
        lock (_gate)
        {
            if (IsDisposed || _done || _sourceStopped)
            {
                return;
            }

            if (isTerminal)
            {
                _sourceStopped = true;
            }

            _queue.Enqueue(notification);
            if (!_timerActive)
            {
                _timerActive = true;
                delay = DelayUntil(notification.DueAt);
                shouldSchedule = true;
            }
        }

        if (!shouldSchedule)
        {
            return;
        }

        Schedule(delay);
    }

    /// <summary>Schedules the single active drain timer.</summary>
    /// <param name="delay">The delay before the drain should run.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Schedule(TimeSpan delay) => TimerSlot.Arm(_timer, _sequencer, delay, Tick);

    /// <summary>Queues every due notification in FIFO order under the gate, delivers them, then re-arms for the next one.</summary>
    private void Tick()
    {
        TimeSpan delay = default;
        var shouldReschedule = false;
        var posted = false;
        var terminal = false;
        lock (_gate)
        {
            while (!IsDisposed && !_done && _queue.Count > 0)
            {
                delay = DelayUntil(_queue.Peek().DueAt);
                if (delay > TimeSpan.Zero)
                {
                    shouldReschedule = true;
                    break;
                }

                posted = true;
                terminal = Post(_queue.Dequeue());
            }

            if (!shouldReschedule)
            {
                _timerActive = false;
            }
        }

        if (posted)
        {
            _delivery.Flush(new PendingDrain(this));
        }

        if (shouldReschedule)
        {
            Schedule(delay);
            return;
        }

        if (!terminal)
        {
            return;
        }

        Dispose();
    }

    /// <summary>Queues a due notification for delivery while the caller holds the gate.</summary>
    /// <param name="notification">The due notification.</param>
    /// <returns><see langword="true"/> when the notification is terminal.</returns>
    private bool Post(DelayedNotification notification)
    {
        switch (notification.Kind)
        {
            case NotificationKind.Next:
                {
                    _ = _delivery.Post(notification.Value!);
                    return false;
                }

            case NotificationKind.Error:
                {
                    _done = true;
                    _ = _delivery.PostError(notification.Error!);
                    return true;
                }

            default:
                {
                    _done = true;
                    _ = _delivery.PostCompleted();
                    return true;
                }
        }
    }

    /// <summary>Computes the remaining delay for an absolute due time.</summary>
    /// <param name="dueAt">The absolute due time.</param>
    /// <returns>The remaining non-negative delay.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private TimeSpan DelayUntil(DateTimeOffset dueAt) => Sequencer.Normalize(dueAt - _sequencer.Now);

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(ShiftCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }

    /// <summary>A delayed source notification.</summary>
    private readonly struct DelayedNotification
    {
        /// <summary>Initializes a new instance of the <see cref="DelayedNotification"/> struct.</summary>
        /// <param name="kind">The notification kind.</param>
        /// <param name="value">The notification value.</param>
        /// <param name="error">The notification error.</param>
        /// <param name="dueAt">The notification due time.</param>
        private DelayedNotification(NotificationKind kind, T? value, Exception? error, DateTimeOffset dueAt)
        {
            Kind = kind;
            Value = value;
            Error = error;
            DueAt = dueAt;
        }

        /// <summary>Gets the notification kind.</summary>
        public NotificationKind Kind { get; }

        /// <summary>Gets the notification value.</summary>
        public T? Value { get; }

        /// <summary>Gets the notification error.</summary>
        public Exception? Error { get; }

        /// <summary>Gets the notification due time.</summary>
        public DateTimeOffset DueAt { get; }

        /// <summary>Creates a value notification.</summary>
        /// <param name="value">The notification value.</param>
        /// <param name="dueAt">The notification due time.</param>
        /// <returns>The delayed notification.</returns>
        public static DelayedNotification Next(T value, DateTimeOffset dueAt) =>
            new(NotificationKind.Next, value, null, dueAt);

        /// <summary>Creates an error notification.</summary>
        /// <param name="error">The notification error.</param>
        /// <param name="dueAt">The notification due time.</param>
        /// <returns>The delayed notification.</returns>
        public static DelayedNotification Failure(Exception error, DateTimeOffset dueAt) =>
            new(NotificationKind.Error, default, error, dueAt);

        /// <summary>Creates a completion notification.</summary>
        /// <param name="dueAt">The notification due time.</param>
        /// <returns>The delayed notification.</returns>
        public static DelayedNotification Completed(DateTimeOffset dueAt) =>
            new(NotificationKind.Completed, default, null, dueAt);
    }
}
