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

/// <summary>Coordinates quiet-period emission with one active timer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <remarks>
/// The gate only guards the latest value and the flags. Emissions and terminals are queued in order under the gate and
/// delivered by a <see cref="SerializedDelivery{T}"/> after it is released, so no lock is held while the observer runs.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("CalmCoordinator<{typeof(T).Name,nq}>")]
public sealed class CalmCoordinator<T> : IDisposable
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The normalized quiet period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule quiet-period timers.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Guards the latest value and the flags; never held while the observer runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Active subscription and timer resources.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>The active timer slot.</summary>
    private readonly SingleReplaceableDisposable _timer = new();

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The downstream observer.</summary>
    private IObserver<T>? _observer;

    /// <summary>The latest source value.</summary>
    private T? _latest;

    /// <summary>A value indicating whether a latest source value is waiting to be emitted.</summary>
    private bool _hasLatest;

    /// <summary>A value indicating whether the timer is active.</summary>
    private bool _timerActive;

    /// <summary>The virtual due time for the current quiet period.</summary>
    private DateTimeOffset _dueAt;

    /// <summary>A value indicating whether a terminal notification has been emitted.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="CalmCoordinator{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period.</param>
    /// <param name="sequencer">The sequencer used to schedule timers.</param>
    public CalmCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
    {
        _source = source;
        _dueTime = Sequencer.Normalize(dueTime);
        _sequencer = sequencer;
        _dueAt = sequencer.Now;
    }

    /// <summary>The action to take when a timer fires.</summary>
    private enum TimerAction
    {
        /// <summary>No value is available.</summary>
        None = 0,

        /// <summary>Emit the captured value.</summary>
        Emit = 1,

        /// <summary>Reschedule for the remaining quiet period.</summary>
        Reschedule = 2,
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _timer.Dispose();
        _subscriptions.Dispose();
    }

    /// <summary>Starts quiet-period coordination.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    public CalmCoordinator<T> Run(IObserver<T> observer)
    {
        _observer = observer;
        _subscriptions.Add(_timer);
        _subscriptions.Add(_source.Subscribe(OnNext, OnError, OnCompleted));
        return this;
    }

    /// <summary>Records a source value and schedules a timer when needed.</summary>
    /// <param name="value">The source value.</param>
    private void OnNext(T value)
    {
        var shouldSchedule = false;
        lock (_gate)
        {
            _latest = value;
            _hasLatest = true;
            _dueAt = _sequencer.Now + _dueTime;
            if (!_timerActive)
            {
                _timerActive = true;
                shouldSchedule = true;
            }
        }

        if (!shouldSchedule)
        {
            return;
        }

        Schedule(_dueTime);
    }

    /// <summary>Queues a terminal error, delivers it and releases active resources.</summary>
    /// <param name="error">The terminal error.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
        Dispose();
    }

    /// <summary>Queues any value waiting inside the quiet window followed by completion, delivers them and releases active resources.</summary>
    private void OnCompleted()
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _done = true;

            if (_hasLatest)
            {
                _hasLatest = false;
                _ = _delivery.Post(_latest!);
            }

            _ = _delivery.PostCompleted();
        }

        _delivery.Flush(new PendingDrain(this));
        Dispose();
    }

    /// <summary>Schedules the active timer.</summary>
    /// <param name="delay">The timer delay.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Schedule(TimeSpan delay) => TimerSlot.Arm(_timer, _sequencer, delay, Tick);

    /// <summary>Handles a timer tick.</summary>
    private void Tick()
    {
        var action = GetTimerAction(out var delay);
        if (action == TimerAction.Reschedule)
        {
            Schedule(delay);
            return;
        }

        if (action != TimerAction.Emit)
        {
            return;
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Determines what the active timer should do, queuing the value to emit under the gate.</summary>
    /// <param name="delay">The remaining delay when rescheduling is needed.</param>
    /// <returns>The timer action.</returns>
    private TimerAction GetTimerAction(out TimeSpan delay)
    {
        lock (_gate)
        {
            var remaining = _dueAt - _sequencer.Now;
            if (remaining > TimeSpan.Zero)
            {
                delay = remaining;
                return TimerAction.Reschedule;
            }

            _timerActive = false;
            delay = default;
            if (_done || !_hasLatest)
            {
                return TimerAction.None;
            }

            _hasLatest = false;
            _ = _delivery.Post(_latest!);
            return TimerAction.Emit;
        }
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(CalmCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer!);
    }
}
