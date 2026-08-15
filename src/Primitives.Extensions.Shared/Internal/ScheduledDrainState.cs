// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Extensions.Internal;
#endif

/// <summary>
/// Shared queue-and-single-drain marshaller used by the synchronous scheduler-marshalling operator
/// sinks (<c>ObserveOn</c>, <c>Conflate</c>). Each of those sinks previously hand-rolled the same gate,
/// FIFO queue, drain-in-flight flag, terminal flag, enqueue-and-schedule logic, and dequeue loop on top
/// of identical fields; this helper centralises that machinery so the per-sink class only carries the
/// operator-specific notification handling. Notifications are enqueued and a single drain pass is
/// scheduled per burst (rather than one scheduled action per item), and the drain callback carries no
/// captures — the sink is passed through as an <see cref="IDrainTarget"/>. Sinks compose one instance
/// and forward to it; there is no base class and no per-item virtual dispatch.
/// </summary>
/// <typeparam name="T">The element type carried by <see cref="DrainNotificationKind.Next"/> notifications.</typeparam>
/// <param name="scheduler">The scheduler each drain pass runs on.</param>
/// <param name="target">The sink whose <see cref="IDrainTarget.Drain"/> the scheduled pass invokes.</param>
/// <param name="gate">The gate, owned by the composing sink, protecting the queue and terminal state.</param>
internal sealed class ScheduledDrainState<T>(ISequencer scheduler, IDrainTarget target, Lock gate)
{
    /// <summary>The gate protecting the queue, drain flag, done flag, and the composing sink's own operator-specific state.</summary>
    private readonly Lock _gate = gate;

    /// <summary>The FIFO queue of pending upstream notifications.</summary>
    private readonly Queue<Notification> _queue = new();

    /// <summary>Upstream subscription handle, recorded via <see cref="Attach"/> for teardown on dispose.</summary>
    private IDisposable? _sourceSubscription;

    /// <summary>Set to <see langword="true"/> while a drain pass is in flight on the scheduler.</summary>
    private bool _draining;

    /// <summary>Set to <see langword="true"/> once a terminal notification has been delivered or the sink disposed.</summary>
    private bool _done;

    /// <summary>Gets a value indicating whether the sink has reached a terminal state. Read inside
    /// the sink's gate by callers that need to short-circuit once terminated.</summary>
    internal bool Done => _done;

    /// <summary>Enqueues an OnNext notification and schedules a drain pass if one isn't already running.</summary>
    /// <param name="value">The value to forward downstream.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnqueueNext(T value) => Enqueue(new(DrainNotificationKind.Next, value, null));

    /// <summary>Enqueues an OnError notification and schedules a drain pass if one isn't already running.</summary>
    /// <param name="error">The error to forward downstream.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnqueueError(Exception error) => Enqueue(new(DrainNotificationKind.Error, default!, error));

    /// <summary>Enqueues an OnCompleted notification and schedules a drain pass if one isn't already running.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnqueueCompleted() => Enqueue(new(DrainNotificationKind.Completed, default!, null));

    /// <summary>Records the upstream subscription, or disposes it immediately if the sink is already done.</summary>
    /// <param name="subscription">The upstream subscription handle.</param>
    internal void Attach(IDisposable subscription)
    {
        lock (_gate)
        {
            if (!_done)
            {
                _sourceSubscription = subscription;
                return;
            }
        }

        subscription.Dispose();
    }

    /// <summary>Dequeues the next pending notification, clearing the drain flag when the queue empties or the sink has terminated.</summary>
    /// <param name="notification">The dequeued notification when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> if a notification was dequeued; otherwise <see langword="false"/>.</returns>
    internal bool TryDequeue(out Notification notification)
    {
        lock (_gate)
        {
            if (_done || _queue.Count == 0)
            {
                _draining = false;
                notification = default;
                return false;
            }

            notification = _queue.Dequeue();
            return true;
        }
    }

    /// <summary>Marks the sink terminated and drops any still-queued notifications. Locks the gate.</summary>
    internal void Terminate()
    {
        lock (_gate)
        {
            _done = true;
            _queue.Clear();
        }
    }

    /// <summary>Marks the sink terminated without clearing the queue. Caller must hold the gate;
    /// the still-queued notifications are abandoned because <see cref="TryDequeue"/> checks the done flag first.</summary>
    internal void MarkDoneLocked() => _done = true;

    /// <summary>Begins disposal under the gate: marks the sink done, clears the queue, and detaches
    /// the upstream subscription — returned to the caller so it is disposed outside the gate. Returns
    /// <see langword="null"/> when already disposed.</summary>
    /// <returns>The upstream subscription to dispose outside the gate, or <see langword="null"/>.</returns>
    internal IDisposable? BeginDispose()
    {
        lock (_gate)
        {
            return _done ? null : BeginDisposeLocked();
        }
    }

    /// <summary>Marks the sink done, clears the queue, and detaches the upstream subscription, returning it for
    /// disposal outside the gate. Caller must hold the gate and have confirmed <see cref="Done"/> is
    /// <see langword="false"/>. Lets a composing sink dispose its own scheduled-work slot atomically with the
    /// done transition under the same lock.</summary>
    /// <returns>The upstream subscription to dispose outside the gate, or <see langword="null"/>.</returns>
    internal IDisposable? BeginDisposeLocked()
    {
        _done = true;
        _queue.Clear();
        var subscription = _sourceSubscription;
        _sourceSubscription = null;
        return subscription;
    }

    /// <summary>Enqueues a notification; claims and schedules a single drain pass if one isn't running.</summary>
    /// <param name="notification">The notification to forward to the drain loop.</param>
    private void Enqueue(in Notification notification)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _queue.Enqueue(notification);
            if (_draining)
            {
                return;
            }

            _draining = true;
        }

        _ = scheduler.Schedule(target, static (_, drainTarget) =>
        {
            drainTarget.Drain();
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Discriminated notification payload enqueued for the scheduled drain.</summary>
    /// <param name="Kind">The notification kind.</param>
    /// <param name="Value">The element carried by <see cref="DrainNotificationKind.Next"/>; default otherwise.</param>
    /// <param name="Error">The error carried by <see cref="DrainNotificationKind.Error"/>; null otherwise.</param>
    internal readonly record struct Notification(DrainNotificationKind Kind, T Value, Exception? Error);
}
