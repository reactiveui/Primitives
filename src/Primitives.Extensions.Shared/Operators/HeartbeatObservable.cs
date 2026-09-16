// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Wraps each source value as an update and emits a heartbeat every <paramref name="heartbeatPeriod"/> that the source
/// stays quiet, restarting the timer on each value.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="heartbeatPeriod">The period between heartbeats.</param>
/// <param name="scheduler">The scheduler to run the heartbeat timer on.</param>
internal sealed class HeartbeatObservable<T>(
    IObservable<T> source,
    TimeSpan heartbeatPeriod,
    ISequencer scheduler) : IObservable<Heartbeat<T>>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<Heartbeat<T>> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        HeartbeatSink sink = new(observer, heartbeatPeriod, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        sink.Initialize();
        return sink;
    }

    /// <summary>Sink that forwards upstream values and emits a heartbeat whenever the period elapses without one.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="heartbeatPeriod">The period between heartbeats.</param>
    /// <param name="scheduler">The scheduler to run the heartbeat timer on.</param>
    /// <remarks>Updates, heartbeats and terminals are serialized, so a heartbeat never overlaps an update.</remarks>
    private sealed class HeartbeatSink(
        IObserver<Heartbeat<T>> downstream,
        TimeSpan heartbeatPeriod,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the flags and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The subscription to the periodic heartbeat timer.</summary>
        private readonly MutableDisposable _timerSubscription = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<Heartbeat<T>> _delivery = new();

        /// <summary>Upstream subscription handle, set once via <see cref="AttachSourceSubscription"/> and torn down in <see cref="Dispose"/>.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>Whether the sink has completed or been disposed.</summary>
        private bool _done;

        /// <summary>Records the upstream subscription so <see cref="Dispose"/> can tear it down.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        public void AttachSourceSubscription(IDisposable subscription)
        {
            lock (_gate)
            {
                if (_done)
                {
                    subscription.Dispose();
                    return;
                }

                _sourceSubscription = subscription;
            }
        }

        /// <summary>Starts the heartbeat timer, which the caller does at subscribe time.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize() => ScheduleHeartbeats();

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _ = _delivery.Post(new(value));
            }

            Flush();
            ScheduleHeartbeats();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _timerSubscription.Dispose();
                _ = _delivery.PostError(error);
            }

            Flush();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _timerSubscription.Dispose();
                _ = _delivery.PostCompleted();
            }

            Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            IDisposable? subscription;
            lock (_gate)
            {
                _done = true;
                _timerSubscription.Dispose();
                subscription = _sourceSubscription;
                _sourceSubscription = null;
            }

            subscription?.Dispose();
        }

        /// <summary>Restarts the periodic heartbeat timer, dropping the one it replaces.</summary>
        private void ScheduleHeartbeats()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _timerSubscription.Disposable = scheduler.SchedulePeriodic(
                    this,
                    heartbeatPeriod,
                    static sink => sink.EmitHeartbeat());
            }
        }

        /// <summary>Queues and delivers a heartbeat; nothing is delivered once a terminal notification is queued.</summary>
        private void EmitHeartbeat()
        {
            _ = _delivery.Post(new());
            Flush();
        }

        /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(HeartbeatSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
