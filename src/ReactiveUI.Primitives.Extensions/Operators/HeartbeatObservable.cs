// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Injects heartbeat values into the sequence when the source remains quiet for a specified period.</summary>
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

    /// <summary>The sink for the heartbeat operator.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="heartbeatPeriod">The period between heartbeats.</param>
    /// <param name="scheduler">The scheduler to run the heartbeat timer on.</param>
    private sealed class HeartbeatSink(
        IObserver<Heartbeat<T>> downstream,
        TimeSpan heartbeatPeriod,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate to synchronize access to the sink's state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The subscription to the periodic heartbeat timer.</summary>
        private readonly MutableDisposable _timerSubscription = new();

        /// <summary>Upstream subscription handle; set once via <see cref="AttachSourceSubscription"/> so the sink can tear it down in <see cref="Dispose"/> without needing a wrapper bag.</summary>
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

        /// <summary>Initializes the heartbeat timer.</summary>
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

                downstream.OnNext(new(value));
                ScheduleHeartbeats();
            }
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
                downstream.OnError(error);
            }
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
                downstream.OnCompleted();
            }
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

        /// <summary>Schedules the next heartbeat.</summary>
        private void ScheduleHeartbeats()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _timerSubscription.Disposable = scheduler.SchedulePeriodic(
                    downstream,
                    heartbeatPeriod,
                    static d => d.OnNext(new()));
            }
        }
    }
}
