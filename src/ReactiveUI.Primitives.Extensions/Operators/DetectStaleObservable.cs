// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Detects when a sequence becomes stale (no emissions for a specified period).</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="stalenessPeriod">The period after which the sequence is considered stale.</param>
/// <param name="scheduler">The scheduler to run the staleness timer on.</param>
internal sealed class DetectStaleObservable<T>(
    IObservable<T> source,
    TimeSpan stalenessPeriod,
    ISequencer scheduler) : IObservable<Stale<T>>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<Stale<T>> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new DetectStaleSink(observer, stalenessPeriod, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        sink.Initialize();
        return sink;
    }

    /// <summary>
    /// Sink that manages staleness detection. Composes <see cref="TimerSinkState{T}"/> for the
    /// shared gate / timer / done-flag plumbing so this class only carries the OnNext / schedule logic.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="stalenessPeriod">The staleness period.</param>
    /// <param name="scheduler">The scheduler.</param>
    private sealed class DetectStaleSink(
        IObserver<Stale<T>> downstream,
        TimeSpan stalenessPeriod,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate protecting state transitions and downstream notification.</summary>
        private readonly Lock _gate = new();

        /// <summary>Shared timer / done-flag plumbing.</summary>
        private readonly TimerSinkState<Stale<T>> _state = new(downstream);

        /// <summary>Upstream subscription handle, set once via <see cref="AttachSourceSubscription"/> so the sink can tear it down on dispose without a wrapper bag.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>Records the upstream subscription for disposal.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        public void AttachSourceSubscription(IDisposable subscription)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    subscription.Dispose();
                    return;
                }

                _sourceSubscription = subscription;
            }
        }

        /// <summary>Initializes the staleness timer.</summary>
        public void Initialize() => ScheduleStale();

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                downstream.OnNext(new Stale<T>(value));
                ScheduleStale();
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _state.HandleErrorLocked(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                _state.HandleCompletedLocked();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _state.HandleDisposeLocked();
            }

            Interlocked.Exchange(ref _sourceSubscription, null)?.Dispose();
        }

        /// <summary>Schedules the staleness notification. Uses the state-carrying scheduler
        /// overload with a static lambda so no per-reschedule closure capturing <c>this</c> is
        /// allocated (the timer re-arms on every upstream emission).</summary>
        private void ScheduleStale() =>
            _state.Timer.Disposable = scheduler.Schedule(this, stalenessPeriod, static (_, self) => self.OnStaleTimer());

        /// <summary>Fires the stale marker downstream when the staleness window elapses.</summary>
        /// <returns>The singleton empty disposable for the scheduler contract.</returns>
        private EmptyDisposable OnStaleTimer()
        {
            lock (_gate)
            {
                if (!_state.Done)
                {
                    downstream.OnNext(new Stale<T>());
                }
            }

            return EmptyDisposable.Instance;
        }
    }
}
