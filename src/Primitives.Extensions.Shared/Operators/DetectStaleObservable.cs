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

/// <summary>Wraps each value as an update and emits one staleness marker per quiet stretch of <paramref name="stalenessPeriod"/>.</summary>
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

        DetectStaleSink sink = new(observer, stalenessPeriod, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        sink.Initialize();
        return sink;
    }

    /// <summary>Sink that re-arms the staleness timer on each upstream value and emits a stale marker when the window elapses.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="stalenessPeriod">The staleness period.</param>
    /// <param name="scheduler">The sequencer that times the staleness window.</param>
    /// <remarks>Updates and stale markers are queued in order under the gate and delivered after it is released.</remarks>
    private sealed class DetectStaleSink(
        IObserver<Stale<T>> downstream,
        TimeSpan stalenessPeriod,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the terminal state and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timer slot, terminal state and serialized delivery shared with the operator's handlers.</summary>
        private readonly TimerSinkState<Stale<T>> _state = new(downstream);

        /// <summary>Upstream subscription handle, set once via <see cref="AttachSourceSubscription"/> and disposed with the sink.</summary>
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

        /// <summary>Arms the first staleness window, which the caller does at subscribe time.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize() => ScheduleStale();

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (!_state.QueueLocked(new(value)))
                {
                    return;
                }
            }

            _state.Flush();
            ScheduleStale();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _ = _state.QueueErrorLocked(error);
            }

            _state.Flush();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                _ = _state.QueueCompletedLocked();
            }

            _state.Flush();
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

        /// <summary>Arms the staleness timer, replacing any window that is counting down.</summary>
        private void ScheduleStale() =>
            _state.Timer.Disposable =
                scheduler.Schedule(this, stalenessPeriod, static (_, self) => self.OnStaleTimer());

        /// <summary>Queues and delivers the stale marker when the staleness window elapses.</summary>
        /// <returns>The singleton empty disposable for the scheduler contract.</returns>
        private EmptyDisposable OnStaleTimer()
        {
            lock (_gate)
            {
                _ = _state.QueueLocked(new());
            }

            _state.Flush();
            return EmptyDisposable.Instance;
        }
    }
}
