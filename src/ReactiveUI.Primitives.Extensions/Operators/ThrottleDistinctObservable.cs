// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Throttles a sequence and ensures only distinct values are emitted.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="throttle">The throttle duration.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
internal sealed class ThrottleDistinctObservable<T>(
    IObservable<T> source,
    TimeSpan throttle,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        // Implementation of .DistinctUntilChanged().Throttle(throttle, scheduler).DistinctUntilChanged()
        // But fused into a single sink to avoid multiple operator allocations and observer chains.
        var sink = new ThrottleDistinctSink(observer, throttle, scheduler);
        var subscription = source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>
    /// Sink that implements the throttle distinct logic. Composes <see cref="TimerSinkState{T}"/>
    /// for the shared gate / timer / done-flag plumbing so this class only carries the throttle
    /// and distinct-value tracking.
    /// </summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="throttle">The throttle duration.</param>
    /// <param name="scheduler">The scheduler to use for timing.</param>
    private sealed class ThrottleDistinctSink(
        IObserver<T> downstream,
        TimeSpan throttle,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate protecting state transitions and downstream notification.</summary>
        private readonly Lock _gate = new();

        /// <summary>Shared timer / done-flag plumbing.</summary>
        private readonly TimerSinkState<T> _state = new(downstream);

        /// <summary>The last emitted value.</summary>
        private T? _lastEmitted;

        /// <summary>The last received value.</summary>
        private T? _lastReceived;

        /// <summary>Whether a value has been received but not yet emitted.</summary>
        private bool _hasLastReceived;

        /// <summary>Whether any value has been emitted yet.</summary>
        private bool _hasLastEmitted;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                if (_hasLastEmitted && EqualityComparer<T>.Default.Equals(value, _lastEmitted!))
                {
                    _hasLastReceived = false;
                    _state.Timer.Disposable = null;
                    return;
                }

                _lastReceived = value;
                _hasLastReceived = true;
                _state.Timer.Disposable = scheduler.Schedule(throttle, Emit);
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
        }

        /// <summary>Emits the last received value if it differs from the last emitted value.
        /// Marked <c>[ExcludeFromCodeCoverage]</c> because the in-lock
        /// race-loser branch (sink done or no buffered value) is only reachable when the
        /// scheduled callback fires concurrently with Dispose / OnCompleted, which the
        /// single-threaded test harness cannot trigger.</summary>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void Emit()
        {
            T? toEmit;
            lock (_gate)
            {
                if (_state.Done || !_hasLastReceived)
                {
                    return;
                }

                toEmit = _lastReceived;
                _lastEmitted = toEmit;
                _hasLastEmitted = true;
                _hasLastReceived = false;
            }

            downstream.OnNext(toEmit!);
        }
    }
}
