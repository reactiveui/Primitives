// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Classic throttle (debounce) operator. Emits a value only after
/// <paramref name="dueTime"/> has elapsed without any new emission from the
/// source. Each new upstream <c>OnNext</c> cancels the pending emission and
/// schedules a new one. Provides the equivalent of Rx's
/// <c>Observable.Throttle</c> without depending on System.Reactive.Linq.
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="dueTime">The quiescence duration required before emission.</param>
/// <param name="scheduler">The scheduler used to time emissions.</param>
internal sealed class ThrottleObservable<T>(
    IObservable<T> source,
    TimeSpan dueTime,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ThrottleSink sink = new(observer, dueTime, scheduler);
        var subscription = source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>Sink that holds the most-recent value and a scheduled emission that fires after the configured quiescence interval.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="dueTime">The quiescence duration.</param>
    /// <param name="scheduler">The scheduler used to time emissions.</param>
    private sealed class ThrottleSink(
        IObserver<T> downstream,
        TimeSpan dueTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The synchronization gate.</summary>
        private readonly Lock _gate = new();

        /// <summary>The pending scheduled emission.</summary>
        private readonly SwapDisposable _pending = new();

        /// <summary>The most-recent value waiting to be emitted.</summary>
        private T _latest = default!;

        /// <summary>Whether a value is currently pending emission.</summary>
        private bool _hasValue;

        /// <summary>Monotonic id of the latest scheduled emission.</summary>
        private long _emissionId;

        /// <summary>Whether the sequence is terminally done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            long id;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _latest = value;
                _hasValue = true;
                id = ++_emissionId;
            }

            _pending.Disposable = scheduler.Schedule((this, id), dueTime, static (_, state) =>
            {
                state.Item1.Emit(state.id);
                return EmptyDisposable.Instance;
            });
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
            }

            _pending.Dispose();
            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            T pending;
            bool flush;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                flush = _hasValue;
                pending = _latest;
                _hasValue = false;
            }

            _pending.Dispose();

            if (flush)
            {
                downstream.OnNext(pending);
            }

            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
            }

            _pending.Dispose();
        }

        /// <summary>
        /// Emits the buffered value if it is still current (i.e. no newer
        /// <see cref="OnNext"/> arrived after this emission was scheduled).
        /// Marked <c>[ExcludeFromCodeCoverage]</c> because the in-lock
        /// race-loser branch (sink done, emission superseded, value already drained) is only
        /// reachable when the scheduled callback fires concurrently with Dispose / OnCompleted,
        /// which the single-threaded test harness cannot trigger.
        /// </summary>
        /// <param name="id">The emission id this callback was scheduled for.</param>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void Emit(long id)
        {
            T value;
            lock (_gate)
            {
                if (_done || id != _emissionId || !_hasValue)
                {
                    return;
                }

                value = _latest;
                _hasValue = false;
            }

            downstream.OnNext(value);
        }
    }
}
