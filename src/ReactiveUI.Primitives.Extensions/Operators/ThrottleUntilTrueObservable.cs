// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Throttles a sequence until a predicate becomes true for an element.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="throttle">The throttle duration.</param>
/// <param name="predicate">The predicate to determine if an element should be emitted immediately or throttled.</param>
internal sealed class ThrottleUntilTrueObservable<T>(
    IObservable<T> source,
    TimeSpan throttle,
    Func<T, bool> predicate) : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = InvalidOperationExceptionHelper.Check(source);

    /// <summary>The throttle duration.</summary>
    private readonly TimeSpan _throttle = throttle;

    /// <summary>The predicate to determine if an element should be emitted immediately or throttled.</summary>
    private readonly Func<T, bool> _predicate = InvalidOperationExceptionHelper.Check(predicate);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ThrottleUntilTrueSink sink = new(observer, _throttle, _predicate, Sequencer.Default);
        var subscription = _source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>Sinks the source observable and throttles elements until a predicate is true.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="throttle">The throttle duration.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="scheduler">The scheduler used to time throttled emissions.</param>
    private sealed class ThrottleUntilTrueSink(
        IObserver<T> downstream,
        TimeSpan throttle,
        Func<T, bool> predicate,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for synchronization.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream = downstream;

        /// <summary>The throttle duration.</summary>
        private readonly TimeSpan _throttle = throttle;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <summary>The timer for throttling.</summary>
        private readonly SwapDisposable _timer = new();

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                if (_predicate(value))
                {
                    _timer.Disposable = null;
                    _downstream.OnNext(value);
                }
                else
                {
                    _timer.Disposable = scheduler.Schedule(
                        (sink: this, value),
                        _throttle,
                        static (_, state) =>
                        {
                            lock (state.sink._gate)
                            {
                                if (!state.sink._done)
                                {
                                    state.sink._downstream.OnNext(state.value);
                                }
                            }

                            return EmptyDisposable.Instance;
                        });
                }
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
                _timer.Dispose();
                _downstream.OnError(error);
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
                _timer.Dispose();
                _downstream.OnCompleted();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _timer.Dispose();
            }
        }
    }
}
