// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Throttles a sequence by only emitting the first element in each window.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="window">The window duration.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
internal sealed class ThrottleFirstObservable<T>(
    IObservable<T> source,
    TimeSpan window,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new ThrottleFirstWitness(observer, window, scheduler));
    }

    /// <summary>Observer that implements the throttle first logic.</summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="window">The window duration.</param>
    /// <param name="scheduler">The scheduler to use for timing.</param>
    private sealed class ThrottleFirstWitness(
        IObserver<T> downstream,
        TimeSpan window,
        ISequencer scheduler) : IObserver<T>
    {
        /// <summary>The gate to synchronize access to the observer state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The last time an element was emitted.</summary>
        private DateTimeOffset _last;

        /// <summary>Whether at least one element has been emitted.</summary>
        private bool _hasLast;

        /// <summary>Whether the observer has finished.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            var now = scheduler.Now;
            bool emit;

            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                emit = !_hasLast || now - _last >= window;
                if (emit)
                {
                    _last = now;
                    _hasLast = true;
                }
            }

            if (!emit)
            {
                return;
            }

            downstream.OnNext(value);
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
                downstream.OnCompleted();
            }
        }
    }
}
