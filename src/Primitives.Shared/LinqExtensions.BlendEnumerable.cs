// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Enumerable source overloads for blend operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Operators for enumerable observable sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The observable sources.</param>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Concurrently merges the supplied observable sources.</summary>
        /// <returns>An observable that forwards values from every source.</returns>
        public IObservable<T> Blend()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new EnumerableBlendSignal<T>(sources);
        }

        /// <summary>Concurrently merges the supplied observable sources with a maximum number of active subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of sources to subscribe to at the same time.</param>
        /// <returns>An observable that forwards values from every source.</returns>
        public IObservable<T> Blend(int maxConcurrent)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);

            return maxConcurrent == int.MaxValue ? sources.Blend() : new MaxConcurrentEnumerableBlendSignal<T>(sources, maxConcurrent);
        }
    }

    /// <summary>Dedicated signal for enumerable <c>Blend</c> sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class EnumerableBlendSignal<T> : IObservable<T>
    {
        /// <summary>The sources to merge.</summary>
        private readonly IEnumerable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="EnumerableBlendSignal{T}"/> class.</summary>
        /// <param name="sources">The sources to merge.</param>
        internal EnumerableBlendSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new BlendCoordinator<T>(observer).Run(_sources);
        }
    }

    /// <summary>Dedicated signal for enumerable <c>Blend</c> sources with bounded concurrency.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class MaxConcurrentEnumerableBlendSignal<T> : IObservable<T>
    {
        /// <summary>The sources to merge.</summary>
        private readonly IEnumerable<IObservable<T>> _sources;

        /// <summary>The maximum number of active inner subscriptions.</summary>
        private readonly int _maxConcurrent;

        /// <summary>Initializes a new instance of the <see cref="MaxConcurrentEnumerableBlendSignal{T}"/> class.</summary>
        /// <param name="sources">The sources to merge.</param>
        /// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
        internal MaxConcurrentEnumerableBlendSignal(IEnumerable<IObservable<T>> sources, int maxConcurrent)
        {
            _sources = sources;
            _maxConcurrent = maxConcurrent;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new MaxConcurrentBlendCoordinator<T>(observer).Run(_sources, _maxConcurrent);
        }
    }

    /// <summary>Coordinates bounded-concurrency merging of enumerable observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class MaxConcurrentBlendCoordinator<T> : IDisposable
    {
        /// <summary>Serializes enumeration, counters, and downstream callbacks.</summary>
        private readonly Lock _gate = new();

        /// <summary>Active subscriptions and enumerable lifetime.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The source enumerator.</summary>
        private IEnumerator<IObservable<T>>? _enumerator;

        /// <summary>The number of active inner sources.</summary>
        private int _active;

        /// <summary>Whether all enumerable sources have been consumed.</summary>
        private bool _enumerationCompleted;

        /// <summary>Whether a terminal notification has been emitted.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="MaxConcurrentBlendCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal MaxConcurrentBlendCoordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            var enumerator = _enumerator;
            _enumerator = null;
            enumerator?.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>Starts bounded-concurrency merging.</summary>
        /// <param name="sources">The enumerable sources.</param>
        /// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MaxConcurrentBlendCoordinator<T> Run(IEnumerable<IObservable<T>> sources, int maxConcurrent)
        {
            _enumerator = sources.GetEnumerator();

            for (var i = 0; i < maxConcurrent; i++)
            {
                if (!SubscribeNext())
                {
                    break;
                }
            }

            return this;
        }

        /// <summary>Subscribes to the next enumerable source when one is available.</summary>
        /// <returns><see langword="true"/> when a new inner source was subscribed.</returns>
        private bool SubscribeNext()
        {
            var next = TakeNextSource(out var failed);
            if (failed)
            {
                Dispose();
            }

            if (next is null)
            {
                return false;
            }

            OnceDisposable inner = new();
            _subscriptions.Add(inner);
            inner.Disposable = next.Subscribe(OnInnerNext, OnAnyError, () => OnInnerCompleted(inner));
            return true;
        }

        /// <summary>Reads the next source from the enumerable under the gate.</summary>
        /// <param name="failed">Set to <see langword="true"/> when reading the next source failed.</param>
        /// <returns>The next source, or <see langword="null"/> when no source should be subscribed.</returns>
        private IObservable<T>? TakeNextSource(out bool failed)
        {
            failed = false;
            lock (_gate)
            {
                if (_done || _enumerationCompleted)
                {
                    return null;
                }

                var enumerator = _enumerator;
                try
                {
                    if (enumerator?.MoveNext() != true)
                    {
                        _enumerationCompleted = true;
                        DisposeEnumerator();
                        TryCompleteCore();
                        return null;
                    }
                }
                catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
                {
                    FailCore(error);
                    failed = true;
                    return null;
                }

                var next = enumerator!.Current;
                if (next is null)
                {
                    FailCore(new InvalidOperationException("Blend source contained null."));
                    failed = true;
                    return null;
                }

                _active++;
                return next;
            }
        }

        /// <summary>Forwards an inner value under the serialization gate.</summary>
        /// <param name="value">The value to forward.</param>
        private void OnInnerNext(T value)
        {
            lock (_gate)
            {
                if (!_done)
                {
                    _observer.OnNext(value);
                }
            }
        }

        /// <summary>Forwards the first terminal error and releases active subscriptions.</summary>
        /// <param name="error">The error to forward.</param>
        private void OnAnyError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                FailCore(error);
            }

            Dispose();
        }

        /// <summary>Completes one inner source and starts another if possible.</summary>
        /// <param name="inner">The completed inner subscription.</param>
        private void OnInnerCompleted(OnceDisposable inner)
        {
            _subscriptions.Remove(inner);

            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _active--;
                TryCompleteCore();
            }

            SubscribeNext();
        }

        /// <summary>Marks the coordinator failed and forwards the error. Caller must hold the gate.</summary>
        /// <param name="error">The terminal error.</param>
        private void FailCore(Exception error)
        {
            _done = true;
            _observer.OnError(error);
        }

        /// <summary>Completes downstream once enumeration and all active sources have completed. Caller must hold the gate.</summary>
        private void TryCompleteCore()
        {
            if (_done || !_enumerationCompleted || _active != 0)
            {
                return;
            }

            _done = true;
            _observer.OnCompleted();
        }

        /// <summary>Disposes the enumerable source exactly once.</summary>
        private void DisposeEnumerator()
        {
            var enumerator = _enumerator;
            _enumerator = null;
            enumerator?.Dispose();
        }
    }
}
