// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>FlatMap helper implementations.</summary>
public static partial class LinqExtensions
{
    /// <summary>Chaining FlatMap signal that avoids the Map + Chain composition path.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects source values to inner observables.</param>
    private sealed class FlatMapSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector) : IObservable<TResult>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source = source;

        /// <summary>Projects source values to inner observables.</summary>
        private readonly Func<TSource, IObservable<TResult>> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new FlatMapCoordinator<TSource, TResult>(_source, _selector, observer).Run();
        }
    }

    /// <summary>Chaining FlatMap signal with an outer/inner result selector.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TCollection">The inner value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="collectionSelector">Projects source values to inner observables.</param>
    /// <param name="resultSelector">Projects outer and inner values to result values.</param>
    private sealed class FlatMapResultSignal<TSource, TCollection, TResult>(
        IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector) : IObservable<TResult>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source = source;

        /// <summary>Projects source values to inner observables.</summary>
        private readonly Func<TSource, IObservable<TCollection>> _collectionSelector = collectionSelector;

        /// <summary>Projects outer and inner values to result values.</summary>
        private readonly Func<TSource, TCollection, TResult> _resultSelector = resultSelector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new FlatMapResultCoordinator<TSource, TCollection, TResult>(
                _source,
                _collectionSelector,
                _resultSelector,
                observer).Run();
        }
    }

    /// <summary>Coordinates chain-style FlatMap subscriptions.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class FlatMapCoordinator<TSource, TResult> : IDisposable
    {
        /// <summary>Synchronizes subscription state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source;

        /// <summary>Projects source values to inner observables.</summary>
        private readonly Func<TSource, IObservable<TResult>> _selector;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>Observer used for the outer source.</summary>
        private readonly OuterWitness _outerObserver;

        /// <summary>Observer used for active inner sources.</summary>
        private readonly InnerWitness _innerObserver;

        /// <summary>Queued inner sources waiting for the active inner source to complete.</summary>
        private Queue<IObservable<TResult>>? _queue;

        /// <summary>Outer subscription.</summary>
        private IDisposable? _outer;

        /// <summary>Active inner subscription.</summary>
        private IDisposable? _inner;

        /// <summary>Value indicating whether an inner source is active.</summary>
        private bool _active;

        /// <summary>Value indicating whether the outer source has completed.</summary>
        private bool _outerCompleted;

        /// <summary>Value indicating whether the coordinator has stopped.</summary>
        private bool _disposed;

        /// <summary>Value indicating whether the active inner source is currently subscribing.</summary>
        private bool _subscribingInner;

        /// <summary>Value indicating whether the active inner source completed while its subscribe call was still on the stack.</summary>
        private bool _completedInnerWhileSubscribing;

        /// <summary>Initializes a new instance of the <see cref="FlatMapCoordinator{TSource, TResult}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="selector">Projects source values to inner observables.</param>
        /// <param name="observer">The downstream observer.</param>
        [SuppressMessage(
            "Correctness",
            "SST2403:Do not let 'this' escape from a constructor",
            Justification =
                "The witnesses are this coordinator's own sinks, stored back into its fields, and nothing notifies them until Run subscribes.")]
        internal FlatMapCoordinator(
            IObservable<TSource> source,
            Func<TSource, IObservable<TResult>> selector,
            IObserver<TResult> observer)
        {
            _source = source;
            _selector = selector;
            _observer = observer;
            _outerObserver = new(this);
            _innerObserver = new(this);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            IDisposable? outer;
            IDisposable? inner;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                outer = _outer;
                inner = _inner;
                _outer = null;
                _inner = null;
                _queue = null;
            }

            outer?.Dispose();
            inner?.Dispose();
        }

        /// <summary>Subscribes to the outer source.</summary>
        /// <returns>The subscription cleanup.</returns>
        internal IDisposable Run()
        {
            var outer = _source.Subscribe(_outerObserver);
            lock (_gate)
            {
                if (_disposed)
                {
                    outer.Dispose();
                    return EmptyDisposable.Instance;
                }

                _outer = outer;
                return this;
            }
        }

        /// <summary>Handles a source value.</summary>
        /// <param name="value">The source value.</param>
        private void OnOuterNext(TSource value)
        {
            IObservable<TResult> inner;
            try
            {
                inner = _selector(value) ?? throw new InvalidOperationException("The FlatMap selector returned null.");
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            if (!TryStartOrQueue(inner))
            {
                return;
            }

            SubscribeInner(inner);
        }

        /// <summary>Handles outer source completion.</summary>
        private void OnOuterCompleted()
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _outerCompleted = true;
            }

            Drain();
        }

        /// <summary>Forwards an inner value.</summary>
        /// <param name="value">The inner value.</param>
        private void OnInnerNext(TResult value)
        {
            // Hot path: only one inner is active at a time (sequential concat semantics), so the
            // forward is already serialized. A lock-free volatile read of the disposed flag avoids
            // a monitor acquire on every value; the lock releases elsewhere publish the write.
            if (Volatile.Read(ref _disposed))
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>Handles active inner source completion.</summary>
        private void OnInnerCompleted()
        {
            IDisposable? inner;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                if (_subscribingInner)
                {
                    _completedInnerWhileSubscribing = true;
                    _active = false;
                    return;
                }

                inner = _inner;
                _inner = null;
                _active = false;
            }

            inner?.Dispose();
            Drain();
        }

        /// <summary>Handles an outer or inner error.</summary>
        /// <param name="error">The error.</param>
        private void OnError(Exception error)
        {
            IDisposable? outer;
            IDisposable? inner;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                outer = _outer;
                inner = _inner;
                _outer = null;
                _inner = null;
                _queue = null;
            }

            outer?.Dispose();
            inner?.Dispose();
            _observer.OnError(error);
        }

        /// <summary>Starts an inner source immediately or queues it behind the active inner source.</summary>
        /// <param name="inner">The inner source.</param>
        /// <returns><c>true</c> when the source should be subscribed immediately; otherwise, <c>false</c>.</returns>
        private bool TryStartOrQueue(IObservable<TResult> inner)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return false;
                }

                if (!_active)
                {
                    _active = true;
                    return true;
                }

                (_queue ??= new()).Enqueue(inner);
                return false;
            }
        }

        /// <summary>Subscribes to an inner source.</summary>
        /// <param name="inner">The inner source.</param>
        private void SubscribeInner(IObservable<TResult> inner)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _subscribingInner = true;
                _completedInnerWhileSubscribing = false;
            }

            IDisposable subscription;
            try
            {
                subscription = inner.Subscribe(_innerObserver);
            }
            catch (Exception error)
            {
                CompleteSubscribe(error);
                return;
            }

            var completed = CompleteSubscribe(subscription);
            if (!completed)
            {
                return;
            }

            subscription.Dispose();
            Drain();
        }

        /// <summary>Completes an inner subscribe call that threw.</summary>
        /// <param name="error">The subscribe error.</param>
        private void CompleteSubscribe(Exception error)
        {
            lock (_gate)
            {
                _subscribingInner = false;
                _active = false;
            }

            OnError(error);
        }

        /// <summary>Completes an inner subscribe call.</summary>
        /// <param name="subscription">The inner subscription.</param>
        /// <returns><c>true</c> when the inner source completed synchronously; otherwise, <c>false</c>.</returns>
        private bool CompleteSubscribe(IDisposable subscription)
        {
            lock (_gate)
            {
                _subscribingInner = false;
                if (_disposed || _completedInnerWhileSubscribing)
                {
                    return true;
                }

                _inner = subscription;
                return false;
            }
        }

        /// <summary>Drains queued inner sources and completes when the outer source and queue are finished.</summary>
        private void Drain()
        {
            while (true)
            {
                IObservable<TResult>? next = null;
                var complete = false;
                lock (_gate)
                {
                    if (_disposed || _active)
                    {
                        return;
                    }

                    if (_queue is { Count: > 0 } queue)
                    {
                        _active = true;
                        next = queue.Dequeue();
                    }
                    else if (_outerCompleted)
                    {
                        _disposed = true;
                        complete = true;
                    }
                }

                if (next is not null)
                {
                    SubscribeInner(next);
                    continue;
                }

                if (complete)
                {
                    _observer.OnCompleted();
                }

                return;
            }
        }

        /// <summary>Outer source observer.</summary>
        private sealed class OuterWitness : IObserver<TSource>
        {
            /// <summary>Owning coordinator.</summary>
            private readonly FlatMapCoordinator<TSource, TResult> _parent;

            /// <summary>Initializes a new instance of the <see cref="OuterWitness"/> class.</summary>
            /// <param name="parent">Owning coordinator.</param>
            internal OuterWitness(FlatMapCoordinator<TSource, TResult> parent) => _parent = parent;

            /// <inheritdoc/>
            public void OnCompleted() => _parent.OnOuterCompleted();

            /// <inheritdoc/>
            public void OnError(Exception error) => _parent.OnError(error);

            /// <inheritdoc/>
            public void OnNext(TSource value) => _parent.OnOuterNext(value);
        }

        /// <summary>Inner source observer.</summary>
        private sealed class InnerWitness : IObserver<TResult>
        {
            /// <summary>Owning coordinator.</summary>
            private readonly FlatMapCoordinator<TSource, TResult> _parent;

            /// <summary>Initializes a new instance of the <see cref="InnerWitness"/> class.</summary>
            /// <param name="parent">Owning coordinator.</param>
            internal InnerWitness(FlatMapCoordinator<TSource, TResult> parent) => _parent = parent;

            /// <inheritdoc/>
            public void OnCompleted() => _parent.OnInnerCompleted();

            /// <inheritdoc/>
            public void OnError(Exception error) => _parent.OnError(error);

            /// <inheritdoc/>
            public void OnNext(TResult value) => _parent.OnInnerNext(value);
        }
    }

    /// <summary>Coordinates chain-style FlatMap subscriptions with a result selector.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TCollection">The inner value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class FlatMapResultCoordinator<TSource, TCollection, TResult> : IDisposable
    {
        /// <summary>The inner FlatMap coordinator.</summary>
        private readonly FlatMapCoordinator<TSource, TResult> _inner;

        /// <summary>Initializes a new instance of the <see cref="FlatMapResultCoordinator{TSource, TCollection, TResult}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="collectionSelector">Projects source values to inner observables.</param>
        /// <param name="resultSelector">Projects outer and inner values to result values.</param>
        /// <param name="observer">The downstream observer.</param>
        internal FlatMapResultCoordinator(
            IObservable<TSource> source,
            Func<TSource, IObservable<TCollection>> collectionSelector,
            Func<TSource, TCollection, TResult> resultSelector,
            IObserver<TResult> observer) =>
            _inner = new(
                source,
                value => new FlatMapResultInnerSignal<TSource, TCollection, TResult>(
                    value,
                    collectionSelector(value),
                    resultSelector),
                observer);

        /// <inheritdoc/>
        public void Dispose() => _inner.Dispose();

        /// <summary>Subscribes to the outer source.</summary>
        /// <returns>The subscription cleanup.</returns>
        internal IDisposable Run() => _inner.Run();
    }

    /// <summary>Maps inner values with a captured outer value.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TCollection">The inner value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class FlatMapResultInnerSignal<TSource, TCollection, TResult> : IObservable<TResult>
    {
        /// <summary>Captured outer value.</summary>
        private readonly TSource _sourceValue;

        /// <summary>Inner observable.</summary>
        private readonly IObservable<TCollection> _source;

        /// <summary>Projects outer and inner values to result values.</summary>
        private readonly Func<TSource, TCollection, TResult> _selector;

        /// <summary>Initializes a new instance of the <see cref="FlatMapResultInnerSignal{TSource, TCollection, TResult}"/> class.</summary>
        /// <param name="sourceValue">Captured outer value.</param>
        /// <param name="source">Inner observable.</param>
        /// <param name="selector">Projects outer and inner values to result values.</param>
        internal FlatMapResultInnerSignal(
            TSource sourceValue,
            IObservable<TCollection> source,
            Func<TSource, TCollection, TResult> selector)
        {
            _sourceValue = sourceValue;
            _source = source ?? throw new InvalidOperationException("The FlatMap collection selector returned null.");
            _selector = selector;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return _source.Subscribe(new ResultWitness(_sourceValue, _selector, observer));
        }

        /// <summary>Maps inner source values.</summary>
        /// <param name="sourceValue">Captured outer value.</param>
        /// <param name="selector">Projects outer and inner values to result values.</param>
        /// <param name="observer">The downstream observer.</param>
        private sealed class ResultWitness(
            TSource sourceValue,
            Func<TSource, TCollection, TResult> selector,
            IObserver<TResult> observer) : IObserver<TCollection>
        {
            /// <summary>Captured outer value.</summary>
            private readonly TSource _sourceValue = sourceValue;

            /// <summary>Projects outer and inner values to result values.</summary>
            private readonly Func<TSource, TCollection, TResult> _selector = selector;

            /// <summary>The downstream observer.</summary>
            private readonly IObserver<TResult> _observer = observer;

            /// <inheritdoc/>
            public void OnCompleted() => _observer.OnCompleted();

            /// <inheritdoc/>
            public void OnError(Exception error) => _observer.OnError(error);

            /// <inheritdoc/>
            public void OnNext(TCollection value) => _observer.OnNext(_selector(_sourceValue, value));
        }
    }
}
