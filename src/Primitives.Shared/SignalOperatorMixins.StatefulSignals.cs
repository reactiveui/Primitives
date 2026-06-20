// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// Dedicated <see cref="IObservable{T}"/> implementations for the stateful single-source operators.
/// Building through these instead of <c>Signal.CreateSafe(observer =&gt; ...)</c> removes the
/// per-subscription closure, delegate, CreateSafe wrapper, and safe-guard sink, leaving only the
/// signal object at chain-build time and the operator sink at subscribe time.
/// </summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>MapIndexed</c>.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    private sealed class MapIndexedSignal<TSource, TResult> : IRequireCurrentThread<TResult>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source;

        /// <summary>The indexed selector.</summary>
        private readonly Func<TSource, int, TResult> _selector;

        /// <summary>Initializes a new instance of the <see cref="MapIndexedSignal{TSource, TResult}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="selector">The indexed selector.</param>
        internal MapIndexedSignal(IObservable<TSource> source, Func<TSource, int, TResult> selector)
        {
            _source = source;
            _selector = selector;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return _source.Subscribe(new MapIndexedWitness(observer, _selector));
        }

        /// <summary>Applies the indexed selector to source values.</summary>
        private sealed class MapIndexedWitness : IObserver<TSource>
        {
            /// <summary>The downstream observer.</summary>
            private readonly IObserver<TResult> _observer;

            /// <summary>The indexed selector.</summary>
            private readonly Func<TSource, int, TResult> _selector;

            /// <summary>The next zero-based index.</summary>
            private int _index;

            /// <summary>Whether a terminal notification has been forwarded.</summary>
            private bool _stopped;

            /// <summary>Initializes a new instance of the <see cref="MapIndexedWitness"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="selector">The indexed selector.</param>
            internal MapIndexedWitness(IObserver<TResult> observer, Func<TSource, int, TResult> selector)
            {
                _observer = observer;
                _selector = selector;
            }

            /// <inheritdoc/>
            public void OnNext(TSource value)
            {
                if (_stopped)
                {
                    return;
                }

                TResult result;
                try
                {
                    result = _selector(value, _index++);
                }
                catch (Exception error)
                {
                    OnError(error);
                    return;
                }

                _observer.OnNext(result);
            }

            /// <inheritdoc/>
            public void OnError(Exception error)
            {
                if (_stopped)
                {
                    return;
                }

                _stopped = true;
                _observer.OnError(error);
            }

            /// <inheritdoc/>
            public void OnCompleted()
            {
                if (_stopped)
                {
                    return;
                }

                _stopped = true;
                _observer.OnCompleted();
            }
        }
    }

    /// <summary>Dedicated signal for <c>Take</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TakeSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The maximum number of values to forward.</summary>
        private readonly int _count;

        /// <summary>Initializes a new instance of the <see cref="TakeSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="count">The maximum number of values to forward.</param>
        internal TakeSignal(IObservable<T> source, int count)
        {
            _source = source;
            _count = count;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_count == 0)
            {
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            TakeWitness<T> sink = new(observer, _count);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>TakeUntil</c> that holds its sources without a per-subscription closure.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TOther">The cancellation value type.</typeparam>
    private sealed class TakeUntilSignal<T, TOther> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The observable that stops the source when it emits.</summary>
        private readonly IObservable<TOther> _other;

        /// <summary>Initializes a new instance of the <see cref="TakeUntilSignal{T, TOther}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="other">The observable that stops the source when it emits.</param>
        internal TakeUntilSignal(IObservable<T> source, IObservable<TOther> other)
        {
            _source = source;
            _other = other;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var coordinator = new TakeUntilCoordinator(observer);
            coordinator.Add(_other.Subscribe(new TakeUntilOtherWitness(coordinator)));
            if (coordinator.IsStopped)
            {
                return coordinator;
            }

            coordinator.Add(_source.Subscribe(new TakeUntilSourceWitness(coordinator)));
            return coordinator;
        }

        /// <summary>Coordinates serialized observer callbacks and subscription lifetime.</summary>
        private sealed class TakeUntilCoordinator : IDisposable
        {
            /// <summary>The downstream observer.</summary>
            private readonly IObserver<T> _observer;

            /// <summary>Serializes downstream observer callbacks.</summary>
            private readonly Lock _gate = new();

            /// <summary>Tracks the source and cancellation subscriptions.</summary>
            private readonly MultipleDisposable _subscriptions = [];

            /// <summary>Indicates whether the sequence has stopped.</summary>
            private int _stopped;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilCoordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            internal TakeUntilCoordinator(IObserver<T> observer) => _observer = observer;

            /// <summary>Gets a value indicating whether the sequence has stopped.</summary>
            internal bool IsStopped => Volatile.Read(ref _stopped) != 0;

            /// <inheritdoc/>
            public void Dispose() => _subscriptions.Dispose();

            /// <summary>Adds a subscription to the coordinator lifetime.</summary>
            /// <param name="subscription">The subscription to add.</param>
            internal void Add(IDisposable subscription) => _subscriptions.Add(subscription);

            /// <summary>Forwards a source value when the sequence has not stopped.</summary>
            /// <param name="value">The source value.</param>
            internal void Next(T value)
            {
                lock (_gate)
                {
                    if (!IsStopped)
                    {
                        _observer.OnNext(value);
                    }
                }
            }

            /// <summary>Completes the downstream observer once and disposes all subscriptions.</summary>
            internal void Complete()
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                lock (_gate)
                {
                    _observer.OnCompleted();
                }

                _subscriptions.Dispose();
            }

            /// <summary>Sends an error to the downstream observer once and disposes all subscriptions.</summary>
            /// <param name="exception">The exception to forward.</param>
            internal void Error(Exception exception)
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                lock (_gate)
                {
                    _observer.OnError(exception);
                }

                _subscriptions.Dispose();
            }
        }

        /// <summary>Observes the source stream and routes its notifications through the coordinator.</summary>
        private sealed class TakeUntilSourceWitness : IObserver<T>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly TakeUntilCoordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilSourceWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal TakeUntilSourceWitness(TakeUntilCoordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            public void OnNext(T value) => _coordinator.Next(value);

            /// <inheritdoc/>
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            public void OnCompleted() => _coordinator.Complete();
        }

        /// <summary>Observes the cancellation stream; its first value or error stops the source.</summary>
        private sealed class TakeUntilOtherWitness : IObserver<TOther>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly TakeUntilCoordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilOtherWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal TakeUntilOtherWitness(TakeUntilCoordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            public void OnNext(TOther value) => _coordinator.Complete();

            /// <inheritdoc/>
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            public void OnCompleted()
            {
                // Completion of the cancellation stream without a value does not stop the source.
            }
        }
    }

    /// <summary>Dedicated signal for <c>Skip</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SkipSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The number of leading values to drop.</summary>
        private readonly int _count;

        /// <summary>Initializes a new instance of the <see cref="SkipSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="count">The number of leading values to drop.</param>
        internal SkipSignal(IObservable<T> source, int count)
        {
            _source = source;
            _count = count;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SkipWitness<T> sink = new(observer, _count);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>Distinct</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class DistinctSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The comparer used to identify duplicates.</summary>
        private readonly IEqualityComparer<T>? _comparer;

        /// <summary>Initializes a new instance of the <see cref="DistinctSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="comparer">The comparer used to identify duplicates.</param>
        internal DistinctSignal(IObservable<T> source, IEqualityComparer<T>? comparer)
        {
            _source = source;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            DistinctWitness<T> sink = new(observer, CreateSeen());
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Creates the duplicate-tracking set, pre-sized when the source has a known element count.</summary>
        /// <returns>The set used to track already-observed values.</returns>
        private HashSet<T> CreateSeen()
        {
#if NET8_0_OR_GREATER
            var capacity = _source is RangeSignal range ? range.Count : 0;
            return capacity switch
            {
                > 0 => new(capacity, _comparer),
                _ when _comparer is null => [],
                _ => new(_comparer),
            };
#else
            return new(_comparer);
#endif
        }
    }

    /// <summary>Dedicated signal for <c>Unique</c> (adjacent distinct).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class UniqueSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The comparer used to compare adjacent values.</summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>Initializes a new instance of the <see cref="UniqueSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        internal UniqueSignal(IObservable<T> source, IEqualityComparer<T> comparer)
        {
            _source = source;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            UniqueWitness<T> sink = new(observer, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>UniqueBy</c> (adjacent distinct by key).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class UniqueBySignal<T, TKey> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The key projection.</summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>The comparer used to compare adjacent keys.</summary>
        private readonly IEqualityComparer<TKey> _comparer;

        /// <summary>Initializes a new instance of the <see cref="UniqueBySignal{T, TKey}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="keySelector">The key projection.</param>
        /// <param name="comparer">The comparer used to compare adjacent keys.</param>
        internal UniqueBySignal(IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer)
        {
            _source = source;
            _keySelector = keySelector;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            UniqueByWitness<T, TKey> sink = new(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>Fold</c> (running accumulation).</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    private sealed class FoldSignal<TSource, TAccumulate> : IObservable<TAccumulate>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source;

        /// <summary>The initial accumulated value.</summary>
        private readonly TAccumulate _seed;

        /// <summary>The accumulator function.</summary>
        private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

        /// <summary>Initializes a new instance of the <see cref="FoldSignal{TSource, TAccumulate}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        internal FoldSignal(IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
        {
            _source = source;
            _seed = seed;
            _accumulator = accumulator;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TAccumulate> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            FoldWitness<TSource, TAccumulate> sink = new(observer, _seed, _accumulator);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>Reduce</c> (final accumulation).</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    private sealed class ReduceSignal<TSource, TAccumulate> : IObservable<TAccumulate>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<TSource> _source;

        /// <summary>The initial accumulated value.</summary>
        private readonly TAccumulate _seed;

        /// <summary>The accumulator function.</summary>
        private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

        /// <summary>Initializes a new instance of the <see cref="ReduceSignal{TSource, TAccumulate}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        internal ReduceSignal(IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
        {
            _source = source;
            _seed = seed;
            _accumulator = accumulator;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TAccumulate> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            ReduceWitness<TSource, TAccumulate> sink = new(observer, _seed, _accumulator);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>TakeWhile</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TakeWhileSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The predicate that determines whether to keep taking values.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="TakeWhileSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
        internal TakeWhileSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            TakeWhileWitness<T> sink = new(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>SkipWhile</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SkipWhileSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The predicate that determines whether to keep skipping values.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="SkipWhileSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
        internal SkipWhileSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SkipWhileWitness<T> sink = new(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>KeepNotNull</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class KeepNotNullSignal<T> : IObservable<T>
        where T : class
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T?> _source;

        /// <summary>Initializes a new instance of the <see cref="KeepNotNullSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal KeepNotNullSignal(IObservable<T?> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            KeepNotNullWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>KeepType</c>.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class KeepTypeSignal<TResult> : IObservable<TResult>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<object?> _source;

        /// <summary>Initializes a new instance of the <see cref="KeepTypeSignal{TResult}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal KeepTypeSignal(IObservable<object?> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            KeepTypeWitness<TResult> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
