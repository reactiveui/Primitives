// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>Private helper types for aggregate and distinct parity operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Source that can count values through an operator-specific fast path.</summary>
    private interface ICountSource
    {
        /// <summary>Subscribes a count observer directly to the underlying source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The subscription cleanup.</returns>
        IDisposable SubscribeCount(IObserver<int> observer);

        /// <summary>Subscribes a long-count observer directly to the underlying source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The subscription cleanup.</returns>
        IDisposable SubscribeLongCount(IObserver<long> observer);
    }

    /// <summary>Distinct-by operator implemented without delegate observer wrappers.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class DistinctBySignal<T, TKey> : IRequireCurrentThread<T>, ICountSource
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The key selector.</summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>The key comparer.</summary>
        private readonly IEqualityComparer<TKey>? _comparer;

        /// <summary>Initializes a new instance of the <see cref="DistinctBySignal{T,TKey}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key comparer.</param>
        internal DistinctBySignal(IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            _source = source;
            _keySelector = keySelector;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var sink = new DistinctByWitness<T, TKey>(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable SubscribeCount(IObserver<int> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                observer.OnNext(CountDistinctRange(range, _keySelector, _comparer));
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            var sink = new AggregateWitness<T, int, DistinctByCountAggregator<T, TKey>>(observer, new DistinctByCountAggregator<T, TKey>(_keySelector, _comparer));
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable SubscribeLongCount(IObserver<long> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                observer.OnNext(CountDistinctRange(range, _keySelector, _comparer));
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            var sink = new AggregateWitness<T, long, DistinctByLongCountAggregator<T, TKey>>(observer, new DistinctByLongCountAggregator<T, TKey>(_keySelector, _comparer));
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Counts distinct selected keys directly over a range source.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key comparer.</param>
        /// <returns>The number of distinct keys.</returns>
        private static int CountDistinctRange(RangeSignal range, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            HashSet<TKey> seen = comparer is null ? [] : new(comparer);
            var typedSelector = (Func<int, TKey>)(object)keySelector;
            for (var i = 0; i < range.Count; i++)
            {
                _ = seen.Add(typedSelector(range.Start + i));
            }

            return seen.Count;
        }
    }

    /// <summary>Count operator implemented without fold composition.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountSignal<T> : IRequireCurrentThread<int>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="CountSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal CountSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range)
            {
                observer.OnNext(range.Count);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            if (_source is ICountSource countSource)
            {
                return countSource.SubscribeCount(observer);
            }

            var sink = new AggregateWitness<T, int, CountAggregator<T>>(observer, default);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Predicate count operator implemented without fold composition.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountPredicateSignal<T> : IRequireCurrentThread<int>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="CountPredicateSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate.</param>
        internal CountPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitCountRange(range, _predicate, observer);
                return EmptyDisposable.Instance;
            }

            var sink = new AggregateWitness<T, int, CountPredicateAggregator<T>>(observer, new CountPredicateAggregator<T>(_predicate));
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Counts matching range values directly and emits the result.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="predicate">The predicate.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitCountRange(RangeSignal range, Func<T, bool> predicate, IObserver<int> observer)
        {
            try
            {
                var typedPredicate = (Func<int, bool>)(object)predicate;
                var count = 0;
                for (var i = 0; i < range.Count; i++)
                {
                    if (typedPredicate(range.Start + i))
                    {
                        count = checked(count + 1);
                    }
                }

                observer.OnNext(count);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }

    /// <summary>Long-count operator implemented without fold composition.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountSignal<T> : IRequireCurrentThread<long>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="LongCountSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal LongCountSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<long> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range)
            {
                observer.OnNext(range.Count);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            if (_source is ICountSource countSource)
            {
                return countSource.SubscribeLongCount(observer);
            }

            var sink = new AggregateWitness<T, long, LongCountAggregator<T>>(observer, default);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Predicate long-count operator implemented without fold composition.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountPredicateSignal<T> : IRequireCurrentThread<long>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="LongCountPredicateSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate.</param>
        internal LongCountPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<long> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitLongCountRange(range, _predicate, observer);
                return EmptyDisposable.Instance;
            }

            var sink = new AggregateWitness<T, long, LongCountPredicateAggregator<T>>(observer, new LongCountPredicateAggregator<T>(_predicate));
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Counts matching range values directly and emits the long result.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="predicate">The predicate.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitLongCountRange(RangeSignal range, Func<T, bool> predicate, IObserver<long> observer)
        {
            try
            {
                var typedPredicate = (Func<int, bool>)(object)predicate;
                long count = 0;
                for (var i = 0; i < range.Count; i++)
                {
                    if (typedPredicate(range.Start + i))
                    {
                        count = checked(count + 1L);
                    }
                }

                observer.OnNext(count);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }

    /// <summary>Any operator implemented without predicate composition.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnySignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="AnySignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal AnySignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal)
            {
                observer.OnNext(true);
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            var sink = new AnyWitness<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Predicate any operator implemented without delegate observer wrappers.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnyPredicateSignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="AnyPredicateSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate.</param>
        internal AnyPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitAnyRange(range, _predicate, observer);
                return EmptyDisposable.Instance;
            }

            var sink = new AnyPredicateWitness<T>(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Evaluates a predicate directly over a range source and emits the any result.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="predicate">The predicate.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitAnyRange(RangeSignal range, Func<T, bool> predicate, IObserver<bool> observer)
        {
            try
            {
                var typedPredicate = (Func<int, bool>)(object)predicate;
                for (var i = 0; i < range.Count; i++)
                {
                    if (!typedPredicate(range.Start + i))
                    {
                        continue;
                    }

                    observer.OnNext(true);
                    observer.OnCompleted();
                    return;
                }

                observer.OnNext(false);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }
}
