// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// Private helper types for aggregate and distinct parity operators.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Source that can count values through an operator-specific fast path.
    /// </summary>
    private interface ICountSource
    {
        /// <summary>
        /// Subscribes a count observer directly to the underlying source.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The subscription cleanup.</returns>
        IDisposable SubscribeCount(IObserver<int> observer);

        /// <summary>
        /// Subscribes a long-count observer directly to the underlying source.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The subscription cleanup.</returns>
        IDisposable SubscribeLongCount(IObserver<long> observer);
    }

    /// <summary>
    /// Distinct-by operator implemented without delegate observer wrappers.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class DistinctBySignal<T, TKey> : IRequireCurrentThread<T>, ICountSource
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The key selector.
        /// </summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>
        /// The key comparer.
        /// </summary>
        private readonly IEqualityComparer<TKey>? _comparer;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistinctBySignal{T,TKey}"/> class.
        /// </summary>
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
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new DistinctByObserver<T, TKey>(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable SubscribeCount(IObserver<int> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new DistinctByCountObserver<T, TKey>(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable SubscribeLongCount(IObserver<long> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new DistinctByLongCountObserver<T, TKey>(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Count operator implemented without fold composition.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountSignal<T> : IRequireCurrentThread<int>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        internal CountSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (_source is ICountSource countSource)
            {
                return countSource.SubscribeCount(observer);
            }

            var sink = new CountObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Predicate count operator implemented without fold composition.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountPredicateSignal<T> : IRequireCurrentThread<int>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountPredicateSignal{T}"/> class.
        /// </summary>
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
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new CountPredicateObserver<T>(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Long-count operator implemented without fold composition.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountSignal<T> : IRequireCurrentThread<long>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// Initializes a new instance of the <see cref="LongCountSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        internal LongCountSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<long> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (_source is ICountSource countSource)
            {
                return countSource.SubscribeLongCount(observer);
            }

            var sink = new LongCountObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Predicate long-count operator implemented without fold composition.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountPredicateSignal<T> : IRequireCurrentThread<long>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// Initializes a new instance of the <see cref="LongCountPredicateSignal{T}"/> class.
        /// </summary>
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
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new LongCountPredicateObserver<T>(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Any operator implemented without predicate composition.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnySignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// Initializes a new instance of the <see cref="AnySignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        internal AnySignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new AnyObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Predicate any operator implemented without delegate observer wrappers.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnyPredicateSignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// Initializes a new instance of the <see cref="AnyPredicateSignal{T}"/> class.
        /// </summary>
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
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new AnyPredicateObserver<T>(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Observer for distinct-by.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class DistinctByObserver<T, TKey> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The key selector.
        /// </summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>
        /// The observed keys.
        /// </summary>
        private readonly HashSet<TKey> _seen;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistinctByObserver{T,TKey}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key comparer.</param>
        internal DistinctByObserver(IObserver<T> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            _observer = observer;
            _keySelector = keySelector;
            _seen = comparer == null ? [] : new(comparer);
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_seen.Add(_keySelector(value)))
            {
                return;
            }

            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for counting all values.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<int> _observer;

        /// <summary>
        /// The running count.
        /// </summary>
        private int _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        internal CountObserver(IObserver<int> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            _count = checked(_count + 1);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for predicate count.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CountPredicateObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<int> _observer;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// The running count.
        /// </summary>
        private int _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountPredicateObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="predicate">The predicate.</param>
        internal CountPredicateObserver(IObserver<int> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_predicate(value))
            {
                return;
            }

            _count = checked(_count + 1);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for counting distinct keys.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class DistinctByCountObserver<T, TKey> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<int> _observer;

        /// <summary>
        /// The key selector.
        /// </summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>
        /// The observed keys.
        /// </summary>
        private readonly HashSet<TKey> _seen;

        /// <summary>
        /// The running count.
        /// </summary>
        private int _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistinctByCountObserver{T,TKey}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key comparer.</param>
        internal DistinctByCountObserver(IObserver<int> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            _observer = observer;
            _keySelector = keySelector;
            _seen = comparer == null ? [] : new(comparer);
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_seen.Add(_keySelector(value)))
            {
                return;
            }

            _count = checked(_count + 1);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for long-counting all values.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<long> _observer;

        /// <summary>
        /// The running count.
        /// </summary>
        private long _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="LongCountObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        internal LongCountObserver(IObserver<long> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            _count = checked(_count + 1L);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for predicate long-count.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class LongCountPredicateObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<long> _observer;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// The running count.
        /// </summary>
        private long _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="LongCountPredicateObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="predicate">The predicate.</param>
        internal LongCountPredicateObserver(IObserver<long> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_predicate(value))
            {
                return;
            }

            _count = checked(_count + 1L);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for long-counting distinct keys.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    private sealed class DistinctByLongCountObserver<T, TKey> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<long> _observer;

        /// <summary>
        /// The key selector.
        /// </summary>
        private readonly Func<T, TKey> _keySelector;

        /// <summary>
        /// The observed keys.
        /// </summary>
        private readonly HashSet<TKey> _seen;

        /// <summary>
        /// The running count.
        /// </summary>
        private long _count;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistinctByLongCountObserver{T,TKey}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key comparer.</param>
        internal DistinctByLongCountObserver(IObserver<long> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            _observer = observer;
            _keySelector = keySelector;
            _seen = comparer == null ? [] : new(comparer);
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_seen.Add(_keySelector(value)))
            {
                return;
            }

            _count = checked(_count + 1L);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(_count);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for detecting whether any value is present.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnyObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<bool> _observer;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="AnyObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        internal AnyObserver(IObserver<bool> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(true);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(false);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for detecting whether any value matches a predicate.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AnyPredicateObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<bool> _observer;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="AnyPredicateObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="predicate">The predicate.</param>
        internal AnyPredicateObserver(IObserver<bool> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done || !_predicate(value))
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(true);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(false);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
