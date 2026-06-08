// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>
/// Dedicated <see cref="IObservable{T}"/> implementations for the stateful single-source operators.
/// Building through these instead of <c>Signal.CreateSafe(observer =&gt; ...)</c> removes the
/// per-subscription closure, delegate, CreateSafe wrapper, and safe-guard sink, leaving only the
/// signal object at chain-build time and the operator sink at subscribe time.
/// </summary>
public static partial class LinqExtensions
{
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (_count == 0)
            {
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            var sink = new TakeObserver<T>(observer, _count);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new SkipObserver<T>(observer, _count);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new DistinctObserver<T>(observer, CreateSeen());
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Creates the duplicate-tracking set, pre-sized when the source has a known element count.</summary>
        /// <returns>The set used to track already-observed values.</returns>
        private HashSet<T> CreateSeen()
        {
#if NET8_0_OR_GREATER
            var capacity = _source is Signals.Core.RangeSignal range ? range.Count : 0;
            return capacity switch
            {
                > 0 => new HashSet<T>(capacity, _comparer),
                _ when _comparer is null => [],
                _ => new HashSet<T>(_comparer),
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new UniqueObserver<T>(observer, _comparer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new UniqueByObserver<T, TKey>(observer, _keySelector, _comparer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new FoldObserver<TSource, TAccumulate>(observer, _seed, _accumulator);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new ReduceObserver<TSource, TAccumulate>(observer, _seed, _accumulator);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new TakeWhileObserver<T>(observer, _predicate);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new SkipWhileObserver<T>(observer, _predicate);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new KeepNotNullObserver<T>(observer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new KeepTypeObserver<TResult>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
