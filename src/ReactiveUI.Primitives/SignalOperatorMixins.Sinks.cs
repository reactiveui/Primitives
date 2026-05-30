// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <content>
/// Dedicated single-source observer sinks for stateful pass-through operators, replacing
/// per-subscription closure and delegate allocations.
/// </content>
public static partial class LinqMixins
{
    /// <summary>
    /// Sink that drops the first <c>count</c> values, then forwards the rest.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SkipObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The remaining number of values to drop.</summary>
        private int _remaining;

        /// <summary>
        /// Initializes a new instance of the <see cref="SkipObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="count">The number of leading values to drop.</param>
        internal SkipObserver(IObserver<T> observer, int count)
        {
            _observer = observer;
            _remaining = count;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_remaining > 0)
            {
                _remaining--;
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
    /// Sink that forwards the first occurrence of each value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class DistinctObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The set of values already observed.</summary>
        private readonly HashSet<T> _seen;

        /// <summary>
        /// Initializes a new instance of the <see cref="DistinctObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="comparer">The comparer used to identify duplicates.</param>
        internal DistinctObserver(IObserver<T> observer, IEqualityComparer<T>? comparer)
        {
            _observer = observer;
            _seen =
#if NET8_0_OR_GREATER
                comparer is null ? [] : new HashSet<T>(comparer);
#else
                new(comparer);
#endif
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (!_seen.Add(value))
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
    /// Sink that suppresses adjacent duplicate values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class UniqueObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The comparer used to compare adjacent values.</summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>A value indicating whether a previous value has been observed.</summary>
        private bool _hasLast;

        /// <summary>The most recently forwarded value.</summary>
        private T? _last;

        /// <summary>
        /// Initializes a new instance of the <see cref="UniqueObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        internal UniqueObserver(IObserver<T> observer, IEqualityComparer<T> comparer)
        {
            _observer = observer;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_hasLast && _comparer.Equals(_last!, value))
            {
                return;
            }

            _hasLast = true;
            _last = value;
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
    /// Sink that emits a running accumulation for every source value.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    private sealed class FoldObserver<TSource, TAccumulate> : SingleSourceObserver<TSource>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TAccumulate> _observer;

        /// <summary>The accumulator function.</summary>
        private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

        /// <summary>The current accumulated value.</summary>
        private TAccumulate _current;

        /// <summary>
        /// Initializes a new instance of the <see cref="FoldObserver{TSource, TAccumulate}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        internal FoldObserver(IObserver<TAccumulate> observer, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
        {
            _observer = observer;
            _current = seed;
            _accumulator = accumulator;
        }

        /// <inheritdoc/>
        public override void OnNext(TSource value)
        {
            _current = _accumulator(_current, value);
            try
            {
                _observer.OnNext(_current);
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
    /// Sink that emits the final accumulation once the source completes.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    private sealed class ReduceObserver<TSource, TAccumulate> : SingleSourceObserver<TSource>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TAccumulate> _observer;

        /// <summary>The accumulator function.</summary>
        private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

        /// <summary>The current accumulated value.</summary>
        private TAccumulate _current;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReduceObserver{TSource, TAccumulate}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        internal ReduceObserver(IObserver<TAccumulate> observer, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
        {
            _observer = observer;
            _current = seed;
            _accumulator = accumulator;
        }

        /// <inheritdoc/>
        public override void OnNext(TSource value) => _current = _accumulator(_current, value);

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
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
            try
            {
                _observer.OnNext(_current);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
