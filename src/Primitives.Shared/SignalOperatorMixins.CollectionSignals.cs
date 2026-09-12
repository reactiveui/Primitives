// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Signals backing the terminal collection operators and their eager range fast paths.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>CollectList</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CollectListSignal<T> : IObservable<IList<T>>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="CollectListSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal CollectListSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<IList<T>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            CollectListWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>CollectArray</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CollectArraySignal<T> : IObservable<T[]>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="CollectArraySignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal CollectArraySignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T[]> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            CollectArrayWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Eager range-backed signal for <c>CollectList</c> (no per-value subscription).</summary>
    /// <typeparam name="T">The result element type, which is always <see cref="int"/>.</typeparam>
    private sealed class RangeListSignal<T> : IObservable<IList<T>>
    {
        /// <summary>The source range.</summary>
        private readonly RangeSignal _range;

        /// <summary>Initializes a new instance of the <see cref="RangeListSignal{T}"/> class.</summary>
        /// <param name="range">The source range.</param>
        internal RangeListSignal(RangeSignal range) => _range = range;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<IList<T>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            List<int> values = [with(capacity: _range.Count)];
            for (var i = 0; i < _range.Count; i++)
            {
                values.Add(_range.Start + i);
            }

            // This helper requires T to be int.
            observer.OnNext((IList<T>)(object)values);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Eager range-backed signal for <c>CollectArray</c> (no per-value subscription).</summary>
    /// <typeparam name="T">The result element type, which is always <see cref="int"/>.</typeparam>
    private sealed class RangeArraySignal<T> : IObservable<T[]>
    {
        /// <summary>The source range.</summary>
        private readonly RangeSignal _range;

        /// <summary>Initializes a new instance of the <see cref="RangeArraySignal{T}"/> class.</summary>
        /// <param name="range">The source range.</param>
        internal RangeArraySignal(RangeSignal range) => _range = range;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T[]> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var values = new int[_range.Count];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = _range.Start + i;
            }

            // This helper requires T to be int.
            observer.OnNext((T[])(object)values);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }
}
