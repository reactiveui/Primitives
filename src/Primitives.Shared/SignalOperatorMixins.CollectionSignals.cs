// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// Dedicated signals/sinks for the terminal collection operators (CollectList, CollectArray) and
/// their eager range-backed fast paths, replacing the per-subscription
/// <c>Signal.CreateSafe(observer =&gt; ...)</c> closures with parameter-holding signals.
/// </summary>
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
    /// <typeparam name="T">The result element type.</typeparam>
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

            if (typeof(T) == typeof(int))
            {
                List<int> ints = new(_range.Count);
                for (var i = 0; i < _range.Count; i++)
                {
                    ints.Add(_range.Start + i);
                }

                observer.OnNext((IList<T>)(object)ints);
            }
            else
            {
                List<T> values = new(_range.Count);
                for (var i = 0; i < _range.Count; i++)
                {
                    values.Add((T)(object)(_range.Start + i));
                }

                observer.OnNext(values);
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Eager range-backed signal for <c>CollectArray</c> (no per-value subscription).</summary>
    /// <typeparam name="T">The result element type.</typeparam>
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

            if (typeof(T) == typeof(int))
            {
                var ints = new int[_range.Count];
                for (var i = 0; i < ints.Length; i++)
                {
                    ints[i] = _range.Start + i;
                }

                observer.OnNext((T[])(object)ints);
            }
            else
            {
                var values = new T[_range.Count];
                for (var i = 0; i < values.Length; i++)
                {
                    values[i] = (T)(object)(_range.Start + i);
                }

                observer.OnNext(values);
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }
}
