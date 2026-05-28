// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// Additional parity operators that preserve Primitives naming while covering common reactive contracts.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Prepends a value before the source sequence. Alias of <see cref="Prepend{T}(IObservable{T}, T)"/> using Primitives vocabulary.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to emit before the source.</param>
    /// <returns>A sequence that emits <paramref name="value"/> before the source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Lead<T>(this IObservable<T> source, T value) => source.Prepend(value);

    /// <summary>
    /// Prepends a value before the source sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to emit before the source.</param>
    /// <returns>A sequence that emits <paramref name="value"/> before the source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Prepend<T>(this IObservable<T> source, T value)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new PrependSignal<T>(source, value);
    }

    /// <summary>
    /// Prepends values before the source sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="values">The values to emit before the source.</param>
    /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Prepend<T>(this IObservable<T> source, params T[] values)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        if (values.Length == 0)
        {
            return source;
        }

        if (values.Length == 1)
        {
            return source.Prepend(values[0]);
        }

        return new StartWithEnumerableSignal<T>(source, values);
    }

    /// <summary>
    /// Prepends values before the source sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="values">The values to emit before the source.</param>
    /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Prepend<T>(this IObservable<T> source, IEnumerable<T> values)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return new StartWithEnumerableSignal<T>(source, values);
    }

    /// <summary>
    /// Appends a value after the source sequence completes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to emit after the source completes.</param>
    /// <returns>A sequence that emits the source values followed by <paramref name="value"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Append<T>(this IObservable<T> source, T value)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is PrependSignal<T> prepended)
        {
            return new PrependAppendSignal<T>(prepended.GetSource(), prepended.GetValue(), value);
        }

        return new AppendSignal<T>(source, value);
    }

    /// <summary>
    /// Returns the source as an observable. This is an identity adapter for BCL observable sources.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>The supplied source sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> AsObservable<T>(this IObservable<T> source) => source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The values to enumerate.</param>
    /// <returns>A signal that emits the enumerable values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="values"/> is <see langword="null"/>.</exception>
    public static IObservable<T> ToObservable<T>(this IEnumerable<T> values) => Signal.FromEnumerable(values);

    /// <summary>
    /// Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The values to enumerate.</param>
    /// <param name="cancellationToken">The token used to stop enumeration.</param>
    /// <returns>A signal that emits the enumerable values until enumeration completes or cancellation is requested.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="values"/> is <see langword="null"/>.</exception>
    public static IObservable<T> ToObservable<T>(this IEnumerable<T> values, CancellationToken cancellationToken) =>
        Signal.FromEnumerable(values, cancellationToken);

    /// <summary>
    /// Schedules observer notifications on the supplied scheduler using the System.Reactive operator name.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The sequencer used to deliver observer notifications.</param>
    /// <returns>The source sequence when <paramref name="scheduler"/> is immediate; otherwise a sequence observed on the sequencer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> ObserveOn<T>(this IObservable<T> source, ISequencer scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (scheduler == Sequencer.Immediate)
        {
            return source;
        }

        return source.WitnessOn(scheduler);
    }

    /// <summary>
    /// Schedules source subscription on the supplied sequencer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The sequencer used to perform subscription.</param>
    /// <returns>A sequence that subscribes to <paramref name="source"/> on <paramref name="scheduler"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> SubscribeOn<T>(this IObservable<T> source, ISequencer scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        return Signal.Create<T>(observer =>
        {
            var subscription = new SingleReplaceableDisposable();
            var scheduled = scheduler.Schedule(() => subscription.Create(source.Subscribe(observer)));
            return MultipleDisposable.Create(scheduled, subscription);
        });
    }

    /// <summary>
    /// Alias for <see cref="DelayStart{T}(IObservable{T}, TimeSpan, ISequencer?)"/> using the System.Reactive operator name.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DelaySubscription<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.DelayStart(dueTime, null);

    /// <summary>
    /// Alias for <see cref="DelayStart{T}(IObservable{T}, TimeSpan, ISequencer?)"/> using the System.Reactive operator name.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DelaySubscription<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler) =>
        source.DelayStart(dueTime, scheduler);

    /// <summary>
    /// Invokes actions for each element in the observable sequence, for error notifications, and for successful
    /// completion.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onError">Action to invoke upon exceptional termination of the observable sequence.</param>
    /// <param name="onCompleted">Action to invoke upon graceful termination of the observable sequence.</param>
    /// <returns>The source sequence with the side-effecting behavior applied.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, <paramref name="onError"/>, or <paramref
    /// name="onCompleted"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Tap<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onError == null)
        {
            throw new ArgumentNullException(nameof(onError));
        }

        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(
            value =>
            {
                onNext(value);
                observer.OnNext(value);
            },
            error =>
            {
                onError(error);
                observer.OnError(error);
            },
            () =>
            {
                onCompleted();
                observer.OnCompleted();
            }));
    }

    /// <summary>
    /// Ignores all source values and only forwards terminal messages.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that forwards only error and completion notifications.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> IgnoreValues<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(_ => { }, observer.OnError, observer.OnCompleted));
    }

    /// <summary>
    /// Emits the supplied value if the source completes without values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits <see langword="default"/> when the source is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DefaultIfEmpty<T>(this IObservable<T> source) =>
        source.DefaultIfEmpty(default!);

    /// <summary>
    /// Emits the supplied value if the source completes without values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="defaultValue">The value to emit when the source is empty.</param>
    /// <returns>A sequence that emits <paramref name="defaultValue"/> when the source is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DefaultIfEmpty<T>(this IObservable<T> source, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is ImmutableEmptySignal<T>)
        {
            return Signal.Emit(defaultValue);
        }

        if (source is RangeSignal { Count: > 0 })
        {
            return source;
        }

        return new DefaultIfEmptySignal<T>(source, defaultValue);
    }

    /// <summary>
    /// Suppresses duplicate keys according to the comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <returns>A sequence containing the first value for each observed key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector) =>
        source.DistinctBy(keySelector, null);

    /// <summary>
    /// Suppresses duplicate keys according to the comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <param name="comparer">The comparer used to identify duplicate keys.</param>
    /// <returns>A sequence containing the first value for each observed key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        return new DistinctBySignal<T, TKey>(source, keySelector, comparer);
    }

    /// <summary>
    /// Suppresses adjacent duplicate keys according to the comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <returns>A sequence with adjacent duplicate keys removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> UniqueBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector) =>
        source.UniqueBy(keySelector, null);

    /// <summary>
    /// Suppresses adjacent duplicate keys according to the comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <param name="comparer">The comparer used to compare adjacent keys.</param>
    /// <returns>A sequence with adjacent duplicate keys removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> UniqueBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        comparer ??= EqualityComparer<TKey>.Default;
        return Signal.CreateSafe<T>(observer =>
        {
            var hasLast = false;
            var last = default(TKey);
            return source.Subscribe(
                value =>
                {
                    var key = keySelector(value);
                    if (hasLast && comparer.Equals(last!, key))
                    {
                        return;
                    }

                    hasLast = true;
                    last = key;
                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Emits values while the predicate remains true, then completes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that determines whether to keep taking values.</param>
    /// <returns>A sequence that emits the leading values that satisfy <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> TakeWhile<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var taking = true;
            return source.Subscribe(
                value =>
                {
                    if (!taking)
                    {
                        return;
                    }

                    if (predicate(value))
                    {
                        observer.OnNext(value);
                    }
                    else
                    {
                        taking = false;
                        observer.OnCompleted();
                    }
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Skips values while the predicate remains true, then mirrors the remaining source.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that determines whether to keep skipping values.</param>
    /// <returns>A sequence that emits values after the leading values that satisfy <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> SkipWhile<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var skipping = true;
            return source.Subscribe(
                value =>
                {
                    if (skipping && predicate(value))
                    {
                        return;
                    }

                    skipping = false;
                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Projects each source value to an inner signal and concatenates all inner values.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The function that projects each source value to an inner sequence.</param>
    /// <returns>A sequence containing the concatenated inner values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Bind<TSource, TResult>(this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector) => source.FlatMap(selector);

    /// <summary>
    /// Projects each source value to an inner signal and concatenates all inner values.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The function that projects each source value to an inner sequence.</param>
    /// <returns>A sequence containing the concatenated inner values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> FlatMap<TSource, TResult>(this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new FlatMapSignal<TSource, TResult>(source, selector);
    }

    /// <summary>
    /// Projects each source value to an inner signal and maps outer/inner values with a result selector.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TCollection">The inner value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="collectionSelector">The function that projects each source value to an inner sequence.</param>
    /// <param name="resultSelector">The function that combines source and inner values.</param>
    /// <returns>A sequence containing selected outer/inner combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> FlatMap<TSource, TCollection, TResult>(
        this IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        if (collectionSelector == null)
        {
            throw new ArgumentNullException(nameof(collectionSelector));
        }

        if (resultSelector == null)
        {
            throw new ArgumentNullException(nameof(resultSelector));
        }

        return new FlatMapResultSignal<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
    }

    /// <summary>
    /// Counts the source values as an <see cref="int"/>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits the number of source values when the source completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<int> Count<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new CountSignal<T>(source);
    }

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="int"/>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that identifies values to count.</param>
    /// <returns>A sequence that emits the matching value count when the source completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<int> Count<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new CountPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Counts the source values as an <see cref="long"/>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits the number of source values when the source completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<long> LongCount<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new LongCountSignal<T>(source);
    }

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="long"/>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that identifies values to count.</param>
    /// <returns>A sequence that emits the matching value count when the source completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<long> LongCount<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new LongCountPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Emits true when any value is present.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits whether the source produced any values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> Any<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new AnySignal<T>(source);
    }

    /// <summary>
    /// Emits true when any value satisfies the predicate.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that tests each value.</param>
    /// <returns>A sequence that emits whether any source value satisfies <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> Any<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new AnyPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Emits true when every value satisfies the predicate.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that tests each value.</param>
    /// <returns>A sequence that emits whether every source value satisfies <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> All<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new AllPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Emits true when the source contains the requested value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <returns>A sequence that emits whether the source contains <paramref name="value"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> Contains<T>(this IObservable<T> source, T value) =>
        source.Contains(value, null);

    /// <summary>
    /// Emits true when the source contains the requested value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used to compare source values.</param>
    /// <returns>A sequence that emits whether the source contains <paramref name="value"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> Contains<T>(this IObservable<T> source, T value, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        comparer ??= EqualityComparer<T>.Default;
        return new ContainsSignal<T>(source, value, comparer);
    }

    /// <summary>
    /// Emits true when the source completes without values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits whether the source completed without values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<bool> IsEmpty<T>(this IObservable<T> source) => source.Any().Map(hasValue => !hasValue);

    /// <summary>
    /// Emits values from source after delaying subscription by the due time.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DelayStart<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.DelayStart(dueTime, null);

    /// <summary>
    /// Emits values from source after delaying subscription by the due time.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DelayStart<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.Create<T>(observer =>
        {
            var pocket = new MultipleDisposable();
            pocket.Add(scheduler.Schedule(Sequencer.Normalize(dueTime), () => pocket.Add(source.Subscribe(observer))));
            return pocket;
        });
    }

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <returns>A sequence that emits the latest value after each quiet period.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Calm<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Calm(dueTime, null);

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
    /// <returns>A sequence that emits the latest value after each quiet period.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Calm<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.CreateSafe<T>(
            observer => new CalmCoordinator<T>(source, dueTime, scheduler).Run(observer),
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <returns>A sequence that emits the latest value after each quiet period.</returns>
    public static IObservable<T> Stabilize<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Calm(dueTime);

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
    /// <returns>A sequence that emits the latest value after each quiet period.</returns>
    public static IObservable<T> Stabilize<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler) =>
        source.Calm(dueTime, scheduler);

    /// <summary>
    /// Emits the latest source value whenever the sampling period ticks.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="period">The interval between sampling ticks.</param>
    /// <returns>A sequence that emits the latest source value on each sampling tick.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
    public static IObservable<T> Probe<T>(this IObservable<T> source, TimeSpan period) =>
        source.Probe(period, null);

    /// <summary>
    /// Emits the latest source value whenever the sampling period ticks.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="period">The interval between sampling ticks.</param>
    /// <param name="scheduler">The sequencer used to schedule sampling ticks.</param>
    /// <returns>A sequence that emits the latest source value on each sampling tick.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
    public static IObservable<T> Probe<T>(this IObservable<T> source, TimeSpan period, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (period < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ProbeSignal<T>(source, period, scheduler);
    }

    /// <summary>
    /// Annotates values with their scheduler timestamp.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing each value with its timestamp.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<Moment<T>> Timestamp<T>(this IObservable<T> source) =>
        source.Timestamp(null);

    /// <summary>
    /// Annotates values with their scheduler timestamp.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps.</param>
    /// <returns>A sequence containing each value with its timestamp.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<Moment<T>> Timestamp<T>(this IObservable<T> source, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= Sequencer.Immediate;
        return source.Map(value => new Moment<T>(value, scheduler.Now));
    }

    /// <summary>
    /// Annotates each value with the elapsed scheduler time since the previous value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing each value with its elapsed interval since the previous value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<TimeInterval<T>> TimeInterval<T>(this IObservable<T> source) =>
        source.TimeInterval(null);

    /// <summary>
    /// Annotates each value with the elapsed scheduler time since the previous value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps.</param>
    /// <returns>A sequence containing each value with its elapsed interval since the previous value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<TimeInterval<T>> TimeInterval<T>(this IObservable<T> source, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= Sequencer.Immediate;
        return Signal.CreateSafe<TimeInterval<T>>(observer =>
        {
            var last = scheduler.Now;
            var first = true;
            return source.Subscribe(
                value =>
                {
                    var now = scheduler.Now;
                    var interval = first ? TimeSpan.Zero : now - last;
                    first = false;
                    last = now;
                    observer.OnNext(new TimeInterval<T>(value, interval));
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Combines latest values from both sources. Alias for latest-fusion vocabulary.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>A sequence containing selected latest-value combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> PairLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.SyncLatest(right, selector);

    /// <summary>
    /// Alias for <see cref="PairLatest{TLeft, TRight, TResult}(IObservable{TLeft}, IObservable{TRight}, Func{TLeft, TRight, TResult})"/>.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>A sequence containing selected latest-value combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> FuseLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.PairLatest(right, selector);

    /// <summary>
    /// Waits for both sources to complete and emits one value from their last elements when both produced at least one value.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines the final values.</param>
    /// <returns>A sequence that emits one selected value after both sources complete.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> ForkJoin<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
        if (left == null)
        {
            throw new ArgumentNullException(nameof(left));
        }

        if (right == null)
        {
            throw new ArgumentNullException(nameof(right));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        if (typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange)
        {
            return Signal.CreateSafe<TResult>(observer =>
            {
                observer.OnNext(((Func<int, int, TResult>)(object)selector)(
                    leftRange.Start + leftRange.Count - 1,
                    rightRange.Start + rightRange.Count - 1));
                observer.OnCompleted();
                return Disposable.Empty;
            });
        }

        return Signal.CreateSafe<TResult>(observer => new ForkJoinCoordinator<TLeft, TRight, TResult>(observer, selector).Run(left, right));
    }

    /// <summary>
    /// Awaits the first source value.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the first source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
    public static Task<T> FirstAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(false, default!);
    }

    /// <summary>
    /// Awaits the first source value, returning a default value when the source is empty.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(true, default!);
    }

    /// <summary>
    /// Awaits the first source value, returning a default value when the source is empty.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="defaultValue">The value to return when the source is empty.</param>
    /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T> FirstOrDefaultAsync<T>(this IObservable<T> source, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(true, defaultValue);
    }

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the final source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
    public static Task<T> ToTask<T>(this IObservable<T> source) => source.ToTask(CancellationToken.None);

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the final source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "ToTask keeps cancellation, terminal, and synchronous fast paths together to avoid extra allocations.")]
    public static Task<T> ToTask<T>(this IObservable<T> source, CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start + range.Count - 1));
        }

        var completion = new TaskCompletionSource<T>();
        var seen = false;
        var last = default(T);
        var subscription = default(IDisposable);
        CancellationTokenRegistration cancellationRegistration = default;
        if (cancellationToken.CanBeCanceled)
        {
            cancellationRegistration = cancellationToken.Register(() =>
            {
                subscription?.Dispose();
                completion.TrySetCanceled(cancellationToken);
            });
        }

        subscription = source.Subscribe(
            value =>
            {
                seen = true;
                last = value;
            },
            error =>
            {
                cancellationRegistration.Dispose();
                subscription?.Dispose();
                completion.TrySetException(error);
            },
            () =>
            {
                cancellationRegistration.Dispose();
                subscription?.Dispose();
                if (seen)
                {
                    completion.TrySetResult(last!);
                }
                else
                {
                    completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                }
            });

        if (completion.Task.IsCompleted)
        {
            subscription.Dispose();
        }

        return completion.Task;
    }

    /// <summary>
    /// Identity helper that keeps source-compatible <c>FirstAsync().ToTask()</c> migrations compiling.
    /// </summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to return.</param>
    /// <returns>The supplied task.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    public static Task<T> ToTask<T>(this Task<T> task) => task ?? throw new ArgumentNullException(nameof(task));

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the final source value.</returns>
    public static Task<T> LastAsync<T>(this IObservable<T> source) => source.ToTask();

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
    public static Task<T> LastOrDefaultAsync<T>(this IObservable<T> source) =>
        source.LastOrDefaultAsync(default!);

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
    /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
    public static Task<T> LastOrDefaultAsync<T>(this IObservable<T> source, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return source.DefaultIfEmpty(defaultValue).ToTask();
    }

    /// <summary>
    /// Awaits the source count as a task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the number of source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<int> CountAsync<T>(this IObservable<T> source) =>
        source.Count().ToTask();

    /// <summary>
    /// Awaits the source count as a task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task.</param>
    /// <returns>A task that completes with the number of source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<int> CountAsync<T>(this IObservable<T> source, CancellationToken cancellationToken) =>
        source.Count().ToTask(cancellationToken);

    /// <summary>
    /// Awaits the source predicate count as a task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that identifies values to count.</param>
    /// <returns>A task that completes with the matching value count.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static Task<int> CountAsync<T>(this IObservable<T> source, Func<T, bool> predicate) =>
        source.Count(predicate).ToTask();

    /// <summary>
    /// Awaits the source predicate count as a task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that identifies values to count.</param>
    /// <param name="cancellationToken">The token used to cancel the task.</param>
    /// <returns>A task that completes with the matching value count.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static Task<int> CountAsync<T>(this IObservable<T> source, Func<T, bool> predicate, CancellationToken cancellationToken) =>
        source.Count(predicate).ToTask(cancellationToken);

    /// <summary>
    /// Awaits whether any value is present.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with whether the source produced any values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source) =>
        source.Any().ToTask();

    /// <summary>
    /// Awaits whether any value is present.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task.</param>
    /// <returns>A task that completes with whether the source produced any values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, CancellationToken cancellationToken) =>
        source.Any().ToTask(cancellationToken);

    /// <summary>
    /// Awaits whether any value matches a predicate.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that tests each value.</param>
    /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, Func<T, bool> predicate) =>
        source.Any(predicate).ToTask();

    /// <summary>
    /// Awaits whether any value matches a predicate.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The function that tests each value.</param>
    /// <param name="cancellationToken">The token used to cancel the task.</param>
    /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, Func<T, bool> predicate, CancellationToken cancellationToken) =>
        source.Any(predicate).ToTask(cancellationToken);

    /// <summary>
    /// Collects all values into an array task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with all source values in an array.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<T[]> CollectArrayAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeArray<T>(range));
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        if (source is IAsyncEnumerableBackedSignal<T> asyncEnumerable)
        {
            return CollectAsyncEnumerableArrayAsync(asyncEnumerable.Values, asyncEnumerable.CancellationToken);
        }

#endif
        var completion = new TaskCompletionSource<T[]>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult([.. values]));
        return completion.Task;
    }

    /// <summary>
    /// Collects all values into an array.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits a single array containing all source values.</returns>
    public static IObservable<T[]> ToArray<T>(this IObservable<T> source) => source.CollectArray();

    /// <summary>
    /// Collects all values into an array task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with all source values in an array.</returns>
    public static Task<T[]> ToArrayAsync<T>(this IObservable<T> source) => source.CollectArrayAsync();

    /// <summary>
    /// Collects all values into a list task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with all source values in a list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static Task<IList<T>> CollectListAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult((IList<T>)CreateRangeList<T>(range));
        }

        var completion = new TaskCompletionSource<IList<T>>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult(values));
        return completion.Task;
    }

    /// <summary>
    /// Collects all values into a list.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits a single list containing all source values.</returns>
    public static IObservable<IList<T>> ToList<T>(this IObservable<T> source) => source.CollectList();

    /// <summary>
    /// Collects all values into a list task.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with all source values in a list.</returns>
    public static Task<IList<T>> ToListAsync<T>(this IObservable<T> source) => source.CollectListAsync();

    /// <summary>
    /// Awaits the first source value and applies the configured empty-source behavior.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="hasDefault">A value indicating whether to use <paramref name="defaultValue"/> when the source is empty.</param>
    /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
    /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
    private static Task<T> FirstOrDefaultCoreAsync<T>(this IObservable<T> source, bool hasDefault, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var completion = new TaskCompletionSource<T>();
        var seen = false;
        source.Subscribe(
            value =>
            {
                if (seen)
                {
                    return;
                }

                seen = true;
                completion.TrySetResult(value);
            },
            error => completion.TrySetException(error),
            () =>
            {
                if (seen)
                {
                    return;
                }

                if (hasDefault)
                {
                    completion.TrySetResult(defaultValue);
                }
                else
                {
                    completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                }
        });
        return completion.Task;
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
    /// <summary>
    /// Collects an async enumerable directly into an array.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The source async enumerable.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task containing the collected array.</returns>
    private static async Task<T[]> CollectAsyncEnumerableArrayAsync<T>(IAsyncEnumerable<T> values, CancellationToken cancellationToken)
    {
        const int initialCapacity = 16;
        const int growthFactor = 2;
        var array = new T[initialCapacity];
        var count = 0;
        var enumerator = values.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                if (count == array.Length)
                {
                    Array.Resize(ref array, array.Length * growthFactor);
                }

                array[count++] = enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }

        if (count == array.Length)
        {
            return array;
        }

        var result = new T[count];
        Array.Copy(array, result, count);

        return result;
    }

#endif

    /// <summary>
    /// Converts an integer value to the specified numeric type.
    /// </summary>
    /// <remarks>Uses boxing and unboxing to perform the conversion. The generic type parameter is expected to
    /// be validated by the caller.</remarks>
    /// <typeparam name="T">The target numeric type.</typeparam>
    /// <param name="value">The integer value to convert.</param>
    /// <returns>The value converted to type <typeparamref name="T"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static T CreateRangeValue<T>(int value) => (T)(object)value;

    /// <summary>
    /// Creates an array of sequential values from the specified range signal.
    /// </summary>
    /// <typeparam name="T">The element type of the array.</typeparam>
    /// <param name="range">The range signal specifying the start value and count.</param>
    /// <returns>An array containing sequential values from the range start to start + count - 1.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static T[] CreateRangeArray<T>(RangeSignal range)
    {
        if (typeof(T) == typeof(int))
        {
            var values = new int[range.Count];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = range.Start + i;
            }

            return (T[])(object)values;
        }

        var boxed = new T[range.Count];
        for (var i = 0; i < boxed.Length; i++)
        {
            boxed[i] = CreateRangeValue<T>(range.Start + i);
        }

        return boxed;
    }

    /// <summary>
    /// Creates a list of values from the specified range signal.
    /// </summary>
    /// <remarks>Optimized for integer types by directly incrementing values. For other types, uses
    /// <c>CreateRangeValue</c> to generate each element.</remarks>
    /// <typeparam name="T">The type of elements to create in the list.</typeparam>
    /// <param name="range">The range signal containing the start value and count.</param>
    /// <returns>A list containing the generated range values.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static List<T> CreateRangeList<T>(RangeSignal range)
    {
        if (typeof(T) == typeof(int))
        {
            var integers = new List<int>(range.Count);
            for (var i = 0; i < range.Count; i++)
            {
                integers.Add(range.Start + i);
            }

            return (List<T>)(object)integers;
        }

        var values = new List<T>(range.Count);
        for (var i = 0; i < range.Count; i++)
        {
            values.Add(CreateRangeValue<T>(range.Start + i));
        }

        return values;
    }
}
