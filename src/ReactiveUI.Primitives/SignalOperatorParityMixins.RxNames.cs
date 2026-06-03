// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// System.Reactive / LINQ familiar names for the Primitives operator vocabulary. Each method builds the same sink as
/// its Primitives-named counterpart directly, so the two names are interchangeable with identical behaviour and
/// allocation profile. Both name sets are fully supported.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Projects each element of an observable sequence into a new form. LINQ name for <c>Map</c>.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
    /// <param name="source">An observable sequence of elements to project.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the transform function on each source element.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Select<TSource, TResult>(this IObservable<TSource> source, Func<TSource, TResult> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new MapSignal<TSource, TResult>(source, selector);
    }

    /// <summary>
    /// Projects each element into a new form using external state passed to the selector. State-carrying name for <c>MapWith</c>.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
    /// <param name="source">An observable sequence of elements to project.</param>
    /// <param name="state">The state to pass to the selector function.</param>
    /// <param name="selector">A transform function to apply to each source element along with the state.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and the state.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> SelectWith<TSource, TState, TResult>(this IObservable<TSource> source, TState state, Func<TState, TSource, TResult> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new MapWithSignal<TSource, TState, TResult>(source, state, selector);
    }

    /// <summary>
    /// Filters an observable sequence to elements that satisfy a predicate. LINQ name for <c>Keep</c>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>An observable sequence containing the elements that satisfy <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Where<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new KeepSignal<T>(source, predicate);
    }

    /// <summary>
    /// Filters elements using a predicate that uses external state. State-carrying name for <c>KeepWith</c>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <param name="state">The state value to pass to the predicate for each element.</param>
    /// <param name="predicate">A function to test each element along with the state.</param>
    /// <returns>An observable sequence containing only the elements that satisfy the predicate.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> WhereWith<T, TState>(this IObservable<T> source, TState state, Func<TState, T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new KeepWithSignal<T, TState>(source, state, predicate);
    }

    /// <summary>
    /// Filters out null values, emitting only non-null values. Familiar name for <c>KeepNotNull</c>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <returns>An observable sequence that emits only the non-null values from the source sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> WhereNotNull<T>(this IObservable<T?> source)
        where T : class
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new KeepNotNullSignal<T>(source);
    }

    /// <summary>
    /// Invokes an action for each value while preserving the sequence. System.Reactive name for <c>Tap</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <returns>The source values after the action has run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        return new TapSignal<T>(source, onNext, static _ => { }, static () => { });
    }

    /// <summary>
    /// Invokes a stateful action for each value while preserving the sequence. State-carrying name for <c>TapWith</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <returns>The source values after the action has run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DoWith<T, TState>(this IObservable<T> source, TState state, Action<TState, T> onNext)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        return new TapWithSignal<T, TState>(source, state, onNext);
    }

    /// <summary>
    /// Emits the accumulated state after each source value. System.Reactive name for <c>Fold</c>.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The function that combines the current state with the next source value.</param>
    /// <returns>A sequence of intermediate accumulated values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
    public static IObservable<TAccumulate> Scan<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (accumulator == null)
        {
            throw new ArgumentNullException(nameof(accumulator));
        }

        return new FoldSignal<TSource, TAccumulate>(source, seed, accumulator);
    }

    /// <summary>
    /// Emits the final accumulated state when the source completes. System.Reactive name for <c>Reduce</c>.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The function that combines the current state with the next source value.</param>
    /// <returns>A sequence that emits one accumulated value on completion.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
    public static IObservable<TAccumulate> Aggregate<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (accumulator == null)
        {
            throw new ArgumentNullException(nameof(accumulator));
        }

        return new ReduceSignal<TSource, TAccumulate>(source, seed, accumulator);
    }

    /// <summary>
    /// Suppresses adjacent duplicate values. System.Reactive name for <c>Unique</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence with adjacent duplicates removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source) =>
        DistinctUntilChanged(source, null);

    /// <summary>
    /// Suppresses adjacent duplicate values using the supplied comparer. System.Reactive name for <c>Unique</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="comparer">The comparer used to compare adjacent values.</param>
    /// <returns>A sequence with adjacent duplicates removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        comparer ??= EqualityComparer<T>.Default;
        return new UniqueSignal<T>(source, comparer);
    }

    /// <summary>
    /// Suppresses adjacent values with duplicate keys. System.Reactive name for <c>UniqueBy</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <returns>A sequence with adjacent duplicate keys removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctUntilChangedBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector) =>
        DistinctUntilChangedBy(source, keySelector, null);

    /// <summary>
    /// Suppresses adjacent values with duplicate keys using the supplied comparer. System.Reactive name for <c>UniqueBy</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The function that selects the comparison key.</param>
    /// <param name="comparer">The comparer used to compare adjacent keys.</param>
    /// <returns>A sequence with adjacent duplicate keys removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> DistinctUntilChangedBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
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
        return new UniqueBySignal<T, TKey>(source, keySelector, comparer);
    }

    /// <summary>
    /// Drops every value, forwarding only the terminal notification. System.Reactive name for <c>IgnoreValues</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that forwards only completion or error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> IgnoreElements<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new IgnoreValuesSignal<T>(source);
    }

    /// <summary>
    /// Projects each value to an inner sequence and merges the results. LINQ name for <c>FlatMap</c>.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The inner value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The function that projects each source value to an inner sequence.</param>
    /// <returns>A sequence containing the merged values of every inner sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> SelectMany<TSource, TResult>(this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
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
    /// Projects each value to an inner sequence and combines each pair with a result selector. LINQ name for <c>FlatMap</c>.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TCollection">The inner value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="collectionSelector">The function that projects each source value to an inner sequence.</param>
    /// <param name="resultSelector">The function that combines a source value with each inner value.</param>
    /// <returns>A sequence containing selected outer/inner combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> SelectMany<TSource, TCollection, TResult>(
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
    /// Subscribes to all inner sequences and forwards their values as they arrive. System.Reactive name for <c>Blend</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence containing values from all inner sequences.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Merge<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new BlendSignal<T>(sources);
    }

    /// <summary>
    /// Subscribes to inner sequences one at a time in source order. System.Reactive name for <c>Chain</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Concat<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new ChainSignal<T>(sources);
    }

    /// <summary>
    /// Concatenates two sequences. System.Reactive name for <c>Chain</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="first">The first sequence.</param>
    /// <param name="second">The second sequence.</param>
    /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="first"/> completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="first"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Concat<T>(this IObservable<T> first, IObservable<T> second)
    {
        if (first == null)
        {
            throw new ArgumentNullException(nameof(first));
        }

        if (second == null)
        {
            throw new ArgumentNullException(nameof(second));
        }

        return new ChainSignal<T>(first, second);
    }

    /// <summary>
    /// Mirrors the first inner sequence to produce any notification. System.Reactive name for <c>Race</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The competing inner sequences.</param>
    /// <returns>A sequence that mirrors the winning inner sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Amb<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new RaceSignal<T>(sources);
    }

    /// <summary>
    /// Switches to the most recent inner sequence. System.Reactive name for <c>SwitchTo</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Switch<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        if (TryCreateSynchronousSwitchRangeSignal(sources, out var rangeSignal))
        {
            return rangeSignal;
        }

        return new SwitchSignal<T>(sources);
    }

    /// <summary>
    /// Combines paired values from two sequences by index. System.Reactive name for <c>Pair</c>.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines paired values.</param>
    /// <returns>A sequence containing one result for each available value pair.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Zip<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
            return new RangeZipSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
        }

        return new ZipSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>
    /// Combines the latest values once both sequences have produced a value. System.Reactive name for <c>SyncLatest</c>.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>A sequence containing selected latest-value combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> CombineLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
            return CreateRangeCombineLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
        }

        return new CombineLatestSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>
    /// Combines each left value with the latest right value. System.Reactive name for <c>Latch</c>.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The triggering sequence.</param>
    /// <param name="right">The sequence that supplies the latest value.</param>
    /// <param name="selector">The function that combines the left value with the latest right value.</param>
    /// <returns>A sequence containing selected left/latest-right combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> WithLatestFrom<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
            return CreateRangeWithLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
        }

        return new LatchSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>
    /// Delays source notifications by the specified duration. System.Reactive name for <c>Shift</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <returns>A sequence that forwards source notifications after the delay.</returns>
    public static IObservable<T> Delay<T>(this IObservable<T> source, TimeSpan dueTime) =>
        Delay(source, dueTime, null);

    /// <summary>
    /// Delays source notifications by the specified duration on a sequencer. System.Reactive name for <c>Shift</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
    /// <returns>A sequence that forwards source notifications after the delay.</returns>
    public static IObservable<T> Delay<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
        {
            return new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), scheduler);
        }

        return new ShiftSignal<T>(source, dueTime, scheduler);
    }

    /// <summary>
    /// Fails the sequence if it does not terminate before the timeout. System.Reactive name for <c>Expire</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Timeout<T>(this IObservable<T> source, TimeSpan dueTime) =>
        Timeout(source, dueTime, null);

    /// <summary>
    /// Fails the sequence if it does not terminate before the sequencer timeout. System.Reactive name for <c>Expire</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Timeout<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ExpireSignal<T>(source, dueTime, scheduler);
    }

    /// <summary>
    /// Emits the most recent value at the end of each sampling period. System.Reactive name for <c>Probe</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="interval">The sampling period.</param>
    /// <returns>A sequence containing the latest source value sampled at each period boundary.</returns>
    public static IObservable<T> Sample<T>(this IObservable<T> source, TimeSpan interval) =>
        Sample(source, interval, null);

    /// <summary>
    /// Emits the most recent value at the end of each sampling period on a sequencer. System.Reactive name for <c>Probe</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="interval">The sampling period.</param>
    /// <param name="scheduler">The sequencer used to schedule sampling.</param>
    /// <returns>A sequence containing the latest source value sampled at each period boundary.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
    public static IObservable<T> Sample<T>(this IObservable<T> source, TimeSpan interval, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (interval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(interval));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ProbeSignal<T>(source, interval, scheduler);
    }

    /// <summary>
    /// Resubscribes to the source after an error up to <paramref name="retryCount"/> times. System.Reactive name for <c>Reattempt</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
    /// <returns>A sequence that retries the source before forwarding the final error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is less than zero.</exception>
    public static IObservable<T> Retry<T>(this IObservable<T> source, int retryCount)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (retryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryCount));
        }

        return new ReattemptSignal<T>(source, retryCount);
    }

    /// <summary>
    /// Converts source values and terminal notifications into <see cref="Spark{T}"/> values. System.Reactive name for <c>Spark</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence of spark values representing source notifications.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<Spark<T>> Materialize<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new SparkSignal<T>(source);
    }

    /// <summary>
    /// Converts <see cref="Spark{T}"/> values back into observer notifications. System.Reactive name for <c>Unspark</c>.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The spark sequence.</param>
    /// <returns>A sequence represented by the supplied spark values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Dematerialize<T>(this IObservable<Spark<T>> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new UnsparkSignal<T>(source);
    }
}
