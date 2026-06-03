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
/// Additional ReactiveUI.Primitives operator surface using distinct Primitives vocabulary.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Projects each element of an observable sequence into a new form.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
    /// <param name="source">An observable sequence of elements to project.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
    /// source sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Map<TSource, TResult>(this IObservable<TSource> source, Func<TSource, TResult> selector)
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
    /// Projects each element of an observable sequence into a new form by incorporating state that is passed to the
    /// selector function.
    /// </summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
    /// <param name="source">An observable sequence of elements to project.</param>
    /// <param name="state">The state to pass to the selector function.</param>
    /// <param name="selector">A transform function to apply to each source element along with the state.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
    /// source along with the state.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> MapWith<TSource, TState, TResult>(this IObservable<TSource> source, TState state, Func<TState, TSource, TResult> selector)
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
    /// Filters an observable sequence to include only elements that satisfy a specified condition.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>An observable sequence that contains elements from the input sequence that satisfy the condition specified by
    /// <paramref name="predicate"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Keep<T>(this IObservable<T> source, Func<T, bool> predicate)
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
    /// Filters elements from an observable sequence based on a predicate that uses external state.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <param name="state">The state value to pass to the predicate for each element.</param>
    /// <param name="predicate">A function to test each element along with the state; returns <see langword="true"/> to keep the element, <see
    /// langword="false"/> to filter it out.</param>
    /// <returns>An observable sequence containing only the elements from the source sequence that satisfy the predicate.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> KeepWith<T, TState>(this IObservable<T> source, TState state, Func<TState, T, bool> predicate)
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
    /// Filters out null values from the source observable sequence, emitting only non-null values.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <returns>An observable sequence that emits only non-null values from the source sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static IObservable<T> KeepNotNull<T>(this IObservable<T?> source)
        where T : class
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new KeepNotNullSignal<T>(source);
    }

    /// <summary>
    /// Filters values to those assignable to <typeparamref name="TResult"/>.
    /// </summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing only values assignable to <typeparamref name="TResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<TResult> KeepType<TResult>(this IObservable<object?> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new KeepTypeSignal<TResult>(source);
    }

    /// <summary>
    /// Casts each source value to <typeparamref name="TResult"/>.
    /// </summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing each value cast to <typeparamref name="TResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<TResult> CastTo<TResult>(this IObservable<object?> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return source.Map(value => (TResult)value!);
    }

    /// <summary>
    /// Invokes an action for each value while preserving the original sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <returns>The source values after the action has run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Tap<T>(this IObservable<T> source, Action<T> onNext)
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
    /// Invokes a stateful action for each value while preserving the original sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <returns>The source values after the action has run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> TapWith<T, TState>(this IObservable<T> source, TState state, Action<TState, T> onNext)
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
    /// Emits the accumulated state after each source value.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The function that combines the current state with the next source value.</param>
    /// <returns>A sequence of intermediate accumulated values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
    public static IObservable<TAccumulate> Fold<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
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
    /// Emits the final accumulated state when the source completes.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The function that combines the current state with the next source value.</param>
    /// <returns>A sequence that emits one accumulated value on completion.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
    public static IObservable<TAccumulate> Reduce<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
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
    /// Emits at most <paramref name="count"/> values before completing.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="count">The maximum number of values to emit.</param>
    /// <returns>A sequence containing at most <paramref name="count"/> source values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
    public static IObservable<T> Take<T>(this IObservable<T> source, int count)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (source is LoopSignal<T> loop)
        {
            return count == 0 ? Signal.None<T>() : new RepeatSignal<T>(loop.Value, count);
        }

        return new TakeSignal<T>(source, count);
    }

    /// <summary>
    /// Skips the first <paramref name="count"/> source values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="count">The number of values to skip.</param>
    /// <returns>A sequence containing source values after the skipped prefix.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
    public static IObservable<T> Skip<T>(this IObservable<T> source, int count)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        return new SkipSignal<T>(source, count);
    }

    /// <summary>
    /// Suppresses values that have already been observed.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing the first occurrence of each source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Distinct<T>(this IObservable<T> source) =>
        source.Distinct(null);

    /// <summary>
    /// Suppresses values that have already been observed using the supplied comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="comparer">The comparer used to identify duplicate values.</param>
    /// <returns>A sequence containing the first occurrence of each source value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Distinct<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new DistinctSignal<T>(source, comparer);
    }

    /// <summary>
    /// Suppresses adjacent duplicate values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence with adjacent duplicates removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Unique<T>(this IObservable<T> source) =>
        source.Unique(null);

    /// <summary>
    /// Suppresses adjacent duplicate values using the supplied comparer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="comparer">The comparer used to compare adjacent values.</param>
    /// <returns>A sequence with adjacent duplicates removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Unique<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        comparer ??= EqualityComparer<T>.Default;
        return new UniqueSignal<T>(source, comparer);
    }

    /// <summary>
    /// Converts source values and terminal notifications into <see cref="Spark{T}"/> values.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence of spark values representing source notifications; terminal sparks are followed by completion.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<Spark<T>> Spark<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new SparkSignal<T>(source);
    }

    /// <summary>
    /// Converts <see cref="Spark{T}"/> values back into observer notifications.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The spark sequence.</param>
    /// <returns>A sequence represented by the supplied spark values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Unspark<T>(this IObservable<Spark<T>> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new UnsparkSignal<T>(source);
    }

    /// <summary>
    /// Subscribes to inner sequences one at a time in source order.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Chain<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new ChainSignal<T>(sources);
    }

    /// <summary>
    /// Concatenates two sequences.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="first">The first sequence.</param>
    /// <param name="second">The second sequence.</param>
    /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="first"/> completes.</returns>
    public static IObservable<T> Chain<T>(this IObservable<T> first, IObservable<T> second) =>
        Signal.Chain(first, second);

    /// <summary>
    /// Subscribes to all inner sequences and forwards their values as they arrive.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence containing values from all inner sequences.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Blend<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new BlendSignal<T>(sources);
    }

    /// <summary>
    /// Mirrors the first inner sequence to produce any notification.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The competing inner sequences.</param>
    /// <returns>A sequence that mirrors the winning inner sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Race<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return new RaceSignal<T>(sources);
    }

    /// <summary>
    /// Combines paired values from two sequences, completing when no more pairs can be formed.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines paired values.</param>
    /// <returns>A sequence containing one result for each available value pair.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Pair<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
    /// Combines the latest values after both sequences have produced at least one value.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    /// <param name="right">The right sequence.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>A sequence containing selected latest-value combinations.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> SyncLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
    /// Combines each left value with the latest right value after the right sequence has produced a value.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The triggering sequence.</param>
    /// <param name="right">The sequence that supplies the latest value.</param>
    /// <param name="selector">The function that combines the left value with the latest right value.</param>
    /// <returns>A sequence containing selected left/latest-right combinations.</returns>
    /// <remarks>Left values produced before the first right value are ignored.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Latch<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
    /// Switches to the most recent inner sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> SwitchTo<T>(this IObservable<IObservable<T>> sources)
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
    /// Resubscribes to the source after an error up to <paramref name="retryCount"/> times.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
    /// <returns>A sequence that retries the source before forwarding the final error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is less than zero.</exception>
    public static IObservable<T> Reattempt<T>(this IObservable<T> source, int retryCount)
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
    /// Recovers from errors by switching to a handler-provided sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="handler">The function that creates the recovery sequence for an error.</param>
    /// <returns>A sequence that continues with the handler result after an error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Recover<T>(this IObservable<T> source, Func<Exception, IObservable<T>> handler) =>
        source.Recover<T, Exception>(handler);

    /// <summary>
    /// Recovers from errors by switching to a handler-provided sequence.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="handler">The function that creates the recovery sequence for an error.</param>
    /// <returns>A sequence that continues with the handler result after an error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Rescue<T>(this IObservable<T> source, Func<Exception, IObservable<T>> handler) =>
        source.Recover(handler);

    /// <summary>
    /// Continues with a fallback sequence after an error.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="fallback">The sequence to subscribe to after an error.</param>
    /// <returns>A sequence that resumes with <paramref name="fallback"/> after an error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="fallback"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Resume<T>(this IObservable<T> source, IObservable<T> fallback)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (fallback == null)
        {
            throw new ArgumentNullException(nameof(fallback));
        }

        return new ResumeSignal<T>(source, fallback);
    }

    /// <summary>
    /// Delays source notifications by the specified duration.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <returns>A sequence that forwards source notifications after the delay.</returns>
    public static IObservable<T> Shift<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Shift(dueTime, null);

    /// <summary>
    /// Delays source notifications by the specified duration on a sequencer.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
    /// <returns>A sequence that forwards source notifications after the delay.</returns>
    public static IObservable<T> Shift<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
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
    /// Fails the sequence if it does not terminate before the timeout.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Expire<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Expire(dueTime, null);

    /// <summary>
    /// Fails the sequence if it does not terminate before the sequencer timeout.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Expire<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ExpireSignal<T>(source, dueTime, scheduler);
    }

    /// <summary>
    /// Collects all values into a list when the source completes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits one list containing all source values.</returns>
    public static IObservable<IList<T>> CollectList<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
        {
            return new RangeListSignal<T>(range);
        }

        return new CollectListSignal<T>(source);
    }

    /// <summary>
    /// Collects all values into an array when the source completes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that emits one array containing all source values.</returns>
    public static IObservable<T[]> CollectArray<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
        {
            return new RangeArraySignal<T>(range);
        }

        return new CollectArraySignal<T>(source);
    }

    /// <summary>
    /// Converts an enumerable sequence to a signal.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The values to enumerate.</param>
    /// <returns>A signal that emits the enumerable values.</returns>
    public static IObservable<T> ToSignal<T>(this IEnumerable<T> values) => Signal.FromEnumerable(values);

    /// <summary>
    /// Converts an enumerable sequence to a signal that observes cancellation.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The values to enumerate.</param>
    /// <param name="cancellationToken">The token used to stop enumeration.</param>
    /// <returns>A signal that emits the enumerable values until enumeration completes or cancellation is requested.</returns>
    public static IObservable<T> ToSignal<T>(this IEnumerable<T> values, CancellationToken cancellationToken) =>
        Signal.FromEnumerable(values, cancellationToken);

    /// <summary>
    /// Returns an observable sequence as a signal-compatible observable.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>The supplied source sequence.</returns>
    public static IObservable<T> ToSignal<T>(this IObservable<T> source) => source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Creates the optimized range-backed combine-latest sequence.
    /// </summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left range source.</param>
    /// <param name="right">The right range source.</param>
    /// <param name="selector">The function that combines range values.</param>
    /// <returns>The optimized combine-latest sequence.</returns>
    private static RangeCombineLatestSignal<TResult> CreateRangeCombineLatestSignal<TResult>(
        RangeSignal left,
        RangeSignal right,
        Func<int, int, TResult> selector) =>
        new(left, right, selector);

    /// <summary>
    /// Creates the optimized range-backed with-latest sequence.
    /// </summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left range source.</param>
    /// <param name="right">The right range source.</param>
    /// <param name="selector">The function that combines range values.</param>
    /// <returns>The optimized with-latest sequence.</returns>
    private static RangeWithLatestSignal<TResult> CreateRangeWithLatestSignal<TResult>(
        RangeSignal left,
        RangeSignal right,
        Func<int, int, TResult> selector) =>
        new(left, right, selector);

    /// <summary>
    /// Determines whether a generic observer type can receive boxed range integers.
    /// </summary>
    /// <param name="elementType">The observer value type.</param>
    /// <returns><see langword="true"/> when the cast is valid.</returns>
    private static bool CanReadRangeAs(Type elementType) => elementType.IsAssignableFrom(typeof(int));

    /// <summary>
    /// Creates a range-concat signal for synchronous Switch over known range inners.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">Outer sources.</param>
    /// <param name="signal">Optimized signal when available.</param>
    /// <returns><see langword="true"/> when the fast path applies.</returns>
    private static bool TryCreateSynchronousSwitchRangeSignal<T>(IObservable<IObservable<T>> sources, out IObservable<T> signal)
    {
        signal = null!;
        if (typeof(T) != typeof(int) || sources is not FromEnumerableSignal<IObservable<T>> enumerable)
        {
            return false;
        }

        if (!enumerable.TryGetReadOnlyValues(out var innerSources) || innerSources.Count == 0)
        {
            return false;
        }

        var ranges = new RangeSignal[innerSources.Count];
        for (var i = 0; i < innerSources.Count; i++)
        {
            if (innerSources[i] is not RangeSignal range)
            {
                return false;
            }

            ranges[i] = range;
        }

        signal = (IObservable<T>)(object)new RangeConcatSignal(ranges);
        return true;
    }

    /// <summary>
    /// Emits all range values and completion from a scheduled batch.
    /// </summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static IDisposable EmitShiftedRange<T>(IObserver<T> observer, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            observer.OnNext((T)(object)(range.Start + i));
        }

        observer.OnCompleted();
        return Disposable.Empty;
    }

    /// <summary>
    /// Emits all range values and completion from a scheduled batch.
    /// </summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="onNext">The next callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static IDisposable EmitShiftedRange<T>(Action<T> onNext, Action onCompleted, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            onNext((T)(object)(range.Start + i));
        }

        onCompleted();
        return Disposable.Empty;
    }
}
