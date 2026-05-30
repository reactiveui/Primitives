// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Primitives-vocabulary aliases for the async observable surface.
/// </summary>
public static partial class SignalAsync
{
    /// <summary>
    /// Emits a single value.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>An observable sequence that emits the single value.</returns>
    public static IObservableAsync<T> Emit<T>(T value) => Return(value);

    /// <summary>
    /// Emits a single <see cref="RxVoid"/> value.
    /// </summary>
    /// <returns>An observable sequence that emits a single <see cref="RxVoid"/> value.</returns>
    public static IObservableAsync<RxVoid> EmitRxVoid() => Return(RxVoid.Default);

    /// <summary>
    /// Completes without emitting values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <returns>An observable sequence that completes without emitting values.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<T> None<T>() => Empty<T>();

    /// <summary>
    /// Completes with a failure result.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error to fail with.</param>
    /// <returns>An observable sequence that completes with the failure.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<T> Fail<T>(Exception error) => Throw<T>(error);

    /// <summary>
    /// Creates a finite integer sequence.
    /// </summary>
    /// <param name="start">The first integer in the sequence.</param>
    /// <param name="count">The number of integers to emit.</param>
    /// <returns>An observable sequence of the requested integers.</returns>
    public static IObservableAsync<int> Sequence(int start, int count) => Range(start, count);

    /// <summary>
    /// Creates a source from an enumerable sequence.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The enumerable to convert.</param>
    /// <returns>An observable sequence emitting the enumerable's values.</returns>
    public static IObservableAsync<T> FromEnumerable<T>(IEnumerable<T> values) => values.ToAsyncSignal();

    /// <summary>
    /// Creates a source from an async enumerable sequence.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The async enumerable to convert.</param>
    /// <returns>An observable sequence emitting the async enumerable's values.</returns>
    public static IObservableAsync<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values) => values.ToAsyncSignal();

    /// <summary>
    /// Emits a single zero tick after the due time.
    /// </summary>
    /// <param name="dueTime">The delay before the tick is emitted.</param>
    /// <returns>An observable sequence that emits a single tick.</returns>
    public static IObservableAsync<long> After(TimeSpan dueTime) => Timer(dueTime);

    /// <summary>
    /// Emits first after the due time and then at each period.
    /// </summary>
    /// <param name="dueTime">The delay before the first tick is emitted.</param>
    /// <param name="period">The interval between subsequent ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> After(TimeSpan dueTime, TimeSpan period) => Timer(dueTime, period);

    /// <summary>
    /// Emits monotonically increasing ticks at the specified period.
    /// </summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Every(TimeSpan period) => Timer(period, period);

    /// <summary>
    /// Alias for <see cref="Every(TimeSpan)"/>.
    /// </summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Pulse(TimeSpan period) => Every(period);

    /// <summary>
    /// Creates a source whose subscription lifetime owns an async disposable resource.
    /// </summary>
    /// <typeparam name="TResource">The async disposable resource type.</typeparam>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="resourceFactory">The factory that asynchronously creates the resource.</param>
    /// <param name="signalFactory">The factory that creates the source from the resource.</param>
    /// <returns>An observable sequence whose subscription owns the resource.</returns>
    public static IObservableAsync<T> Use<TResource, T>(
        Func<CancellationToken, ValueTask<TResource>> resourceFactory,
        Func<TResource, IObservableAsync<T>> signalFactory)
        where TResource : IAsyncDisposable =>
        Using(resourceFactory, signalFactory);

    /// <summary>
    /// Returns an async observable as an async signal.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable sequence validated.</returns>
    public static IObservableAsync<T> ToAsyncSignal<T>(this IObservableAsync<T> source) =>
        source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Projects each value into a new value.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The projection applied to each value.</param>
    /// <returns>An observable sequence of projected values.</returns>
    public static IObservableAsync<TResult> Map<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, TResult> selector) =>
        source.Select(selector);

    /// <summary>
    /// Projects each value into a new value asynchronously.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The asynchronous projection applied to each value.</param>
    /// <returns>An observable sequence of projected values.</returns>
    public static IObservableAsync<TResult> Map<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, CancellationToken, ValueTask<TResult>> selector) =>
        source.Select(selector);

    /// <summary>
    /// Projects each value using caller-supplied state.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TState">The caller-supplied state type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The caller-supplied state passed to the selector.</param>
    /// <param name="selector">The projection applied to each value and the state.</param>
    /// <returns>An observable sequence of projected values.</returns>
    public static IObservableAsync<TResult> MapWith<TSource, TState, TResult>(
        this IObservableAsync<TSource> source,
        TState state,
        Func<TState, TSource, TResult> selector)
    {
        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return source.Map(value => selector(state, value));
    }

    /// <summary>
    /// Keeps values that satisfy a predicate.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The predicate that values must satisfy.</param>
    /// <returns>An observable sequence of values that satisfy the predicate.</returns>
    public static IObservableAsync<T> Keep<T>(this IObservableAsync<T> source, Func<T, bool> predicate) =>
        source.Where(predicate);

    /// <summary>
    /// Keeps values that satisfy an asynchronous predicate.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The asynchronous predicate that values must satisfy.</param>
    /// <returns>An observable sequence of values that satisfy the predicate.</returns>
    public static IObservableAsync<T> Keep<T>(
        this IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) =>
        source.Where(predicate);

    /// <summary>
    /// Keeps values that satisfy a stateful predicate.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TState">The caller-supplied state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The caller-supplied state passed to the predicate.</param>
    /// <param name="predicate">The predicate that values and the state must satisfy.</param>
    /// <returns>An observable sequence of values that satisfy the predicate.</returns>
    public static IObservableAsync<T> KeepWith<T, TState>(
        this IObservableAsync<T> source,
        TState state,
        Func<TState, T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return source.Keep(value => predicate(state, value));
    }

    /// <summary>
    /// Keeps non-null reference values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable sequence of non-null values.</returns>
    public static IObservableAsync<T> KeepNotNull<T>(this IObservableAsync<T?> source)
        where T : class =>
        source.WhereIsNotNull();

    /// <summary>
    /// Keeps values assignable to <typeparamref name="TResult"/>.
    /// </summary>
    /// <typeparam name="TResult">The result element type to keep.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable sequence of values assignable to <typeparamref name="TResult"/>.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<TResult> KeepType<TResult>(this IObservableAsync<object?> source)
        where TResult : class =>
        source.OfType<object?, TResult>();

    /// <summary>
    /// Casts each value to <typeparamref name="TResult"/>.
    /// </summary>
    /// <typeparam name="TResult">The result element type to cast to.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable sequence of values cast to <typeparamref name="TResult"/>.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<TResult> CastTo<TResult>(this IObservableAsync<object?> source) =>
        source.Cast<object?, TResult>();

    /// <summary>
    /// Invokes an action for each value while preserving the source values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <returns>An observable sequence identical to the source.</returns>
    public static IObservableAsync<T> Tap<T>(this IObservableAsync<T> source, Action<T> onNext) =>
        source.Do(onNext, null, null);

    /// <summary>
    /// Invokes asynchronous side effects while preserving the source values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The asynchronous action invoked for each value.</param>
    /// <param name="onErrorResume">The asynchronous action invoked on a resumable error.</param>
    /// <param name="onCompleted">The asynchronous action invoked on completion.</param>
    /// <returns>An observable sequence identical to the source.</returns>
    public static IObservableAsync<T> Tap<T>(
        this IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        Func<Result, ValueTask>? onCompleted) =>
        source.Do(onNext, onErrorResume, onCompleted);

    /// <summary>
    /// Invokes side effects while preserving the source values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <param name="onError">The action invoked on an error.</param>
    /// <param name="onCompleted">The action invoked on completion.</param>
    /// <returns>An observable sequence identical to the source.</returns>
    public static IObservableAsync<T> Tap<T>(
        this IObservableAsync<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        source.Do(onNext, onError, _ => onCompleted());

    /// <summary>
    /// Emits the accumulated state after each source value.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulator state.</param>
    /// <param name="accumulator">The accumulator applied to each value.</param>
    /// <returns>An observable sequence of accumulated states.</returns>
    public static IObservableAsync<TAccumulate> Fold<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator) =>
        source.Scan(seed, accumulator);

    /// <summary>
    /// Emits the accumulated state after each source value.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulator state.</param>
    /// <param name="accumulator">The asynchronous accumulator applied to each value.</param>
    /// <returns>An observable sequence of accumulated states.</returns>
    public static IObservableAsync<TAccumulate> Fold<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator) =>
        source.Scan(seed, accumulator);

    /// <summary>
    /// Emits the final accumulated state as a task.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="seed">The initial accumulator state.</param>
    /// <param name="accumulator">The accumulator applied to each value.</param>
    /// <returns>A task that completes with the final accumulated state.</returns>
    public static ValueTask<TAccumulate> ReduceAsync<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator) =>
        source.AggregateAsync(seed, accumulator);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The projection producing an inner sequence for each value.</param>
    /// <returns>An observable sequence of merged inner values.</returns>
    public static IObservableAsync<TResult> Bind<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, IObservableAsync<TResult>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The projection producing an inner sequence for each value.</param>
    /// <returns>An observable sequence of merged inner values.</returns>
    public static IObservableAsync<TResult> FlatMap<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, IObservableAsync<TResult>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The asynchronous projection producing an inner sequence for each value.</param>
    /// <returns>An observable sequence of merged inner values.</returns>
    public static IObservableAsync<TResult> FlatMap<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Suppresses adjacent duplicate values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>An observable sequence without adjacent duplicates.</returns>
    public static IObservableAsync<T> Unique<T>(this IObservableAsync<T> source) =>
        source.DistinctUntilChanged();

    /// <summary>
    /// Suppresses adjacent duplicate values.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="comparer">The comparer used to detect duplicates.</param>
    /// <returns>An observable sequence without adjacent duplicates.</returns>
    public static IObservableAsync<T> Unique<T>(this IObservableAsync<T> source, IEqualityComparer<T> comparer) =>
        source.DistinctUntilChanged(comparer);

    /// <summary>
    /// Suppresses adjacent duplicate keys.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The selector that extracts the comparison key.</param>
    /// <returns>An observable sequence without adjacent duplicate keys.</returns>
    public static IObservableAsync<T> UniqueBy<T, TKey>(
        this IObservableAsync<T> source,
        Func<T, TKey> keySelector) =>
        source.DistinctUntilChangedBy(keySelector);

    /// <summary>
    /// Suppresses adjacent duplicate keys.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">The selector that extracts the comparison key.</param>
    /// <param name="comparer">The comparer used to compare keys.</param>
    /// <returns>An observable sequence without adjacent duplicate keys.</returns>
    public static IObservableAsync<T> UniqueBy<T, TKey>(
        this IObservableAsync<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey> comparer) =>
        source.DistinctUntilChangedBy(keySelector, comparer);

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sequence of sources to concatenate.</param>
    /// <returns>An observable sequence that concatenates the sources.</returns>
    public static IObservableAsync<T> Chain<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Concat();

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="first">The first source sequence.</param>
    /// <param name="second">The second source sequence.</param>
    /// <returns>An observable sequence that concatenates the sources.</returns>
    public static IObservableAsync<T> Chain<T>(this IObservableAsync<T> first, IObservableAsync<T> second) =>
        first.Concat(second);

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>An observable sequence that concatenates the sources.</returns>
    public static IObservableAsync<T> Chain<T>(params IObservableAsync<T>[] sources) =>
        sources.Concat();

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sequence of sources to merge.</param>
    /// <returns>An observable sequence that merges the sources.</returns>
    public static IObservableAsync<T> Blend<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Merge();

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="first">The first source sequence.</param>
    /// <param name="second">The second source sequence.</param>
    /// <returns>An observable sequence that merges the sources.</returns>
    public static IObservableAsync<T> Blend<T>(this IObservableAsync<T> first, IObservableAsync<T> second) =>
        first.Merge(second);

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that merges the sources.</returns>
    public static IObservableAsync<T> Blend<T>(params IObservableAsync<T>[] sources) =>
        sources.Merge();

    /// <summary>
    /// Switches to the latest inner source.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sequence of inner sources.</param>
    /// <returns>An observable sequence that emits values from the latest inner source.</returns>
    public static IObservableAsync<T> SwitchTo<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Switch();

    /// <summary>
    /// Combines paired values from two sources.
    /// </summary>
    /// <typeparam name="TLeft">The left element type.</typeparam>
    /// <typeparam name="TRight">The right element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="left">The left source sequence.</param>
    /// <param name="right">The right source sequence.</param>
    /// <param name="selector">The function combining each pair of values.</param>
    /// <returns>An observable sequence of combined values.</returns>
    public static IObservableAsync<TResult> Pair<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.Zip(right, selector);

    /// <summary>
    /// Combines the latest values from two sources.
    /// </summary>
    /// <typeparam name="TLeft">The left element type.</typeparam>
    /// <typeparam name="TRight">The right element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="left">The left source sequence.</param>
    /// <param name="right">The right source sequence.</param>
    /// <param name="selector">The function combining the latest values.</param>
    /// <returns>An observable sequence of combined values.</returns>
    public static IObservableAsync<TResult> SyncLatest<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.CombineLatest(right, selector);

    /// <summary>
    /// Combines latest values from two sources.
    /// </summary>
    /// <typeparam name="TLeft">The left element type.</typeparam>
    /// <typeparam name="TRight">The right element type.</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="left">The left source sequence.</param>
    /// <param name="right">The right source sequence.</param>
    /// <param name="selector">The function combining the latest values.</param>
    /// <returns>An observable sequence of combined values.</returns>
    public static IObservableAsync<TResult> PairLatest<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.CombineLatest(right, selector);

    /// <summary>
    /// Retries the source up to the specified count.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="retryCount">The maximum number of retry attempts.</param>
    /// <returns>An observable sequence that retries on failure.</returns>
    public static IObservableAsync<T> Reattempt<T>(this IObservableAsync<T> source, int retryCount) =>
        source.Retry(retryCount);

    /// <summary>
    /// Recovers from a terminal failure with a replacement sequence.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="handler">The handler that produces a replacement sequence from the error.</param>
    /// <returns>An observable sequence that recovers from failures.</returns>
    public static IObservableAsync<T> Recover<T>(
        this IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler) =>
        source.Catch(handler);

    /// <summary>
    /// Recovers from a terminal failure with a replacement sequence.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="handler">The handler that produces a replacement sequence from the error.</param>
    /// <returns>An observable sequence that recovers from failures.</returns>
    public static IObservableAsync<T> Rescue<T>(
        this IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler) =>
        source.Catch(handler);

    /// <summary>
    /// Resumes with a fallback sequence after a terminal failure.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="fallback">The fallback sequence used after a failure.</param>
    /// <returns>An observable sequence that resumes with the fallback on failure.</returns>
    public static IObservableAsync<T> Resume<T>(this IObservableAsync<T> source, IObservableAsync<T> fallback) =>
        source.Catch(_ => fallback);

    /// <summary>
    /// Delays source notifications.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The delay applied to notifications.</param>
    /// <returns>An observable sequence with delayed notifications.</returns>
    public static IObservableAsync<T> Shift<T>(this IObservableAsync<T> source, TimeSpan dueTime) =>
        source.Delay(dueTime);

    /// <summary>
    /// Fails if the source does not terminate before the timeout.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <returns>An observable sequence that fails on timeout.</returns>
    public static IObservableAsync<T> Expire<T>(this IObservableAsync<T> source, TimeSpan dueTime) =>
        source.Timeout(dueTime);

    /// <summary>
    /// Prepends a value before the source.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="value">The value to prepend.</param>
    /// <returns>An observable sequence that emits the value before the source.</returns>
    public static IObservableAsync<T> Lead<T>(this IObservableAsync<T> source, T value) =>
        source.Prepend(value);

    /// <summary>
    /// Collects all values into a list.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the collected list of values.</returns>
    public static ValueTask<List<T>> CollectListAsync<T>(this IObservableAsync<T> source) =>
        source.ToListAsync();

    /// <summary>
    /// Collects all values into an array.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the collected array of values.</returns>
    public static async ValueTask<T[]> CollectArrayAsync<T>(this IObservableAsync<T> source)
    {
        var values = await source.ToListAsync().ConfigureAwait(false);
        return [.. values];
    }
}
