// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1611, SA1615, SA1618, S4018

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Primitives-vocabulary aliases for the async observable surface.
/// </summary>
public static partial class ObservableAsync
{
    /// <summary>
    /// Emits a single value.
    /// </summary>
    public static IObservableAsync<T> Emit<T>(T value) => Return(value);

    /// <summary>
    /// Emits a single <see cref="RxVoid"/> value.
    /// </summary>
    public static IObservableAsync<RxVoid> EmitRxVoid() => Return(RxVoid.Default);

    /// <summary>
    /// Completes without emitting values.
    /// </summary>
    public static IObservableAsync<T> None<T>() => Empty<T>();

    /// <summary>
    /// Completes with a failure result.
    /// </summary>
    public static IObservableAsync<T> Fail<T>(Exception error) => Throw<T>(error);

    /// <summary>
    /// Creates a finite integer sequence.
    /// </summary>
    public static IObservableAsync<int> Sequence(int start, int count) => Range(start, count);

    /// <summary>
    /// Creates a source from an enumerable sequence.
    /// </summary>
    public static IObservableAsync<T> FromEnumerable<T>(IEnumerable<T> values) => values.ToObservableAsync();

    /// <summary>
    /// Creates a source from an async enumerable sequence.
    /// </summary>
    public static IObservableAsync<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values) => values.ToObservableAsync();

    /// <summary>
    /// Emits a single zero tick after the due time.
    /// </summary>
    public static IObservableAsync<long> After(TimeSpan dueTime) => Timer(dueTime);

    /// <summary>
    /// Emits first after the due time and then at each period.
    /// </summary>
    public static IObservableAsync<long> After(TimeSpan dueTime, TimeSpan period) => Timer(dueTime, period);

    /// <summary>
    /// Emits monotonically increasing ticks at the specified period.
    /// </summary>
    public static IObservableAsync<long> Every(TimeSpan period) => Timer(period, period);

    /// <summary>
    /// Alias for <see cref="Every(TimeSpan)"/>.
    /// </summary>
    public static IObservableAsync<long> Pulse(TimeSpan period) => Every(period);

    /// <summary>
    /// Creates a source whose subscription lifetime owns an async disposable resource.
    /// </summary>
    public static IObservableAsync<T> Use<TResource, T>(
        Func<CancellationToken, ValueTask<TResource>> resourceFactory,
        Func<TResource, IObservableAsync<T>> signalFactory)
        where TResource : IAsyncDisposable =>
        Using(resourceFactory, signalFactory);

    /// <summary>
    /// Converts an enumerable sequence to an async signal.
    /// </summary>
    public static IObservableAsync<T> ToAsyncSignal<T>(this IEnumerable<T> values) => values.ToObservableAsync();

    /// <summary>
    /// Converts an async enumerable sequence to an async signal.
    /// </summary>
    public static IObservableAsync<T> ToAsyncSignal<T>(this IAsyncEnumerable<T> values) => values.ToObservableAsync();

    /// <summary>
    /// Converts a task to an async signal.
    /// </summary>
    public static IObservableAsync<T> ToAsyncSignal<T>(this Task<T> task) => task.ToObservableAsync();

    /// <summary>
    /// Converts a task to an async signal.
    /// </summary>
    public static IObservableAsync<RxVoid> ToAsyncSignal(this Task task) => task.ToObservableAsync();

    /// <summary>
    /// Returns an async observable as an async signal.
    /// </summary>
    public static IObservableAsync<T> ToAsyncSignal<T>(this IObservableAsync<T> source) =>
        source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Projects each value into a new value.
    /// </summary>
    public static IObservableAsync<TResult> Map<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, TResult> selector) =>
        source.Select(selector);

    /// <summary>
    /// Projects each value into a new value asynchronously.
    /// </summary>
    public static IObservableAsync<TResult> Map<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, CancellationToken, ValueTask<TResult>> selector) =>
        source.Select(selector);

    /// <summary>
    /// Projects each value using caller-supplied state.
    /// </summary>
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
    public static IObservableAsync<T> Keep<T>(this IObservableAsync<T> source, Func<T, bool> predicate) =>
        source.Where(predicate);

    /// <summary>
    /// Keeps values that satisfy an asynchronous predicate.
    /// </summary>
    public static IObservableAsync<T> Keep<T>(
        this IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) =>
        source.Where(predicate);

    /// <summary>
    /// Keeps values that satisfy a stateful predicate.
    /// </summary>
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
    public static IObservableAsync<T> KeepNotNull<T>(this IObservableAsync<T?> source)
        where T : class =>
        source.WhereIsNotNull();

    /// <summary>
    /// Keeps values assignable to <typeparamref name="TResult"/>.
    /// </summary>
    public static IObservableAsync<TResult> KeepType<TResult>(this IObservableAsync<object?> source)
        where TResult : class =>
        source.OfType<object?, TResult>();

    /// <summary>
    /// Casts each value to <typeparamref name="TResult"/>.
    /// </summary>
    public static IObservableAsync<TResult> CastTo<TResult>(this IObservableAsync<object?> source) =>
        source.Cast<object?, TResult>();

    /// <summary>
    /// Invokes an action for each value while preserving the source values.
    /// </summary>
    public static IObservableAsync<T> Tap<T>(this IObservableAsync<T> source, Action<T> onNext) =>
        source.Do(onNext, null, null);

    /// <summary>
    /// Invokes asynchronous side effects while preserving the source values.
    /// </summary>
    public static IObservableAsync<T> Tap<T>(
        this IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        Func<Result, ValueTask>? onCompleted) =>
        source.Do(onNext, onErrorResume, onCompleted);

    /// <summary>
    /// Invokes side effects while preserving the source values.
    /// </summary>
    public static IObservableAsync<T> Tap<T>(
        this IObservableAsync<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        source.Do(onNext, onError, _ => onCompleted());

    /// <summary>
    /// Emits the accumulated state after each source value.
    /// </summary>
    public static IObservableAsync<TAccumulate> Fold<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator) =>
        source.Scan(seed, accumulator);

    /// <summary>
    /// Emits the accumulated state after each source value.
    /// </summary>
    public static IObservableAsync<TAccumulate> Fold<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator) =>
        source.Scan(seed, accumulator);

    /// <summary>
    /// Emits the final accumulated state as a task.
    /// </summary>
    public static ValueTask<TAccumulate> ReduceAsync<TSource, TAccumulate>(
        this IObservableAsync<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator) =>
        source.AggregateAsync(seed, accumulator);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    public static IObservableAsync<TResult> Bind<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, IObservableAsync<TResult>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    public static IObservableAsync<TResult> FlatMap<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, IObservableAsync<TResult>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Projects and merges inner async observable sequences.
    /// </summary>
    public static IObservableAsync<TResult> FlatMap<TSource, TResult>(
        this IObservableAsync<TSource> source,
        Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector) =>
        source.SelectMany(selector);

    /// <summary>
    /// Suppresses adjacent duplicate values.
    /// </summary>
    public static IObservableAsync<T> Unique<T>(this IObservableAsync<T> source) =>
        source.DistinctUntilChanged();

    /// <summary>
    /// Suppresses adjacent duplicate values.
    /// </summary>
    public static IObservableAsync<T> Unique<T>(this IObservableAsync<T> source, IEqualityComparer<T> comparer) =>
        source.DistinctUntilChanged(comparer);

    /// <summary>
    /// Suppresses adjacent duplicate keys.
    /// </summary>
    public static IObservableAsync<T> UniqueBy<T, TKey>(
        this IObservableAsync<T> source,
        Func<T, TKey> keySelector) =>
        source.DistinctUntilChangedBy(keySelector);

    /// <summary>
    /// Suppresses adjacent duplicate keys.
    /// </summary>
    public static IObservableAsync<T> UniqueBy<T, TKey>(
        this IObservableAsync<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey> comparer) =>
        source.DistinctUntilChangedBy(keySelector, comparer);

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Chain<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Concat();

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Chain<T>(this IObservableAsync<T> first, IObservableAsync<T> second) =>
        first.Concat(second);

    /// <summary>
    /// Concatenates the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Chain<T>(params IObservableAsync<T>[] sources) =>
        sources.Concat();

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Blend<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Merge();

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Blend<T>(this IObservableAsync<T> first, IObservableAsync<T> second) =>
        first.Merge(second);

    /// <summary>
    /// Merges the supplied sources.
    /// </summary>
    public static IObservableAsync<T> Blend<T>(params IObservableAsync<T>[] sources) =>
        sources.Merge();

    /// <summary>
    /// Switches to the latest inner source.
    /// </summary>
    public static IObservableAsync<T> SwitchTo<T>(this IObservableAsync<IObservableAsync<T>> sources) =>
        sources.Switch();

    /// <summary>
    /// Combines paired values from two sources.
    /// </summary>
    public static IObservableAsync<TResult> Pair<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.Zip(right, selector);

    /// <summary>
    /// Combines the latest values from two sources.
    /// </summary>
    public static IObservableAsync<TResult> SyncLatest<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.CombineLatest(right, selector);

    /// <summary>
    /// Combines latest values from two sources.
    /// </summary>
    public static IObservableAsync<TResult> PairLatest<TLeft, TRight, TResult>(
        this IObservableAsync<TLeft> left,
        IObservableAsync<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        left.CombineLatest(right, selector);

    /// <summary>
    /// Retries the source up to the specified count.
    /// </summary>
    public static IObservableAsync<T> Reattempt<T>(this IObservableAsync<T> source, int retryCount) =>
        source.Retry(retryCount);

    /// <summary>
    /// Recovers from a terminal failure with a replacement sequence.
    /// </summary>
    public static IObservableAsync<T> Recover<T>(
        this IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler) =>
        source.Catch(handler);

    /// <summary>
    /// Recovers from a terminal failure with a replacement sequence.
    /// </summary>
    public static IObservableAsync<T> Rescue<T>(
        this IObservableAsync<T> source,
        Func<Exception, IObservableAsync<T>> handler) =>
        source.Catch(handler);

    /// <summary>
    /// Resumes with a fallback sequence after a terminal failure.
    /// </summary>
    public static IObservableAsync<T> Resume<T>(this IObservableAsync<T> source, IObservableAsync<T> fallback) =>
        source.Catch(_ => fallback);

    /// <summary>
    /// Delays source notifications.
    /// </summary>
    public static IObservableAsync<T> Shift<T>(this IObservableAsync<T> source, TimeSpan dueTime) =>
        source.Delay(dueTime);

    /// <summary>
    /// Fails if the source does not terminate before the timeout.
    /// </summary>
    public static IObservableAsync<T> Expire<T>(this IObservableAsync<T> source, TimeSpan dueTime) =>
        source.Timeout(dueTime);

    /// <summary>
    /// Prepends a value before the source.
    /// </summary>
    public static IObservableAsync<T> Lead<T>(this IObservableAsync<T> source, T value) =>
        source.Prepend(value);

    /// <summary>
    /// Collects all values into a list.
    /// </summary>
    public static ValueTask<List<T>> CollectListAsync<T>(this IObservableAsync<T> source) =>
        source.ToListAsync();

    /// <summary>
    /// Collects all values into an array.
    /// </summary>
    public static async ValueTask<T[]> CollectArrayAsync<T>(this IObservableAsync<T> source)
    {
        var values = await source.ToListAsync().ConfigureAwait(false);
        return [.. values];
    }
}
