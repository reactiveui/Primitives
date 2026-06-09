// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Primitives-vocabulary alias operators for the async observable surface.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Primitives-vocabulary operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The element type.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an async observable as an async signal.</summary>
        /// <returns>An observable sequence validated.</returns>
        public IObservableAsync<T> ToAsyncSignal() =>
            source ?? throw new ArgumentNullException(nameof(source));

        /// <summary>Projects each value into a new value.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The projection applied to each value.</param>
        /// <returns>An observable sequence of projected values.</returns>
        public IObservableAsync<TResult> Map<TResult>(Func<T, TResult> selector) =>
            source.Select(selector);

        /// <summary>Projects each value into a new value asynchronously.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The asynchronous projection applied to each value.</param>
        /// <returns>An observable sequence of projected values.</returns>
        public IObservableAsync<TResult> Map<TResult>(Func<T, CancellationToken, ValueTask<TResult>> selector) =>
            source.Select(selector);

        /// <summary>Projects each value using caller-supplied state.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the selector.</param>
        /// <param name="selector">The projection applied to each value and the state.</param>
        /// <returns>An observable sequence of projected values.</returns>
        public IObservableAsync<TResult> MapWith<TState, TResult>(
            TState state,
            Func<TState, T, TResult> selector)
        {
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            return source.Map(value => selector(state, value));
        }

        /// <summary>Keeps values that satisfy a predicate.</summary>
        /// <param name="predicate">The predicate that values must satisfy.</param>
        /// <returns>An observable sequence of values that satisfy the predicate.</returns>
        public IObservableAsync<T> Keep(Func<T, bool> predicate) =>
            source.Where(predicate);

        /// <summary>Keeps values that satisfy an asynchronous predicate.</summary>
        /// <param name="predicate">The asynchronous predicate that values must satisfy.</param>
        /// <returns>An observable sequence of values that satisfy the predicate.</returns>
        public IObservableAsync<T> Keep(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            source.Where(predicate);

        /// <summary>Keeps values that satisfy a stateful predicate.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the predicate.</param>
        /// <param name="predicate">The predicate that values and the state must satisfy.</param>
        /// <returns>An observable sequence of values that satisfy the predicate.</returns>
        public IObservableAsync<T> KeepWith<TState>(
            TState state,
            Func<TState, T, bool> predicate)
        {
            if (predicate is null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }

            return source.Keep(value => predicate(state, value));
        }

        /// <summary>Invokes an action for each value while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(Action<T> onNext) =>
            source.Do(onNext, null, null);

        /// <summary>Invokes asynchronous side effects while preserving the source values.</summary>
        /// <param name="onNext">The asynchronous action invoked for each value.</param>
        /// <param name="onErrorResume">The asynchronous action invoked on a resumable error.</param>
        /// <param name="onCompleted">The asynchronous action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(
            Func<T, CancellationToken, ValueTask>? onNext,
            Func<Exception, CancellationToken, ValueTask>? onErrorResume,
            Func<Result, ValueTask>? onCompleted) =>
            source.Do(onNext, onErrorResume, onCompleted);

        /// <summary>Invokes side effects while preserving the source values.</summary>
        /// <param name="onNext">The action invoked for each value.</param>
        /// <param name="onError">The action invoked on an error.</param>
        /// <param name="onCompleted">The action invoked on completion.</param>
        /// <returns>An observable sequence identical to the source.</returns>
        public IObservableAsync<T> Tap(
            Action<T> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            source.Do(onNext, onError, _ => onCompleted());

        /// <summary>Emits the accumulated state after each source value.</summary>
        /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
        /// <param name="seed">The initial accumulator state.</param>
        /// <param name="accumulator">The accumulator applied to each value.</param>
        /// <returns>An observable sequence of accumulated states.</returns>
        public IObservableAsync<TAccumulate> Fold<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            source.Scan(seed, accumulator);

        /// <summary>Emits the accumulated state after each source value.</summary>
        /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
        /// <param name="seed">The initial accumulator state.</param>
        /// <param name="accumulator">The asynchronous accumulator applied to each value.</param>
        /// <returns>An observable sequence of accumulated states.</returns>
        public IObservableAsync<TAccumulate> Fold<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, CancellationToken, ValueTask<TAccumulate>> accumulator) =>
            source.Scan(seed, accumulator);

        /// <summary>Emits the final accumulated state as a task.</summary>
        /// <typeparam name="TAccumulate">The accumulator state type.</typeparam>
        /// <param name="seed">The initial accumulator state.</param>
        /// <param name="accumulator">The accumulator applied to each value.</param>
        /// <returns>A task that completes with the final accumulated state.</returns>
        public ValueTask<TAccumulate> ReduceAsync<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            source.AggregateAsync(seed, accumulator);

        /// <summary>Projects and merges inner async observable sequences.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The projection producing an inner sequence for each value.</param>
        /// <returns>An observable sequence of merged inner values.</returns>
        public IObservableAsync<TResult> Bind<TResult>(Func<T, IObservableAsync<TResult>> selector) =>
            source.SelectMany(selector);

        /// <summary>Projects and merges inner async observable sequences.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The projection producing an inner sequence for each value.</param>
        /// <returns>An observable sequence of merged inner values.</returns>
        public IObservableAsync<TResult> FlatMap<TResult>(Func<T, IObservableAsync<TResult>> selector) =>
            source.SelectMany(selector);

        /// <summary>Projects and merges inner async observable sequences.</summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="selector">The asynchronous projection producing an inner sequence for each value.</param>
        /// <returns>An observable sequence of merged inner values.</returns>
        public IObservableAsync<TResult> FlatMap<TResult>(Func<T, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector) =>
            source.SelectMany(selector);

        /// <summary>Suppresses adjacent duplicate values.</summary>
        /// <returns>An observable sequence without adjacent duplicates.</returns>
        public IObservableAsync<T> Unique() =>
            source.DistinctUntilChanged();

        /// <summary>Suppresses adjacent duplicate values.</summary>
        /// <param name="comparer">The comparer used to detect duplicates.</param>
        /// <returns>An observable sequence without adjacent duplicates.</returns>
        public IObservableAsync<T> Unique(IEqualityComparer<T> comparer) =>
            source.DistinctUntilChanged(comparer);

        /// <summary>Suppresses adjacent duplicate keys.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The selector that extracts the comparison key.</param>
        /// <returns>An observable sequence without adjacent duplicate keys.</returns>
        public IObservableAsync<T> UniqueBy<TKey>(Func<T, TKey> keySelector) =>
            source.DistinctUntilChangedBy(keySelector);

        /// <summary>Suppresses adjacent duplicate keys.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The selector that extracts the comparison key.</param>
        /// <param name="comparer">The comparer used to compare keys.</param>
        /// <returns>An observable sequence without adjacent duplicate keys.</returns>
        public IObservableAsync<T> UniqueBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer) =>
            source.DistinctUntilChangedBy(keySelector, comparer);

        /// <summary>Concatenates the supplied sources.</summary>
        /// <param name="second">The second source sequence.</param>
        /// <returns>An observable sequence that concatenates the sources.</returns>
        public IObservableAsync<T> Chain(IObservableAsync<T> second) =>
            source.Concat(second);

        /// <summary>Merges the supplied sources.</summary>
        /// <param name="second">The second source sequence.</param>
        /// <returns>An observable sequence that merges the sources.</returns>
        public IObservableAsync<T> Blend(IObservableAsync<T> second) =>
            source.Merge(second);

        /// <summary>Combines paired values from two sources.</summary>
        /// <typeparam name="TRight">The right element type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="right">The right source sequence.</param>
        /// <param name="selector">The function combining each pair of values.</param>
        /// <returns>An observable sequence of combined values.</returns>
        public IObservableAsync<TResult> Pair<TRight, TResult>(
            IObservableAsync<TRight> right,
            Func<T, TRight, TResult> selector) =>
            source.Zip(right, selector);

        /// <summary>Combines the latest values from two sources.</summary>
        /// <typeparam name="TRight">The right element type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="right">The right source sequence.</param>
        /// <param name="selector">The function combining the latest values.</param>
        /// <returns>An observable sequence of combined values.</returns>
        public IObservableAsync<TResult> SyncLatest<TRight, TResult>(
            IObservableAsync<TRight> right,
            Func<T, TRight, TResult> selector) =>
            source.CombineLatest(right, selector);

        /// <summary>Combines latest values from two sources.</summary>
        /// <typeparam name="TRight">The right element type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="right">The right source sequence.</param>
        /// <param name="selector">The function combining the latest values.</param>
        /// <returns>An observable sequence of combined values.</returns>
        public IObservableAsync<TResult> PairLatest<TRight, TResult>(
            IObservableAsync<TRight> right,
            Func<T, TRight, TResult> selector) =>
            source.CombineLatest(right, selector);

        /// <summary>Retries the source up to the specified count.</summary>
        /// <param name="retryCount">The maximum number of retry attempts.</param>
        /// <returns>An observable sequence that retries on failure.</returns>
        public IObservableAsync<T> Reattempt(int retryCount) =>
            source.Retry(retryCount);

        /// <summary>Recovers from a terminal failure with a replacement sequence.</summary>
        /// <param name="handler">The handler that produces a replacement sequence from the error.</param>
        /// <returns>An observable sequence that recovers from failures.</returns>
        public IObservableAsync<T> Recover(Func<Exception, IObservableAsync<T>> handler) =>
            source.Catch(handler);

        /// <summary>Recovers from a terminal failure with a replacement sequence.</summary>
        /// <param name="handler">The handler that produces a replacement sequence from the error.</param>
        /// <returns>An observable sequence that recovers from failures.</returns>
        public IObservableAsync<T> Rescue(Func<Exception, IObservableAsync<T>> handler) =>
            source.Catch(handler);

        /// <summary>Resumes with a fallback sequence after a terminal failure.</summary>
        /// <param name="fallback">The fallback sequence used after a failure.</param>
        /// <returns>An observable sequence that resumes with the fallback on failure.</returns>
        public IObservableAsync<T> Resume(IObservableAsync<T> fallback) =>
            source.Catch(_ => fallback);

        /// <summary>Delays source notifications.</summary>
        /// <param name="dueTime">The delay applied to notifications.</param>
        /// <returns>An observable sequence with delayed notifications.</returns>
        public IObservableAsync<T> Shift(TimeSpan dueTime) =>
            source.Delay(dueTime);

        /// <summary>Fails if the source does not terminate before the timeout.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <returns>An observable sequence that fails on timeout.</returns>
        public IObservableAsync<T> Expire(TimeSpan dueTime) =>
            source.Timeout(dueTime);

        /// <summary>Prepends a value before the source.</summary>
        /// <param name="value">The value to prepend.</param>
        /// <returns>An observable sequence that emits the value before the source.</returns>
        public IObservableAsync<T> Lead(T value) =>
            source.Prepend(value);

        /// <summary>Collects all values into a list.</summary>
        /// <returns>A task that completes with the collected list of values.</returns>
        public ValueTask<List<T>> CollectListAsync() =>
            source.ToListAsync();

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A task that completes with the collected array of values.</returns>
        public async ValueTask<T[]> CollectArrayAsync()
        {
            var values = await source.ToListAsync().ConfigureAwait(false);
            return [.. values];
        }
    }

    /// <summary>Type-based primitives operators for an untyped observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<object?> source)
    {
        /// <summary>Keeps values assignable to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to keep.</typeparam>
        /// <returns>An observable sequence of values assignable to <typeparamref name="TResult"/>.</returns>
        [SuppressMessage(
            "Minor Code Smell",
            "S4018:All type parameters should be used in the parameter list to enable type inference",
            Justification = "Deliberate lack of type inference.")]
        public IObservableAsync<TResult> KeepType<TResult>()
            where TResult : class =>
            OfType<object?, TResult>(source);

        /// <summary>Casts each value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to cast to.</typeparam>
        /// <returns>An observable sequence of values cast to <typeparamref name="TResult"/>.</returns>
        [SuppressMessage(
            "Minor Code Smell",
            "S4018:All type parameters should be used in the parameter list to enable type inference",
            Justification = "Deliberate lack of type inference.")]
        public IObservableAsync<TResult> CastTo<TResult>() =>
            Cast<object?, TResult>(source);
    }

    /// <summary>Combining operators for a sequence of observable sources.</summary>
    /// <param name="sources">The sequence of inner sources.</param>
    /// <typeparam name="T">The element type.</typeparam>
    extension<T>(IObservableAsync<IObservableAsync<T>> sources)
    {
        /// <summary>Concatenates the supplied sources.</summary>
        /// <returns>An observable sequence that concatenates the sources.</returns>
        public IObservableAsync<T> Chain() =>
            sources.Concat();

        /// <summary>Merges the supplied sources.</summary>
        /// <returns>An observable sequence that merges the sources.</returns>
        public IObservableAsync<T> Blend() =>
            sources.Merge();

        /// <summary>Switches to the latest inner source.</summary>
        /// <returns>An observable sequence that emits values from the latest inner source.</returns>
        public IObservableAsync<T> SwitchTo() =>
            sources.Switch();
    }

    /// <summary>Null-filtering operators for an observable source sequence of reference values.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The element type.</typeparam>
    extension<T>(IObservableAsync<T?> source)
        where T : class
    {
        /// <summary>Keeps non-null reference values.</summary>
        /// <returns>An observable sequence of non-null values.</returns>
        public IObservableAsync<T> KeepNotNull() =>
            source.WhereIsNotNull();
    }
}
