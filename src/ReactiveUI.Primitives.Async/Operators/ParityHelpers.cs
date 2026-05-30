// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides async-native counterparts for high-value helper operators exposed by the synchronous reactive surface in
/// this repository.
/// </summary>
/// <remarks>These members intentionally compose existing async operators from this namespace so parity is achieved via
/// the library's own async primitives instead of by delegating to System.Reactive implementations.</remarks>
[SuppressMessage(
    "StyleCop.CSharp.OrderingRules",
    "SA1201:ElementsShouldAppearInTheCorrectOrder",
    Justification = "C# 14 extension methods")]
public static partial class SignalAsync
{
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Converts the source sequence into a signal sequence that emits <see cref="RxVoid.Default"/> for each source value.
        /// </summary>
        /// <returns>An observable sequence of <see cref="RxVoid"/> values.</returns>
        public IObservableAsync<RxVoid> AsSignal()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AsRxVoidSignal<T>(source);
        }

        /// <summary>
        /// Continues with an empty sequence when the source completes with a failure result.
        /// </summary>
        /// <returns>A sequence that suppresses terminal failures and completes instead.</returns>
        public IObservableAsync<T> CatchIgnore()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.Catch(static _ => Empty<T>());
        }

        /// <summary>
        /// Continues with an empty sequence when the source fails with the specified exception type.
        /// </summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="errorAction">The action to invoke when a matching exception is observed.</param>
        /// <returns>A sequence that suppresses matching terminal failures and completes instead.</returns>
        public IObservableAsync<T> CatchIgnore<TException>(Action<TException> errorAction)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(errorAction);

            return source.Catch(ex =>
            {
                if (ex is TException typedException)
                {
                    errorAction(typedException);
                    return Empty<T>();
                }

                return Throw<T>(ex);
            });
        }

        /// <summary>
        /// Continues with a fallback value when the source completes with a failure result.
        /// </summary>
        /// <param name="fallback">The fallback value to emit.</param>
        /// <returns>A sequence that emits either the original values or the fallback value on terminal failure.</returns>
        public IObservableAsync<T> CatchAndReturn(T fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.Catch(_ => Return(fallback));
        }

        /// <summary>
        /// Continues with a fallback value produced from a specific exception type.
        /// </summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="fallbackFactory">The fallback value factory.</param>
        /// <returns>A sequence that emits either the original values or a fallback value on matching terminal failure.</returns>
        public IObservableAsync<T> CatchAndReturn<TException>(Func<TException, T> fallbackFactory)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(fallbackFactory);

            return source.Catch(ex =>
                ex is TException typedException ? Return(fallbackFactory(typedException)) : Throw<T>(ex));
        }

        /// <summary>
        /// Registers a side effect that runs when the source is subscribed.
        /// </summary>
        /// <param name="action">The action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        public IObservableAsync<T> DoOnSubscribe(Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(action);

            return Create<T>((observer, cancellationToken) =>
            {
                action();
                return source.SubscribeAsync(observer.Wrap(), cancellationToken);
            });
        }

        /// <summary>
        /// Registers an asynchronous side effect that runs when the source is subscribed.
        /// </summary>
        /// <param name="action">The asynchronous action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        public IObservableAsync<T> DoOnSubscribe(Func<CancellationToken, ValueTask> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(action);

            return Create<T>(async (observer, cancellationToken) =>
            {
                await action(cancellationToken).ConfigureAwait(false);
                return await source.SubscribeAsync(observer.Wrap(), cancellationToken).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Drops source values while the previous asynchronous action is still running.
        /// </summary>
        /// <param name="asyncAction">The asynchronous action to execute for accepted values.</param>
        /// <returns>A sequence that emits only values that were accepted while the operator was idle.</returns>
        public IObservableAsync<T> DropIfBusy(Func<T, CancellationToken, ValueTask> asyncAction)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(asyncAction);

            return new DropIfBusySignal<T>(source, asyncAction);
        }

        /// <summary>
        /// Emits the latest value or the provided default value before the source produces its first value.
        /// If the first source value equals <paramref name="defaultValue"/>, it will be suppressed by the
        /// distinct-until-changed filter.
        /// </summary>
        /// <param name="defaultValue">The default value to emit first.</param>
        /// <returns>A sequence that starts with the provided default value and then emits distinct source updates.</returns>
        public IObservableAsync<T> LatestOrDefault(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new LatestOrDefaultSignal<T>(source, defaultValue);
        }

        /// <summary>
        /// Logs errors as they are observed without changing the source sequence.
        /// </summary>
        /// <param name="logger">The error logger.</param>
        /// <returns>The original sequence with error side effects attached.</returns>
        public IObservableAsync<T> LogErrors(Action<Exception> logger)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(logger);

            return Create<T>((observer, subscribeToken) => source.SubscribeAsync(
                observer.OnNextAsync,
                (exception, token) =>
                {
                    logger(exception);
                    return observer.OnErrorResumeAsync(exception, token);
                },
                observer.OnCompletedAsync,
                subscribeToken));
        }

        /// <summary>
        /// Emits the first value that satisfies the predicate and then completes.
        /// </summary>
        /// <param name="predicate">The predicate to satisfy.</param>
        /// <returns>A sequence containing the first matching value.</returns>
        public IObservableAsync<T> WaitUntil(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new WaitUntilSignal<T>(source, predicate);
        }

        /// <summary>
        /// Uses ObserveOn only when a context is provided.
        /// </summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext) => source.ObserveOnSafe(asyncContext, false);

        /// <summary>
        /// Uses ObserveOn only when a context is provided.
        /// </summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return asyncContext is null ? source : source.ObserveOn(asyncContext, forceYielding);
        }

        /// <summary>
        /// Uses ObserveOn only when a scheduler is provided.
        /// </summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler) => source.ObserveOnSafe(taskScheduler, false);

        /// <summary>
        /// Uses ObserveOn only when a scheduler is provided.
        /// </summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return taskScheduler is null ? source : source.ObserveOn(taskScheduler, forceYielding);
        }

        /// <summary>
        /// Observes the source on the provided context only when the condition is true.
        /// </summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext) => source.ObserveOnIf(condition, asyncContext, false);

        /// <summary>
        /// Observes the source on the provided context only when the condition is true.
        /// </summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(asyncContext);

            return condition ? source.ObserveOn(asyncContext, forceYielding) : source;
        }

        /// <summary>
        /// Observes the source on the provided scheduler only when the condition is true.
        /// </summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler) => source.ObserveOnIf(condition, taskScheduler, false);

        /// <summary>
        /// Observes the source on the provided scheduler only when the condition is true.
        /// </summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(taskScheduler);

            return condition ? source.ObserveOn(taskScheduler, forceYielding) : source;
        }

        /// <summary>
        /// Emits adjacent pairs from the source sequence.
        /// </summary>
        /// <returns>A sequence of adjacent (previous, current) pairs.</returns>
        public IObservableAsync<(T Previous, T Current)> Pairwise()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new PairwiseSignal<T>(source);
        }

        /// <summary>
        /// Partitions the source sequence into values that satisfy the predicate and values that do not.
        /// The predicate is evaluated exactly once per element.
        /// </summary>
        /// <param name="predicate">The partition predicate.</param>
        /// <returns>A tuple of true and false partitions.</returns>
        public (IObservableAsync<T> True, IObservableAsync<T> False) Partition(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            var coordinator = new PartitionCoordinator<T>(source, predicate);
            return (coordinator.TrueBranch, coordinator.FalseBranch);
        }

        /// <summary>
        /// Replays the last observed value to new subscribers, starting with the provided initial value.
        /// </summary>
        /// <param name="initialValue">The initial replay value.</param>
        /// <returns>A replaying shared observable sequence.</returns>
        public IObservableAsync<T> ReplayLastOnSubscribe(T initialValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.Publish(initialValue).RefCount();
        }

        /// <summary>
        /// Emits only the first value in each throttle window and suppresses duplicates before and after throttling.
        /// </summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle) => source.ThrottleDistinct(throttle, null);

        /// <summary>
        /// Emits only the first value in each throttle window and suppresses duplicates before and after throttling.
        /// </summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <param name="timeProvider">The optional time provider.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle, TimeProvider? timeProvider)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
#if NET8_0_OR_GREATER
            ArgumentOutOfRangeException.ThrowIfLessThan(throttle, TimeSpan.Zero);
#else
            if (throttle < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(throttle));
            }
#endif

            return new ThrottleDistinctSignal<T>(source, throttle, timeProvider ?? TimeProvider.System);
        }

        /// <summary>
        /// Performs a scan that emits the initial accumulator value before processing the source sequence.
        /// </summary>
        /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
        /// <param name="initial">The initial accumulator value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        /// <returns>A sequence beginning with the initial accumulator value followed by each intermediate accumulated value.</returns>
        public IObservableAsync<TAccumulate> ScanWithInitial<TAccumulate>(
            TAccumulate initial,
            Func<TAccumulate, T, TAccumulate> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ScanWithInitialSignal<T, TAccumulate>(source, initial, accumulator);
        }

        /// <summary>
        /// Performs an asynchronous scan that emits the initial accumulator value before processing the source sequence.
        /// </summary>
        /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
        /// <param name="initial">The initial accumulator value.</param>
        /// <param name="accumulator">The asynchronous accumulator function.</param>
        /// <returns>A sequence beginning with the initial accumulator value followed by each intermediate accumulated value.</returns>
        public IObservableAsync<TAccumulate> ScanWithInitial<TAccumulate>(
            TAccumulate initial,
            Func<TAccumulate, T, CancellationToken, ValueTask<TAccumulate>> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ScanWithInitialAsyncSignal<T, TAccumulate>(source, initial, accumulator);
        }

        /// <summary>
        /// Delays values until the condition becomes true immediately or the debounce interval elapses.
        /// </summary>
        /// <param name="debounce">The debounce interval.</param>
        /// <param name="condition">The condition that bypasses the delay when true.</param>
        /// <returns>A debounced observable sequence.</returns>
        public IObservableAsync<T> DebounceUntil(
            TimeSpan debounce,
            Func<T, bool> condition) => source.DebounceUntil(debounce, condition, null);

        /// <summary>
        /// Delays values until the condition becomes true immediately or the debounce interval elapses.
        /// </summary>
        /// <param name="debounce">The debounce interval.</param>
        /// <param name="condition">The condition that bypasses the delay when true.</param>
        /// <param name="timeProvider">The optional time provider.</param>
        /// <returns>A debounced observable sequence.</returns>
        public IObservableAsync<T> DebounceUntil(
            TimeSpan debounce,
            Func<T, bool> condition,
            TimeProvider? timeProvider)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(condition);
#if NET8_0_OR_GREATER
            ArgumentOutOfRangeException.ThrowIfLessThan(debounce, TimeSpan.Zero);
#else
            if (debounce < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(debounce));
            }
#endif

            return new DebounceUntilSignal<T>(source, debounce, condition, timeProvider ?? TimeProvider.System);
        }
    }

    /// <summary>
    /// Returns the minimum of the latest values from the supplied source sequences.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    /// <param name="sources">Additional source sequences.</param>
    /// <returns>A sequence that emits the minimum of the latest values.</returns>
    public static IObservableAsync<T> GetMin<T>(this IObservableAsync<T> source, params IObservableAsync<T>[] sources)
        where T : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sources);

        var allSources = new IObservableAsync<T>[sources.Length + 1];
        allSources[0] = source;
        sources.CopyTo(allSources, 1);
        return allSources.CombineLatest(static values =>
        {
            var min = values[0];
            for (var i = 1; i < values.Count; i++)
            {
                if (Comparer<T>.Default.Compare(values[i], min) < 0)
                {
                    min = values[i];
                }
            }

            return min;
        });
    }

    /// <summary>
    /// Returns the maximum of the latest values from the supplied source sequences.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    /// <param name="sources">Additional source sequences.</param>
    /// <returns>A sequence that emits the maximum of the latest values.</returns>
    public static IObservableAsync<T> GetMax<T>(this IObservableAsync<T> source, params IObservableAsync<T>[] sources)
        where T : struct
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sources);

        var allSources = new IObservableAsync<T>[sources.Length + 1];
        allSources[0] = source;
        sources.CopyTo(allSources, 1);
        return allSources.CombineLatest(static values =>
        {
            var max = values[0];
            for (var i = 1; i < values.Count; i++)
            {
                if (Comparer<T>.Default.Compare(values[i], max) > 0)
                {
                    max = values[i];
                }
            }

            return max;
        });
    }

    /// <summary>
    /// Emits <see langword="true"/> when the latest value from every source sequence is <see langword="false"/>.
    /// </summary>
    /// <param name="sources">The boolean source sequences.</param>
    /// <returns>A sequence of aggregate boolean states.</returns>
    public static IObservableAsync<bool> CombineLatestValuesAreAllFalse(
        this IEnumerable<IObservableAsync<bool>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        var materializedSources = sources as IObservableAsync<bool>[] ?? [.. sources];
        return materializedSources.Length == 0
            ? Return(true)
            : materializedSources.CombineLatest(static values =>
            {
                for (var i = 0; i < values.Count; i++)
                {
                    if (values[i])
                    {
                        return false;
                    }
                }

                return true;
            });
    }

    /// <summary>
    /// Emits <see langword="true"/> when the latest value from every source sequence is <see langword="true"/>.
    /// </summary>
    /// <param name="sources">The boolean source sequences.</param>
    /// <returns>A sequence of aggregate boolean states.</returns>
    public static IObservableAsync<bool> CombineLatestValuesAreAllTrue(this IEnumerable<IObservableAsync<bool>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        var materializedSources = sources as IObservableAsync<bool>[] ?? [.. sources];
        return materializedSources.Length == 0
            ? Return(true)
            : materializedSources.CombineLatest(static values =>
            {
                for (var i = 0; i < values.Count; i++)
                {
                    if (!values[i])
                    {
                        return false;
                    }
                }

                return true;
            });
    }

    /// <summary>
    /// Flattens each enumerable element emitted by the source into individual values.
    /// </summary>
    /// <typeparam name="T">The flattened element type.</typeparam>
    /// <param name="source">The source sequence of enumerables.</param>
    /// <returns>A flattened observable sequence.</returns>
    public static IObservableAsync<T> ForEach<T>(this IObservableAsync<IEnumerable<T>> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new ForEachEnumerableSignal<T>(source);
    }

    /// <summary>
    /// Negates each boolean value emitted by the source sequence.
    /// </summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence of negated boolean values.</returns>
    public static IObservableAsync<bool> Not(this IObservableAsync<bool> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new NotSignal(source);
    }

    /// <summary>
    /// Skips <see langword="null"/> values until the first non-null value appears.
    /// </summary>
    /// <typeparam name="T">The reference type element.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence that starts with the first non-null value.</returns>
    public static IObservableAsync<T> SkipWhileNull<T>(this IObservableAsync<T?> source)
        where T : class
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new SkipWhileNullSignal<T>(source);
    }

    /// <summary>
    /// Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <returns>An observable sequence that completes after the action has run.</returns>
    public static IObservableAsync<RxVoid> Start(Action action) => Start(action, null);

    /// <summary>
    /// Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="taskScheduler">An optional scheduler used to start the action.</param>
    /// <returns>An observable sequence that completes after the action has run.</returns>
    public static IObservableAsync<RxVoid> Start(Action action, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return taskScheduler is null
            ? FromAsync(_ =>
            {
                action();
                return default;
            })
            : CreateAsBackgroundJob<RxVoid>(
                async (observer, cancellationToken) =>
                {
                    action();
                    await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                },
                taskScheduler);
    }

    /// <summary>
    /// Creates an observable sequence that executes the supplied function and emits its result.
    /// </summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function) => Start(function, null);

    /// <summary>
    /// Creates an observable sequence that executes the supplied function and emits its result.
    /// </summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <param name="taskScheduler">An optional scheduler used to start the function.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(function);

        return taskScheduler is null
            ? FromAsync(_ => new ValueTask<TResult>(function()))
            : CreateAsBackgroundJob<TResult>(
                async (observer, cancellationToken) =>
                {
                    await observer.OnNextAsync(function(), cancellationToken).ConfigureAwait(false);
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                },
                taskScheduler);
    }

    /// <summary>
    /// Filters the source sequence to <see langword="false"/> values.
    /// </summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing only <see langword="false"/> values.</returns>
    public static IObservableAsync<bool> WhereFalse(this IObservableAsync<bool> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new WhereFalseSignal(source);
    }

    /// <summary>
    /// Filters the source sequence to non-null values and narrows the result type accordingly.
    /// </summary>
    /// <typeparam name="T">The reference type element.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A non-nullable observable sequence.</returns>
    public static IObservableAsync<T> WhereIsNotNull<T>(this IObservableAsync<T?> source)
        where T : class
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new WhereIsNotNullSignal<T>(source);
    }

    /// <summary>
    /// Filters the source sequence to <see langword="true"/> values.
    /// </summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>A sequence containing only <see langword="true"/> values.</returns>
    public static IObservableAsync<bool> WhereTrue(this IObservableAsync<bool> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new WhereTrueSignal(source);
    }
}
