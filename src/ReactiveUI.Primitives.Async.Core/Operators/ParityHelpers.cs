// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Signals;

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
public static partial class SignalAsyncExtensions
{
    /// <summary>Aggregation parity helper operators for a sequence of boolean observable source sequences.</summary>
    /// <param name="sources">The boolean source sequences.</param>
    extension(IEnumerable<IObservableAsync<bool>> sources)
    {
        /// <summary>Emits <see langword="true"/> when the latest value from every source sequence is <see langword="false"/>.</summary>
        /// <returns>A sequence of aggregate boolean states.</returns>
        public IObservableAsync<bool> CombineLatestValuesAreAllFalse()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            var materializedSources = (sources as IObservableAsync<bool>[]) ?? [.. sources];
            return materializedSources.Length == 0 ? new SignalAsync.ReturnSignalAsync<bool>(true) : new SyncLatestEnumerableSignal<bool, bool>(materializedSources, static values =>
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

        /// <summary>Emits <see langword="true"/> when the latest value from every source sequence is <see langword="true"/>.</summary>
        /// <returns>A sequence of aggregate boolean states.</returns>
        public IObservableAsync<bool> CombineLatestValuesAreAllTrue()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            var materializedSources = (sources as IObservableAsync<bool>[]) ?? [.. sources];
            return materializedSources.Length == 0 ? new SignalAsync.ReturnSignalAsync<bool>(true) : new SyncLatestEnumerableSignal<bool, bool>(materializedSources, static values =>
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
    }

    /// <summary>Flattening parity helper operators for an observable source sequence of enumerables.</summary>
    /// <param name="source">The source sequence of enumerables.</param>
    /// <typeparam name="T">The flattened element type.</typeparam>
    extension<T>(IObservableAsync<IEnumerable<T>> source)
    {
        /// <summary>Flattens each enumerable element emitted by the source into individual values.</summary>
        /// <returns>A flattened observable sequence.</returns>
        public IObservableAsync<T> ForEach()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ForEachEnumerableSignal<T>(source);
        }
    }

    /// <summary>Async-native parity helper operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Continues with an empty sequence when the source completes with a failure result.</summary>
        /// <returns>A sequence that suppresses terminal failures and completes instead.</returns>
        public IObservableAsync<T> CatchIgnore()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CatchSignal<T>(source, static _ => SignalAsync.EmptySignalAsync<T>.Instance, null);
        }

        /// <summary>Continues with an empty sequence when the source fails with the specified exception type.</summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="errorAction">The action to invoke when a matching exception is observed.</param>
        /// <returns>A sequence that suppresses matching terminal failures and completes instead.</returns>
        public IObservableAsync<T> CatchIgnore<TException>(Action<TException> errorAction)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(errorAction);

            return new CatchSignal<T>(
                source,
                ex =>
                {
                    if (ex is not TException typedException)
                    {
                        return new SignalAsync.ThrowSignalAsync<T>(ex);
                    }

                    errorAction(typedException);
                    return SignalAsync.EmptySignalAsync<T>.Instance;
                },
                null);
        }

        /// <summary>Continues with a fallback value when the source completes with a failure result.</summary>
        /// <param name="fallback">The fallback value to emit.</param>
        /// <returns>A sequence that emits either the original values or the fallback value on terminal failure.</returns>
        public IObservableAsync<T> CatchAndReturn(T fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CatchSignal<T>(source, _ => new SignalAsync.ReturnSignalAsync<T>(fallback), null);
        }

        /// <summary>Continues with a fallback value produced from a specific exception type.</summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="fallbackFactory">The fallback value factory.</param>
        /// <returns>A sequence that emits either the original values or a fallback value on matching terminal failure.</returns>
        public IObservableAsync<T> CatchAndReturn<TException>(Func<TException, T> fallbackFactory)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(fallbackFactory);

            return new CatchSignal<T>(
                source,
                ex =>
                    ex is TException typedException
                        ? new SignalAsync.ReturnSignalAsync<T>(fallbackFactory(typedException))
                        : new SignalAsync.ThrowSignalAsync<T>(ex),
                null);
        }

        /// <summary>Registers a side effect that runs when the source is subscribed.</summary>
        /// <param name="action">The action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        public IObservableAsync<T> DoOnSubscribe(Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(action);

            return new DoOnSubscribeSignal<T>(source, action);
        }

        /// <summary>Registers an asynchronous side effect that runs when the source is subscribed.</summary>
        /// <param name="action">The asynchronous action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        public IObservableAsync<T> DoOnSubscribe(Func<CancellationToken, ValueTask> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(action);

            return new DoOnSubscribeAsyncSignal<T>(source, action);
        }

        /// <summary>Drops source values while the previous asynchronous action is still running.</summary>
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

        /// <summary>Logs errors as they are observed without changing the source sequence.</summary>
        /// <param name="logger">The error logger.</param>
        /// <returns>The original sequence with error side effects attached.</returns>
        public IObservableAsync<T> LogErrors(Action<Exception> logger)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(logger);

            return new LogErrorsSignal<T>(source, logger);
        }

        /// <summary>Emits the first value that satisfies the predicate and then completes.</summary>
        /// <param name="predicate">The predicate to satisfy.</param>
        /// <returns>A sequence containing the first matching value.</returns>
        public IObservableAsync<T> WaitUntil(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new WaitUntilSignal<T>(source, predicate);
        }

        /// <summary>Emits adjacent pairs from the source sequence.</summary>
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

            PartitionCoordinator<T> coordinator = new(source, predicate);
            return (coordinator.TrueBranch, coordinator.FalseBranch);
        }

        /// <summary>Replays the last observed value to new subscribers, starting with the provided initial value.</summary>
        /// <param name="initialValue">The initial replay value.</param>
        /// <returns>A replaying shared observable sequence.</returns>
        public IObservableAsync<T> ReplayLastOnSubscribe(T initialValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new RefCountSignal<T>(
                new ConnectableSignalAsync<T>(
                    source,
                    new SerialReplayLatestSignalAsync<T>(new(initialValue))));
        }

        /// <summary>Emits only the first value in each throttle window and suppresses duplicates before and after throttling.</summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(throttle, TimeSpan.Zero);

            return new ThrottleDistinctSignal<T>(source, throttle, TimeProvider.System);
        }

        /// <summary>Emits only the first value in each throttle window and suppresses duplicates before and after throttling.</summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <param name="timeProvider">The optional time provider.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle, TimeProvider? timeProvider)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(throttle, TimeSpan.Zero);

            return new ThrottleDistinctSignal<T>(source, throttle, timeProvider ?? TimeProvider.System);
        }

        /// <summary>Performs a scan that emits the initial accumulator value before processing the source sequence.</summary>
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

        /// <summary>Performs an asynchronous scan that emits the initial accumulator value before processing the source sequence.</summary>
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

        /// <summary>Delays values until the condition becomes true immediately or the debounce interval elapses.</summary>
        /// <param name="debounce">The debounce interval.</param>
        /// <param name="condition">The condition that bypasses the delay when true.</param>
        /// <returns>A debounced observable sequence.</returns>
        public IObservableAsync<T> DebounceUntil(
            TimeSpan debounce,
            Func<T, bool> condition)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(condition);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(debounce, TimeSpan.Zero);

            return new DebounceUntilSignal<T>(source, debounce, condition, TimeProvider.System);
        }

        /// <summary>Delays values until the condition becomes true immediately or the debounce interval elapses.</summary>
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
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(debounce, TimeSpan.Zero);

            return new DebounceUntilSignal<T>(source, debounce, condition, timeProvider ?? TimeProvider.System);
        }
    }

    /// <summary>Aggregation parity helper operators for a struct-valued observable source sequence.</summary>
    /// <param name="source">The first source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservableAsync<T> source)
        where T : struct
    {
        /// <summary>Returns the minimum of the latest values from the supplied source sequences.</summary>
        /// <param name="sources">Additional source sequences.</param>
        /// <returns>A sequence that emits the minimum of the latest values.</returns>
        public IObservableAsync<T> GetMin(params IObservableAsync<T>[] sources)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(sources);

            var allSources = new IObservableAsync<T>[sources.Length + 1];
            allSources[0] = source;
            sources.CopyTo(allSources, 1);
            return new SyncLatestEnumerableSignal<T, T>(allSources, static values =>
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

        /// <summary>Returns the maximum of the latest values from the supplied source sequences.</summary>
        /// <param name="sources">Additional source sequences.</param>
        /// <returns>A sequence that emits the maximum of the latest values.</returns>
        public IObservableAsync<T> GetMax(params IObservableAsync<T>[] sources)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(sources);

            var allSources = new IObservableAsync<T>[sources.Length + 1];
            allSources[0] = source;
            sources.CopyTo(allSources, 1);
            return new SyncLatestEnumerableSignal<T, T>(allSources, static values =>
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
    }

    /// <summary>Null-filtering parity helper operators for a nullable reference-type observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The reference type element.</typeparam>
    extension<T>(IObservableAsync<T?> source)
        where T : class
    {
        /// <summary>Skips <see langword="null"/> values until the first non-null value appears.</summary>
        /// <returns>A sequence that starts with the first non-null value.</returns>
        public IObservableAsync<T> SkipWhileNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new SkipWhileNullSignal<T>(source);
        }

        /// <summary>Filters the source sequence to non-null values and narrows the result type accordingly.</summary>
        /// <returns>A non-nullable observable sequence.</returns>
        public IObservableAsync<T> WhereIsNotNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new WhereIsNotNullSignal<T>(source);
        }

    }

    /// <summary>Boolean parity helper operators for a boolean observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<bool> source)
    {
        /// <summary>Negates each boolean value emitted by the source sequence.</summary>
        /// <returns>A sequence of negated boolean values.</returns>
        public IObservableAsync<bool> Not()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new NotSignal(source);
        }

        /// <summary>Filters the source sequence to <see langword="false"/> values.</summary>
        /// <returns>A sequence containing only <see langword="false"/> values.</returns>
        public IObservableAsync<bool> WhereFalse()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new WhereFalseSignal(source);
        }

        /// <summary>Filters the source sequence to <see langword="true"/> values.</summary>
        /// <returns>A sequence containing only <see langword="true"/> values.</returns>
        public IObservableAsync<bool> WhereTrue()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new WhereTrueSignal(source);
        }
    }
}
