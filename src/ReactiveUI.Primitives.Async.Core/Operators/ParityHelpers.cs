// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides async-native counterparts for the helper operators on the synchronous reactive surface.</summary>
[SuppressMessage(
    "StyleCop.CSharp.OrderingRules",
    "SA1201:ElementsShouldAppearInTheCorrectOrder",
    Justification = "Extension blocks group members by receiver rather than by element kind.")]
public static partial class SignalAsyncExtensions
{
    /// <summary>Aggregation parity helper operators for a sequence of boolean observable source sequences.</summary>
    /// <param name="sources">The boolean source sequences.</param>
    extension(IEnumerable<IObservableAsync<bool>> sources)
    {
        /// <summary>Emits <see langword="true"/> when the latest value from every source sequence is <see langword="false"/>.</summary>
        /// <returns>A sequence of aggregate boolean states.</returns>
        /// <remarks>An empty <paramref name="sources"/> sequence emits a single <see langword="true"/>; otherwise nothing is
        /// emitted until every source has produced a value.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<bool> CombineLatestValuesAreAllFalse() =>
            SyncLatestCoordinator.CreateAllFalse(sources);

        /// <summary>Emits <see langword="true"/> when the latest value from every source sequence is <see langword="true"/>.</summary>
        /// <returns>A sequence of aggregate boolean states.</returns>
        /// <remarks>An empty <paramref name="sources"/> sequence emits a single <see langword="true"/>; otherwise nothing is
        /// emitted until every source has produced a value.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<bool> CombineLatestValuesAreAllTrue() =>
            SyncLatestCoordinator.CreateAllTrue(sources);
    }

    /// <summary>Flattening parity helper operators for an observable source sequence of enumerables.</summary>
    /// <typeparam name="T">The flattened element type.</typeparam>
    /// <param name="source">The source sequence of enumerables.</param>
    extension<T>(IObservableAsync<IEnumerable<T>> source)
    {
        /// <summary>Flattens each enumerable element emitted by the source into individual values.</summary>
        /// <returns>A flattened observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ForEach() =>
            new ForEachEnumerableSignal<T>(source);
    }

    /// <summary>Async-native parity helper operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Continues with an empty sequence when the source completes with a failure result.</summary>
        /// <returns>A sequence that suppresses terminal failures and completes instead.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> CatchIgnore() =>
            CatchSignal<T>.CreateIgnore(source);

        /// <summary>Continues with an empty sequence when the source fails with the specified exception type.</summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="errorAction">The action to invoke when a matching exception is observed.</param>
        /// <returns>A sequence that suppresses matching terminal failures and completes instead.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> CatchIgnore<TException>(Action<TException> errorAction)
            where TException : Exception =>
            CatchSignal<T>.CreateIgnore(source, errorAction);

        /// <summary>Continues with a fallback value when the source completes with a failure result.</summary>
        /// <param name="fallback">The fallback value to emit.</param>
        /// <returns>A sequence that emits either the original values or the fallback value on terminal failure.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> CatchAndReturn(T fallback) =>
            CatchSignal<T>.CreateReturn(source, fallback);

        /// <summary>Continues with a fallback value produced from a specific exception type.</summary>
        /// <typeparam name="TException">The exception type to intercept.</typeparam>
        /// <param name="fallbackFactory">The fallback value factory.</param>
        /// <returns>A sequence that emits either the original values or a fallback value on matching terminal failure.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> CatchAndReturn<TException>(Func<TException, T> fallbackFactory)
            where TException : Exception =>
            CatchSignal<T>.CreateReturn(source, fallbackFactory);

        /// <summary>Registers a side effect that runs when the source is subscribed.</summary>
        /// <param name="action">The action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DoOnSubscribe(Action action) =>
            new DoOnSubscribeSignal<T>(source, action);

        /// <summary>Registers an asynchronous side effect that runs when the source is subscribed.</summary>
        /// <param name="action">The asynchronous action to execute before subscription.</param>
        /// <returns>The original source sequence with the subscription side effect applied.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DoOnSubscribe(Func<CancellationToken, ValueTask> action) =>
            new DoOnSubscribeAsyncSignal<T>(source, action);

        /// <summary>Drops source values that arrive while the previous asynchronous action runs.</summary>
        /// <param name="asyncAction">The asynchronous action to execute for accepted values.</param>
        /// <returns>A sequence that emits only values that were accepted while the operator was idle.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DropIfBusy(Func<T, CancellationToken, ValueTask> asyncAction) =>
            new DropIfBusySignal<T>(source, asyncAction);

        /// <summary>Starts with the default value and emits subsequent source values only when they change.</summary>
        /// <param name="defaultValue">The default value to emit first.</param>
        /// <returns>A sequence that starts with the provided default value and then emits distinct source updates.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> LatestOrDefault(T defaultValue) =>
            new LatestOrDefaultSignal<T>(source, defaultValue);

        /// <summary>Logs errors as they are observed without changing the source sequence.</summary>
        /// <param name="logger">The error logger.</param>
        /// <returns>The original sequence with error side effects attached.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> LogErrors(Action<Exception> logger) =>
            new LogErrorsSignal<T>(source, logger);

        /// <summary>Emits the first value that satisfies the predicate and then completes.</summary>
        /// <param name="predicate">The predicate to satisfy.</param>
        /// <returns>A sequence containing the first matching value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> WaitUntil(Func<T, bool> predicate) =>
            new WaitUntilSignal<T>(source, predicate);

        /// <summary>Emits adjacent pairs from the source sequence.</summary>
        /// <returns>A sequence of adjacent (previous, current) pairs.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<(T Previous, T Current)> Pairwise() =>
            new PairwiseSignal<T>(source);

        /// <summary>Partitions the source into matches and nonmatches, evaluating the predicate once per element.</summary>
        /// <param name="predicate">The partition predicate.</param>
        /// <returns>A tuple of true and false partitions.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (IObservableAsync<T> True, IObservableAsync<T> False) Partition(Func<T, bool> predicate) =>
            PartitionCoordinator<T>.CreateBranches(source, predicate);

        /// <summary>Replays the last observed value to new subscribers, starting with the provided initial value.</summary>
        /// <param name="initialValue">The initial replay value.</param>
        /// <returns>A replaying shared observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ReplayLastOnSubscribe(T initialValue) =>
            RefCountSignal<T>.CreateReplayLast(source, initialValue);

        /// <summary>Emits only the first value in each throttle window and suppresses duplicates before and after throttling.</summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <returns>A throttled distinct sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle) =>
            ThrottleDistinctSignal<T>.Create(source, throttle);

        /// <summary>Emits only the first value in each throttle window and suppresses duplicates before and after throttling.</summary>
        /// <param name="throttle">The throttle duration.</param>
        /// <param name="timeProvider">The optional time provider.</param>
        /// <returns>A throttled distinct sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ThrottleDistinct(TimeSpan throttle, TimeProvider? timeProvider) =>
            ThrottleDistinctSignal<T>.Create(source, throttle, timeProvider);

        /// <summary>Performs a scan that emits the initial accumulator value before processing the source sequence.</summary>
        /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
        /// <param name="initial">The initial accumulator value.</param>
        /// <param name="accumulator">The accumulator function.</param>
        /// <returns>A sequence beginning with the initial accumulator value followed by each intermediate accumulated value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TAccumulate> ScanWithInitial<TAccumulate>(
            TAccumulate initial,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            new ScanWithInitialSignal<T, TAccumulate>(source, initial, accumulator);

        /// <summary>Performs an asynchronous scan that emits the initial accumulator value before processing the source sequence.</summary>
        /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
        /// <param name="initial">The initial accumulator value.</param>
        /// <param name="accumulator">The asynchronous accumulator function.</param>
        /// <returns>A sequence beginning with the initial accumulator value followed by each intermediate accumulated value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TAccumulate> ScanWithInitial<TAccumulate>(
            TAccumulate initial,
            Func<TAccumulate, T, CancellationToken, ValueTask<TAccumulate>> accumulator) =>
            new ScanWithInitialAsyncSignal<T, TAccumulate>(source, initial, accumulator);

        /// <summary>Delays values until the condition becomes true immediately or the debounce interval elapses.</summary>
        /// <param name="debounce">The debounce interval.</param>
        /// <param name="condition">The condition that bypasses the delay when true.</param>
        /// <returns>A debounced observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DebounceUntil(
            TimeSpan debounce,
            Func<T, bool> condition) =>
            DebounceUntilSignal<T>.Create(source, debounce, condition);

        /// <summary>Delays values until the condition becomes true immediately or the debounce interval elapses.</summary>
        /// <param name="debounce">The debounce interval.</param>
        /// <param name="condition">The condition that bypasses the delay when true.</param>
        /// <param name="timeProvider">The optional time provider.</param>
        /// <returns>A debounced observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DebounceUntil(
            TimeSpan debounce,
            Func<T, bool> condition,
            TimeProvider? timeProvider) =>
            DebounceUntilSignal<T>.Create(source, debounce, condition, timeProvider);
    }

    /// <summary>Aggregation parity helper operators for a struct-valued observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The first source sequence.</param>
    extension<T>(IObservableAsync<T> source)
        where T : struct
    {
        /// <summary>Returns the minimum of the latest values from the supplied source sequences.</summary>
        /// <param name="sources">Additional source sequences.</param>
        /// <returns>A sequence that emits the minimum of the latest values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> GetMin(params IObservableAsync<T>[] sources) =>
            SyncLatestCoordinator.CreateMin(source, sources);

        /// <summary>Returns the maximum of the latest values from the supplied source sequences.</summary>
        /// <param name="sources">Additional source sequences.</param>
        /// <returns>A sequence that emits the maximum of the latest values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> GetMax(params IObservableAsync<T>[] sources) =>
            SyncLatestCoordinator.CreateMax(source, sources);
    }

    /// <summary>Null-filtering parity helper operators for a nullable reference-type observable source sequence.</summary>
    /// <typeparam name="T">The reference type element.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservableAsync<T?> source)
        where T : class
    {
        /// <summary>Skips <see langword="null"/> values until the first non-null value appears.</summary>
        /// <returns>A sequence that starts with the first non-null value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> SkipWhileNull() =>
            new SkipWhileNullSignal<T>(source);

        /// <summary>Filters the source sequence to non-null values and narrows the result type accordingly.</summary>
        /// <returns>A non-nullable observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> WhereIsNotNull() =>
            new WhereIsNotNullSignal<T>(source);
    }

    /// <summary>Boolean parity helper operators for a boolean observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<bool> source)
    {
        /// <summary>Negates each boolean value emitted by the source sequence.</summary>
        /// <returns>A sequence of negated boolean values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<bool> Not() =>
            NotSignal.Create(source);

        /// <summary>Filters the source sequence to <see langword="false"/> values.</summary>
        /// <returns>A sequence containing only <see langword="false"/> values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<bool> WhereFalse() =>
            WhereFalseSignal.Create(source);

        /// <summary>Filters the source sequence to <see langword="true"/> values.</summary>
        /// <returns>A sequence containing only <see langword="true"/> values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<bool> WhereTrue() =>
            WhereTrueSignal.Create(source);
    }
}
