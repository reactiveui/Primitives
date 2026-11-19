// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Additional ReactiveUI.Primitives operator surface using distinct Primitives vocabulary.</summary>
public static partial class LinqExtensions
{
    /// <summary>Signal-conversion operators for an enumerable source.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The values to enumerate.</param>
    extension<T>(IEnumerable<T> values)
    {
        /// <summary>Converts an enumerable sequence to a signal.</summary>
        /// <returns>A signal that emits the enumerable values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> ToSignal() => new FromEnumerableSignal<T>(values);

        /// <summary>Converts an enumerable sequence to a signal that observes cancellation.</summary>
        /// <param name="cancellationToken">The token used to stop enumeration.</param>
        /// <returns>A signal that emits the enumerable values until enumeration completes or cancellation is requested.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> ToSignal(CancellationToken cancellationToken) =>
            FromEnumerableSignal<T>.Create(values, cancellationToken);
    }

    /// <summary>Combining operators for an observable source of inner observable sequences.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    extension<T>(IObservable<IObservable<T>> sources)
    {
        /// <summary>Subscribes to all inner sequences and forwards their values as they arrive.</summary>
        /// <returns>A sequence containing values from all inner sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Blend() => BlendSignal<T>.Create(sources);

        /// <summary>Mirrors the first inner sequence to produce any notification.</summary>
        /// <returns>A sequence that mirrors the winning inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Race() => RaceSignal<T>.Create(sources);

        /// <summary>Subscribes to inner sequences one at a time in source order.</summary>
        /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Chain() => ChainSignal<T>.Create(sources);

        /// <summary>Switches to the most recent inner sequence.</summary>
        /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> SwitchTo() => SwitchSignal<T>.Create(sources);
    }

    /// <summary>Notification-materialization operator for an observable source of spark values.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The spark sequence.</param>
    extension<T>(IObservable<Spark<T>> source)
    {
        /// <summary>Converts <see cref="Spark{T}"/> values back into observer notifications.</summary>
        /// <returns>A sequence represented by the supplied spark values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Unspark() => UnsparkSignal<T>.Create(source);
    }

    /// <summary>Projection, filtering, combination, and timing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Projects each element of an observable sequence into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
        /// source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Map<TResult>(Func<T, TResult> selector) =>
            FusedMapSignal<T, TResult>.Create(source, selector);

        /// <summary>Projects each element and its zero-based index into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each source element and its index.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and index.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> MapIndexed<TResult>(Func<T, int, TResult> selector) =>
            MapIndexedSignal<T, TResult>.Create(source, selector);

        /// <summary>Projects each element of an observable sequence into a new form by incorporating state that is passed to the selector function.</summary>
        /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="state">The state to pass to the selector function.</param>
        /// <param name="selector">A transform function to apply to each source element along with the state.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
        /// source along with the state.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> MapWith<TState, TResult>(TState state, Func<TState, T, TResult> selector) =>
            MapWithSignal<T, TState, TResult>.Create(source, state, selector);

        /// <summary>Filters an observable sequence to include only elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the input sequence that satisfy the condition specified by
        /// <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Keep(Func<T, bool> predicate) =>
            FusedKeepSignal<T>.Create(source, predicate);

        /// <summary>Filters elements from an observable sequence based on a predicate that uses external state.</summary>
        /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
        /// <param name="state">The state value to pass to the predicate for each element.</param>
        /// <param name="predicate">A function to test each element along with the state; returns <see langword="true"/> to keep the element, <see
        /// langword="false"/> to filter it out.</param>
        /// <returns>An observable sequence containing only the elements from the source sequence that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> KeepWith<TState>(TState state, Func<TState, T, bool> predicate) =>
            KeepWithSignal<T, TState>.Create(source, state, predicate);

        /// <summary>Invokes an action for each value while preserving the original sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Tap(Action<T> onNext) =>
            TapSignal<T>.Create(source, onNext);

        /// <summary>Invokes a stateful action for each value while preserving the original sequence.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> TapWith<TState>(TState state, Action<TState, T> onNext) =>
            TapWithSignal<T, TState>.Create(source, state, onNext);

        /// <summary>Emits the accumulated state after each source value.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence of intermediate accumulated values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TAccumulate> Fold<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            FoldSignal<T, TAccumulate>.Create(source, seed, accumulator);

        /// <summary>Emits the final accumulated state when the source completes.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence that emits one accumulated value on completion.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TAccumulate> Reduce<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            ReduceSignal<T, TAccumulate>.Create(source, seed, accumulator);

        /// <summary>Emits at most <paramref name="count"/> values before completing.</summary>
        /// <param name="count">The maximum number of values to emit.</param>
        /// <returns>A sequence containing at most <paramref name="count"/> source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Take(int count) => TakeSignal<T>.Create(source, count);

        /// <summary>Forwards source values until <paramref name="other"/> emits a value; other completion without a value does not stop the source.</summary>
        /// <typeparam name="TOther">The cancellation value type.</typeparam>
        /// <param name="other">The observable that stops the source when it emits.</param>
        /// <returns>An observable that completes when the source completes or <paramref name="other"/> emits.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> TakeUntil<TOther>(IObservable<TOther> other) =>
            TakeUntilSignal<T, TOther>.Create(source, other);

        /// <summary>Forwards source values until <paramref name="cancellationToken"/> is canceled; cancellation completes the sequence gracefully.</summary>
        /// <param name="cancellationToken">The token whose cancellation stops the source.</param>
        /// <returns>An observable that completes when the source completes or <paramref name="cancellationToken"/> is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> TakeUntil(CancellationToken cancellationToken) =>
            TakeUntilSignal<T, RxVoid>.CreateWithCancellation(source, cancellationToken);

        /// <summary>Skips the first <paramref name="count"/> source values.</summary>
        /// <param name="count">The number of values to skip.</param>
        /// <returns>A sequence containing source values after the skipped prefix.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Skip(int count) => SkipSignal<T>.Create(source, count);

        /// <summary>Suppresses every value that matches one seen earlier in the sequence.</summary>
        /// <returns>A sequence containing the first occurrence of each source value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Distinct() => DistinctSignal<T>.Create(source);

        /// <summary>Suppresses every value that the supplied comparer matches to one seen earlier.</summary>
        /// <param name="comparer">The comparer used to identify duplicate values.</param>
        /// <returns>A sequence containing the first occurrence of each source value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Distinct(IEqualityComparer<T>? comparer) =>
            DistinctSignal<T>.Create(source, comparer);

        /// <summary>Suppresses adjacent duplicate values.</summary>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Unique() => UniqueSignal<T>.Create(source);

        /// <summary>Suppresses adjacent duplicate values using the supplied comparer.</summary>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Unique(IEqualityComparer<T>? comparer) =>
            UniqueSignal<T>.Create(source, comparer);

        /// <summary>Converts source values and terminal notifications into <see cref="Spark{T}"/> values.</summary>
        /// <returns>A sequence of spark values representing source notifications; terminal sparks are followed by completion.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<Spark<T>> Spark() => new SparkSignal<T>(source);

        /// <summary>Concatenates two sequences.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="source"/> completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Chain(IObservable<T> second) => ChainSignal<T>.Create(source, second);

        /// <summary>Combines paired values from two sequences, completing when no more pairs can be formed.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines paired values.</param>
        /// <returns>A sequence containing one result for each available value pair.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Pair<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector) =>
            ZipSignal<T, TRight, TResult>.Create(source, right, selector);

        /// <summary>Combines the latest values after both sequences have produced at least one value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SyncLatest<TRight, TResult>(
            IObservable<TRight> right,
            Func<T, TRight, TResult> selector) =>
            CombineLatestSignal<T, TRight, TResult>.CreateSyncLatest(source, right, selector);

        /// <summary>Combines each left value with the latest right value after the right sequence has produced a value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The sequence that supplies the latest value.</param>
        /// <param name="selector">The function that combines the left value with the latest right value.</param>
        /// <returns>A sequence containing selected left/latest-right combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        /// <remarks>Left values produced before the first right value are ignored.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Latch<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector) =>
            LatchSignal<T, TRight, TResult>.Create(source, right, selector);

        /// <summary>Resubscribes to the source after an error up to <paramref name="retryCount"/> times.</summary>
        /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
        /// <returns>A sequence that retries the source before forwarding the final error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is less than zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Reattempt(int retryCount) =>
            ReattemptSignal<T>.Create(source, retryCount);

        /// <summary>Recovers from errors by switching to a handler-provided sequence.</summary>
        /// <param name="handler">The function that creates the recovery sequence for an error.</param>
        /// <returns>A sequence that continues with the handler result after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Recover(Func<Exception, IObservable<T>> handler) =>
            RecoverSignal<T, Exception>.Create(source, handler);

        /// <summary>Recovers from errors by switching to a handler-provided sequence.</summary>
        /// <param name="handler">The function that creates the recovery sequence for an error.</param>
        /// <returns>A sequence that continues with the handler result after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Rescue(Func<Exception, IObservable<T>> handler) =>
            RecoverSignal<T, Exception>.Create(source, handler);

        /// <summary>Continues with a fallback sequence after an error.</summary>
        /// <param name="fallback">The sequence to subscribe to after an error.</param>
        /// <returns>A sequence that resumes with <paramref name="fallback"/> after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="fallback"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Resume(IObservable<T> fallback) =>
            ResumeSignal<T>.Create(source, fallback);

        /// <summary>Delays source notifications by the specified duration.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Shift(TimeSpan dueTime) =>
            ShiftSignal<T>.Create(source, dueTime);

        /// <summary>Delays source notifications by the specified duration on a sequencer.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Shift(TimeSpan dueTime, ISequencer? scheduler) =>
            ShiftSignal<T>.Create(source, dueTime, scheduler);

        /// <summary>Fails the sequence when no value arrives within the timeout; each value restarts the clock.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Expire(TimeSpan dueTime) =>
            new ExpireSignal<T>(source, dueTime);

        /// <summary>Fails the sequence on the sequencer when no value arrives within the timeout; each value restarts the clock.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Expire(TimeSpan dueTime, ISequencer? scheduler) =>
            ExpireSignal<T>.Create(source, dueTime, scheduler);

        /// <summary>Collects all values into a list when the source completes.</summary>
        /// <returns>A sequence that emits one list containing all source values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> CollectList() => CollectListSignal<T>.Create(source);

        /// <summary>Collects all values into an array when the source completes.</summary>
        /// <returns>A sequence that emits one array containing all source values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T[]> CollectArray() => CollectArraySignal<T>.Create(source);

        /// <summary>Returns an observable sequence as a signal-compatible observable.</summary>
        /// <returns>The supplied source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> ToSignal() => AsObservableSignal<T>.Identity(source);
    }

    /// <summary>Null-filtering operator for an observable source of nullable reference values.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    extension<T>(IObservable<T?> source)
        where T : class
    {
        /// <summary>Filters out null values from the source observable sequence, emitting only non-null values.</summary>
        /// <returns>An observable sequence that emits only non-null values from the source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> KeepNotNull() => FusedKeepNotNullSignal<T>.Create(source);
    }

    /// <summary>Combining operators for an observable source of tasks.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="sources">The outer sequence of task sources.</param>
    extension<T>(IObservable<Task<T>> sources)
    {
        /// <summary>Subscribes to task results one at a time in source order.</summary>
        /// <returns>A sequence that emits each task result after the previous task signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Chain() => new TaskChainSignal<T>(sources);
    }

    /// <summary>Type-filtering and casting operators for an untyped observable source.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservable<object?> source)
    {
        /// <summary>Filters values to those assignable to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <returns>A sequence containing only values assignable to <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "No argument carries the result type, so it can only come from an explicit type argument.")]
        public IObservable<TResult> KeepType<TResult>() => KeepTypeSignal<TResult>.Create(source);

        /// <summary>Casts each source value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <returns>A sequence containing each value cast to <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "No argument carries the result type, so it can only come from an explicit type argument.")]
        public IObservable<TResult> CastTo<TResult>() => CastSignal<TResult>.Create(source);
    }

    /// <summary>Signal-conversion operators for a task source.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to convert.</param>
    extension<T>(Task<T> task)
    {
        /// <summary>Converts a task to a signal that emits the task result.</summary>
        /// <returns>A signal that emits the completed task result or faults with the task error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> ToSignal() => TaskInstanceSignal<T>.Create(task);
    }
}
