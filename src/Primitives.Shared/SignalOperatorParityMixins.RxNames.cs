// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Provides System.Reactive and LINQ aliases for the Primitives operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named combining operators for enumerable observable sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The observable sources.</param>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Concurrently merges the supplied observable sources.</summary>
        /// <returns>An observable that forwards values from every source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Merge() => new MergeSignal<T>(sources);

        /// <summary>Concurrently merges observable sources with a maximum number of active subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of sources to subscribe to at the same time.</param>
        /// <returns>An observable that forwards values from every source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrent"/> is less than or equal to zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Merge(int maxConcurrent) => new MergeSignal<T>(sources, maxConcurrent);
    }

    /// <summary>System.Reactive-named combining operators for an observable source of inner observable sequences.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    extension<T>(IObservable<IObservable<T>> sources)
    {
        /// <summary>Subscribes to all inner sequences and forwards their values as they arrive.</summary>
        /// <returns>A sequence containing values from all inner sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Merge() => BlendSignal<T>.Create(sources);

        /// <summary>Subscribes to inner sequences one at a time in source order.</summary>
        /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Concat() => ChainSignal<T>.Create(sources);

        /// <summary>Mirrors the first inner sequence to produce any notification.</summary>
        /// <returns>A sequence that mirrors the winning inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Amb() => RaceSignal<T>.Create(sources);

        /// <summary>Switches to the most recent inner sequence.</summary>
        /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Switch() => SwitchSignal<T>.Create(sources);
    }

    /// <summary>System.Reactive-named notification-materialization operator for an observable source of spark values.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The spark sequence.</param>
    extension<T>(IObservable<Spark<T>> source)
    {
        /// <summary>Converts <see cref="Spark{T}"/> values back into observer notifications.</summary>
        /// <returns>A sequence represented by the supplied spark values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Dematerialize() => UnsparkSignal<T>.Create(source);
    }

    /// <summary>System.Reactive-named side-effect, accumulation, and projection operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Subscribes an observer with downstream exception protection.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafe(IObserver<T> observer) =>
            SubscribeSafeWitness<T>.Subscribe(source, observer);

        /// <summary>Subscribes an observer with downstream exception protection, using a Primitives-specific name.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The observer overload is ambiguous when System.Reactive also supplies SubscribeSafe in the System namespace; other overloads are
        /// unaffected.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafePrimitives(IObserver<T> observer) =>
            SubscribeSafeWitness<T>.Subscribe(source, observer);

        /// <summary>Subscribes callbacks with downstream exception protection.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafe(Action<T> onNext, Action<Exception> onError) =>
            SubscribeSafeWitness<T>.Subscribe(source, onNext, onError);

        /// <summary>Subscribes callbacks with downstream exception protection.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafe(Action<T> onNext, Action<Exception> onError, Action onCompleted) =>
            SubscribeSafeWitness<T>.Subscribe(source, onNext, onError, onCompleted);

        /// <summary>Subscribes a value callback with downstream exception protection.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        /// <remarks>With no error handler, a source error is rethrown to the caller of the source's <c>OnError</c>.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafe(Action<T> onNext) =>
            SubscribeSafeWitness<T>.Subscribe(source, onNext);

        /// <summary>Subscribes error callbacks with downstream exception protection.</summary>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafeErrors(Action<Exception> onError) =>
            SubscribeSafeWitness<T>.SubscribeErrors(source, onError);

        /// <summary>Subscribes error and completion callbacks with downstream exception protection.</summary>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable SubscribeSafeErrors(Action<Exception> onError, Action onCompleted) =>
            SubscribeSafeWitness<T>.SubscribeErrors(source, onError, onCompleted);

        /// <summary>Invokes an action for each value while preserving the sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Do(Action<T> onNext) => new TapSignal<T>(source, onNext);

        /// <summary>Invokes actions for each value and error while preserving the sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onError"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Do(Action<T> onNext, Action<Exception> onError) =>
            new TapSignal<T>(source, onNext, onError);

        /// <summary>Invokes actions for each value and completion while preserving the sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Do(Action<T> onNext, Action onCompleted) =>
            new TapSignal<T>(source, onNext, onCompleted);

        /// <summary>Invokes actions for each value, error, and completion while preserving the sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Do(
            Action<T> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            new TapSignal<T>(source, onNext, onError, onCompleted);

        /// <summary>Serializes concurrent notifications so values precede a single terminal notification.</summary>
        /// <returns>A sequence that forwards the source notifications one at a time.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Synchronize() => new SynchronizeSignal<T>(source);

        /// <summary>Delivers concurrent notifications one at a time without holding a lock while the observer runs.</summary>
        /// <returns>A sequence that forwards the source notifications one at a time.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// A notification that arrives while another thread is delivering is queued behind it instead of blocking, so an
        /// observer that marshals to a producer's thread cannot deadlock that producer.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Serialize() => new SerializeSignal<T>(source);

        /// <summary>Serializes notifications with every sequence using the supplied gate.</summary>
        /// <param name="gate">The gate shared with other synchronized sequences.</param>
        /// <returns>A sequence that forwards the source notifications one at a time under the shared gate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="gate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Synchronize(Lock gate) => new SynchronizeGateSignal<T>(source, gate);

#if NET9_0_OR_GREATER
        /// <summary>
        /// Serializes notifications behind an object gate for System.Reactive compatibility on callers that
        /// intentionally share an object lock.
        /// </summary>
        /// <param name="gate">The object gate shared with other synchronized sequences.</param>
        /// <returns>A sequence that forwards the source notifications one at a time under the shared object gate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="gate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Synchronize(object gate) => new SynchronizeObjectSignal<T>(source, gate);
#endif

        /// <summary>Invokes a stateful action for each value while preserving the sequence.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> DoWith<TState>(TState state, Action<TState, T> onNext) =>
            new TapWithSignal<T, TState>(source, state, onNext);

        /// <summary>Emits the accumulated state after each source value.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence of intermediate accumulated values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TAccumulate> Scan<TAccumulate>(
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
        public IObservable<TAccumulate> Aggregate<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            ReduceSignal<T, TAccumulate>.Create(source, seed, accumulator);

        /// <summary>Suppresses adjacent duplicate values.</summary>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> DistinctUntilChanged() => UniqueSignal<T>.Create(source);

        /// <summary>Suppresses adjacent duplicate values using the supplied comparer.</summary>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> DistinctUntilChanged(IEqualityComparer<T>? comparer) =>
            UniqueSignal<T>.Create(source, comparer);

        /// <summary>Suppresses adjacent values with duplicate keys.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> DistinctUntilChangedBy<TKey>(Func<T, TKey> keySelector) =>
            UniqueBySignal<T, TKey>.Create(source, keySelector);

        /// <summary>Suppresses adjacent values with duplicate keys using the supplied comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <param name="comparer">The comparer used to compare adjacent keys.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> DistinctUntilChangedBy<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer) =>
            UniqueBySignal<T, TKey>.Create(source, keySelector, comparer);

        /// <summary>Drops every value, forwarding only the terminal notification.</summary>
        /// <returns>A sequence that forwards only completion or error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> IgnoreElements() => IgnoreValuesSignal<T>.Create(source);

        /// <summary>Prepends values before the source sequence.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> StartWith(params T[] values) =>
            StartWithEnumerableSignal<T>.Create(source, values);

        /// <summary>Prepends values before the source sequence.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> StartWith(IEnumerable<T> values) =>
            new StartWithEnumerableSignal<T>(source, values);

        /// <summary>Collects values into time-windowed batches.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <returns>A sequence that emits non-empty batches of source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> Buffer(TimeSpan timeSpan) => new BufferSignal<T>(source, timeSpan);

        /// <summary>Collects values into time-windowed batches on the supplied scheduler.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <param name="scheduler">The scheduler used to schedule buffer flushes.</param>
        /// <returns>A sequence that emits non-empty batches of source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> Buffer(TimeSpan timeSpan, ISequencer scheduler) =>
            new BufferSignal<T>(source, timeSpan, scheduler);

        /// <summary>Invokes an action when the subscription terminates or is disposed.</summary>
        /// <param name="finallyAction">The action to invoke exactly once.</param>
        /// <returns>A sequence that mirrors the source and invokes <paramref name="finallyAction"/> on cleanup.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="finallyAction"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Finally(Action finallyAction) =>
            new FinallySignal<T>(source, finallyAction);

        /// <summary>Emits a value only after no newer value arrives within the quiet period.</summary>
        /// <param name="dueTime">The quiet period.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Throttle(TimeSpan dueTime) => new CalmSignal<T>(source, dueTime);

        /// <summary>Emits a value only after no newer value arrives within the scheduler quiet period.</summary>
        /// <param name="dueTime">The quiet period.</param>
        /// <param name="scheduler">The scheduler used to schedule quiet-period timers.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Throttle(TimeSpan dueTime, ISequencer? scheduler) =>
            CalmSignal<T>.Create(source, dueTime, scheduler);

        /// <summary>Handles errors of the specified type by switching to a replacement sequence.</summary>
        /// <typeparam name="TException">The exception type to handle.</typeparam>
        /// <param name="handler">The function that produces a replacement sequence for handled errors.</param>
        /// <returns>A sequence that switches to the handler result for matching errors.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Catch<TException>(Func<TException, IObservable<T>> handler)
            where TException : Exception =>
            RecoverSignal<T, TException>.Create(source, handler);

        /// <summary>Projects each value to an inner sequence and merges the results.</summary>
        /// <typeparam name="TResult">The inner value type.</typeparam>
        /// <param name="selector">The function that projects each source value to an inner sequence.</param>
        /// <returns>A sequence containing the merged values of every inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SelectMany<TResult>(Func<T, IObservable<TResult>> selector) =>
            SelectManySignal<T, TResult>.Create(source, selector);

        /// <summary>Projects each value to the same inner sequence and merges the results.</summary>
        /// <typeparam name="TResult">The inner value type.</typeparam>
        /// <param name="other">The inner sequence used for each source value.</param>
        /// <returns>A sequence containing the merged values of every inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SelectMany<TResult>(IObservable<TResult> other) =>
            SelectManySignal<T, TResult>.Create(source, other);

        /// <summary>Projects each value to an enumerable sequence and emits the projected values.</summary>
        /// <typeparam name="TResult">The projected value type.</typeparam>
        /// <param name="selector">The function that projects each source value to enumerable values.</param>
        /// <returns>A sequence containing the projected enumerable values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SelectMany<TResult>(Func<T, IEnumerable<TResult>> selector) =>
            SelectManyEnumerableSignal<T, TResult>.Create(source, selector);

        /// <summary>Projects each value to an inner sequence and concurrently combines each pair with a result selector.</summary>
        /// <typeparam name="TCollection">The inner value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="collectionSelector">The function that projects each source value to an inner sequence.</param>
        /// <param name="resultSelector">The function that combines a source value with each inner value.</param>
        /// <returns>A sequence containing selected outer/inner combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SelectMany<TCollection, TResult>(
            Func<T, IObservable<TCollection>> collectionSelector,
            Func<T, TCollection, TResult> resultSelector) =>
            SelectManyResultSignal<T, TCollection, TResult>.Create(source, collectionSelector, resultSelector);

        /// <summary>Merges this sequence with another observable sequence.</summary>
        /// <param name="second">The second sequence to merge.</param>
        /// <returns>A sequence containing values from both sources as they arrive.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Merge(IObservable<T> second) => MergeSignal<T>.Create(source, second);

        /// <summary>Concatenates two sequences.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="source"/> completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Concat(IObservable<T> second) => ChainSignal<T>.Create(source, second);

        /// <summary>Continues with <paramref name="second"/> after this sequence completes or errors.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that forwards both sources in order, ignoring errors from the first.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> OnErrorResumeNext(IObservable<T> second) =>
            OnErrorResumeNextSignal<T>.Create(source, second);
    }

    /// <summary>Null-filtering operator using System.Reactive vocabulary for an observable source of nullable reference values.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to filter.</param>
    extension<T>(IObservable<T?> source)
        where T : class
    {
        /// <summary>Filters out null values, emitting only non-null values.</summary>
        /// <returns>An observable sequence that emits only the non-null values from the source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> WhereNotNull() => FusedKeepNotNullSignal<T>.Create(source);
    }

    /// <summary>System.Reactive-named pairwise combination and timing operators for an observable source sequence.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <param name="left">The left sequence.</param>
    extension<TLeft>(IObservable<TLeft> left)
    {
        /// <summary>Combines paired values from two sequences by index.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines paired values.</param>
        /// <returns>A sequence containing one result for each available value pair.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Zip<TRight, TResult>(
            IObservable<TRight> right,
            Func<TLeft, TRight, TResult> selector) =>
            ZipSignal<TLeft, TRight, TResult>.Create(left, right, selector);

        /// <summary>Combines the latest values once both sequences have produced a value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> CombineLatest<TRight, TResult>(
            IObservable<TRight> right,
            Func<TLeft, TRight, TResult> selector) =>
            CombineLatestSignal<TLeft, TRight, TResult>.CreateCombineLatest(left, right, selector);

        /// <summary>Combines each left value with the latest right value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The sequence that supplies the latest value.</param>
        /// <param name="selector">The function that combines the left value with the latest right value.</param>
        /// <returns>A sequence containing selected left/latest-right combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> WithLatestFrom<TRight, TResult>(
            IObservable<TRight> right,
            Func<TLeft, TRight, TResult> selector) =>
            LatchSignal<TLeft, TRight, TResult>.Create(left, right, selector);

        /// <summary>Delays source notifications by the specified duration.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Delay(TimeSpan dueTime) => ShiftSignal<TLeft>.Create(left, dueTime);

        /// <summary>Delays source notifications by the specified duration on a sequencer.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Delay(TimeSpan dueTime, ISequencer? scheduler) =>
            ShiftSignal<TLeft>.Create(left, dueTime, scheduler);

        /// <summary>Delays source notifications until the specified absolute due time.</summary>
        /// <param name="dueTime">The absolute time at which notifications may be forwarded.</param>
        /// <returns>A sequence that forwards source notifications after the due time.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Delay(DateTimeOffset dueTime) =>
            new AbsoluteShiftSignal<TLeft>(left, dueTime);

        /// <summary>Delays source notifications until the specified absolute due time on a sequencer.</summary>
        /// <param name="dueTime">The absolute time at which notifications may be forwarded.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        /// <returns>A sequence that forwards source notifications after the due time.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Delay(DateTimeOffset dueTime, ISequencer? scheduler) =>
            AbsoluteShiftSignal<TLeft>.Create(left, dueTime, scheduler);

        /// <summary>Fails the sequence when no value arrives within the timeout; each value restarts the clock.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Timeout(TimeSpan dueTime) =>
            new ExpireSignal<TLeft>(left, dueTime);

        /// <summary>Fails the sequence on the sequencer when no value arrives within the timeout; each value restarts the clock.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Timeout(TimeSpan dueTime, ISequencer? scheduler) =>
            ExpireSignal<TLeft>.Create(left, dueTime, scheduler);

        /// <summary>Fails the sequence if it has not terminated by the absolute time, whatever values arrive first.</summary>
        /// <param name="dueTime">The absolute timeout time.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Timeout(DateTimeOffset dueTime) =>
            new AbsoluteExpireSignal<TLeft>(left, dueTime);

        /// <summary>Fails the sequence on the sequencer if it has not terminated by the absolute time, whatever values arrive first.</summary>
        /// <param name="dueTime">The absolute timeout time.</param>
        /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Timeout(DateTimeOffset dueTime, ISequencer? scheduler) =>
            AbsoluteExpireSignal<TLeft>.Create(left, dueTime, scheduler);

        /// <summary>Emits the most recent value once the period has passed since the value that started the timer.</summary>
        /// <param name="interval">The sampling period.</param>
        /// <returns>A sequence carrying the latest source value once each period elapses; a quiet source sends nothing.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Sample(TimeSpan interval) =>
            new ProbeSignal<TLeft>(left, interval);

        /// <summary>Emits the most recent value on a sequencer once the period has passed since the value that started the timer.</summary>
        /// <param name="interval">The sampling period.</param>
        /// <param name="scheduler">The sequencer used to schedule sampling.</param>
        /// <returns>A sequence carrying the latest source value once each period elapses; a quiet source sends nothing.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Sample(TimeSpan interval, ISequencer? scheduler) =>
            ProbeSignal<TLeft>.Create(left, interval, scheduler);

        /// <summary>Runs the source up to <paramref name="retryCount"/> times in total, stopping at the first run that ends without an error.</summary>
        /// <param name="retryCount">The total number of runs. Zero runs the source not at all and completes.</param>
        /// <returns>A sequence that runs the source again after an error before forwarding the final error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is less than zero.</exception>
        /// <remarks>The count is total runs, matching the System.Reactive operator of this name. Use <c>Reattempt</c> to count extra tries instead.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TLeft> Retry(int retryCount) =>
            ReattemptSignal<TLeft>.CreateTotalRuns(left, retryCount);

        /// <summary>Converts source values and terminal notifications into <see cref="Spark{T}"/> values.</summary>
        /// <returns>A sequence of spark values representing source notifications.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<Spark<TLeft>> Materialize() => new SparkSignal<TLeft>(left);
    }

    /// <summary>LINQ-named projection and filtering operators for an observable source sequence.</summary>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">An observable sequence of elements to project.</param>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Projects each element of an observable sequence into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each source element.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Select<TResult>(Func<TSource, TResult> selector) =>
            FusedMapSignal<TSource, TResult>.Create(source, selector);

        /// <summary>Projects each element and its zero-based index into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element and its index.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and index.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> Select<TResult>(Func<TSource, int, TResult> selector) =>
            MapIndexedSignal<TSource, TResult>.Create(source, selector);

        /// <summary>Projects each element into a new form using external state passed to the selector.</summary>
        /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="state">The state to pass to the selector function.</param>
        /// <param name="selector">A transform function to apply to each source element along with the state.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and the state.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SelectWith<TState, TResult>(TState state, Func<TState, TSource, TResult> selector) =>
            MapWithSignal<TSource, TState, TResult>.Create(source, state, selector);

        /// <summary>Filters an observable sequence to elements that satisfy a predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence containing the elements that satisfy <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TSource> Where(Func<TSource, bool> predicate) =>
            FusedKeepSignal<TSource>.Create(source, predicate);

        /// <summary>Filters elements using a predicate that uses external state.</summary>
        /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
        /// <param name="state">The state value to pass to the predicate for each element.</param>
        /// <param name="predicate">A function to test each element along with the state.</param>
        /// <returns>An observable sequence containing only the elements that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TSource> WhereWith<TState>(TState state, Func<TState, TSource, bool> predicate) =>
            KeepWithSignal<TSource, TState>.Create(source, state, predicate);
    }

    /// <summary>System.Reactive-named combining operators for an observable source of tasks.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="sources">The outer sequence of task sources.</param>
    extension<T>(IObservable<Task<T>> sources)
    {
        /// <summary>Subscribes to task results one at a time in source order.</summary>
        /// <returns>A sequence that emits each task result after the previous task signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> Concat() => new TaskChainSignal<T>(sources);
    }

    /// <summary>System.Reactive-named type filtering and casting operators for an untyped observable source.</summary>
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
        public IObservable<TResult> OfType<TResult>() => KeepTypeSignal<TResult>.Create(source);

        /// <summary>Casts each source value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <returns>A sequence containing each value cast to <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "No argument carries the result type, so it can only come from an explicit type argument.")]
        public IObservable<TResult> Cast<TResult>() => CastSignal<TResult>.Create(source);
    }
}
