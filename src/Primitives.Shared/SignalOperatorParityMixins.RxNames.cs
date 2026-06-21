// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// System.Reactive / LINQ familiar names for the Primitives operator vocabulary. Each method builds the same sink as
/// its Primitives-named counterpart directly, so the two names are interchangeable with identical behaviour and
/// allocation profile. Both name sets are fully supported.
/// </summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named combining operators for enumerable observable sources.</summary>
    /// <param name="sources">The observable sources.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Concurrently merges the supplied observable sources. System.Reactive name for <c>Blend</c>.</summary>
        /// <returns>An observable that forwards values from every source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Merge()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return sources.Blend();
        }

        /// <summary>Concurrently merges observable sources with a maximum number of active subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of sources to subscribe to at the same time.</param>
        /// <returns>An observable that forwards values from every source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="maxConcurrent"/> is less than or equal to zero.</exception>
        public IObservable<T> Merge(int maxConcurrent)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);

            return sources.Blend(maxConcurrent);
        }
    }

    /// <summary>System.Reactive-named combining operators for an observable source of inner observable sequences.</summary>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<IObservable<T>> sources)
    {
        /// <summary>Subscribes to all inner sequences and forwards their values as they arrive. System.Reactive name for <c>Blend</c>.</summary>
        /// <returns>A sequence containing values from all inner sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Merge()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new BlendSignal<T>(sources);
        }

        /// <summary>Subscribes to inner sequences one at a time in source order. System.Reactive name for <c>Chain</c>.</summary>
        /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Concat()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new ChainSignal<T>(sources);
        }

        /// <summary>Mirrors the first inner sequence to produce any notification. System.Reactive name for <c>Race</c>.</summary>
        /// <returns>A sequence that mirrors the winning inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Amb()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new RaceSignal<T>(sources);
        }

        /// <summary>Switches to the most recent inner sequence. System.Reactive name for <c>SwitchTo</c>.</summary>
        /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Switch()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            if (TryCreateSynchronousSwitchRangeSignal(sources, out var rangeSignal))
            {
                return rangeSignal;
            }

            return new SwitchSignal<T>(sources);
        }
    }

    /// <summary>System.Reactive-named notification-materialization operator for an observable source of spark values.</summary>
    /// <param name="source">The spark sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<Spark<T>> source)
    {
        /// <summary>Converts <see cref="Spark{T}"/> values back into observer notifications. System.Reactive name for <c>Unspark</c>.</summary>
        /// <returns>A sequence represented by the supplied spark values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Dematerialize()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new UnsparkSignal<T>(source);
        }
    }

    /// <summary>System.Reactive-named side-effect, accumulation, and projection operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Subscribes an observer with downstream exception protection.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
        public IDisposable SubscribeSafe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(observer);

            SubscribeSafeObserver<T> safe = new(observer);
            safe.SetSubscription(source.Subscribe(safe));
            return safe;
        }

        /// <summary>Subscribes callbacks with downstream exception protection.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public IDisposable SubscribeSafe(Action<T> onNext, Action<Exception> onError) =>
            source.SubscribeSafe(Witness.Create(onNext, onError));

        /// <summary>Subscribes callbacks with downstream exception protection.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public IDisposable SubscribeSafe(Action<T> onNext, Action<Exception> onError, Action onCompleted) =>
            source.SubscribeSafe(Witness.Create(onNext, onError, onCompleted));

        /// <summary>Subscribes terminal callbacks with downstream exception protection.</summary>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public IDisposable SubscribeSafe(Action<Exception> onError) =>
            source.SubscribeSafe(Witness.Create<T>(static _ => { }, onError));

        /// <summary>Subscribes terminal callbacks with downstream exception protection.</summary>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>A disposable that cancels the subscription.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public IDisposable SubscribeSafe(Action<Exception> onError, Action onCompleted) =>
            source.SubscribeSafe(Witness.Create<T>(static _ => { }, onError, onCompleted));

        /// <summary>Invokes an action for each value while preserving the sequence. System.Reactive name for <c>Tap</c>.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> Do(Action<T> onNext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            return new TapSignal<T>(source, onNext, static _ => { }, static () => { });
        }

        /// <summary>Invokes actions for each value and error while preserving the sequence. System.Reactive name for <c>Tap</c>.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onError"/> is <see langword="null"/>.</exception>
        public IObservable<T> Do(Action<T> onNext, Action<Exception> onError)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onError);

            return new TapSignal<T>(source, onNext, onError, static () => { });
        }

        /// <summary>Invokes actions for each value and completion while preserving the sequence. System.Reactive name for <c>Tap</c>.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
        public IObservable<T> Do(Action<T> onNext, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return new TapSignal<T>(source, onNext, static _ => { }, onCompleted);
        }

        /// <summary>Invokes actions for each value, error, and completion while preserving the sequence. System.Reactive name for <c>Tap</c>.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <param name="onError">The action to invoke for an error.</param>
        /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
        /// <returns>The source values after the actions have run.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        public IObservable<T> Do(
            Action<T> onNext,
            Action<Exception> onError,
            Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onError);

            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return new TapSignal<T>(source, onNext, onError, onCompleted);
        }

        /// <summary>
        /// Serializes notifications behind a gate so downstream operators observe the single-threaded
        /// <c>OnNext*</c> then <c>OnError</c>|<c>OnCompleted</c> grammar even when the source delivers
        /// concurrently. System.Reactive name for the same operation.
        /// </summary>
        /// <returns>A sequence that forwards the source notifications one at a time.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Synchronize()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new SynchronizeSignal<T>(source);
        }

        /// <summary>
        /// Serializes notifications behind the supplied <paramref name="gate"/>, so this sequence and every other
        /// sequence synchronized on the same gate observe the single-threaded grammar relative to one another.
        /// System.Reactive name for the same operation.
        /// </summary>
        /// <param name="gate">The gate shared with other synchronized sequences.</param>
        /// <returns>A sequence that forwards the source notifications one at a time under the shared gate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="gate"/> is <see langword="null"/>.</exception>
        public IObservable<T> Synchronize(Lock gate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (gate is null)
            {
                throw new ArgumentNullException(nameof(gate));
            }

            return new SynchronizeSignal<T>(source, gate);
        }

#if NET9_0_OR_GREATER
        /// <summary>Serializes notifications behind an object gate. System.Reactive name for object-gated synchronization.</summary>
        /// <param name="gate">The gate shared with other synchronized sequences.</param>
        /// <returns>A sequence that forwards the source notifications one at a time under the shared gate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="gate"/> is <see langword="null"/>.</exception>
        public IObservable<T> Synchronize(object gate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(gate);

            return new SynchronizeObjectSignal<T>(source, gate);
        }
#endif

        /// <summary>Serializes notifications behind an object gate when a caller cannot provide a dedicated lock gate.</summary>
        /// <param name="gate">The gate shared with other synchronized sequences.</param>
        /// <returns>A sequence that forwards the source notifications one at a time under the shared gate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="gate"/> is <see langword="null"/>.</exception>
        public IObservable<T> SynchronizeObject(object gate)
        {
#if NET9_0_OR_GREATER
            return LinqExtensions.Synchronize<T>(source, gate);
#else
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(gate);

            return new SynchronizeObjectSignal<T>(source, gate);
#endif
        }

        /// <summary>Invokes a stateful action for each value while preserving the sequence. State-carrying name for <c>TapWith</c>.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> DoWith<TState>(TState state, Action<TState, T> onNext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            return new TapWithSignal<T, TState>(source, state, onNext);
        }

        /// <summary>Emits the accumulated state after each source value. System.Reactive name for <c>Fold</c>.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence of intermediate accumulated values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        public IObservable<TAccumulate> Scan<TAccumulate>(TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new FoldSignal<T, TAccumulate>(source, seed, accumulator);
        }

        /// <summary>Emits the final accumulated state when the source completes. System.Reactive name for <c>Reduce</c>.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence that emits one accumulated value on completion.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        public IObservable<TAccumulate> Aggregate<TAccumulate>(TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ReduceSignal<T, TAccumulate>(source, seed, accumulator);
        }

        /// <summary>Suppresses adjacent duplicate values. System.Reactive name for <c>Unique</c>.</summary>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctUntilChanged() =>
            DistinctUntilChanged(source, null);

        /// <summary>Suppresses adjacent duplicate values using the supplied comparer. System.Reactive name for <c>Unique</c>.</summary>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctUntilChanged(IEqualityComparer<T>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            comparer ??= EqualityComparer<T>.Default;
            return new UniqueSignal<T>(source, comparer);
        }

        /// <summary>Suppresses adjacent values with duplicate keys. System.Reactive name for <c>UniqueBy</c>.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctUntilChangedBy<TKey>(Func<T, TKey> keySelector) =>
            DistinctUntilChangedBy(source, keySelector, null);

        /// <summary>Suppresses adjacent values with duplicate keys using the supplied comparer. System.Reactive name for <c>UniqueBy</c>.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <param name="comparer">The comparer used to compare adjacent keys.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctUntilChangedBy<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            comparer ??= EqualityComparer<TKey>.Default;
            return new UniqueBySignal<T, TKey>(source, keySelector, comparer);
        }

        /// <summary>Drops every value, forwarding only the terminal notification. System.Reactive name for <c>IgnoreValues</c>.</summary>
        /// <returns>A sequence that forwards only completion or error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> IgnoreElements()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new IgnoreValuesSignal<T>(source);
        }

        /// <summary>Prepends values before the source sequence. System.Reactive name for <c>Prepend</c>.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
        public IObservable<T> StartWith(params T[] values) =>
            source.Prepend(values);

        /// <summary>Prepends values before the source sequence. System.Reactive name for <c>Prepend</c>.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
        public IObservable<T> StartWith(IEnumerable<T> values) =>
            source.Prepend(values);

        /// <summary>Collects values into time-windowed batches. System.Reactive name for <c>Collect</c>.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <returns>A sequence that emits non-empty batches of source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<IList<T>> Buffer(TimeSpan timeSpan) =>
            source.Collect(timeSpan);

        /// <summary>Collects values into time-windowed batches on the supplied scheduler.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <param name="scheduler">The scheduler used to schedule buffer flushes.</param>
        /// <returns>A sequence that emits non-empty batches of source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        public IObservable<IList<T>> Buffer(TimeSpan timeSpan, ISequencer scheduler) =>
            source.Collect(timeSpan, scheduler);

        /// <summary>Invokes an action when the subscription terminates or is disposed. System.Reactive name for <c>OnCleanup</c>.</summary>
        /// <param name="finallyAction">The action to invoke exactly once.</param>
        /// <returns>A sequence that mirrors the source and invokes <paramref name="finallyAction"/> on cleanup.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="finallyAction"/> is <see langword="null"/>.</exception>
        public IObservable<T> Finally(Action finallyAction) =>
            source.OnCleanup(finallyAction);

        /// <summary>Emits a value only after no newer value arrives within the quiet period. System.Reactive name for <c>Calm</c>.</summary>
        /// <param name="dueTime">The quiet period.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Throttle(TimeSpan dueTime) =>
            source.Calm(dueTime);

        /// <summary>Emits a value only after no newer value arrives within the scheduler quiet period. System.Reactive name for <c>Calm</c>.</summary>
        /// <param name="dueTime">The quiet period.</param>
        /// <param name="scheduler">The scheduler used to schedule quiet-period timers.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        public IObservable<T> Throttle(TimeSpan dueTime, ISequencer scheduler) =>
            source.Calm(dueTime, scheduler);

        /// <summary>Handles errors of the specified type by switching to a replacement sequence.</summary>
        /// <typeparam name="TException">The exception type to handle.</typeparam>
        /// <param name="handler">The function that produces a replacement sequence for handled errors.</param>
        /// <returns>A sequence that switches to the handler result for matching errors.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        public IObservable<T> Catch<TException>(Func<TException, IObservable<T>> handler)
            where TException : Exception =>
            source.Recover(handler);

        /// <summary>Projects each value to an inner sequence and merges the results. LINQ name for concurrent flattening.</summary>
        /// <typeparam name="TResult">The inner value type.</typeparam>
        /// <param name="selector">The function that projects each source value to an inner sequence.</param>
        /// <returns>A sequence containing the merged values of every inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectMany<TResult>(Func<T, IObservable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return source.Map(selector).Merge();
        }

        /// <summary>Projects each value to the same inner sequence and merges the results.</summary>
        /// <typeparam name="TResult">The inner value type.</typeparam>
        /// <param name="other">The inner sequence used for each source value.</param>
        /// <returns>A sequence containing the merged values of every inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectMany<TResult>(IObservable<TResult> other)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(other);

            return source.Map(_ => other).Merge();
        }

        /// <summary>Projects each value to an enumerable sequence and emits the projected values.</summary>
        /// <typeparam name="TResult">The projected value type.</typeparam>
        /// <param name="selector">The function that projects each source value to enumerable values.</param>
        /// <returns>A sequence containing the projected enumerable values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectMany<TResult>(Func<T, IEnumerable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new SelectManyEnumerableSignal<T, TResult>(source, selector);
        }

        /// <summary>Projects each value to an inner sequence and concurrently combines each pair with a result selector.</summary>
        /// <typeparam name="TCollection">The inner value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="collectionSelector">The function that projects each source value to an inner sequence.</param>
        /// <param name="resultSelector">The function that combines a source value with each inner value.</param>
        /// <returns>A sequence containing selected outer/inner combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectMany<TCollection, TResult>(
            Func<T, IObservable<TCollection>> collectionSelector,
            Func<T, TCollection, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(collectionSelector);

            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return source.Map(value => collectionSelector(value).Map(inner => resultSelector(value, inner))).Merge();
        }

        /// <summary>Merges this sequence with another observable sequence. System.Reactive name for <c>Blend</c>.</summary>
        /// <param name="second">The second sequence to merge.</param>
        /// <returns>A sequence containing values from both sources as they arrive.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        public IObservable<T> Merge(IObservable<T> second)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(second);

            return new[] { source, second }.Blend();
        }

        /// <summary>Concatenates two sequences. System.Reactive name for <c>Chain</c>.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="source"/> completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        public IObservable<T> Concat(IObservable<T> second)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(second);

            return new ChainSignal<T>(source, second);
        }
    }

    /// <summary>Null-filtering operator using System.Reactive vocabulary for an observable source of nullable reference values.</summary>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    extension<T>(IObservable<T?> source)
        where T : class
    {
        /// <summary>Filters out null values, emitting only non-null values. Familiar name for <c>KeepNotNull</c>.</summary>
        /// <returns>An observable sequence that emits only the non-null values from the source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> WhereNotNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new KeepNotNullSignal<T>(source);
        }
    }

    /// <summary>System.Reactive-named pairwise combination and timing operators for an observable source sequence.</summary>
    /// <param name="left">The left sequence.</param>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    extension<TLeft>(IObservable<TLeft> left)
    {
        /// <summary>Combines paired values from two sequences by index. System.Reactive name for <c>Pair</c>.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines paired values.</param>
        /// <returns>A sequence containing one result for each available value pair.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Zip<TRight, TResult>(IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return new RangeZipSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new ZipSignal<TLeft, TRight, TResult>(left, right, selector);
        }

        /// <summary>Combines the latest values once both sequences have produced a value. System.Reactive name for <c>SyncLatest</c>.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> CombineLatest<TRight, TResult>(IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return CreateRangeCombineLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new CombineLatestSignal<TLeft, TRight, TResult>(left, right, selector);
        }

        /// <summary>Combines each left value with the latest right value. System.Reactive name for <c>Latch</c>.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The sequence that supplies the latest value.</param>
        /// <param name="selector">The function that combines the left value with the latest right value.</param>
        /// <returns>A sequence containing selected left/latest-right combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> WithLatestFrom<TRight, TResult>(IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return CreateRangeWithLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new LatchSignal<TLeft, TRight, TResult>(left, right, selector);
        }

        /// <summary>Delays source notifications by the specified duration. System.Reactive name for <c>Shift</c>.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        public IObservable<TLeft> Delay(TimeSpan dueTime) =>
            Delay(left, dueTime, null);

        /// <summary>Delays source notifications by the specified duration on a sequencer. System.Reactive name for <c>Shift</c>.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        public IObservable<TLeft> Delay(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            scheduler ??= ThreadPoolSequencer.Instance;
            if (left is RangeSignal range && CanReadRangeAs(typeof(TLeft)))
            {
                return new ShiftedRangeSignal<TLeft>(range, Sequencer.Normalize(dueTime), scheduler);
            }

            return new ShiftSignal<TLeft>(left, dueTime, scheduler);
        }

        /// <summary>Fails the sequence if it does not terminate before the timeout. System.Reactive name for <c>Expire</c>.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        public IObservable<TLeft> Timeout(TimeSpan dueTime) =>
            Timeout(left, dueTime, null);

        /// <summary>Fails the sequence if it does not terminate before the sequencer timeout. System.Reactive name for <c>Expire</c>.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        public IObservable<TLeft> Timeout(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            scheduler ??= ThreadPoolSequencer.Instance;
            return new ExpireSignal<TLeft>(left, dueTime, scheduler);
        }

        /// <summary>Emits the most recent value at the end of each sampling period. System.Reactive name for <c>Probe</c>.</summary>
        /// <param name="interval">The sampling period.</param>
        /// <returns>A sequence containing the latest source value sampled at each period boundary.</returns>
        public IObservable<TLeft> Sample(TimeSpan interval) =>
            Sample(left, interval, null);

        /// <summary>Emits the most recent value at the end of each sampling period on a sequencer. System.Reactive name for <c>Probe</c>.</summary>
        /// <param name="interval">The sampling period.</param>
        /// <param name="scheduler">The sequencer used to schedule sampling.</param>
        /// <returns>A sequence containing the latest source value sampled at each period boundary.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="interval"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
        public IObservable<TLeft> Sample(TimeSpan interval, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(interval, TimeSpan.Zero);

            scheduler ??= ThreadPoolSequencer.Instance;
            return new ProbeSignal<TLeft>(left, interval, scheduler);
        }

        /// <summary>Resubscribes to the source after an error up to <paramref name="retryCount"/> times. System.Reactive name for <c>Reattempt</c>.</summary>
        /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
        /// <returns>A sequence that retries the source before forwarding the final error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="retryCount"/> is less than zero.</exception>
        public IObservable<TLeft> Retry(int retryCount)
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

            return new ReattemptSignal<TLeft>(left, retryCount);
        }

        /// <summary>Converts source values and terminal notifications into <see cref="Spark{T}"/> values. System.Reactive name for <c>Spark</c>.</summary>
        /// <returns>A sequence of spark values representing source notifications.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        public IObservable<Spark<TLeft>> Materialize()
        {
            ArgumentExceptionHelper.ThrowIfNull(left);

            return new SparkSignal<TLeft>(left);
        }
    }

    /// <summary>LINQ-named projection and filtering operators for an observable source sequence.</summary>
    /// <param name="source">An observable sequence of elements to project.</param>
    /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Projects each element of an observable sequence into a new form. LINQ name for <c>Map</c>.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each source element.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Select<TResult>(Func<TSource, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapSignal<TSource, TResult>(source, selector);
        }

        /// <summary>Projects each element and its zero-based index into a new form. LINQ name for <c>MapIndexed</c>.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element and its index.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and index.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Select<TResult>(Func<TSource, int, TResult> selector) =>
            source.MapIndexed(selector);

        /// <summary>Projects each element into a new form using external state passed to the selector. State-carrying name for <c>MapWith</c>.</summary>
        /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="state">The state to pass to the selector function.</param>
        /// <param name="selector">A transform function to apply to each source element along with the state.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and the state.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectWith<TState, TResult>(TState state, Func<TState, TSource, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapWithSignal<TSource, TState, TResult>(source, state, selector);
        }

        /// <summary>Filters an observable sequence to elements that satisfy a predicate. LINQ name for <c>Keep</c>.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence containing the elements that satisfy <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> Where(Func<TSource, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new KeepSignal<TSource>(source, predicate);
        }

        /// <summary>Filters elements using a predicate that uses external state. State-carrying name for <c>KeepWith</c>.</summary>
        /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
        /// <param name="state">The state value to pass to the predicate for each element.</param>
        /// <param name="predicate">A function to test each element along with the state.</param>
        /// <returns>An observable sequence containing only the elements that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> WhereWith<TState>(TState state, Func<TState, TSource, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new KeepWithSignal<TSource, TState>(source, state, predicate);
        }
    }

    /// <summary>System.Reactive-named combining operators for an observable source of tasks.</summary>
    /// <param name="sources">The outer sequence of task sources.</param>
    /// <typeparam name="T">The task result type.</typeparam>
    extension<T>(IObservable<Task<T>> sources)
    {
        /// <summary>Subscribes to task results one at a time in source order. System.Reactive name for <c>Chain</c>.</summary>
        /// <returns>A sequence that emits each task result after the previous task signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Concat()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new TaskChainSignal<T>(sources);
        }
    }
}
