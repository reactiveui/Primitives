// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Additional parity operators that preserve Primitives naming while covering common reactive contracts.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named conversion operators for an enumerable source.</summary>
    /// <param name="values">The values to enumerate.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IEnumerable<T> values)
    {
        /// <summary>Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.</summary>
        /// <returns>A signal that emits the enumerable values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> ToObservable()
        {
            ArgumentExceptionHelper.ThrowIfNull(values);

            return new FromEnumerableSignal<T>(values);
        }

        /// <summary>Converts an enumerable sequence to a Primitives signal on the supplied scheduler.</summary>
        /// <param name="scheduler">The scheduler used to enumerate and emit the values.</param>
        /// <returns>A signal that emits the enumerable values on the supplied scheduler.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        public IObservable<T> ToObservable(ISequencer scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(values);

            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            return scheduler == Sequencer.Immediate || scheduler == Sequencer.CurrentThread ? new FromEnumerableSignal<T>(values) : new ScheduledEnumerableSignal<T>(values, scheduler);
        }

        /// <summary>Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.</summary>
        /// <param name="cancellationToken">The token used to stop enumeration.</param>
        /// <returns>A signal that emits the enumerable values until enumeration completes or cancellation is requested.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> ToObservable(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(values);

            return cancellationToken.CanBeCanceled
                ? new FromEnumerableSignal<T>(values, cancellationToken)
                : new FromEnumerableSignal<T>(values);
        }
    }

    /// <summary>System.Reactive-named parity operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Prepends a value before the source sequence. Alias of <c>Prepend</c> using Primitives vocabulary.</summary>
        /// <param name="value">The value to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="value"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> Lead(T value)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new LeadSignal<T>(source, value);
        }

        /// <summary>Prepends a value before the source sequence.</summary>
        /// <param name="value">The value to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="value"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> Prepend(T value)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new PrependSignal<T>(source, value);
        }

        /// <summary>Prepends values before the source sequence.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="values"/> is <see langword="null"/>.</exception>
        public IObservable<T> Prepend(params T[] values)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(values);

            if (values.Length == 0)
            {
                return source;
            }

            return values.Length == 1 ? new PrependSignal<T>(source, values[0]) : new StartWithEnumerableSignal<T>(source, values);
        }

        /// <summary>Prepends values before the source sequence.</summary>
        /// <param name="values">The values to emit before the source.</param>
        /// <returns>A sequence that emits <paramref name="values"/> before the source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="values"/> is <see langword="null"/>.</exception>
        public IObservable<T> Prepend(IEnumerable<T> values)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(values);

            return new StartWithEnumerableSignal<T>(source, values);
        }

        /// <summary>Appends a value after the source sequence completes.</summary>
        /// <param name="value">The value to emit after the source completes.</param>
        /// <returns>A sequence that emits the source values followed by <paramref name="value"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> Append(T value)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is PrependSignal<T> prepended ? new PrependAppendSignal<T>(prepended.GetSource(), prepended.GetValue(), value) : new AppendSignal<T>(source, value);
        }

        /// <summary>Returns the source as an observable. This is an identity adapter for BCL observable sources.</summary>
        /// <returns>The supplied source sequence.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0001:Simplify Names", Justification = "The argument validation uses ArgumentExceptionHelper")]
        public IObservable<T> AsObservable() => source ?? throw new ArgumentNullException(nameof(source));

        /// <summary>Schedules observer notifications on the supplied scheduler using the System.Reactive operator name.</summary>
        /// <param name="scheduler">The sequencer used to deliver observer notifications.</param>
        /// <returns>The source sequence when <paramref name="scheduler"/> is immediate; otherwise a sequence observed on the sequencer.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        public IObservable<T> ObserveOn(ISequencer scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            return scheduler == Sequencer.Immediate ? source : new WitnessOnSignal<T>(source, scheduler);
        }

        /// <summary>Schedules source subscription on the supplied sequencer.</summary>
        /// <param name="scheduler">The sequencer used to perform subscription.</param>
        /// <returns>A sequence that subscribes to the receiver on <paramref name="scheduler"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        public IObservable<T> SubscribeOn(ISequencer scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            return new SubscribeOnSignal<T>(source, scheduler);
        }

        /// <summary>Alias for <c>DelayStart</c> using the System.Reactive operator name.</summary>
        /// <param name="dueTime">The delay before subscribing to the source.</param>
        /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DelaySubscription(TimeSpan dueTime)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T))
                ? new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), ThreadPoolSequencer.Instance)
                : new DelayStartSignal<T>(source, dueTime, ThreadPoolSequencer.Instance);
        }

        /// <summary>Alias for <c>DelayStart</c> using the System.Reactive operator name.</summary>
        /// <param name="dueTime">The delay before subscribing to the source.</param>
        /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
        /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DelaySubscription(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= ThreadPoolSequencer.Instance;
            return source is RangeSignal range && CanReadRangeAs(typeof(T))
                ? new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), scheduler)
                : new DelayStartSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Invokes actions for each element in the observable sequence, for error notifications, and for successful completion.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <param name="onError">Action to invoke upon exceptional termination of the observable sequence.</param>
        /// <param name="onCompleted">Action to invoke upon graceful termination of the observable sequence.</param>
        /// <returns>The source sequence with the side-effecting behavior applied.</returns>
        public IObservable<T> Tap(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onError);

            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return new TapSignal<T>(source, onNext, onError, onCompleted);
        }

        /// <summary>Ignores all source values and only forwards terminal messages.</summary>
        /// <returns>A sequence that forwards only error and completion notifications.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> IgnoreValues()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new IgnoreValuesSignal<T>(source);
        }

        /// <summary>Emits the supplied value if the source completes without values.</summary>
        /// <returns>A sequence that emits <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DefaultIfEmpty()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is ImmutableEmptySignal<T>)
            {
                return new ImmediateReturnSignal<T>(default!);
            }

            return source is RangeSignal { Count: > 0 } ? source : new DefaultIfEmptySignal<T>(source, default!);
        }

        /// <summary>Emits the supplied value if the source completes without values.</summary>
        /// <param name="defaultValue">The value to emit when the source is empty.</param>
        /// <returns>A sequence that emits <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DefaultIfEmpty(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is ImmutableEmptySignal<T>)
            {
                return new ImmediateReturnSignal<T>(defaultValue);
            }

            return source is RangeSignal { Count: > 0 } ? source : new DefaultIfEmptySignal<T>(source, defaultValue);
        }

        /// <summary>Suppresses duplicate keys according to the comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <returns>A sequence containing the first value for each observed key.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new DistinctBySignal<T, TKey>(source, keySelector, null);
        }

        /// <summary>Suppresses duplicate keys according to the comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <param name="comparer">The comparer used to identify duplicate keys.</param>
        /// <returns>A sequence containing the first value for each observed key.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> DistinctBy<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new DistinctBySignal<T, TKey>(source, keySelector, comparer);
        }

        /// <summary>Suppresses adjacent duplicate keys according to the comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> UniqueBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new UniqueBySignal<T, TKey>(source, keySelector, EqualityComparer<TKey>.Default);
        }

        /// <summary>Suppresses adjacent duplicate keys according to the comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">The function that selects the comparison key.</param>
        /// <param name="comparer">The comparer used to compare adjacent keys.</param>
        /// <returns>A sequence with adjacent duplicate keys removed.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public IObservable<T> UniqueBy<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            comparer ??= EqualityComparer<TKey>.Default;
            return new UniqueBySignal<T, TKey>(source, keySelector, comparer);
        }

        /// <summary>Emits values while the predicate remains true, then completes.</summary>
        /// <param name="predicate">The function that determines whether to keep taking values.</param>
        /// <returns>A sequence that emits the leading values that satisfy <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<T> TakeWhile(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new TakeWhileSignal<T>(source, predicate);
        }

        /// <summary>Skips values while the predicate remains true, then mirrors the remaining source.</summary>
        /// <param name="predicate">The function that determines whether to keep skipping values.</param>
        /// <returns>A sequence that emits values after the leading values that satisfy <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<T> SkipWhile(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new SkipWhileSignal<T>(source, predicate);
        }

        /// <summary>Projects each source value to an inner signal and concatenates all inner values.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="selector">The function that projects each source value to an inner sequence.</param>
        /// <returns>A sequence containing the concatenated inner values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Bind<TResult>(Func<T, IObservable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            ArgumentExceptionHelper.ThrowIfNull(source);

            return new FlatMapSignal<T, TResult>(source, selector);
        }

        /// <summary>Projects each source value to an inner signal and concatenates all inner values.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="selector">The function that projects each source value to an inner sequence.</param>
        /// <returns>A sequence containing the concatenated inner values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> FlatMap<TResult>(Func<T, IObservable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new FlatMapSignal<T, TResult>(source, selector);
        }

        /// <summary>Projects each source value to an inner signal and maps outer/inner values with a result selector.</summary>
        /// <typeparam name="TCollection">The inner value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="collectionSelector">The function that projects each source value to an inner sequence.</param>
        /// <param name="resultSelector">The function that combines source and inner values.</param>
        /// <returns>A sequence containing selected outer/inner combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> FlatMap<TCollection, TResult>(
            Func<T, IObservable<TCollection>> collectionSelector,
            Func<T, TCollection, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(collectionSelector);

            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new FlatMapResultSignal<T, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        /// <summary>Projects each value into an enumerable and emits every projected item.</summary>
        /// <typeparam name="TResult">The projected item type.</typeparam>
        /// <param name="selector">The projection that returns items for each source value.</param>
        /// <returns>A signal that emits every item returned by <paramref name="selector"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> FlatMapValues<TResult>(Func<T, IEnumerable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new SelectManyEnumerableSignal<T, TResult>(source, selector);
        }

        /// <summary>Counts the source values as an <see cref="int"/>.</summary>
        /// <returns>A sequence that emits the number of source values when the source completes.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<int> Count()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CountSignal<T>(source);
        }

        /// <summary>Counts source values that satisfy the predicate as an <see cref="int"/>.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <returns>A sequence that emits the matching value count when the source completes.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<int> Count(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CountPredicateSignal<T>(source, predicate);
        }

        /// <summary>Counts the source values as an <see cref="long"/>.</summary>
        /// <returns>A sequence that emits the number of source values when the source completes.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<long> LongCount()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new LongCountSignal<T>(source);
        }

        /// <summary>Counts source values that satisfy the predicate as an <see cref="long"/>.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <returns>A sequence that emits the matching value count when the source completes.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<long> LongCount(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            ArgumentExceptionHelper.ThrowIfNull(source);

            return new LongCountPredicateSignal<T>(source, predicate);
        }

        /// <summary>Emits true when any value is present.</summary>
        /// <returns>A sequence that emits whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<bool> Any()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AnySignal<T>(source);
        }

        /// <summary>Emits true when any value satisfies the predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <returns>A sequence that emits whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<bool> Any(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new AnyPredicateSignal<T>(source, predicate);
        }

        /// <summary>Emits true when every value satisfies the predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <returns>A sequence that emits whether every source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<bool> All(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new AllPredicateSignal<T>(source, predicate);
        }

        /// <summary>Emits true when the source contains the requested value.</summary>
        /// <param name="value">The value to locate.</param>
        /// <returns>A sequence that emits whether the source contains <paramref name="value"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<bool> Contains(T value)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ContainsSignal<T>(source, value, EqualityComparer<T>.Default);
        }

        /// <summary>Emits true when the source contains the requested value.</summary>
        /// <param name="value">The value to locate.</param>
        /// <param name="comparer">The comparer used to compare source values.</param>
        /// <returns>A sequence that emits whether the source contains <paramref name="value"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<bool> Contains(T value, IEqualityComparer<T>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            comparer ??= EqualityComparer<T>.Default;
            return new ContainsSignal<T>(source, value, comparer);
        }

        /// <summary>Emits true when the source completes without values.</summary>
        /// <returns>A sequence that emits whether the source completed without values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<bool> IsEmpty()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new IsEmptySignal<T>(source);
        }

        /// <summary>Emits values from source after delaying subscription by the due time.</summary>
        /// <param name="dueTime">The delay before subscribing to the source.</param>
        /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DelayStart(TimeSpan dueTime)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var scheduler = ThreadPoolSequencer.Instance;
            var normalizedDueTime = Sequencer.Normalize(dueTime);
            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new ShiftedRangeSignal<T>(range, normalizedDueTime, scheduler) : new DelayStartSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Emits values from source after delaying subscription by the due time.</summary>
        /// <param name="dueTime">The delay before subscribing to the source.</param>
        /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
        /// <returns>A sequence that subscribes to the source after <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> DelayStart(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var sequencer = scheduler ?? ThreadPoolSequencer.Instance;
            var normalizedDueTime = Sequencer.Normalize(dueTime);
            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new ShiftedRangeSignal<T>(range, normalizedDueTime, sequencer) : new DelayStartSignal<T>(source, dueTime, sequencer);
        }

        /// <summary>Emits only the most recent value after the quiet period elapses.</summary>
        /// <param name="dueTime">The quiet period before emitting the latest value.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> Calm(TimeSpan dueTime)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CalmSignal<T>(source, dueTime, ThreadPoolSequencer.Instance);
        }

        /// <summary>Emits only the most recent value after the quiet period elapses.</summary>
        /// <param name="dueTime">The quiet period before emitting the latest value.</param>
        /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<T> Calm(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= ThreadPoolSequencer.Instance;
            return new CalmSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Emits only the most recent value after the quiet period elapses.</summary>
        /// <param name="dueTime">The quiet period before emitting the latest value.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        public IObservable<T> Stabilize(TimeSpan dueTime)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var scheduler = ThreadPoolSequencer.Instance;
            return new CalmSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Emits only the most recent value after the quiet period elapses.</summary>
        /// <param name="dueTime">The quiet period before emitting the latest value.</param>
        /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
        /// <returns>A sequence that emits the latest value after each quiet period.</returns>
        public IObservable<T> Stabilize(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var sequencer = scheduler ?? ThreadPoolSequencer.Instance;
            return new CalmSignal<T>(source, dueTime, sequencer);
        }

        /// <summary>Emits the latest source value whenever the sampling period ticks.</summary>
        /// <param name="period">The interval between sampling ticks.</param>
        /// <returns>A sequence that emits the latest source value on each sampling tick.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="period"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
        public IObservable<T> Probe(TimeSpan period)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

            return new ProbeSignal<T>(source, period, ThreadPoolSequencer.Instance);
        }

        /// <summary>Emits the latest source value whenever the sampling period ticks.</summary>
        /// <param name="period">The interval between sampling ticks.</param>
        /// <param name="scheduler">The sequencer used to schedule sampling ticks.</param>
        /// <returns>A sequence that emits the latest source value on each sampling tick.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="period"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
        public IObservable<T> Probe(TimeSpan period, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

            scheduler ??= ThreadPoolSequencer.Instance;
            return new ProbeSignal<T>(source, period, scheduler);
        }

        /// <summary>Annotates values with their scheduler timestamp.</summary>
        /// <returns>A sequence containing each value with its timestamp.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<Moment<T>> Timestamp()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T))
                ? new TimestampRangeSignal<T>(range, Sequencer.Immediate)
                : new MapWithSignal<T, ISequencer, Moment<T>>(source, Sequencer.Immediate, CreateMoment);
        }

        /// <summary>Annotates values with their scheduler timestamp.</summary>
        /// <param name="scheduler">The sequencer that supplies timestamps.</param>
        /// <returns>A sequence containing each value with its timestamp.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<Moment<T>> Timestamp(ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= Sequencer.Immediate;
            return source is RangeSignal range && CanReadRangeAs(typeof(T))
                ? new TimestampRangeSignal<T>(range, scheduler)
                : new MapWithSignal<T, ISequencer, Moment<T>>(source, scheduler, CreateMoment);
        }

        /// <summary>Annotates each value with the elapsed scheduler time since the previous value.</summary>
        /// <returns>A sequence containing each value with its elapsed interval since the previous value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<TimeInterval<T>> TimeInterval()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new TimeIntervalRangeSignal<T>(range, Sequencer.Immediate) : new TimeIntervalSignal<T>(source, Sequencer.Immediate);
        }

        /// <summary>Annotates each value with the elapsed scheduler time since the previous value.</summary>
        /// <param name="scheduler">The sequencer that supplies timestamps.</param>
        /// <returns>A sequence containing each value with its elapsed interval since the previous value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public IObservable<TimeInterval<T>> TimeInterval(ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= Sequencer.Immediate;
            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new TimeIntervalRangeSignal<T>(range, scheduler) : new TimeIntervalSignal<T>(source, scheduler);
        }

        /// <summary>Combines latest values from both sources. Alias for latest-fusion vocabulary.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException">The left sequence, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> PairLatest<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange
                ? new RangeSyncLatestSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
                : new CombineLatestSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Alias for <c>PairLatest</c>.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException">The left sequence, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> FuseLatest<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(source);

            return typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange
                ? new RangeSyncLatestSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
                : new CombineLatestSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Waits for both sources to complete and emits one value from their last elements when both produced at least one value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the final values.</param>
        /// <returns>A sequence that emits one selected value after both sources complete.</returns>
        /// <exception cref="ArgumentNullException">The left sequence, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> ForkJoin<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange
                ? new RangeForkJoinSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
                : new ForkJoinSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Awaits the first source value.</summary>
        /// <returns>A task that completes with the first source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> FirstAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? Task.FromResult((T)(object)range.Start) : source.FirstOrDefaultCoreAsync(false, default!);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <returns>A task that completes with the first source value, or <see langword="default"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> FirstOrDefaultAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? Task.FromResult((T)(object)range.Start) : source.FirstOrDefaultCoreAsync(true, default!);
        }

        /// <summary>Awaits the first source value, returning a default value when the source is empty.</summary>
        /// <param name="defaultValue">The value to return when the source is empty.</param>
        /// <returns>A task that completes with the first source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T> FirstOrDefaultAsync(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? Task.FromResult((T)(object)range.Start) : source.FirstOrDefaultCoreAsync(true, defaultValue);
        }

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <returns>A task that completes with the final source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> ToTask() => Signal.ToTask(source);

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
        /// <returns>A task that completes with the final source value.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The source completes without producing a value.</exception>
        public Task<T> ToTask(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return Signal.ToTask(source, cancellationToken);
        }

        /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
        /// <returns>A task that completes with the final source value.</returns>
        public Task<T> LastAsync() => Signal.ToTask(source);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <see langword="default"/> when the source is empty.</summary>
        /// <returns>A task that completes with the final source value, or <see langword="default"/> when the source is empty.</returns>
        public Task<T> LastOrDefaultAsync() =>
            source.LastOrDefaultAsync(default!);

        /// <summary>Awaits source completion and returns the last value produced by the source, or <paramref name="defaultValue"/> when the source is empty.</summary>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <returns>A task that completes with the final source value, or <paramref name="defaultValue"/> when the source is empty.</returns>
        public Task<T> LastOrDefaultAsync(T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.DefaultIfEmpty(defaultValue).ToTask();
        }

        /// <summary>Awaits the source count as a task.</summary>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<int> CountAsync() => CountTaskAsync(source, CancellationToken.None);

        /// <summary>Awaits the source count as a task.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the number of source values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<int> CountAsync(CancellationToken cancellationToken) => CountTaskAsync(source, cancellationToken);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public Task<int> CountAsync(Func<T, bool> predicate) => CountTaskAsync(source, predicate, CancellationToken.None);

        /// <summary>Awaits the source predicate count as a task.</summary>
        /// <param name="predicate">The function that identifies values to count.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with the matching value count.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public Task<int> CountAsync(Func<T, bool> predicate, CancellationToken cancellationToken) => CountTaskAsync(source, predicate, cancellationToken);

        /// <summary>Awaits whether any value is present.</summary>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<bool> AnyAsync() => AnyTaskAsync(source, CancellationToken.None);

        /// <summary>Awaits whether any value is present.</summary>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether the source produced any values.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<bool> AnyAsync(CancellationToken cancellationToken) => AnyTaskAsync(source, cancellationToken);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public Task<bool> AnyAsync(Func<T, bool> predicate) => AnyTaskAsync(source, predicate, CancellationToken.None);

        /// <summary>Awaits whether any value matches a predicate.</summary>
        /// <param name="predicate">The function that tests each value.</param>
        /// <param name="cancellationToken">The token used to cancel the task.</param>
        /// <returns>A task that completes with whether any source value satisfies <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public Task<bool> AnyAsync(Func<T, bool> predicate, CancellationToken cancellationToken) => AnyTaskAsync(source, predicate, cancellationToken);

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<T[]> CollectArrayAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
            {
                if (typeof(T) == typeof(int))
                {
                    var integers = new int[range.Count];
                    for (var i = 0; i < integers.Length; i++)
                    {
                        integers[i] = range.Start + i;
                    }

                    return Task.FromResult((T[])(object)integers);
                }

                var boxed = new T[range.Count];
                for (var i = 0; i < boxed.Length; i++)
                {
                    boxed[i] = (T)(object)(range.Start + i);
                }

                return Task.FromResult(boxed);
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            if (TryCollectArrayFromAsyncEnumerable(source, out var asyncEnumerableTask))
            {
                return asyncEnumerableTask;
            }
#endif

            TaskCompletionSource<T[]> completion = new();
            List<T> values = [];
            _ = source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult([.. values]));
            return completion.Task;
        }

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A sequence that emits a single array containing all source values.</returns>
        public IObservable<T[]> ToArray()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new RangeArraySignal<T>(range) : new CollectArraySignal<T>(source);
        }

        /// <summary>Collects all values into an array task.</summary>
        /// <returns>A task that completes with all source values in an array.</returns>
        public Task<T[]> ToArrayAsync() => source.CollectArrayAsync();

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        /// <exception cref="ArgumentNullException">The receiver sequence is <see langword="null"/>.</exception>
        public Task<IList<T>> CollectListAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
            {
                if (typeof(T) == typeof(int))
                {
                    List<int> integers = new(range.Count);
                    for (var i = 0; i < range.Count; i++)
                    {
                        integers.Add(range.Start + i);
                    }

                    return Task.FromResult((IList<T>)(object)integers);
                }

                List<T> rangeValues = new(range.Count);
                for (var i = 0; i < range.Count; i++)
                {
                    rangeValues.Add((T)(object)(range.Start + i));
                }

                return Task.FromResult<IList<T>>(rangeValues);
            }

            TaskCompletionSource<IList<T>> completion = new();
            List<T> values = [];
            _ = source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult(values));
            return completion.Task;
        }

        /// <summary>Collects all values into a list.</summary>
        /// <returns>A sequence that emits a single list containing all source values.</returns>
        public IObservable<IList<T>> ToList()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source is RangeSignal range && CanReadRangeAs(typeof(T)) ? new RangeListSignal<T>(range) : new CollectListSignal<T>(source);
        }

        /// <summary>Collects all values into a list task.</summary>
        /// <returns>A task that completes with all source values in a list.</returns>
        public Task<IList<T>> ToListAsync() => source.CollectListAsync();

        /// <summary>Awaits the first source value and applies the configured empty-source behavior.</summary>
        /// <param name="hasDefault">A value indicating whether to use <paramref name="defaultValue"/> when the source is empty.</param>
        /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        private Task<T> FirstOrDefaultCoreAsync(bool hasDefault, T defaultValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            TaskCompletionSource<T> completion = new();
            var seen = false;
            _ = source.Subscribe(
                value =>
                {
                    if (seen)
                    {
                        return;
                    }

                    seen = true;
                    _ = completion.TrySetResult(value);
                },
                error => completion.TrySetException(error),
                () =>
                {
                    if (seen)
                    {
                        return;
                    }

                    if (hasDefault)
                    {
                        _ = completion.TrySetResult(defaultValue);
                    }
                    else
                    {
                        _ = completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                    }
                });
            return completion.Task;
        }
    }

    /// <summary>Task-compatibility helpers for migrations from System.Reactive.</summary>
    /// <param name="task">The task.</param>
    /// <typeparam name="T">The task result type.</typeparam>
    extension<T>(Task<T> task)
    {
        /// <summary>Converts a task to an observable sequence that emits the task result.</summary>
        /// <returns>An observable sequence that emits the completed task result or faults with the task error.</returns>
        /// <exception cref="ArgumentNullException">The receiver task is <see langword="null"/>.</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S4462:Calls to \"async\" methods should not be blocking",
            Justification = "Synchronous read is limited to the already-completed task fast path.")]
        public IObservable<T> ToObservable()
        {
            ArgumentExceptionHelper.ThrowIfNull(task);

            if (task.Status == TaskStatus.RanToCompletion)
            {
                return new ImmediateReturnSignal<T>(task.Result);
            }

            if (task.IsCanceled)
            {
                return new ImmediateThrowSignal<T>(new TaskCanceledException(task));
            }

            return task.IsFaulted ? new ImmediateThrowSignal<T>(task.Exception!.InnerException ?? task.Exception) : new TaskInstanceSignal<T>(task);
        }

        /// <summary>Identity helper that keeps source-compatible <c>FirstAsync().ToTask()</c> migrations compiling.</summary>
        /// <returns>The supplied task.</returns>
        /// <exception cref="ArgumentNullException">The receiver task is <see langword="null"/>.</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0001:Simplify Names", Justification = "The argument validation uses ArgumentExceptionHelper")]
        public Task<T> ToTask() => task ?? throw new ArgumentNullException(nameof(task));
    }

    /// <summary>Stamps a value with the supplied scheduler's current time. A non-capturing selector reused by <c>Timestamp</c> via <c>MapWith</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="scheduler">The sequencer that supplies the timestamp.</param>
    /// <param name="value">The value to stamp.</param>
    /// <returns>The value paired with the scheduler timestamp.</returns>
    private static Moment<T> CreateMoment<T>(ISequencer scheduler, T value) => new(value, scheduler.Now);
}
