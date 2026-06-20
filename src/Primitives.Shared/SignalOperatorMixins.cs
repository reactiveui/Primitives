// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Additional ReactiveUI.Primitives operator surface using distinct Primitives vocabulary.</summary>
public static partial class LinqExtensions
{
    /// <summary>Signal-conversion operators for an enumerable source.</summary>
    /// <param name="values">The values to enumerate.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IEnumerable<T> values)
    {
        /// <summary>Converts an enumerable sequence to a signal.</summary>
        /// <returns>A signal that emits the enumerable values.</returns>
        public IObservable<T> ToSignal() => Signal.FromEnumerable(values);

        /// <summary>Converts an enumerable sequence to a signal that observes cancellation.</summary>
        /// <param name="cancellationToken">The token used to stop enumeration.</param>
        /// <returns>A signal that emits the enumerable values until enumeration completes or cancellation is requested.</returns>
        public IObservable<T> ToSignal(CancellationToken cancellationToken) =>
            Signal.FromEnumerable(values, cancellationToken);
    }

    /// <summary>Combining operators for an observable source of inner observable sequences.</summary>
    /// <param name="sources">The outer sequence of inner sequences.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<IObservable<T>> sources)
    {
        /// <summary>Subscribes to all inner sequences and forwards their values as they arrive.</summary>
        /// <returns>A sequence containing values from all inner sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Blend()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new BlendSignal<T>(sources);
        }

        /// <summary>Mirrors the first inner sequence to produce any notification.</summary>
        /// <returns>A sequence that mirrors the winning inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Race()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new RaceSignal<T>(sources);
        }

        /// <summary>Subscribes to inner sequences one at a time in source order.</summary>
        /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Chain()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new ChainSignal<T>(sources);
        }

        /// <summary>Switches to the most recent inner sequence.</summary>
        /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> SwitchTo()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            if (TryCreateSynchronousSwitchRangeSignal(sources, out var rangeSignal))
            {
                return rangeSignal;
            }

            return new SwitchSignal<T>(sources);
        }
    }

    /// <summary>Notification-materialization operator for an observable source of spark values.</summary>
    /// <param name="source">The spark sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<Spark<T>> source)
    {
        /// <summary>Converts <see cref="Spark{T}"/> values back into observer notifications.</summary>
        /// <returns>A sequence represented by the supplied spark values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Unspark()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new UnsparkSignal<T>(source);
        }
    }

    /// <summary>Projection, filtering, combination, and timing operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Projects each element of an observable sequence into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
        /// source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Map<TResult>(Func<T, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapSignal<T, TResult>(source, selector);
        }

        /// <summary>Projects each element and its zero-based index into a new form.</summary>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="selector">A transform function to apply to each source element and its index.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and index.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> MapIndexed<TResult>(Func<T, int, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapIndexedSignal<T, TResult>(source, selector);
        }

        /// <summary>
        /// Projects each element of an observable sequence into a new form by incorporating state that is passed to the
        /// selector function.
        /// </summary>
        /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="state">The state to pass to the selector function.</param>
        /// <param name="selector">A transform function to apply to each source element along with the state.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform function on each element of the
        /// source along with the state.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> MapWith<TState, TResult>(TState state, Func<TState, T, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapWithSignal<T, TState, TResult>(source, state, selector);
        }

        /// <summary>Filters an observable sequence to include only elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the input sequence that satisfy the condition specified by
        /// <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<T> Keep(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new KeepSignal<T>(source, predicate);
        }

        /// <summary>Filters elements from an observable sequence based on a predicate that uses external state.</summary>
        /// <typeparam name="TState">The type of the state parameter passed to the predicate.</typeparam>
        /// <param name="state">The state value to pass to the predicate for each element.</param>
        /// <param name="predicate">A function to test each element along with the state; returns <see langword="true"/> to keep the element, <see
        /// langword="false"/> to filter it out.</param>
        /// <returns>An observable sequence containing only the elements from the source sequence that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<T> KeepWith<TState>(TState state, Func<TState, T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new KeepWithSignal<T, TState>(source, state, predicate);
        }

        /// <summary>Invokes an action for each value while preserving the original sequence.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> Tap(Action<T> onNext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            return new TapSignal<T>(source, onNext, static _ => { }, static () => { });
        }

        /// <summary>Invokes a stateful action for each value while preserving the original sequence.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> TapWith<TState>(TState state, Action<TState, T> onNext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onNext);

            return new TapWithSignal<T, TState>(source, state, onNext);
        }

        /// <summary>Emits the accumulated state after each source value.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence of intermediate accumulated values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        public IObservable<TAccumulate> Fold<TAccumulate>(TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new FoldSignal<T, TAccumulate>(source, seed, accumulator);
        }

        /// <summary>Emits the final accumulated state when the source completes.</summary>
        /// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
        /// <param name="seed">The initial accumulated value.</param>
        /// <param name="accumulator">The function that combines the current state with the next source value.</param>
        /// <returns>A sequence that emits one accumulated value on completion.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
        public IObservable<TAccumulate> Reduce<TAccumulate>(TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ReduceSignal<T, TAccumulate>(source, seed, accumulator);
        }

        /// <summary>Emits at most <paramref name="count"/> values before completing.</summary>
        /// <param name="count">The maximum number of values to emit.</param>
        /// <returns>A sequence containing at most <paramref name="count"/> source values.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="count"/> is less than zero.</exception>
        public IObservable<T> Take(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

            if (source is LoopSignal<T> loop)
            {
                return count == 0 ? Signal.None<T>() : new RepeatSignal<T>(loop.Value, count);
            }

            return new TakeSignal<T>(source, count);
        }

        /// <summary>Forwards source values until <paramref name="other"/> emits a value; other completion without a value does not stop the source.</summary>
        /// <typeparam name="TOther">The cancellation value type.</typeparam>
        /// <param name="other">The observable that stops the source when it emits.</param>
        /// <returns>An observable that completes when the source completes or <paramref name="other"/> emits.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="other"/> is <see langword="null"/>.</exception>
        public IObservable<T> TakeUntil<TOther>(IObservable<TOther> other)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(other);

            return new TakeUntilSignal<T, TOther>(source, other);
        }

        /// <summary>Skips the first <paramref name="count"/> source values.</summary>
        /// <param name="count">The number of values to skip.</param>
        /// <returns>A sequence containing source values after the skipped prefix.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="count"/> is less than zero.</exception>
        public IObservable<T> Skip(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

            return new SkipSignal<T>(source, count);
        }

        /// <summary>Suppresses values that have already been observed.</summary>
        /// <returns>A sequence containing the first occurrence of each source value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Distinct() =>
            source.Distinct(null);

        /// <summary>Suppresses values that have already been observed using the supplied comparer.</summary>
        /// <param name="comparer">The comparer used to identify duplicate values.</param>
        /// <returns>A sequence containing the first occurrence of each source value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Distinct(IEqualityComparer<T>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new DistinctSignal<T>(source, comparer);
        }

        /// <summary>Suppresses adjacent duplicate values.</summary>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Unique() =>
            source.Unique(null);

        /// <summary>Suppresses adjacent duplicate values using the supplied comparer.</summary>
        /// <param name="comparer">The comparer used to compare adjacent values.</param>
        /// <returns>A sequence with adjacent duplicates removed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> Unique(IEqualityComparer<T>? comparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            comparer ??= EqualityComparer<T>.Default;
            return new UniqueSignal<T>(source, comparer);
        }

        /// <summary>Converts source values and terminal notifications into <see cref="Spark{T}"/> values.</summary>
        /// <returns>A sequence of spark values representing source notifications; terminal sparks are followed by completion.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<Spark<T>> Spark()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new SparkSignal<T>(source);
        }

        /// <summary>Concatenates two sequences.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="source"/> completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        public IObservable<T> Chain(IObservable<T> second)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(second);

            return new ChainSignal<T>(source, second);
        }

        /// <summary>Combines paired values from two sequences, completing when no more pairs can be formed.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines paired values.</param>
        /// <returns>A sequence containing one result for each available value pair.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Pair<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return new RangeZipSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new ZipSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Combines the latest values after both sequences have produced at least one value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The right sequence.</param>
        /// <param name="selector">The function that combines the latest values.</param>
        /// <returns>A sequence containing selected latest-value combinations.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return CreateRangeCombineLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new CombineLatestSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Combines each left value with the latest right value after the right sequence has produced a value.</summary>
        /// <typeparam name="TRight">The right value type.</typeparam>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <param name="right">The sequence that supplies the latest value.</param>
        /// <param name="selector">The function that combines the left value with the latest right value.</param>
        /// <returns>A sequence containing selected left/latest-right combinations.</returns>
        /// <remarks>Left values produced before the first right value are ignored.</remarks>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Latch<TRight, TResult>(IObservable<TRight> right, Func<T, TRight, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(right);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            if (typeof(T) == typeof(int) && typeof(TRight) == typeof(int) && source is RangeSignal leftRange && right is RangeSignal rightRange)
            {
                return CreateRangeWithLatestSignal(leftRange, rightRange, (Func<int, int, TResult>)(object)selector);
            }

            return new LatchSignal<T, TRight, TResult>(source, right, selector);
        }

        /// <summary>Resubscribes to the source after an error up to <paramref name="retryCount"/> times.</summary>
        /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
        /// <returns>A sequence that retries the source before forwarding the final error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="retryCount"/> is less than zero.</exception>
        public IObservable<T> Reattempt(int retryCount)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

            return new ReattemptSignal<T>(source, retryCount);
        }

        /// <summary>Recovers from errors by switching to a handler-provided sequence.</summary>
        /// <param name="handler">The function that creates the recovery sequence for an error.</param>
        /// <returns>A sequence that continues with the handler result after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        public IObservable<T> Recover(Func<Exception, IObservable<T>> handler) =>
            source.Recover<T, Exception>(handler);

        /// <summary>Recovers from errors by switching to a handler-provided sequence.</summary>
        /// <param name="handler">The function that creates the recovery sequence for an error.</param>
        /// <returns>A sequence that continues with the handler result after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
        public IObservable<T> Rescue(Func<Exception, IObservable<T>> handler) =>
            source.Recover(handler);

        /// <summary>Continues with a fallback sequence after an error.</summary>
        /// <param name="fallback">The sequence to subscribe to after an error.</param>
        /// <returns>A sequence that resumes with <paramref name="fallback"/> after an error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="fallback"/> is <see langword="null"/>.</exception>
        public IObservable<T> Resume(IObservable<T> fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(fallback);

            return new ResumeSignal<T>(source, fallback);
        }

        /// <summary>Delays source notifications by the specified duration.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        public IObservable<T> Shift(TimeSpan dueTime) =>
            source.Shift(dueTime, null);

        /// <summary>Delays source notifications by the specified duration on a sequencer.</summary>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        /// <returns>A sequence that forwards source notifications after the delay.</returns>
        public IObservable<T> Shift(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= ThreadPoolSequencer.Instance;
            if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
            {
                return new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), scheduler);
            }

            return new ShiftSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Fails the sequence if it does not terminate before the timeout.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        public IObservable<T> Expire(TimeSpan dueTime) =>
            source.Expire(dueTime, null);

        /// <summary>Fails the sequence if it does not terminate before the sequencer timeout.</summary>
        /// <param name="dueTime">The timeout duration.</param>
        /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
        /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
        public IObservable<T> Expire(TimeSpan dueTime, ISequencer? scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            scheduler ??= ThreadPoolSequencer.Instance;
            return new ExpireSignal<T>(source, dueTime, scheduler);
        }

        /// <summary>Collects all values into a list when the source completes.</summary>
        /// <returns>A sequence that emits one list containing all source values.</returns>
        public IObservable<IList<T>> CollectList()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
            {
                return new RangeListSignal<T>(range);
            }

            return new CollectListSignal<T>(source);
        }

        /// <summary>Collects all values into an array when the source completes.</summary>
        /// <returns>A sequence that emits one array containing all source values.</returns>
        public IObservable<T[]> CollectArray()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is RangeSignal range && CanReadRangeAs(typeof(T)))
            {
                return new RangeArraySignal<T>(range);
            }

            return new CollectArraySignal<T>(source);
        }

        /// <summary>Returns an observable sequence as a signal-compatible observable.</summary>
        /// <returns>The supplied source sequence.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0001:Simplify Names", Justification = "The argument validation uses ArgumentExceptionHelper")]
        public IObservable<T> ToSignal() => source ?? throw new ArgumentNullException(nameof(source));
    }

    /// <summary>Null-filtering operator for an observable source of nullable reference values.</summary>
    /// <param name="source">The source observable sequence to filter.</param>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    extension<T>(IObservable<T?> source)
        where T : class
    {
        /// <summary>Filters out null values from the source observable sequence, emitting only non-null values.</summary>
        /// <returns>An observable sequence that emits only non-null values from the source sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        public IObservable<T> KeepNotNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new KeepNotNullSignal<T>(source);
        }
    }

    /// <summary>Combining operators for an observable source of tasks.</summary>
    /// <param name="sources">The outer sequence of task sources.</param>
    /// <typeparam name="T">The task result type.</typeparam>
    extension<T>(IObservable<Task<T>> sources)
    {
        /// <summary>Subscribes to task results one at a time in source order.</summary>
        /// <returns>A sequence that emits each task result after the previous task signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Chain()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return new TaskChainSignal<T>(sources);
        }
    }

    /// <summary>Type-filtering and casting operators for an untyped observable source.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservable<object?> source)
    {
        /// <summary>Filters values to those assignable to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <returns>A sequence containing only values assignable to <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S4018:Generic methods should provide type parameters",
            Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
        public IObservable<TResult> KeepType<TResult>()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new KeepTypeSignal<TResult>(source);
        }

        /// <summary>Casts each source value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result value type.</typeparam>
        /// <returns>A sequence containing each value cast to <typeparamref name="TResult"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S4018:Generic methods should provide type parameters",
            Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
        public IObservable<TResult> CastTo<TResult>()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return source.Map(value => (TResult)value!);
        }
    }

    /// <summary>Signal-conversion operators for a task source.</summary>
    /// <param name="task">The task to convert.</param>
    /// <typeparam name="T">The task result type.</typeparam>
    extension<T>(Task<T> task)
    {
        /// <summary>Converts a task to a signal that emits the task result.</summary>
        /// <returns>A signal that emits the completed task result or faults with the task error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
        public IObservable<T> ToSignal() => Signal.FromTask(task);
    }

    /// <summary>Creates the optimized range-backed combine-latest sequence.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left range source.</param>
    /// <param name="right">The right range source.</param>
    /// <param name="selector">The function that combines range values.</param>
    /// <returns>The optimized combine-latest sequence.</returns>
    private static RangeCombineLatestSignal<TResult> CreateRangeCombineLatestSignal<TResult>(
        RangeSignal left,
        RangeSignal right,
        Func<int, int, TResult> selector) =>
        new(left, right, selector);

    /// <summary>Creates the optimized range-backed with-latest sequence.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left range source.</param>
    /// <param name="right">The right range source.</param>
    /// <param name="selector">The function that combines range values.</param>
    /// <returns>The optimized with-latest sequence.</returns>
    private static RangeWithLatestSignal<TResult> CreateRangeWithLatestSignal<TResult>(
        RangeSignal left,
        RangeSignal right,
        Func<int, int, TResult> selector) =>
        new(left, right, selector);

    /// <summary>Determines whether a generic observer type can receive boxed range integers.</summary>
    /// <param name="elementType">The observer value type.</param>
    /// <returns><see langword="true"/> when the cast is valid.</returns>
    private static bool CanReadRangeAs(Type elementType) => elementType.IsAssignableFrom(typeof(int));

    /// <summary>Creates a range-concat signal for synchronous Switch over known range inners.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sources">Outer sources.</param>
    /// <param name="signal">Optimized signal when available.</param>
    /// <returns><see langword="true"/> when the fast path applies.</returns>
    private static bool TryCreateSynchronousSwitchRangeSignal<T>(IObservable<IObservable<T>> sources, out IObservable<T> signal)
    {
        signal = null!;
        if (typeof(T) != typeof(int) || sources is not FromEnumerableSignal<IObservable<T>> enumerable)
        {
            return false;
        }

        if (!enumerable.TryGetReadOnlyValues(out var innerSources) || innerSources.Count == 0)
        {
            return false;
        }

        var ranges = new RangeSignal[innerSources.Count];
        for (var i = 0; i < innerSources.Count; i++)
        {
            if (innerSources[i] is not RangeSignal range)
            {
                return false;
            }

            ranges[i] = range;
        }

        signal = (IObservable<T>)(object)new RangeConcatSignal(ranges);
        return true;
    }

    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange<T>(IObserver<T> observer, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            observer.OnNext((T)(object)(range.Start + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="onNext">The next callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange<T>(Action<T> onNext, Action onCompleted, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            onNext((T)(object)(range.Start + i));
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
