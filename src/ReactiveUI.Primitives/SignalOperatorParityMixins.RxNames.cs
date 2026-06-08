// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// System.Reactive / LINQ familiar names for the Primitives operator vocabulary. Each method builds the same sink as
/// its Primitives-named counterpart directly, so the two names are interchangeable with identical behaviour and
/// allocation profile. Both name sets are fully supported.
/// </summary>
public static partial class LinqExtensions
{
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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            return new MapSignal<TSource, TResult>(source, selector);
        }

        /// <summary>Projects each element into a new form using external state passed to the selector. State-carrying name for <c>MapWith</c>.</summary>
        /// <typeparam name="TState">The type of the state used in the selector function.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="state">The state to pass to the selector function.</param>
        /// <param name="selector">A transform function to apply to each source element along with the state.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the transform on each source element and the state.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectWith<TState, TResult>(TState state, Func<TState, TSource, TResult> selector)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            return new MapWithSignal<TSource, TState, TResult>(source, state, selector);
        }

        /// <summary>Filters an observable sequence to elements that satisfy a predicate. LINQ name for <c>Keep</c>.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence containing the elements that satisfy <paramref name="predicate"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> Where(Func<TSource, bool> predicate)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (predicate is null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }

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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (predicate is null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }

            return new KeepWithSignal<TSource, TState>(source, state, predicate);
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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return new KeepNotNullSignal<T>(source);
        }
    }

    /// <summary>System.Reactive-named side-effect, accumulation, and projection operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Invokes an action for each value while preserving the sequence. System.Reactive name for <c>Tap</c>.</summary>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> Do(Action<T> onNext)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (onNext is null)
            {
                throw new ArgumentNullException(nameof(onNext));
            }

            return new TapSignal<T>(source, onNext, static _ => { }, static () => { });
        }

        /// <summary>Invokes a stateful action for each value while preserving the sequence. State-carrying name for <c>TapWith</c>.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to <paramref name="onNext"/>.</param>
        /// <param name="onNext">The action to invoke for each value.</param>
        /// <returns>The source values after the action has run.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
        public IObservable<T> DoWith<TState>(TState state, Action<TState, T> onNext)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (onNext is null)
            {
                throw new ArgumentNullException(nameof(onNext));
            }

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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (accumulator is null)
            {
                throw new ArgumentNullException(nameof(accumulator));
            }

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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (accumulator is null)
            {
                throw new ArgumentNullException(nameof(accumulator));
            }

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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (keySelector is null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }

            comparer ??= EqualityComparer<TKey>.Default;
            return new UniqueBySignal<T, TKey>(source, keySelector, comparer);
        }

        /// <summary>Drops every value, forwarding only the terminal notification. System.Reactive name for <c>IgnoreValues</c>.</summary>
        /// <returns>A sequence that forwards only completion or error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<T> IgnoreElements()
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return new IgnoreValuesSignal<T>(source);
        }

        /// <summary>Projects each value to an inner sequence and merges the results. LINQ name for <c>FlatMap</c>.</summary>
        /// <typeparam name="TResult">The inner value type.</typeparam>
        /// <param name="selector">The function that projects each source value to an inner sequence.</param>
        /// <returns>A sequence containing the merged values of every inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> SelectMany<TResult>(Func<T, IObservable<TResult>> selector)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

            return new FlatMapSignal<T, TResult>(source, selector);
        }

        /// <summary>Projects each value to an inner sequence and combines each pair with a result selector. LINQ name for <c>FlatMap</c>.</summary>
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
            if (collectionSelector is null)
            {
                throw new ArgumentNullException(nameof(collectionSelector));
            }

            if (resultSelector is null)
            {
                throw new ArgumentNullException(nameof(resultSelector));
            }

            return new FlatMapResultSignal<T, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        /// <summary>Concatenates two sequences. System.Reactive name for <c>Chain</c>.</summary>
        /// <param name="second">The second sequence.</param>
        /// <returns>A sequence that emits <paramref name="second"/> after <paramref name="source"/> completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
        public IObservable<T> Concat(IObservable<T> second)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (second is null)
            {
                throw new ArgumentNullException(nameof(second));
            }

            return new ChainSignal<T>(source, second);
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
            if (sources is null)
            {
                throw new ArgumentNullException(nameof(sources));
            }

            return new BlendSignal<T>(sources);
        }

        /// <summary>Subscribes to inner sequences one at a time in source order. System.Reactive name for <c>Chain</c>.</summary>
        /// <returns>A sequence that emits each inner sequence after the previous one completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Concat()
        {
            if (sources is null)
            {
                throw new ArgumentNullException(nameof(sources));
            }

            return new ChainSignal<T>(sources);
        }

        /// <summary>Mirrors the first inner sequence to produce any notification. System.Reactive name for <c>Race</c>.</summary>
        /// <returns>A sequence that mirrors the winning inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Amb()
        {
            if (sources is null)
            {
                throw new ArgumentNullException(nameof(sources));
            }

            return new RaceSignal<T>(sources);
        }

        /// <summary>Switches to the most recent inner sequence. System.Reactive name for <c>SwitchTo</c>.</summary>
        /// <returns>A sequence that mirrors only the latest inner sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservable<T> Switch()
        {
            if (sources is null)
            {
                throw new ArgumentNullException(nameof(sources));
            }

            if (TryCreateSynchronousSwitchRangeSignal(sources, out var rangeSignal))
            {
                return rangeSignal;
            }

            return new SwitchSignal<T>(sources);
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
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (right is null)
            {
                throw new ArgumentNullException(nameof(right));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

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
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (right is null)
            {
                throw new ArgumentNullException(nameof(right));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

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
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (right is null)
            {
                throw new ArgumentNullException(nameof(right));
            }

            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }

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
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

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
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

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
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is less than <see cref="TimeSpan.Zero"/>.</exception>
        public IObservable<TLeft> Sample(TimeSpan interval, ISequencer? scheduler)
        {
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (interval < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(interval));
            }

            scheduler ??= ThreadPoolSequencer.Instance;
            return new ProbeSignal<TLeft>(left, interval, scheduler);
        }

        /// <summary>Resubscribes to the source after an error up to <paramref name="retryCount"/> times. System.Reactive name for <c>Reattempt</c>.</summary>
        /// <param name="retryCount">The maximum number of retry attempts after the initial subscription.</param>
        /// <returns>A sequence that retries the source before forwarding the final error.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is less than zero.</exception>
        public IObservable<TLeft> Retry(int retryCount)
        {
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (retryCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(retryCount));
            }

            return new ReattemptSignal<TLeft>(left, retryCount);
        }

        /// <summary>Converts source values and terminal notifications into <see cref="Spark{T}"/> values. System.Reactive name for <c>Spark</c>.</summary>
        /// <returns>A sequence of spark values representing source notifications.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left"/> is <see langword="null"/>.</exception>
        public IObservable<Spark<TLeft>> Materialize()
        {
            if (left is null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            return new SparkSignal<TLeft>(left);
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
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return new UnsparkSignal<T>(source);
        }
    }
}
