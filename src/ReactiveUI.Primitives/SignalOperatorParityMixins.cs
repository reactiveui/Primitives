// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

#pragma warning disable SA1107, SA1116, SA1117, SA1501, SA1611, SA1615, SA1618

namespace ReactiveUI.Primitives;

/// <summary>
/// Additional parity operators that preserve Primitives naming while covering common reactive contracts.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Prepends a value before the source sequence. Alias of <see cref="Prepend{T}(IObservable{T}, T)"/> using Primitives vocabulary.
    /// </summary>
    public static IObservable<T> Lead<T>(this IObservable<T> source, T value) => source.Prepend(value);

    /// <summary>
    /// Prepends a value before the source sequence.
    /// </summary>
    public static IObservable<T> Prepend<T>(this IObservable<T> source, T value)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new PrependSignal<T>(source, value);
    }

    /// <summary>
    /// Appends a value after the source sequence completes.
    /// </summary>
    public static IObservable<T> Append<T>(this IObservable<T> source, T value)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new AppendSignal<T>(source, value);
    }

    /// <summary>
    /// Prepends a value before the source sequence using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> StartWith<T>(this IObservable<T> source, T value) => source.Prepend(value);

    /// <summary>
    /// Prepends values before the source sequence using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> StartWith<T>(this IObservable<T> source, params T[] values)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return new StartWithEnumerableSignal<T>(source, values);
    }

    /// <summary>
    /// Prepends values before the source sequence using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> StartWith<T>(this IObservable<T> source, IEnumerable<T> values)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return new StartWithEnumerableSignal<T>(source, values);
    }

    /// <summary>
    /// Returns the source as an observable. This is an identity adapter for BCL observable sources.
    /// </summary>
    public static IObservable<T> AsObservable<T>(this IObservable<T> source) => source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.
    /// </summary>
    public static IObservable<T> ToObservable<T>(this IEnumerable<T> values) => Signal.FromEnumerable(values);

    /// <summary>
    /// Converts an enumerable sequence to a Primitives signal using the System.Reactive conversion name.
    /// </summary>
    public static IObservable<T> ToObservable<T>(this IEnumerable<T> values, CancellationToken cancellationToken) =>
        Signal.FromEnumerable(values, cancellationToken);

    /// <summary>
    /// Schedules observer notifications on the supplied scheduler using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> ObserveOn<T>(this IObservable<T> source, ISequencer scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (scheduler == Sequencer.Immediate)
        {
            return source;
        }

        return source.WitnessOn(scheduler);
    }

    /// <summary>
    /// Alias for <see cref="DelayStart{T}(IObservable{T}, TimeSpan, ISequencer?)"/> using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> DelaySubscription<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.DelayStart(dueTime, null);

    /// <summary>
    /// Alias for <see cref="DelayStart{T}(IObservable{T}, TimeSpan, ISequencer?)"/> using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> DelaySubscription<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler) =>
        source.DelayStart(dueTime, scheduler);

    /// <summary>
    /// Runs a side effect for each source value using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext) => source.Tap(onNext);

    /// <summary>
    /// Runs side effects for source notifications using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onError == null)
        {
            throw new ArgumentNullException(nameof(onError));
        }

        if (onCompleted == null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(
            value =>
            {
                onNext(value);
                observer.OnNext(value);
            },
            error =>
            {
                onError(error);
                observer.OnError(error);
            },
            () =>
            {
                onCompleted();
                observer.OnCompleted();
            }));
    }

    /// <summary>
    /// Alias for <see cref="Rescue{T}(IObservable{T}, Func{Exception, IObservable{T}})"/> using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> Catch<T>(this IObservable<T> source, Func<Exception, IObservable<T>> handler) =>
        source.Rescue(handler);

    /// <summary>
    /// Ignores all source values and only forwards terminal messages.
    /// </summary>
    public static IObservable<T> IgnoreValues<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(_ => { }, observer.OnError, observer.OnCompleted));
    }

    /// <summary>
    /// Emits the supplied value if the source completes without values.
    /// </summary>
    public static IObservable<T> DefaultIfEmpty<T>(this IObservable<T> source) =>
        source.DefaultIfEmpty(default!);

    /// <summary>
    /// Emits the supplied value if the source completes without values.
    /// </summary>
    public static IObservable<T> DefaultIfEmpty<T>(this IObservable<T> source, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new DefaultIfEmptySignal<T>(source, defaultValue);
    }

    /// <summary>
    /// Suppresses duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector) =>
        source.DistinctBy(keySelector, null);

    /// <summary>
    /// Suppresses duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        return new DistinctBySignal<T, TKey>(source, keySelector, comparer);
    }

    /// <summary>
    /// Suppresses adjacent duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctUntilChangedBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector) =>
        source.DistinctUntilChangedBy(keySelector, null);

    /// <summary>
    /// Suppresses adjacent duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctUntilChangedBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        comparer ??= EqualityComparer<TKey>.Default;
        return Signal.CreateSafe<T>(observer =>
        {
            var hasLast = false;
            var last = default(TKey);
            return source.Subscribe(
                value =>
                {
                    var key = keySelector(value);
                    if (hasLast && comparer.Equals(last!, key))
                    {
                        return;
                    }

                    hasLast = true;
                    last = key;
                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Emits values while the predicate remains true, then completes.
    /// </summary>
    public static IObservable<T> TakeWhile<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var taking = true;
            return source.Subscribe(
                value =>
                {
                    if (!taking)
                    {
                        return;
                    }

                    if (predicate(value))
                    {
                        observer.OnNext(value);
                    }
                    else
                    {
                        taking = false;
                        observer.OnCompleted();
                    }
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Skips values while the predicate remains true, then mirrors the remaining source.
    /// </summary>
    public static IObservable<T> SkipWhile<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var skipping = true;
            return source.Subscribe(
                value =>
                {
                    if (skipping && predicate(value))
                    {
                        return;
                    }

                    skipping = false;
                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Projects each source value to an inner signal and concatenates all inner values.
    /// </summary>
    public static IObservable<TResult> Bind<TSource, TResult>(this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector) => source.SelectMany(selector);

    /// <summary>
    /// Projects each source value to an inner signal and concatenates all inner values.
    /// </summary>
    public static IObservable<TResult> SelectMany<TSource, TResult>(this IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new SelectManySignal<TSource, TResult>(source, selector);
    }

    /// <summary>
    /// Projects each source value to an inner signal and maps outer/inner values with a result selector.
    /// </summary>
    public static IObservable<TResult> SelectMany<TSource, TCollection, TResult>(
        this IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        if (collectionSelector == null)
        {
            throw new ArgumentNullException(nameof(collectionSelector));
        }

        if (resultSelector == null)
        {
            throw new ArgumentNullException(nameof(resultSelector));
        }

        return new SelectManyResultSignal<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
    }

    /// <summary>
    /// Counts the source values as an <see cref="int"/>.
    /// </summary>
    public static IObservable<int> Count<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new CountSignal<T>(source);
    }

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="int"/>.
    /// </summary>
    public static IObservable<int> Count<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new CountPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Counts the source values as an <see cref="long"/>.
    /// </summary>
    public static IObservable<long> LongCount<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new LongCountSignal<T>(source);
    }

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="long"/>.
    /// </summary>
    public static IObservable<long> LongCount<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new LongCountPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Emits true when any value is present.
    /// </summary>
    public static IObservable<bool> Any<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new AnySignal<T>(source);
    }

    /// <summary>
    /// Emits true when any value satisfies the predicate.
    /// </summary>
    public static IObservable<bool> Any<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new AnyPredicateSignal<T>(source, predicate);
    }

    /// <summary>
    /// Emits true when every value satisfies the predicate.
    /// </summary>
    public static IObservable<bool> All<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return Signal.CreateSafe<bool>(observer =>
        {
            var failed = false;
            return source.Subscribe(
                value =>
                {
                    if (failed || predicate(value))
                    {
                        return;
                    }

                    failed = true;
                    observer.OnNext(false);
                    observer.OnCompleted();
                },
                observer.OnError,
                () =>
                {
                    if (failed)
                    {
                        return;
                    }

                    observer.OnNext(true);
                    observer.OnCompleted();
                });
        });
    }

    /// <summary>
    /// Emits true when the source contains the requested value.
    /// </summary>
    public static IObservable<bool> Contains<T>(this IObservable<T> source, T value) =>
        source.Contains(value, null);

    /// <summary>
    /// Emits true when the source contains the requested value.
    /// </summary>
    public static IObservable<bool> Contains<T>(this IObservable<T> source, T value, IEqualityComparer<T>? comparer)
    {
        comparer ??= EqualityComparer<T>.Default;
        return source.Any(candidate => comparer.Equals(candidate, value));
    }

    /// <summary>
    /// Emits true when the source completes without values.
    /// </summary>
    public static IObservable<bool> IsEmpty<T>(this IObservable<T> source) => source.Any().Map(hasValue => !hasValue);

    /// <summary>
    /// Emits values from source after delaying subscription by the due time.
    /// </summary>
    public static IObservable<T> DelayStart<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.DelayStart(dueTime, null);

    /// <summary>
    /// Emits values from source after delaying subscription by the due time.
    /// </summary>
    public static IObservable<T> DelayStart<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.Create<T>(observer =>
        {
            var pocket = new MultipleDisposable();
            pocket.Add(scheduler.Schedule(Sequencer.Normalize(dueTime), () => pocket.Add(source.Subscribe(observer))));
            return pocket;
        });
    }

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    public static IObservable<T> Throttle<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Throttle(dueTime, null);

    /// <summary>
    /// Emits only the most recent value after the quiet period elapses.
    /// </summary>
    public static IObservable<T> Throttle<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.CreateSafe<T>(
            observer =>
            {
                var gate = new OperatorGate();
                var pocket = new MultipleDisposable();
                var slot = new SingleReplaceableDisposable();
                var version = 0;
                pocket.Add(slot);
                pocket.Add(source.Subscribe(
                    value =>
                    {
                        int current;
                        lock (gate.SyncRoot)
                        {
                            current = ++version;
                        }

                        slot.Create(scheduler.Schedule(Sequencer.Normalize(dueTime), () =>
                        {
                            lock (gate.SyncRoot)
                            {
                                if (current == version)
                                {
                                    observer.OnNext(value);
                                }
                            }
                        }));
                    },
                    observer.OnError,
                    observer.OnCompleted));
                return pocket;
            },
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Emits the latest source value whenever the sampling period ticks.
    /// </summary>
    public static IObservable<T> Sample<T>(this IObservable<T> source, TimeSpan period) =>
        source.Sample(period, null);

    /// <summary>
    /// Emits the latest source value whenever the sampling period ticks.
    /// </summary>
    public static IObservable<T> Sample<T>(this IObservable<T> source, TimeSpan period, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (period < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.CreateSafe<T>(
            observer => new SampleCoordinator<T>(source, period, scheduler).Run(observer),
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Annotates values with their scheduler timestamp.
    /// </summary>
    public static IObservable<Moment<T>> Timestamp<T>(this IObservable<T> source) =>
        source.Timestamp(null);

    /// <summary>
    /// Annotates values with their scheduler timestamp.
    /// </summary>
    public static IObservable<Moment<T>> Timestamp<T>(this IObservable<T> source, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= Sequencer.Immediate;
        return source.Map(value => new Moment<T>(value, scheduler.Now));
    }

    /// <summary>
    /// Annotates each value with the elapsed scheduler time since the previous value.
    /// </summary>
    public static IObservable<TimeInterval<T>> TimeInterval<T>(this IObservable<T> source) =>
        source.TimeInterval(null);

    /// <summary>
    /// Annotates each value with the elapsed scheduler time since the previous value.
    /// </summary>
    public static IObservable<TimeInterval<T>> TimeInterval<T>(this IObservable<T> source, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= Sequencer.Immediate;
        return Signal.CreateSafe<TimeInterval<T>>(observer =>
        {
            var last = scheduler.Now;
            var first = true;
            return source.Subscribe(
                value =>
                {
                    var now = scheduler.Now;
                    var interval = first ? TimeSpan.Zero : now - last;
                    first = false;
                    last = now;
                    observer.OnNext(new TimeInterval<T>(value, interval));
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Combines latest values from both sources. Alias for latest-fusion vocabulary.
    /// </summary>
    public static IObservable<TResult> ZipLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.CombineLatest(right, selector);

    /// <summary>
    /// Alias for <see cref="ZipLatest{TLeft, TRight, TResult}(IObservable{TLeft}, IObservable{TRight}, Func{TLeft, TRight, TResult})"/>.
    /// </summary>
    public static IObservable<TResult> FuseLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.ZipLatest(right, selector);

    /// <summary>
    /// Waits for both sources to complete and emits one value from their last elements when both produced at least one value.
    /// </summary>
    public static IObservable<TResult> ForkJoin<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
        if (left == null)
        {
            throw new ArgumentNullException(nameof(left));
        }

        if (right == null)
        {
            throw new ArgumentNullException(nameof(right));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        if (typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange && right is RangeSignal rightRange)
        {
            return Signal.CreateSafe<TResult>(observer =>
            {
                observer.OnNext(((Func<int, int, TResult>)(object)selector)(
                    leftRange.Start + leftRange.Count - 1,
                    rightRange.Start + rightRange.Count - 1));
                observer.OnCompleted();
                return Disposable.Empty;
            });
        }

        return Signal.CreateSafe<TResult>(observer => new ForkJoinCoordinator<TLeft, TRight, TResult>(observer, selector).Run(left, right));
    }

    /// <summary>
    /// Awaits the first source value.
    /// </summary>
    public static Task<T> FirstAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(false, default!);
    }

    /// <summary>
    /// Awaits the first source value, returning a default value when the source is empty.
    /// </summary>
    public static Task<T> FirstOrDefaultAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(true, default!);
    }

    /// <summary>
    /// Awaits the first source value, returning a default value when the source is empty.
    /// </summary>
    public static Task<T> FirstOrDefaultAsync<T>(this IObservable<T> source, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start));
        }

        return source.FirstOrDefaultCoreAsync(true, defaultValue);
    }

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    public static Task<T> ToTask<T>(this IObservable<T> source) => source.ToTask(CancellationToken.None);

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "ToTask keeps cancellation, terminal, and synchronous fast paths together to avoid extra allocations.")]
    public static Task<T> ToTask<T>(this IObservable<T> source, CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeValue<T>(range.Start + range.Count - 1));
        }

        var completion = new TaskCompletionSource<T>();
        var seen = false;
        var last = default(T);
        var subscription = default(IDisposable);
        CancellationTokenRegistration cancellationRegistration = default;
        if (cancellationToken.CanBeCanceled)
        {
            cancellationRegistration = cancellationToken.Register(() =>
            {
                subscription?.Dispose();
                completion.TrySetCanceled(cancellationToken);
            });
        }

        subscription = source.Subscribe(
            value =>
            {
                seen = true;
                last = value;
            },
            error =>
            {
                cancellationRegistration.Dispose();
                subscription?.Dispose();
                completion.TrySetException(error);
            },
            () =>
            {
                cancellationRegistration.Dispose();
                subscription?.Dispose();
                if (seen)
                {
                    completion.TrySetResult(last!);
                }
                else
                {
                    completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                }
            });

        if (completion.Task.IsCompleted)
        {
            subscription.Dispose();
        }

        return completion.Task;
    }

    /// <summary>
    /// Identity helper that keeps source-compatible <c>FirstAsync().ToTask()</c> migrations compiling.
    /// </summary>
    public static Task<T> ToTask<T>(this Task<T> task) => task ?? throw new ArgumentNullException(nameof(task));

    /// <summary>
    /// Awaits the source count as a task.
    /// </summary>
    public static Task<int> CountAsync<T>(this IObservable<T> source) =>
        source.Count().ToTask();

    /// <summary>
    /// Awaits the source count as a task.
    /// </summary>
    public static Task<int> CountAsync<T>(this IObservable<T> source, CancellationToken cancellationToken) =>
        source.Count().ToTask(cancellationToken);

    /// <summary>
    /// Awaits the source predicate count as a task.
    /// </summary>
    public static Task<int> CountAsync<T>(this IObservable<T> source, Func<T, bool> predicate) =>
        source.Count(predicate).ToTask();

    /// <summary>
    /// Awaits the source predicate count as a task.
    /// </summary>
    public static Task<int> CountAsync<T>(this IObservable<T> source, Func<T, bool> predicate, CancellationToken cancellationToken) =>
        source.Count(predicate).ToTask(cancellationToken);

    /// <summary>
    /// Awaits whether any value is present.
    /// </summary>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source) =>
        source.Any().ToTask();

    /// <summary>
    /// Awaits whether any value is present.
    /// </summary>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, CancellationToken cancellationToken) =>
        source.Any().ToTask(cancellationToken);

    /// <summary>
    /// Awaits whether any value matches a predicate.
    /// </summary>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, Func<T, bool> predicate) =>
        source.Any(predicate).ToTask();

    /// <summary>
    /// Awaits whether any value matches a predicate.
    /// </summary>
    public static Task<bool> AnyAsync<T>(this IObservable<T> source, Func<T, bool> predicate, CancellationToken cancellationToken) =>
        source.Any(predicate).ToTask(cancellationToken);

    /// <summary>
    /// Collects all values into an array task.
    /// </summary>
    public static Task<T[]> CollectArrayAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult(CreateRangeArray<T>(range));
        }

        var completion = new TaskCompletionSource<T[]>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult([.. values]));
        return completion.Task;
    }

    /// <summary>
    /// Collects all values into a list task.
    /// </summary>
    public static Task<IList<T>> CollectListAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (source is RangeSignal range && CanReadRangeAs<T>())
        {
            return Task.FromResult((IList<T>)CreateRangeList<T>(range));
        }

        var completion = new TaskCompletionSource<IList<T>>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult(values));
        return completion.Task;
    }

    /// <summary>
    /// Awaits the first source value and applies the configured empty-source behavior.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="hasDefault">A value indicating whether to use <paramref name="defaultValue"/> when the source is empty.</param>
    /// <param name="defaultValue">The fallback value to use when the source is empty.</param>
    /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
    private static Task<T> FirstOrDefaultCoreAsync<T>(this IObservable<T> source, bool hasDefault, T defaultValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var completion = new TaskCompletionSource<T>();
        var seen = false;
        source.Subscribe(
            value =>
            {
                if (seen)
                {
                    return;
                }

                seen = true;
                completion.TrySetResult(value);
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
                    completion.TrySetResult(defaultValue);
                }
                else
                {
                    completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                }
        });
        return completion.Task;
    }

    /// <summary>
    /// Creates a generic value from an integer range item.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static T CreateRangeValue<T>(int value) => (T)(object)value;

    /// <summary>
    /// Creates a range-backed array for task terminal fast paths.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static T[] CreateRangeArray<T>(RangeSignal range)
    {
        if (typeof(T) == typeof(int))
        {
            var values = new int[range.Count];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = range.Start + i;
            }

            return (T[])(object)values;
        }

        var boxed = new T[range.Count];
        for (var i = 0; i < boxed.Length; i++)
        {
            boxed[i] = CreateRangeValue<T>(range.Start + i);
        }

        return boxed;
    }

    /// <summary>
    /// Creates a range-backed list for task terminal fast paths.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The generic type is validated by the caller before reading range values.")]
    private static List<T> CreateRangeList<T>(RangeSignal range)
    {
        if (typeof(T) == typeof(int))
        {
            var integers = new List<int>(range.Count);
            for (var i = 0; i < range.Count; i++)
            {
                integers.Add(range.Start + i);
            }

            return (List<T>)(object)integers;
        }

        var values = new List<T>(range.Count);
        for (var i = 0; i < range.Count; i++)
        {
            values.Add(CreateRangeValue<T>(range.Start + i));
        }

        return values;
    }
}
