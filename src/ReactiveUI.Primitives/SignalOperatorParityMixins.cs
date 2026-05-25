// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

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

        return Signal.Concat(Signal.Return(value), source);
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

        return Signal.Concat(source, Signal.Return(value));
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

        return Signal.Concat(Signal.FromEnumerable(values), source);
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

        return Signal.Concat(Signal.FromEnumerable(values), source);
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

        return Signal.WitnessOn(source, scheduler);
    }

    /// <summary>
    /// Alias for <see cref="DelayStart{T}(IObservable{T}, TimeSpan, ISequencer?)"/> using the System.Reactive operator name.
    /// </summary>
    public static IObservable<T> DelaySubscription<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler = null) =>
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
    public static IObservable<T> DefaultIfEmpty<T>(this IObservable<T> source, T defaultValue = default!)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var seen = false;
            return source.Subscribe(
                value =>
                {
                    seen = true;
                    observer.OnNext(value);
                },
                observer.OnError,
                () =>
                {
                    if (!seen)
                    {
                        observer.OnNext(defaultValue);
                    }

                    observer.OnCompleted();
                });
        });
    }

    /// <summary>
    /// Suppresses duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var seen = new HashSet<TKey>(comparer);
            return source.Subscribe(
                value =>
                {
                    if (seen.Add(keySelector(value)))
                    {
                        observer.OnNext(value);
                    }
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Suppresses adjacent duplicate keys according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctUntilChangedBy<T, TKey>(this IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
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
                    if (!hasLast || !comparer.Equals(last!, key))
                    {
                        hasLast = true;
                        last = key;
                        observer.OnNext(value);
                    }
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

        return Signal.Create<TResult>(observer =>
        {
            var sources = source.Map(selector);
            return sources.Concat().Subscribe(observer);
        });
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

        return source.SelectMany(value => collectionSelector(value).Map(inner => resultSelector(value, inner)));
    }

    /// <summary>
    /// Counts the source values as an <see cref="int"/>.
    /// </summary>
    public static IObservable<int> Count<T>(this IObservable<T> source) => source.Count(_ => true);

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="int"/>.
    /// </summary>
    public static IObservable<int> Count<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return source.Fold(0, (count, value) => predicate(value) ? checked(count + 1) : count);
    }

    /// <summary>
    /// Counts the source values as an <see cref="long"/>.
    /// </summary>
    public static IObservable<long> LongCount<T>(this IObservable<T> source) => source.LongCount(_ => true);

    /// <summary>
    /// Counts source values that satisfy the predicate as an <see cref="long"/>.
    /// </summary>
    public static IObservable<long> LongCount<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return source.Fold(0L, (count, value) => predicate(value) ? checked(count + 1L) : count);
    }

    /// <summary>
    /// Emits true when any value is present.
    /// </summary>
    public static IObservable<bool> Any<T>(this IObservable<T> source) => source.Any(_ => true);

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

        return Signal.CreateSafe<bool>(observer =>
        {
            var matched = false;
            return source.Subscribe(
                value =>
                {
                    if (!matched && predicate(value))
                    {
                        matched = true;
                        observer.OnNext(true);
                        observer.OnCompleted();
                    }
                },
                observer.OnError,
                () =>
                {
                    if (!matched)
                    {
                        observer.OnNext(false);
                        observer.OnCompleted();
                    }
                });
        });
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
                    if (!failed && !predicate(value))
                    {
                        failed = true;
                        observer.OnNext(false);
                        observer.OnCompleted();
                    }
                },
                observer.OnError,
                () =>
                {
                    if (!failed)
                    {
                        observer.OnNext(true);
                        observer.OnCompleted();
                    }
                });
        });
    }

    /// <summary>
    /// Emits true when the source contains the requested value.
    /// </summary>
    public static IObservable<bool> Contains<T>(this IObservable<T> source, T value, IEqualityComparer<T>? comparer = null)
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
    public static IObservable<T> DelayStart<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler = null)
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
    public static IObservable<T> Throttle<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler = null)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.CreateSafe<T>(observer =>
        {
            var gate = new object();
            var pocket = new MultipleDisposable();
            var slot = new SingleReplaceableDisposable();
            var version = 0;
            pocket.Add(slot);
            pocket.Add(source.Subscribe(
                value =>
                {
                    int current;
                    lock (gate)
                    {
                        current = ++version;
                    }

                    slot.Create(scheduler.Schedule(Sequencer.Normalize(dueTime), () =>
                    {
                        lock (gate)
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
        }, scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Emits the latest source value whenever the sampling period ticks.
    /// </summary>
    public static IObservable<T> Sample<T>(this IObservable<T> source, TimeSpan period, ISequencer? scheduler = null)
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
        return Signal.CreateSafe<T>(observer =>
        {
            var gate = new object();
            var pocket = new MultipleDisposable();
            var timer = new SingleReplaceableDisposable();
            var hasLatest = false;
            var latest = default(T);
            var done = false;
            pocket.Add(timer);
            pocket.Add(source.Subscribe(
                value =>
                {
                    lock (gate)
                    {
                        hasLatest = true;
                        latest = value;
                    }
                },
                observer.OnError,
                () =>
                {
                    lock (gate)
                    {
                        done = true;
                    }

                    observer.OnCompleted();
                }));

            Action? tick = null;
            tick = () => timer.Create(scheduler.Schedule(period, () =>
            {
                T value;
                var emit = false;
                lock (gate)
                {
                    if (hasLatest)
                    {
                        value = latest!;
                        hasLatest = false;
                        emit = true;
                    }
                    else
                    {
                        value = default!;
                    }

                    if (done)
                    {
                        return;
                    }
                }

                if (emit)
                {
                    observer.OnNext(value);
                }

                if (!timer.IsDisposed)
                {
                    tick!();
                }
            }));

            tick();
            return pocket;
        }, scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Annotates values with their scheduler timestamp.
    /// </summary>
    public static IObservable<Moment<T>> Timestamp<T>(this IObservable<T> source, ISequencer? scheduler = null)
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
    public static IObservable<TimeInterval<T>> TimeInterval<T>(this IObservable<T> source, ISequencer? scheduler = null)
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

        return Signal.CreateSafe<TResult>(observer =>
        {
            var gate = new object();
            var hasLeft = false;
            var hasRight = false;
            var leftDone = false;
            var rightDone = false;
            var latestLeft = default(TLeft);
            var latestRight = default(TRight);

            void FinishIfReady()
            {
                TResult result;
                var emit = false;
                lock (gate)
                {
                    if (!leftDone || !rightDone)
                    {
                        return;
                    }

                    if (hasLeft && hasRight)
                    {
                        result = selector(latestLeft!, latestRight!);
                        emit = true;
                    }
                    else
                    {
                        result = default!;
                    }
                }

                if (emit)
                {
                    observer.OnNext(result);
                }

                observer.OnCompleted();
            }

            return MultipleDisposable.Create(
                left.Subscribe(value => { lock (gate) { hasLeft = true; latestLeft = value; } }, observer.OnError, () => { lock (gate) { leftDone = true; } FinishIfReady(); }),
                right.Subscribe(value => { lock (gate) { hasRight = true; latestRight = value; } }, observer.OnError, () => { lock (gate) { rightDone = true; } FinishIfReady(); }));
        });
    }

    /// <summary>
    /// Awaits the first source value.
    /// </summary>
    public static Task<T> FirstAsync<T>(this IObservable<T> source) => source.FirstOrDefaultCoreAsync(false, default!);

    /// <summary>
    /// Awaits the first source value, returning a default value when the source is empty.
    /// </summary>
    public static Task<T> FirstOrDefaultAsync<T>(this IObservable<T> source, T defaultValue = default!) => source.FirstOrDefaultCoreAsync(true, defaultValue);

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    public static Task<T> ToTask<T>(this IObservable<T> source) => source.ToTask(CancellationToken.None);

    /// <summary>
    /// Awaits source completion and returns the last value produced by the source.
    /// </summary>
    public static Task<T> ToTask<T>(this IObservable<T> source, CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
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
    /// Collects all values into an array task.
    /// </summary>
    public static Task<T[]> CollectArrayAsync<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var completion = new TaskCompletionSource<T[]>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult(values.ToArray()));
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

        var completion = new TaskCompletionSource<IList<T>>();
        var values = new List<T>();
        source.Subscribe(values.Add, error => completion.TrySetException(error), () => completion.TrySetResult(values));
        return completion.Task;
    }

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
                if (!seen)
                {
                    seen = true;
                    completion.TrySetResult(value);
                }
            },
            error => completion.TrySetException(error),
            () =>
            {
                if (!seen)
                {
                    if (hasDefault)
                    {
                        completion.TrySetResult(defaultValue);
                    }
                    else
                    {
                        completion.TrySetException(new InvalidOperationException("The source completed without producing a value."));
                    }
                }
            });
        return completion.Task;
    }
}
