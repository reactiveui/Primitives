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
/// Additional ReactiveUI.Primitives operator surface. Canonical LINQ names are kept where idiomatic;
/// Primitives aliases (`Map`, `Keep`, `Sparkify`, `Unspark`) make the public surface distinct.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Maps every value with <paramref name="selector"/>.
    /// </summary>
    public static IObservable<TResult> Map<TSource, TResult>(this IObservable<TSource> source, Func<TSource, TResult> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new MapSignal<TSource, TResult>(source, selector);
    }

    /// <summary>
    /// Maps every value with explicit state to avoid closure allocations in hot paths.
    /// </summary>
    public static IObservable<TResult> MapWith<TSource, TState, TResult>(this IObservable<TSource> source, TState state, Func<TState, TSource, TResult> selector)
    {
        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return source.Map(value => selector(state, value));
    }

    /// <summary>
    /// Keeps values that satisfy <paramref name="predicate"/>.
    /// </summary>
    public static IObservable<T> Keep<T>(this IObservable<T> source, Func<T, bool> predicate)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return new KeepSignal<T>(source, predicate);
    }

    /// <summary>
    /// Keeps values that satisfy a stateful predicate.
    /// </summary>
    public static IObservable<T> KeepWith<T, TState>(this IObservable<T> source, TState state, Func<TState, T, bool> predicate)
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        return source.Keep(value => predicate(state, value));
    }

    /// <summary>
    /// Keeps non-null values and narrows nullable references.
    /// </summary>
    public static IObservable<T> KeepNotNull<T>(this IObservable<T?> source)
        where T : class
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(
            value =>
            {
                if (value == null)
                {
                    return;
                }

                observer.OnNext(value);
            },
            observer.OnError,
            observer.OnCompleted));
    }

    /// <summary>
    /// Projects only values assignable to <typeparamref name="TResult"/>.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "LINQ-style OfType requires the caller to provide the result type.")]
    public static IObservable<TResult> OfType<TResult>(this IObservable<object?> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<TResult>(observer => source.Subscribe(
            value =>
            {
                if (value is not TResult result)
                {
                    return;
                }

                observer.OnNext(result);
            },
            observer.OnError,
            observer.OnCompleted));
    }

    /// <summary>
    /// Casts every value to <typeparamref name="TResult"/>.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "LINQ-style Cast requires the caller to provide the result type.")]
    public static IObservable<TResult> Cast<TResult>(this IObservable<object?> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return source.Map(value => (TResult)value!);
    }

    /// <summary>
    /// Runs a side effect for every value while preserving the source values.
    /// </summary>
    public static IObservable<T> Tap<T>(this IObservable<T> source, Action<T> onNext)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        return source.Map(value =>
        {
            onNext(value);
            return value;
        });
    }

    /// <summary>
    /// Runs a stateful side effect for every value while preserving the source values.
    /// </summary>
    public static IObservable<T> TapWith<T, TState>(this IObservable<T> source, TState state, Action<TState, T> onNext)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        return source.Tap(value => onNext(state, value));
    }

    /// <summary>
    /// Emits accumulated state for every source value.
    /// </summary>
    public static IObservable<TAccumulate> Scan<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (accumulator == null)
        {
            throw new ArgumentNullException(nameof(accumulator));
        }

        return Signal.CreateSafe<TAccumulate>(observer =>
        {
            var current = seed;
            return source.Subscribe(
                value =>
                {
                    current = accumulator(current, value);
                    observer.OnNext(current);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Emits one final accumulated value when the source completes.
    /// </summary>
    public static IObservable<TAccumulate> Fold<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (accumulator == null)
        {
            throw new ArgumentNullException(nameof(accumulator));
        }

        return Signal.CreateSafe<TAccumulate>(observer =>
        {
            var current = seed;
            return source.Subscribe(
                value => current = accumulator(current, value),
                observer.OnError,
                () =>
                {
                    observer.OnNext(current);
                    observer.OnCompleted();
                });
        });
    }

    /// <summary>
    /// Emits at most <paramref name="count"/> values before completing.
    /// </summary>
    public static IObservable<T> Take<T>(this IObservable<T> source, int count)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            if (count == 0)
            {
                observer.OnCompleted();
                return Disposable.Empty;
            }

            var remaining = count;
            return source.Subscribe(
                value =>
                {
                    if (remaining <= 0)
                    {
                        return;
                    }

                    observer.OnNext(value);
                    remaining--;
                    if (remaining != 0)
                    {
                        return;
                    }

                    observer.OnCompleted();
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Skips <paramref name="count"/> values.
    /// </summary>
    public static IObservable<T> Skip<T>(this IObservable<T> source, int count)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var remaining = count;
            return source.Subscribe(
                value =>
                {
                    if (remaining > 0)
                    {
                        remaining--;
                        return;
                    }

                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Suppresses duplicate values according to the comparer.
    /// </summary>
    public static IObservable<T> Distinct<T>(this IObservable<T> source) =>
        source.Distinct(null);

    /// <summary>
    /// Suppresses duplicate values according to the comparer.
    /// </summary>
    public static IObservable<T> Distinct<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer =>
        {
            var seen = new HashSet<T>(comparer);
            return source.Subscribe(
                value =>
                {
                    if (!seen.Add(value))
                    {
                        return;
                    }

                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Suppresses adjacent duplicate values according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source) =>
        source.DistinctUntilChanged(null);

    /// <summary>
    /// Suppresses adjacent duplicate values according to the comparer.
    /// </summary>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        comparer ??= EqualityComparer<T>.Default;
        return Signal.CreateSafe<T>(observer =>
        {
            var hasLast = false;
            var last = default(T);
            return source.Subscribe(
                value =>
                {
                    if (hasLast && comparer.Equals(last!, value))
                    {
                        return;
                    }

                    hasLast = true;
                    last = value;
                    observer.OnNext(value);
                },
                observer.OnError,
                observer.OnCompleted);
        });
    }

    /// <summary>
    /// Converts values and terminal messages into sparks.
    /// </summary>
    public static IObservable<Spark<T>> Sparkify<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<Spark<T>>(observer => source.Subscribe(
            value => observer.OnNext(Spark.CreateOnNext(value)),
            error =>
            {
                observer.OnNext(Spark.CreateOnError<T>(error));
                observer.OnCompleted();
            },
            () =>
            {
                observer.OnNext(Spark.CreateOnCompleted<T>());
                observer.OnCompleted();
            }));
    }

    /// <summary>
    /// Converts spark values back into source notifications.
    /// </summary>
    public static IObservable<T> Unspark<T>(this IObservable<Spark<T>> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<T>(observer => source.Subscribe(
            spark => spark.Accept(observer),
            observer.OnError,
            observer.OnCompleted));
    }

    /// <summary>
    /// Concatenates a signal of signals.
    /// </summary>
    public static IObservable<T> Concat<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return Signal.Create<T>(observer =>
        {
            var gate = new OperatorGate();
            var queue = new Queue<IObservable<T>>();
            var pocket = new MultipleDisposable();
            var active = false;
            var outerCompleted = false;

            void Drain()
            {
                IObservable<T>? next = null;
                lock (gate.SyncRoot)
                {
                    if (active)
                    {
                        return;
                    }

                    if (queue.Count > 0)
                    {
                        active = true;
                        next = queue.Dequeue();
                    }
                    else if (outerCompleted)
                    {
                        observer.OnCompleted();
                        return;
                    }
                }

                if (next == null)
                {
                    return;
                }

                pocket.Add(next.Subscribe(
                    observer.OnNext,
                    observer.OnError,
                    () =>
                    {
                        lock (gate.SyncRoot)
                        {
                            active = false;
                        }

                        Drain();
                    }));
            }

            pocket.Add(sources.Subscribe(
                source =>
                {
                    if (source == null)
                    {
                        observer.OnError(new InvalidOperationException("Concat source contained null."));
                        return;
                    }

                    lock (gate.SyncRoot)
                    {
                        queue.Enqueue(source);
                    }

                    Drain();
                },
                observer.OnError,
                () =>
                {
                    lock (gate.SyncRoot)
                    {
                        outerCompleted = true;
                    }

                    Drain();
                }));

            return pocket;
        });
    }

    /// <summary>
    /// Concatenates this signal followed by <paramref name="second"/>.
    /// </summary>
    public static IObservable<T> Concat<T>(this IObservable<T> first, IObservable<T> second) =>
        Signal.Concat(first, second);

    /// <summary>
    /// Merges a signal of signals.
    /// </summary>
    public static IObservable<T> Merge<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return Signal.Create<T>(observer =>
        {
            var gate = new OperatorGate();
            var pocket = new MultipleDisposable();
            var outerCompleted = false;
            var active = 0;

            void TryComplete()
            {
                lock (gate.SyncRoot)
                {
                    if (outerCompleted && active == 0)
                    {
                        observer.OnCompleted();
                    }
                }
            }

            pocket.Add(sources.Subscribe(
                source =>
                {
                    if (source == null)
                    {
                        observer.OnError(new InvalidOperationException("Merge source contained null."));
                        return;
                    }

                    lock (gate.SyncRoot)
                    {
                        active++;
                    }

                    pocket.Add(source.Subscribe(
                        observer.OnNext,
                        observer.OnError,
                        () =>
                        {
                            lock (gate.SyncRoot)
                            {
                                active--;
                            }

                            TryComplete();
                        }));
                },
                observer.OnError,
                () =>
                {
                    lock (gate.SyncRoot)
                    {
                        outerCompleted = true;
                    }

                    TryComplete();
                }));

            return pocket;
        });
    }

    /// <summary>
    /// Races the supplied source signals and mirrors the first source to emit any notification.
    /// </summary>
    public static IObservable<T> Race<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return Signal.Create<T>(observer => new RaceCoordinator<T>(observer).Run(sources));
    }

    /// <summary>
    /// Zips two signals by waiting for one value from both sides.
    /// </summary>
    public static IObservable<TResult> Zip<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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

        return Signal.CreateSafe<TResult>(observer => new ZipCoordinator<TLeft, TRight, TResult>(observer, selector).Run(left, right));
    }

    /// <summary>
    /// Combines the latest values after both sides have produced at least one value.
    /// </summary>
    public static IObservable<TResult> CombineLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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

        return Signal.CreateSafe<TResult>(observer => new CombineLatestCoordinator<TLeft, TRight, TResult>(observer, selector).Run(left, right));
    }

    /// <summary>
    /// Combines each left value with the latest right value after the right side has produced one value.
    /// </summary>
    public static IObservable<TResult> WithLatest<TLeft, TRight, TResult>(this IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
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
            var gate = new OperatorGate();
            var hasRight = false;
            var latestRight = default(TRight);
            return MultipleDisposable.Create(
                right.Subscribe(
                    value =>
                    {
                        lock (gate.SyncRoot)
                        {
                            hasRight = true;
                            latestRight = value;
                        }
                    },
                    observer.OnError,
                    () => { }),
                left.Subscribe(
                    value =>
                    {
                        TRight rightValue;
                        lock (gate.SyncRoot)
                        {
                            if (!hasRight)
                            {
                                return;
                            }

                            rightValue = latestRight!;
                        }

                        observer.OnNext(selector(value, rightValue));
                    },
                    observer.OnError,
                    observer.OnCompleted));
        });
    }

    /// <summary>
    /// Switches to the most recent inner signal.
    /// </summary>
    public static IObservable<T> Switch<T>(this IObservable<IObservable<T>> sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        return Signal.Create<T>(observer => new SwitchCoordinator<T>(observer).Run(sources));
    }

    /// <summary>
    /// Retries the source up to <paramref name="retryCount"/> times after failures.
    /// </summary>
    public static IObservable<T> Retry<T>(this IObservable<T> source, int retryCount)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (retryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryCount));
        }

        return Signal.Create<T>(observer =>
        {
            var pocket = new MultipleDisposable();
            var attempts = 0;

            void SubscribeNext()
            {
                var subscription = source.Subscribe(
                    observer.OnNext,
                    error =>
                    {
                        if (attempts++ < retryCount)
                        {
                            SubscribeNext();
                        }
                        else
                        {
                            observer.OnError(error);
                        }
                    },
                    observer.OnCompleted);
                pocket.Add(subscription);
            }

            SubscribeNext();
            return pocket;
        });
    }

    /// <summary>
    /// Recovers from errors by switching to a handler-provided signal.
    /// </summary>
    public static IObservable<T> Rescue<T>(this IObservable<T> source, Func<Exception, IObservable<T>> handler) =>
        source.Catch<T, Exception>(handler);

    /// <summary>
    /// Continues with a fallback signal after an error.
    /// </summary>
    public static IObservable<T> Resume<T>(this IObservable<T> source, IObservable<T> fallback)
    {
        if (fallback == null)
        {
            throw new ArgumentNullException(nameof(fallback));
        }

        return source.Catch<T, Exception>(_ => fallback);
    }

    /// <summary>
    /// Delays notifications by <paramref name="dueTime"/>.
    /// </summary>
    public static IObservable<T> Delay<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Delay(dueTime, null);

    /// <summary>
    /// Delays notifications by <paramref name="dueTime"/>.
    /// </summary>
    public static IObservable<T> Delay<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.CreateSafe<T>(
            observer =>
            {
                var pocket = new MultipleDisposable();
                pocket.Add(source.Subscribe(
                    value => pocket.Add(scheduler.Schedule(dueTime, () => observer.OnNext(value))),
                    error => pocket.Add(scheduler.Schedule(dueTime, () => observer.OnError(error))),
                    () => pocket.Add(scheduler.Schedule(dueTime, observer.OnCompleted))));
                return pocket;
            },
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Fails the signal if no terminal signal arrives before the timeout.
    /// </summary>
    public static IObservable<T> Timeout<T>(this IObservable<T> source, TimeSpan dueTime) =>
        source.Timeout(dueTime, null);

    /// <summary>
    /// Fails the signal if no terminal signal arrives before the timeout.
    /// </summary>
    public static IObservable<T> Timeout<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        scheduler ??= ThreadPoolSequencer.Instance;
        return Signal.Create<T>(observer =>
        {
            var pocket = new MultipleDisposable();
            var done = 0;
            pocket.Add(scheduler.Schedule(dueTime, () =>
            {
                if (Interlocked.Exchange(ref done, 1) != 0)
                {
                    return;
                }

                observer.OnError(new TimeoutException());
                pocket.Dispose();
            }));
            pocket.Add(source.Subscribe(
                value =>
                {
                    if (Volatile.Read(ref done) != 0)
                    {
                        return;
                    }

                    observer.OnNext(value);
                },
                error =>
                {
                    if (Interlocked.Exchange(ref done, 1) != 0)
                    {
                        return;
                    }

                    observer.OnError(error);
                },
                () =>
                {
                    if (Interlocked.Exchange(ref done, 1) != 0)
                    {
                        return;
                    }

                    observer.OnCompleted();
                }));
            return pocket;
        });
    }

    /// <summary>
    /// Collects all values into a list when the source completes.
    /// </summary>
    public static IObservable<IList<T>> CollectList<T>(this IObservable<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return Signal.CreateSafe<IList<T>>(observer =>
        {
            var values = new List<T>();
            return source.Subscribe(
                values.Add,
                observer.OnError,
                () =>
                {
                    observer.OnNext(values);
                    observer.OnCompleted();
                });
        });
    }

    /// <summary>
    /// Collects all values into an array when the source completes.
    /// </summary>
    public static IObservable<T[]> CollectArray<T>(this IObservable<T> source) =>
        source.CollectList().Map(values => values.ToArray());

    /// <summary>
    /// Converts an enumerable to a signal.
    /// </summary>
    public static IObservable<T> ToSignal<T>(this IEnumerable<T> values) => Signal.FromEnumerable(values);

    /// <summary>
    /// Converts an observable to a signal-compatible observable.
    /// </summary>
    public static IObservable<T> ToSignal<T>(this IObservable<T> source) => source ?? throw new ArgumentNullException(nameof(source));
}
