// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

#pragma warning disable SA1107, SA1116, SA1117, SA1501, SA1611, SA1615, SA1618

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Additional ReactiveUI.Primitives factory surface for finite, resource, conversion, and time signals.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Creates a finite integer signal from <paramref name="start"/> for <paramref name="count"/> values.
    /// </summary>
    public static IObservable<int> Sequence(int start, int count) =>
        Sequence(start, count, Sequencer.CurrentThread);

    /// <summary>
    /// Creates a finite integer signal from <paramref name="start"/> for <paramref name="count"/> values on <paramref name="scheduler"/>.
    /// </summary>
    public static IObservable<int> Sequence(int start, int count, ISequencer scheduler)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (count == 0)
        {
            return None<int>();
        }

        if (scheduler == Sequencer.Immediate || scheduler == Sequencer.CurrentThread)
        {
            return new RangeSignal(start, count);
        }

        return CreateSafe<int>(
            observer => scheduler.Schedule(() =>
            {
                for (var i = 0; i < count; i++)
                {
                    observer.OnNext(start + i);
                }

                observer.OnCompleted();
            }),
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Creates a signal that repeats a value forever.
    /// </summary>
    public static IObservable<T> Loop<T>(T value) =>
        new LoopSignal<T>(value);

    /// <summary>
    /// Creates a signal that repeats a value <paramref name="count"/> times.
    /// </summary>
    public static IObservable<T> Loop<T>(T value, int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (count == 0)
        {
            return None<T>();
        }

        return new RepeatSignal<T>(value, count);
    }

    /// <summary>
    /// Unfolds state into a finite signal.
    /// </summary>
    public static IObservable<TResult> Unfold<TState, TResult>(
        TState initialState,
        Func<TState, bool> condition,
        Func<TState, TState> iterate,
        Func<TState, TResult> resultSelector)
    {
        if (condition == null)
        {
            throw new ArgumentNullException(nameof(condition));
        }

        if (iterate == null)
        {
            throw new ArgumentNullException(nameof(iterate));
        }

        if (resultSelector == null)
        {
            throw new ArgumentNullException(nameof(resultSelector));
        }

        return new UnfoldSignal<TState, TResult>(initialState, condition, iterate, resultSelector);
    }

    /// <summary>
    /// Generates a finite signal from state. Alias of <see cref="Unfold{TState, TResult}(TState, Func{TState, bool}, Func{TState, TState}, Func{TState, TResult})"/>.
    /// </summary>
    public static IObservable<TResult> Iterate<TState, TResult>(
        TState initialState,
        Func<TState, bool> condition,
        Func<TState, TState> iterator,
        Func<TState, TResult> resultSelector) =>
        Unfold(initialState, condition, iterator, resultSelector);

    /// <summary>
    /// Creates a signal whose subscription lifetime owns a resource.
    /// </summary>
    public static IObservable<T> Use<TResource, T>(Func<TResource> resourceFactory, Func<TResource, IObservable<T>> signalFactory)
        where TResource : IDisposable
    {
        if (resourceFactory == null)
        {
            throw new ArgumentNullException(nameof(resourceFactory));
        }

        if (signalFactory == null)
        {
            throw new ArgumentNullException(nameof(signalFactory));
        }

        return new UseSignal<TResource, T>(resourceFactory, signalFactory);
    }

    /// <summary>
    /// Converts an event into a signal of event pattern values.
    /// </summary>
    public static IObservable<EventPattern<EventArgs>> FromEventPattern(
        Action<EventHandler> addHandler,
        Action<EventHandler> removeHandler)
    {
        if (addHandler == null)
        {
            throw new ArgumentNullException(nameof(addHandler));
        }

        if (removeHandler == null)
        {
            throw new ArgumentNullException(nameof(removeHandler));
        }

        return Create<EventPattern<EventArgs>>(observer =>
        {
            void Handler(object? sender, EventArgs eventArgs) =>
                observer.OnNext(new EventPattern<EventArgs>(sender, eventArgs));

            addHandler(Handler);
            return Disposable.Create(() => removeHandler(Handler));
        });
    }

    /// <summary>
    /// Converts an event into a signal of event pattern values.
    /// </summary>
    public static IObservable<EventPattern<TEventArgs>> FromEventPattern<TEventArgs>(
        Action<EventHandler<TEventArgs>> addHandler,
        Action<EventHandler<TEventArgs>> removeHandler)
        where TEventArgs : EventArgs
    {
        if (addHandler == null)
        {
            throw new ArgumentNullException(nameof(addHandler));
        }

        if (removeHandler == null)
        {
            throw new ArgumentNullException(nameof(removeHandler));
        }

        return Create<EventPattern<TEventArgs>>(observer =>
        {
            void Handler(object? sender, TEventArgs eventArgs) =>
                observer.OnNext(new EventPattern<TEventArgs>(sender, eventArgs));

            addHandler(Handler);
            return Disposable.Create(() => removeHandler(Handler));
        });
    }

    /// <summary>
    /// Creates a signal from an enumerable sequence.
    /// </summary>
    public static IObservable<T> FromEnumerable<T>(IEnumerable<T> values)
    {
        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return new FromEnumerableSignal<T>(values);
    }

    /// <summary>
    /// Creates a signal from an enumerable sequence and stops enumeration when the token is cancelled.
    /// </summary>
    public static IObservable<T> FromEnumerable<T>(IEnumerable<T> values, CancellationToken cancellationToken)
    {
        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return cancellationToken.CanBeCanceled
            ? new FromEnumerableSignal<T>(values, cancellationToken)
            : new FromEnumerableSignal<T>(values);
    }

    /// <summary>
    /// Creates a signal from a task instance.
    /// </summary>
    public static IObservable<T> FromTask<T>(Task<T> task)
    {
        if (task == null)
        {
            throw new ArgumentNullException(nameof(task));
        }

        if (task.Status == TaskStatus.RanToCompletion)
        {
#pragma warning disable S4462 // Completed-task fast path avoids async state machine allocation.
            return Emit(task.GetAwaiter().GetResult());
#pragma warning restore S4462
        }

        if (task.IsCanceled)
        {
            return Fail<T>(new TaskCanceledException(task));
        }

        if (task.IsFaulted)
        {
            return Fail<T>(task.Exception!.InnerException ?? task.Exception);
        }

        return CreateSafe<T>(observer =>
        {
            var disposed = 0;
            task.ContinueWith(
                completed =>
                {
                    if (Volatile.Read(ref disposed) != 0)
                    {
                        return;
                    }

                    if (completed.IsCanceled)
                    {
                        observer.OnError(new TaskCanceledException(completed));
                    }
                    else if (completed.IsFaulted)
                    {
                        observer.OnError(completed.Exception!.InnerException ?? completed.Exception);
                    }
                    else
                    {
                        observer.OnNext(completed.Result);
                        observer.OnCompleted();
                    }
                },
                TaskScheduler.Default);

            return Disposable.Create(() => Volatile.Write(ref disposed, 1));
        });
    }

    /// <summary>
    /// Creates a signal by invoking an asynchronous factory at subscription time.
    /// </summary>
    public static IObservable<T> FromAsync<T>(Func<Task<T>> taskFactory)
    {
        if (taskFactory == null)
        {
            throw new ArgumentNullException(nameof(taskFactory));
        }

        return Lazy(() => FromTask(taskFactory()));
    }

    /// <summary>
    /// Creates a signal by invoking an asynchronous factory at subscription time.
    /// </summary>
    public static IObservable<T> FromAsync<T>(Func<CancellationToken, Task<T>> taskFactory) =>
        FromAsync(taskFactory, CancellationToken.None);

    /// <summary>
    /// Creates a signal by invoking an asynchronous factory at subscription time.
    /// </summary>
    public static IObservable<T> FromAsync<T>(Func<CancellationToken, Task<T>> taskFactory, CancellationToken cancellationToken)
    {
        if (taskFactory == null)
        {
            throw new ArgumentNullException(nameof(taskFactory));
        }

        return Lazy(() => FromTask(taskFactory(cancellationToken)));
    }

    /// <summary>
    /// Runs a function on the supplied scheduler and emits its result.
    /// </summary>
    public static IObservable<T> Start<T>(Func<T> function) =>
        Start(function, Sequencer.Default);

    /// <summary>
    /// Runs a function on the supplied scheduler and emits its result.
    /// </summary>
    public static IObservable<T> Start<T>(Func<T> function, ISequencer scheduler)
    {
        if (function == null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (scheduler == Sequencer.Immediate)
        {
            return CreateSafe<T>(
                observer =>
                {
                    try
                    {
                        observer.OnNext(function());
                        observer.OnCompleted();
                    }
                    catch (Exception error)
                    {
                        observer.OnError(error);
                    }

                    return Disposable.Empty;
                });
        }

        return CreateSafe<T>(
            observer => scheduler.Schedule(() =>
            {
                try
                {
                    observer.OnNext(function());
                    observer.OnCompleted();
                }
                catch (Exception error)
                {
                    observer.OnError(error);
                }
            }),
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Runs an action on the supplied scheduler and emits <see cref="RxVoid.Default"/> when it completes.
    /// </summary>
    public static IObservable<RxVoid> Start(Action action) =>
        Start(action, Sequencer.Default);

    /// <summary>
    /// Runs an action on the supplied scheduler and emits <see cref="RxVoid.Default"/> when it completes.
    /// </summary>
    public static IObservable<RxVoid> Start(Action action, ISequencer scheduler)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return Start(
            () =>
            {
                action();
                return RxVoid.Default;
            },
            scheduler);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
    /// <summary>
    /// Creates a signal from an async enumerable sequence and cancels enumeration when disposed.
    /// </summary>
    public static IObservable<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values) =>
        FromAsyncEnumerable(values, CancellationToken.None);

    /// <summary>
    /// Creates a signal from an async enumerable sequence and cancels enumeration when disposed.
    /// </summary>
    public static IObservable<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values, CancellationToken cancellationToken)
    {
        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        return new AsyncEnumerableSignal<T>(values, cancellationToken);
    }

#endif

    /// <summary>
    /// Emits a single zero tick after the due time.
    /// </summary>
    public static IObservable<long> After(TimeSpan dueTime) =>
        After(dueTime, ThreadPoolSequencer.Instance);

    /// <summary>
    /// Emits a single zero tick after the due time.
    /// </summary>
    public static IObservable<long> After(TimeSpan dueTime, ISequencer scheduler)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        return CreateSafe<long>(
            observer => scheduler.Schedule(
                Sequencer.Normalize(dueTime),
                () =>
                {
                    observer.OnNext(0L);
                    observer.OnCompleted();
                }),
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Emits a single zero tick at the specified absolute due time.
    /// </summary>
    public static IObservable<long> After(DateTimeOffset dueTime) =>
        After(dueTime, ThreadPoolSequencer.Instance);

    /// <summary>
    /// Emits a single zero tick at the specified absolute due time.
    /// </summary>
    public static IObservable<long> After(DateTimeOffset dueTime, ISequencer scheduler)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        return After(Sequencer.Normalize(dueTime - scheduler.Now), scheduler);
    }

    /// <summary>
    /// Emits first after <paramref name="dueTime"/> and then at <paramref name="period"/>.
    /// </summary>
    public static IObservable<long> After(TimeSpan dueTime, TimeSpan period) =>
        After(dueTime, period, ThreadPoolSequencer.Instance);

    /// <summary>
    /// Emits first after <paramref name="dueTime"/> and then at <paramref name="period"/>.
    /// </summary>
    public static IObservable<long> After(TimeSpan dueTime, TimeSpan period, ISequencer scheduler)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        return CreateSafe<long>(
            observer =>
            {
                var pocket = new MultipleDisposable();
                var current = 0L;
                pocket.Add(
                    scheduler.Schedule(
                        Sequencer.Normalize(dueTime),
                        () =>
                        {
                            observer.OnNext(current++);
                            pocket.Add(Every(period, scheduler).Subscribe(value => observer.OnNext(current + value), observer.OnError, observer.OnCompleted));
                        }));

                return pocket;
            },
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Emits monotonically increasing ticks at the specified period.
    /// </summary>
    public static IObservable<long> Every(TimeSpan period) =>
        Every(period, ThreadPoolSequencer.Instance);

    /// <summary>
    /// Emits monotonically increasing ticks at the specified period.
    /// </summary>
    public static IObservable<long> Every(TimeSpan period, ISequencer scheduler)
    {
        if (period < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }

        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        return CreateSafe<long>(
            observer =>
            {
                var slot = new SingleReplaceableDisposable();
                var tick = 0L;
                Action? scheduleNext = null;
                scheduleNext = () => slot.Create(scheduler.Schedule(period, () =>
                {
                    observer.OnNext(tick++);
                    if (slot.IsDisposed)
                    {
                        return;
                    }

                    scheduleNext!();
                }));

                scheduleNext();
                return slot;
            },
            scheduler == Sequencer.CurrentThread);
    }

    /// <summary>
    /// Alias for <see cref="Every(TimeSpan, ISequencer?)"/>.
    /// </summary>
    public static IObservable<long> Pulse(TimeSpan period) => Every(period);

    /// <summary>
    /// Alias for <see cref="Every(TimeSpan, ISequencer?)"/>.
    /// </summary>
    public static IObservable<long> Pulse(TimeSpan period, ISequencer scheduler) => Every(period, scheduler);

    /// <summary>
    /// Concatenates the supplied signals.
    /// </summary>
    public static IObservable<T> Chain<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat == null ? FromEnumerable(validated).Chain() : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>
    /// Merges the supplied signals.
    /// </summary>
    public static IObservable<T> Blend<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat == null ? FromEnumerable(validated).Blend() : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>
    /// Races the supplied signals and mirrors the first one to produce a value or terminal signal.
    /// </summary>
    public static IObservable<T> Race<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        if (validated.Length > 0 && validated[0] is RangeSignal)
        {
            return validated[0];
        }

        return FromEnumerable(validated).Race();
    }

    /// <summary>
    /// Mirrors the first supplied signal to produce a value or terminal signal.
    /// </summary>
    public static IObservable<TResult> Pair<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.Pair(right, selector);

    /// <summary>
    /// Combines the latest values from two signals.
    /// </summary>
    public static IObservable<TResult> SyncLatest<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.SyncLatest(right, selector);

    /// <summary>
    /// Combines latest values from two signals using latest-fusion semantics.
    /// </summary>
    public static IObservable<TResult> PairLatest<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.PairLatest(right, selector);

    /// <summary>
    /// Waits for both signals to complete and emits one result from their last values.
    /// </summary>
    public static IObservable<TResult> ForkJoin<TLeft, TRight, TResult>(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector) =>
        left.ForkJoin(right, selector);

    /// <summary>
    /// Validates source arrays supplied to params-based factories.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="sources">The source array.</param>
    /// <returns>The validated source array.</returns>
    private static IObservable<T>[] ValidateSources<T>(IObservable<T>[] sources)
    {
        if (sources == null)
        {
            throw new ArgumentNullException(nameof(sources));
        }

        for (var i = 0; i < sources.Length; i++)
        {
            if (sources[i] == null)
            {
                throw new ArgumentNullException(nameof(sources));
            }
        }

        return sources;
    }

    /// <summary>
    /// Creates a range concat signal when every source is a synchronous integer range.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="sources">The validated sources.</param>
    /// <returns>A range concat signal, or <see langword="null"/> when the fast path is not applicable.</returns>
    private static RangeConcatSignal? TryCreateRangeConcat<T>(IObservable<T>[] sources)
    {
        if (typeof(T) != typeof(int) || sources.Length == 0)
        {
            return null;
        }

        var ranges = new RangeSignal[sources.Length];
        for (var i = 0; i < sources.Length; i++)
        {
            if (sources[i] is not RangeSignal range)
            {
                return null;
            }

            ranges[i] = range;
        }

        return new RangeConcatSignal(ranges);
    }
}
