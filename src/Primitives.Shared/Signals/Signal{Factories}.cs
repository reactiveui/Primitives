// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Additional ReactiveUI.Primitives factory surface for finite, resource, conversion, and time signals.</summary>
public static partial class Signal
{
    /// <summary>Creates a finite integer signal from <paramref name="start"/> for <paramref name="count"/> values.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">The number of values to emit.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<int> Sequence(int start, int count)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        return count == 0 ? ImmutableEmptySignal<int>.Instance : new RangeSignal(start, count);
    }

    /// <summary>Creates a finite integer signal from <paramref name="start"/> for <paramref name="count"/> values on <paramref name="scheduler"/>.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">The number of values to emit.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<int> Sequence(int start, int count, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        if (count == 0)
        {
            return ImmutableEmptySignal<int>.Instance;
        }

        return scheduler == Sequencer.Immediate || scheduler == Sequencer.CurrentThread
            ? new RangeSignal(start, count)
            : new SequenceSignal(start, count, scheduler);
    }

    /// <summary>Creates a signal that repeats a value forever.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Loop<T>(T value) =>
        new LoopSignal<T>(value);

    /// <summary>Creates a signal that repeats a value <paramref name="count"/> times.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="count">The number of times to repeat the value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Loop<T>(T value, int count)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        return count == 0 ? ImmutableEmptySignal<T>.Instance : new RepeatSignal<T>(value, count);
    }

    /// <summary>Unfolds state into a finite signal.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="initialState">The initial state.</param>
    /// <param name="condition">The condition that determines whether to continue.</param>
    /// <param name="iterate">The function that advances the state.</param>
    /// <param name="resultSelector">The function that produces the result from the state.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<TResult> Unfold<TState, TResult>(
        TState initialState,
        Func<TState, bool> condition,
        Func<TState, TState> iterate,
        Func<TState, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(condition);

        ArgumentExceptionHelper.ThrowIfNull(iterate);

        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        return new UnfoldSignal<TState, TResult>(initialState, condition, iterate, resultSelector);
    }

    /// <summary>Generates a finite signal from state. Alias of <see cref="Unfold{TState, TResult}(TState, Func{TState, bool}, Func{TState, TState}, Func{TState, TResult})"/>.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="initialState">The initial state.</param>
    /// <param name="condition">The condition that determines whether to continue.</param>
    /// <param name="iterator">The function that advances the state.</param>
    /// <param name="resultSelector">The function that produces the result from the state.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<TResult> Iterate<TState, TResult>(
        TState initialState,
        Func<TState, bool> condition,
        Func<TState, TState> iterator,
        Func<TState, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(condition);

        ArgumentExceptionHelper.ThrowIfNull(iterator);

        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        return new UnfoldSignal<TState, TResult>(initialState, condition, iterator, resultSelector);
    }

    /// <summary>Creates a signal whose subscription lifetime owns a resource.</summary>
    /// <typeparam name="TResource">The type of the resource.</typeparam>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="resourceFactory">The factory that creates the resource.</param>
    /// <param name="signalFactory">The factory that creates the signal from the resource.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Use<TResource, T>(
        Func<TResource> resourceFactory,
        Func<TResource, IObservable<T>> signalFactory)
        where TResource : IDisposable
    {
        ArgumentExceptionHelper.ThrowIfNull(resourceFactory);

        ArgumentExceptionHelper.ThrowIfNull(signalFactory);

        return new UseSignal<TResource, T>(resourceFactory, signalFactory);
    }

    /// <summary>Converts an event into a signal of event pattern values.</summary>
    /// <param name="addHandler">The action that subscribes the event handler.</param>
    /// <param name="removeHandler">The action that unsubscribes the event handler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<EventPattern<EventArgs>> FromEventPattern(
        Action<EventHandler> addHandler,
        Action<EventHandler> removeHandler)
    {
        ArgumentExceptionHelper.ThrowIfNull(addHandler);

        ArgumentExceptionHelper.ThrowIfNull(removeHandler);

        return new FromEventPatternSignal<EventHandler, EventArgs>(addHandler, removeHandler);
    }

    /// <summary>Converts an event into a signal of event pattern values.</summary>
    /// <typeparam name="TEventArgs">The type of the event arguments.</typeparam>
    /// <param name="addHandler">The action that subscribes the event handler.</param>
    /// <param name="removeHandler">The action that unsubscribes the event handler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<EventPattern<TEventArgs>> FromEventPattern<TEventArgs>(
        Action<EventHandler<TEventArgs>> addHandler,
        Action<EventHandler<TEventArgs>> removeHandler)
        where TEventArgs : EventArgs
    {
        ArgumentExceptionHelper.ThrowIfNull(addHandler);

        ArgumentExceptionHelper.ThrowIfNull(removeHandler);

        return new FromEventPatternSignal<EventHandler<TEventArgs>, TEventArgs>(addHandler, removeHandler);
    }

    /// <summary>Creates a signal from an event add/remove pair.</summary>
    /// <typeparam name="TEventHandler">The delegate type used by the event.</typeparam>
    /// <typeparam name="TEventArgs">The event argument type.</typeparam>
    /// <param name="addHandler">The action that attaches the generated event handler.</param>
    /// <param name="removeHandler">The action that detaches the generated event handler.</param>
    /// <returns>A signal that emits event patterns for each raised event.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="addHandler"/> or <paramref name="removeHandler"/> is <see langword="null"/>.</exception>
    /// <exception cref="NotSupportedException"><typeparamref name="TEventHandler"/> is not a supported event delegate type.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification =
            "The event argument type is part of the returned EventPattern and must be specified for non-generic event handlers.")]
    public static IObservable<EventPattern<TEventArgs>> FromEventPattern<TEventHandler, TEventArgs>(
        Action<TEventHandler> addHandler,
        Action<TEventHandler> removeHandler)
        where TEventHandler : Delegate
        where TEventArgs : EventArgs
    {
        ArgumentExceptionHelper.ThrowIfNull(addHandler);

        ArgumentExceptionHelper.ThrowIfNull(removeHandler);

        return new FromEventPatternSignal<TEventHandler, TEventArgs>(addHandler, removeHandler);
    }

    /// <summary>Creates a signal from an enumerable sequence.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="values">The values to emit.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromEnumerable<T>(IEnumerable<T> values)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        return new FromEnumerableSignal<T>(values);
    }

    /// <summary>Creates a signal from an enumerable sequence and stops enumeration when the token is cancelled.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="values">The values to emit.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromEnumerable<T>(IEnumerable<T> values, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        return cancellationToken.CanBeCanceled
            ? new FromEnumerableSignal<T>(values, cancellationToken)
            : new FromEnumerableSignal<T>(values);
    }

    /// <summary>Creates a signal from a task instance.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="task">The task to convert.</param>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4462:Calls to \"async\" methods should not be blocking",
        Justification =
            "Synchronous read of an already-completed (RanToCompletion) task for an allocation-free fast path; await is invalid in this synchronous factory.")]
    public static IObservable<T> FromTask<T>(Task<T> task)
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

        return task.IsFaulted
            ? new ImmediateThrowSignal<T>(task.Exception!.InnerException ?? task.Exception)
            : new TaskInstanceSignal<T>(task);
    }

    /// <summary>Creates a signal by invoking an asynchronous factory at subscription time.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="taskFactory">The factory that creates the task.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromAsync<T>(Func<Task<T>> taskFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskFactory);

        return new FromAsyncSignal<T>(_ => taskFactory());
    }

    /// <summary>Creates a signal by invoking an asynchronous factory at subscription time.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="taskFactory">The factory that creates the task.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromAsync<T>(Func<CancellationToken, Task<T>> taskFactory) =>
        new FromAsyncSignal<T>(taskFactory);

    /// <summary>Creates a signal by invoking an asynchronous factory at subscription time.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="taskFactory">The factory that creates the task.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromAsync<T>(
        Func<CancellationToken, Task<T>> taskFactory,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskFactory);

        return new FromAsyncExternalCancellationSignal<T>(taskFactory, cancellationToken);
    }

    /// <summary>Fails the sequence if it does not terminate before the timeout.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Expire<T>(IObservable<T> source, TimeSpan dueTime)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new ExpireSignal<T>(source, dueTime, ThreadPoolSequencer.Instance);
    }

    /// <summary>Fails the sequence if it does not terminate before the sequencer timeout.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="dueTime">The timeout duration.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    /// <returns>A sequence that errors with <see cref="TimeoutException"/> when the timeout elapses first.</returns>
    public static IObservable<T> Expire<T>(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        scheduler ??= ThreadPoolSequencer.Instance;
        return new ExpireSignal<T>(source, dueTime, scheduler);
    }

    /// <summary>Runs a function on the supplied scheduler and emits its result.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="function">The function to run.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Start<T>(Func<T> function)
    {
        ArgumentExceptionHelper.ThrowIfNull(function);

        return new StartSignal<T>(function, Sequencer.Default);
    }

    /// <summary>Runs a function on the supplied scheduler and emits its result.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="function">The function to run.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Start<T>(Func<T> function, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(function);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new StartSignal<T>(function, scheduler);
    }

    /// <summary>Runs an action on the supplied scheduler and emits <see cref="RxVoid.Default"/> when it completes.</summary>
    /// <param name="action">The action to run.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<RxVoid> Start(Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return new StartSignal(action, Sequencer.Default);
    }

    /// <summary>Runs an action on the supplied scheduler and emits <see cref="RxVoid.Default"/> when it completes.</summary>
    /// <param name="action">The action to run.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<RxVoid> Start(Action action, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new StartSignal(action, scheduler);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
    /// <summary>Creates a signal from an async enumerable sequence and cancels enumeration when disposed.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="values">The values to emit.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        return new AsyncEnumerableSignal<T>(values, CancellationToken.None);
    }

    /// <summary>Creates a signal from an async enumerable sequence and cancels enumeration when disposed.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="values">The values to emit.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(values);

        return new AsyncEnumerableSignal<T>(values, cancellationToken);
    }

#endif

    /// <summary>Emits a single zero tick after the due time.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(TimeSpan dueTime) =>
        new AfterSignal(dueTime, ThreadPoolSequencer.Instance);

    /// <summary>Emits a single zero tick after the due time.</summary>
    /// <param name="dueTime">The relative time after which to emit the tick.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(dueTime, scheduler);
    }

    /// <summary>Emits a single zero tick at the specified absolute due time.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(DateTimeOffset dueTime) =>
        new AfterSignal(Sequencer.Normalize(dueTime - ThreadPoolSequencer.Instance.Now), ThreadPoolSequencer.Instance);

    /// <summary>Emits a single zero tick at the specified absolute due time.</summary>
    /// <param name="dueTime">The absolute time at which to emit the tick.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(DateTimeOffset dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(Sequencer.Normalize(dueTime - scheduler.Now), scheduler);
    }

    /// <summary>Emits first after <paramref name="dueTime"/> and then at <paramref name="period"/>.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(TimeSpan dueTime, TimeSpan period) =>
        new AfterSignal(dueTime, period, ThreadPoolSequencer.Instance);

    /// <summary>Emits first after <paramref name="dueTime"/> and then at <paramref name="period"/>.</summary>
    /// <param name="dueTime">The relative time before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> After(TimeSpan dueTime, TimeSpan period, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new AfterSignal(dueTime, period, scheduler);
    }

    /// <summary>Emits monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> Every(TimeSpan period)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        return new EverySignal(period, ThreadPoolSequencer.Instance);
    }

    /// <summary>Emits monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<long> Every(TimeSpan period, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new EverySignal(period, scheduler);
    }

    /// <summary>Concatenates the supplied signals.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="sources">The signals to concatenate.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Chain<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat is null ? new ChainSignal<T>(validated) : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>Merges the supplied signals.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="sources">The signals to merge.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Blend<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        var rangeConcat = TryCreateRangeConcat(validated);
        return rangeConcat is null ? new EnumerableBlendSignal<T>(validated) : (IObservable<T>)(object)rangeConcat;
    }

    /// <summary>Races the supplied signals and mirrors the first one to produce a value or terminal signal.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="sources">The signals to race.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Race<T>(params IObservable<T>[] sources)
    {
        var validated = ValidateSources(sources);
        return validated.Length > 0 && validated[0] is RangeSignal ? validated[0] : new RaceSignal<T>(validated);
    }

    /// <summary>Mirrors the first supplied signal to produce a value or terminal signal.</summary>
    /// <typeparam name="TLeft">The type of the left signal values.</typeparam>
    /// <typeparam name="TRight">The type of the right signal values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="left">The left signal.</param>
    /// <param name="right">The right signal.</param>
    /// <param name="selector">The function that combines the paired values.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<TResult> Pair<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);

        ArgumentExceptionHelper.ThrowIfNull(right);

        ArgumentExceptionHelper.ThrowIfNull(selector);

        return typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange &&
               right is RangeSignal rightRange
            ? new RangeZipSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
            : new PairSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>Combines the latest values from two signals.</summary>
    /// <typeparam name="TLeft">The type of the left signal values.</typeparam>
    /// <typeparam name="TRight">The type of the right signal values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="left">The left signal.</param>
    /// <param name="right">The right signal.</param>
    /// <param name="selector">The function that combines the latest values.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<TResult> SyncLatest<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);

        ArgumentExceptionHelper.ThrowIfNull(right);

        ArgumentExceptionHelper.ThrowIfNull(selector);

        return typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange &&
               right is RangeSignal rightRange
            ? new RangeSyncLatestSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
            : new SyncLatestSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>Waits for both signals to complete and emits one result from their last values.</summary>
    /// <typeparam name="TLeft">The type of the left signal values.</typeparam>
    /// <typeparam name="TRight">The type of the right signal values.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="left">The left signal.</param>
    /// <param name="right">The right signal.</param>
    /// <param name="selector">The function that combines the last values.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<TResult> ForkJoin<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);

        ArgumentExceptionHelper.ThrowIfNull(right);

        ArgumentExceptionHelper.ThrowIfNull(selector);

        return typeof(TLeft) == typeof(int) && typeof(TRight) == typeof(int) && left is RangeSignal leftRange &&
               right is RangeSignal rightRange
            ? new RangeForkJoinSignal<TResult>(leftRange, rightRange, (Func<int, int, TResult>)(object)selector)
            : new ForkJoinSignal<TLeft, TRight, TResult>(left, right, selector);
    }

    /// <summary>Validates source arrays supplied to params-based factories.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="sources">The source array.</param>
    /// <returns>The validated source array.</returns>
    private static IObservable<T>[] ValidateSources<T>(IObservable<T>[] sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources[i]);
        }

        return sources;
    }

    /// <summary>Creates a range concat signal when every source is a synchronous integer range.</summary>
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

        return new(ranges);
    }
}
