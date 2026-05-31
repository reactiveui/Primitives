// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Extension methods for Reactive objects.
/// </summary>
[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with \'Async\'", Justification = "Existing API")]
public static class ReactiveExtensions
{
    /// <summary>Default backoff factor for <see cref="RetryWithBackoff{T}(IObservable{T}, int, TimeSpan)"/>: each retry doubles the previous delay.</summary>
    private const double DefaultBackoffFactor = 2.0;

    /// <summary>
    /// Returns only values that are not null.
    /// Converts the nullability.
    /// </summary>
    /// <typeparam name="T">The type of value emitted by the observable.</typeparam>
    /// <param name="observable">The observable that can contain nulls.</param>
    /// <returns>A non nullable version of the observable that only emits valid values.</returns>
    public static IObservable<T> WhereIsNotNull<T>(this IObservable<T> observable) =>
        new WhereIsNotNullObservable<T>(observable);

    /// <summary>
    /// Change the source observable type to <see cref="RxVoid"/>.
    /// This allows us to be notified when the observable emits a value.
    /// </summary>
    /// <typeparam name="T">The current type of the observable.</typeparam>
    /// <param name="observable">The observable to convert.</param>
    /// <returns>The signal.</returns>
    public static IObservable<RxVoid> AsSignal<T>(this IObservable<T> observable) =>
        new AsSignalObservable<T>(observable);

    /// <summary>
    /// Synchronized timer all instances of this with the same TimeSpan use the same timer.
    /// </summary>
    /// <param name="timeSpan">The time span.</param>
    /// <returns>An observable sequence producing the shared DateTime ticks.</returns>
    public static IObservable<DateTime> SyncTimer(TimeSpan timeSpan) =>
        SyncTimerObservable.Get(timeSpan, Sequencer.Default);

    /// <summary>
    /// Synchronized timer all instances of this with the same TimeSpan and scheduler use the same timer.
    /// </summary>
    /// <param name="timeSpan">The time span.</param>
    /// <param name="scheduler">Scheduler used to emit ticks.</param>
    /// <returns>An observable sequence producing the shared DateTime ticks.</returns>
    public static IObservable<DateTime> SyncTimer(TimeSpan timeSpan, ISequencer scheduler) =>
        SyncTimerObservable.Get(timeSpan, scheduler);

    /// <summary>
    /// Buffers until Start char and End char are found.
    /// </summary>
    /// <param name="this">The source observable of characters.</param>
    /// <param name="startsWith">The starting delimiter.</param>
    /// <param name="endsWith">The ending delimiter.</param>
    /// <returns>A sequence of buffered strings including the start and end delimiters.</returns>
    public static IObservable<string> BufferUntil(this IObservable<char> @this, char startsWith, char endsWith) =>
        new BufferUntilObservable(@this, startsWith, endsWith);

    /// <summary>
    /// Emit a batch when the stream goes quiet.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="idleTime">The idle time.</param>
    /// <returns>A sequence of buffered lists.</returns>
    public static IObservable<IList<T>> BufferUntilIdle<T>(
        this IObservable<T> source,
        TimeSpan idleTime) =>
        source.BufferUntilIdle(idleTime, null);

    /// <summary>
    /// Emit a batch when the stream goes quiet.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="idleTime">The idle time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>A sequence of buffered lists.</returns>
    public static IObservable<IList<T>> BufferUntilIdle<T>(
        this IObservable<T> source,
        TimeSpan idleTime,
        ISequencer? scheduler) =>
        new BufferUntilIdleObservable<T>(source, idleTime, scheduler ?? Sequencer.Default);

    /// <summary>
    /// Catch exception and return Observable.Empty.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>A sequence that ignores errors and completes.</returns>
    public static IObservable<TSource?> CatchIgnore<TSource>(this IObservable<TSource?> source) =>
        new CatchIgnoreEmptyObservable<TSource?>(source);

    /// <summary>
    /// Catch exception and return Observable.Empty.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="errorAction">The error action.</param>
    /// <returns>A sequence that invokes <paramref name="errorAction"/> on error and completes.</returns>
    public static IObservable<TSource> CatchIgnore<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> errorAction)
        where TException : Exception =>
        new CatchIgnoreObservable<TSource, TException>(source, errorAction);

    /// <summary>
    /// Latest values of each sequence are all false.
    /// </summary>
    /// <param name="sources">The sources.</param>
    /// <returns>A sequence that emits true when all latest booleans are false.</returns>
    public static IObservable<bool> CombineLatestValuesAreAllFalse(this IEnumerable<IObservable<bool>> sources) =>
        new BooleanReduceObservable(sources, target: false);

    /// <summary>
    /// Latest values of each sequence are all true.
    /// </summary>
    /// <param name="sources">The sources.</param>
    /// <returns>A sequence that emits true when all latest booleans are true.</returns>
    public static IObservable<bool> CombineLatestValuesAreAllTrue(this IEnumerable<IObservable<bool>> sources) =>
        new BooleanReduceObservable(sources, target: true);

    /// <summary>
    /// Gets the maximum from all sources.
    /// </summary>
    /// <typeparam name="T">The Value Type.</typeparam>
    /// <param name="this">The first observable.</param>
    /// <param name="sources">Other sources.</param>
    /// <returns>A sequence emitting the maximum of the latest values.</returns>
    public static IObservable<T> GetMax<T>(this IObservable<T> @this, params IObservable<T>[] sources)
        where T : struct, IComparable<T>
    {
        var allSources = new IObservable<T>[sources.Length + 1];
        allSources[0] = @this;
        Array.Copy(sources, 0, allSources, 1, sources.Length);
        return new MinMaxObservable<T>(allSources, emitMaximum: true);
    }

    /// <summary>
    /// Gets the minimum from all sources.
    /// </summary>
    /// <typeparam name="T">The Value Type.</typeparam>
    /// <param name="this">The first observable.</param>
    /// <param name="sources">Other sources.</param>
    /// <returns>A sequence emitting the minimum of the latest values.</returns>
    public static IObservable<T> GetMin<T>(this IObservable<T> @this, params IObservable<T>[] sources)
        where T : struct, IComparable<T>
    {
        var allSources = new IObservable<T>[sources.Length + 1];
        allSources[0] = @this;
        Array.Copy(sources, 0, allSources, 1, sources.Length);
        return new MinMaxObservable<T>(allSources, emitMaximum: false);
    }

    /// <summary>
    /// Detects when a stream becomes inactive for some period of time.
    /// </summary>
    /// <typeparam name="T">update type.</typeparam>
    /// <param name="source">source stream.</param>
    /// <param name="stalenessPeriod">If source stream does not OnNext any update during this period, it is declared stale.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>Observable stale markers or updates.</returns>
    public static IObservable<Stale<T>> DetectStale<T>(
        this IObservable<T> source,
        TimeSpan stalenessPeriod,
        ISequencer scheduler) =>
        new DetectStaleObservable<T>(source, stalenessPeriod, scheduler);

    /// <summary>
    /// Applies a conflation algorithm to an observable stream. Anytime the stream OnNext twice
    /// below minimumUpdatePeriod, the second update gets delayed to respect the
    /// minimumUpdatePeriod. If more than 2 updates happen, only the last update is pushed.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The stream.</param>
    /// <param name="minimumUpdatePeriod">Minimum delay between two updates.</param>
    /// <param name="scheduler">Scheduler to publish updates.</param>
    /// <returns>The conflated stream.</returns>
    public static IObservable<T> Conflate<T>(
        this IObservable<T> source,
        TimeSpan minimumUpdatePeriod,
        ISequencer scheduler) =>
        new ConflateObservable<T>(source, minimumUpdatePeriod, scheduler);

    /// <summary>
    /// Injects heartbeats in a stream when the source stream becomes quiet.
    /// </summary>
    /// <typeparam name="T">Update type.</typeparam>
    /// <param name="source">Source stream.</param>
    /// <param name="heartbeatPeriod">Period between heartbeats.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Observable heartbeat values.</returns>
    public static IObservable<Heartbeat<T>> Heartbeat<T>(
        this IObservable<T> source,
        TimeSpan heartbeatPeriod,
        ISequencer scheduler) =>
        new HeartbeatObservable<T>(source, heartbeatPeriod, scheduler);

    /// <summary>
    /// Emit the latest value or a default if none exists.
    /// </summary>
    /// <typeparam name="T">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <returns>A sequence that emits the latest value or the default.</returns>
    public static IObservable<T> LatestOrDefault<T>(
        this IObservable<T> source,
        T defaultValue) => new LatestOrDefaultObservable<T>(source, defaultValue);

    /// <summary>
    /// Logs the errors. Inline error logging without terminating the stream.
    /// </summary>
    /// <typeparam name="T">The type of the source.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="logger">The logger.</param>
    /// <returns>A sequence that logs errors.</returns>
    public static IObservable<T> LogErrors<T>(
        this IObservable<T> source,
        Action<Exception> logger)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(logger);
        return new LogErrorsObservable<T>(source, logger);
    }

    /// <summary>
    /// Executes with limited concurrency.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="taskFunctions">Tasks to execute.</param>
    /// <param name="maxConcurrency">Maximum concurrency.</param>
    /// <returns>A sequence of task results.</returns>
    public static IObservable<T> WithLimitedConcurrency<T>(this IEnumerable<Task<T>> taskFunctions, int maxConcurrency)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskFunctions);

        return new ConcurrencyLimiter<T>(taskFunctions, maxConcurrency).Observable;
    }

    /// <summary>
    /// Pushes multiple values to an observer.
    /// </summary>
    /// <typeparam name="T">Type of value.</typeparam>
    /// <param name="observer">Observer to push to.</param>
    /// <param name="events">Values to push.</param>
    public static void OnNext<T>(this IObserver<T> observer, params T[] events)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(events);

        observer.FastForEach(events);
    }

    /// <summary>
    /// If the scheduler is not null observes on that scheduler.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="scheduler">Scheduler to notify observers on (optional).</param>
    /// <returns>The source sequence whose callbacks happen on the specified scheduler.</returns>
    public static IObservable<TSource>
        ObserveOnSafe<TSource>(this IObservable<TSource> source, ISequencer? scheduler) =>
        scheduler == null ? source : new ObserveOnObservable<TSource>(source, scheduler);

    /// <summary>
    /// Conditionally switch schedulers.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="condition">if set to <c>true</c> [condition].</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ObserveOnIf<T>(
        this IObservable<T> source,
        bool condition,
        ISequencer scheduler) => condition ? new ObserveOnObservable<T>(source, scheduler) : source;

    /// <summary>
    /// Conditionally switch schedulers.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="condition">if set to <c>true</c> [condition].</param>
    /// <param name="trueScheduler">The true scheduler.</param>
    /// <param name="falseScheduler">The false scheduler.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ObserveOnIf<T>(
        this IObservable<T> source,
        bool condition,
        ISequencer trueScheduler,
        ISequencer falseScheduler) => condition
        ? new ObserveOnObservable<T>(source, trueScheduler)
        : new ObserveOnObservable<T>(source, falseScheduler);

    /// <summary>
    /// Conditionally switch schedulers based on a reactive condition.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="condition">The reactive condition.</param>
    /// <param name="trueScheduler">The scheduler to use when condition is true.</param>
    /// <param name="falseScheduler">The scheduler to use when condition is false.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ObserveOnIf<T>(
        this IObservable<T> source,
        IObservable<bool> condition,
        ISequencer trueScheduler,
        ISequencer falseScheduler) =>
        new ObserveOnIfObservable<T>(source, condition, trueScheduler, falseScheduler);

    /// <summary>
    /// Conditionally switch schedulers based on a reactive condition.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="condition">The reactive condition.</param>
    /// <param name="scheduler">The scheduler to use when condition is true.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ObserveOnIf<T>(
        this IObservable<T> source,
        IObservable<bool> condition,
        ISequencer scheduler) =>
        new ObserveOnIfObservable<T>(source, condition, scheduler, Sequencer.Immediate);

    /// <summary>
    /// Skip null values until the first non-null appears.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> SkipWhileNull<T>(this IObservable<T> source)
        where T : class
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return new SkipWhileNullObservable<T>(source);
    }

    /// <summary>
    /// Invokes the action asynchronously surfacing the result through a RxVoid observable.
    /// </summary>
    /// <param name="action">Action to run.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>A sequence producing RxVoid upon completion.</returns>
    public static IObservable<RxVoid> Start(Action action, ISequencer? scheduler) =>
        new StartActionObservable(action, scheduler);

    /// <summary>
    /// Invokes the specified function asynchronously surfacing the result.
    /// </summary>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="function">Function to run.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>A sequence producing the function result.</returns>
    public static IObservable<TResult> Start<TResult>(Func<TResult> function, ISequencer? scheduler) =>
        new StartFuncObservable<TResult>(function, scheduler);

    /// <summary>
    /// Flattens a sequence of enumerables into individual values.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source of enumerables.</param>
    /// <returns>A flattened observable.</returns>
    public static IObservable<T> ForEach<T>(this IObservable<IEnumerable<T>> source) =>
        source.ForEach(null);

    /// <summary>
    /// Flattens a sequence of enumerables into individual values.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source of enumerables.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>A flattened observable.</returns>
    public static IObservable<T> ForEach<T>(this IObservable<IEnumerable<T>> source, ISequencer? scheduler) =>
        new ForEachObservable<T>(source, scheduler);

    /// <summary>
    /// Schedules an action immediately if scheduler null, else on scheduler.
    /// </summary>
    /// <param name="scheduler">Scheduler.</param>
    /// <param name="action">Action.</param>
    /// <returns>Disposable for the scheduled action.</returns>
    public static IDisposable ScheduleSafe(this ISequencer? scheduler, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler != null)
        {
            return scheduler.Schedule(action);
        }

        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>
    /// Schedules an action after a due time.
    /// </summary>
    /// <param name="scheduler">Scheduler.</param>
    /// <param name="dueTime">Delay.</param>
    /// <param name="action">Action.</param>
    /// <returns>Disposable for the scheduled action.</returns>
    public static IDisposable ScheduleSafe(this ISequencer? scheduler, TimeSpan dueTime, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        if (scheduler == null)
        {
            Thread.Sleep(dueTime);
            action();
            return EmptyDisposable.Instance;
        }

        return scheduler.Schedule(dueTime, action);
    }

    /// <summary>
    /// Emits each element of an IEnumerable.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source enumerable.</param>
    /// <returns>Observable of elements.</returns>
    public static IObservable<T> FromArray<T>(this IEnumerable<T> source) =>
        source.FromArray(null);

    /// <summary>
    /// Emits each element of an IEnumerable.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source enumerable.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>Observable of elements.</returns>
    public static IObservable<T> FromArray<T>(this IEnumerable<T> source, ISequencer? scheduler) =>
        new FromArrayObservable<T>(source, scheduler);

    /// <summary>
    /// Using helper with Action.
    /// </summary>
    /// <typeparam name="T">Disposable type.</typeparam>
    /// <param name="obj">Object to use.</param>
    /// <param name="action">Action to run.</param>
    /// <returns>Completion signal.</returns>
    public static IObservable<RxVoid> Using<T>(this T obj, Action<T>? action)
        where T : IDisposable =>
        obj.Using(action, null);

    /// <summary>
    /// Using helper with Action.
    /// </summary>
    /// <typeparam name="T">Disposable type.</typeparam>
    /// <param name="obj">Object to use.</param>
    /// <param name="action">Action to run.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Completion signal.</returns>
    public static IObservable<RxVoid> Using<T>(this T obj, Action<T>? action, ISequencer? scheduler)
        where T : IDisposable =>
        new UsingActionObservable<T>(obj, action, scheduler);

    /// <summary>
    /// Using helper with Func.
    /// </summary>
    /// <typeparam name="T">Disposable type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="obj">Object to use.</param>
    /// <param name="function">Function to invoke.</param>
    /// <returns>Observable of result.</returns>
    public static IObservable<TResult> Using<T, TResult>(
        this T obj,
        Func<T, TResult> function)
        where T : IDisposable => obj.Using(function, null);

    /// <summary>
    /// Using helper with Func.
    /// </summary>
    /// <typeparam name="T">Disposable type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="obj">Object to use.</param>
    /// <param name="function">Function to invoke.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Observable of result.</returns>
    public static IObservable<TResult> Using<T, TResult>(
        this T obj,
        Func<T, TResult> function,
        ISequencer? scheduler)
        where T : IDisposable => new UsingFuncObservable<T, TResult>(obj, function, scheduler);

    /// <summary>
    /// While construct.
    /// </summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>Observable representing the loop.</returns>
    public static IObservable<RxVoid> While(Func<bool> condition, Action action) =>
        While(condition, action, null);

    /// <summary>
    /// While construct.
    /// </summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="action">Action to execute.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Observable representing the loop.</returns>
    public static IObservable<RxVoid> While(Func<bool> condition, Action action, ISequencer? scheduler) =>
        new WhileObservable(condition, action, scheduler);

    /// <summary>
    /// Sample the latest value whenever a trigger fires.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="trigger">The trigger.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> SampleLatest<T>(
        this IObservable<T> source,
        IObservable<object> trigger) => new SampleLatestObservable<T>(source, trigger);

    /// <summary>
    /// Scan that always emits the initial value first.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulate.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="initial">The initial.</param>
    /// <param name="accumulator">The accumulator.</param>
    /// <returns>An IObservable of TAccumulate.</returns>
    public static IObservable<TAccumulate> ScanWithInitial<TSource, TAccumulate>(
        this IObservable<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, TAccumulate> accumulator) =>
        new ScanWithInitialObservable<TSource, TAccumulate>(source, initial, accumulator);

    /// <summary>
    /// Schedules a single value after a delay.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value.</param>
    /// <param name="dueTime">Delay.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Observable that emits the value.</returns>
    public static IObservable<T> Schedule<T>(this T value, TimeSpan dueTime, ISequencer scheduler) =>
        new ScheduledValueObservable<T>(value, scheduler, dueTime, null, null, null);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(this IObservable<T> source, TimeSpan dueTime, ISequencer scheduler) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime));

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(this T value, DateTimeOffset dueTime, ISequencer scheduler) =>
        new ScheduledValueObservable<T>(value, scheduler, null, dueTime, null, null);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T>
        Schedule<T>(this IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Absolute(scheduler, dueTime));

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(this T value, TimeSpan dueTime, ISequencer scheduler, Action<T> action) =>
        new ScheduledValueObservable<T>(value, scheduler, dueTime, null, null, action);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(
        this IObservable<T> source,
        TimeSpan dueTime,
        ISequencer scheduler,
        Action<T> action) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime).WithAction(action));

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(
        this T value,
        DateTimeOffset dueTime,
        ISequencer scheduler,
        Action<T> action) =>
        new ScheduledValueObservable<T>(value, scheduler, null, dueTime, null, action);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(
        this IObservable<T> source,
        DateTimeOffset dueTime,
        ISequencer scheduler,
        Action<T> action) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Absolute(scheduler, dueTime).WithAction(action));

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="function">The function.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(this T value, ISequencer scheduler, Func<T, T> function) =>
        new ScheduledValueObservable<T>(value, scheduler, null, null, function, null);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="function">The function.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(this IObservable<T> source, ISequencer scheduler, Func<T, T> function) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Immediate(scheduler).WithTransform(function));

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="function">The function.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T>
        Schedule<T>(this T value, TimeSpan dueTime, ISequencer scheduler, Func<T, T> function) =>
        new ScheduledValueObservable<T>(value, scheduler, dueTime, null, function, null);

    /// <summary>
    /// Schedules the specified due time.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The value.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="function">The function.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> Schedule<T>(
        this IObservable<T> source,
        TimeSpan dueTime,
        ISequencer scheduler,
        Func<T, T> function) =>
        new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime).WithTransform(function));

    /// <summary>
    /// Filters strings by regex.
    /// </summary>
    /// <param name="source">Source sequence.</param>
    /// <param name="regexPattern">Regex pattern.</param>
    /// <returns>Filtered sequence.</returns>
    public static IObservable<string> Filter(this IObservable<string> source, string regexPattern) =>
        source.Filter(new Regex(regexPattern, RegexOptions.None, TimeSpan.FromSeconds(1)));

    /// <summary>
    /// Filters strings by regex.
    /// </summary>
    /// <param name="source">Source sequence.</param>
    /// <param name="regex">Regex.</param>
    /// <returns>Filtered sequence.</returns>
    public static IObservable<string> Filter(this IObservable<string> source, Regex regex) =>
        new FilterRegexObservable(source, regex);

    /// <summary>
    /// Randomly shuffles arrays emitted by the source.
    /// </summary>
    /// <typeparam name="T">Array element type.</typeparam>
    /// <param name="source">Source array sequence.</param>
    /// <returns>Sequence of shuffled arrays (in-place).</returns>
    public static IObservable<T[]> Shuffle<T>(this IObservable<T[]> source) => new ShuffleObservable<T>(source);

    /// <summary>
    /// Repeats the source until it terminates successfully (alias of Retry).
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <returns>Retried sequence.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource>(this IObservable<TSource> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        return new RetryForeverObservable<TSource>(source);
    }

    /// <summary>
    /// When caught exception, do onError action and repeat observable sequence.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onError">The on error.</param>
    /// <returns>A sequence that retries on error with optional delay.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> onError)
        where TException : Exception => source.OnErrorRetry(onError, TimeSpan.Zero);

    /// <summary>
    /// When caught exception, do onError action and repeat observable sequence after delay time.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="delay">The delay.</param>
    /// <returns>A sequence that retries on error with optional delay.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> onError,
        TimeSpan delay)
        where TException : Exception => source.OnErrorRetry(onError, int.MaxValue, delay);

    /// <summary>
    /// When caught exception, do onError action and repeat observable sequence during within retryCount.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="retryCount">The retry count.</param>
    /// <returns>A sequence that retries on error with optional delay.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> onError,
        int retryCount)
        where TException : Exception => source.OnErrorRetry(onError, retryCount, TimeSpan.Zero);

    /// <summary>
    /// When caught exception, do onError action and repeat observable sequence after delay time
    /// during within retryCount.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="retryCount">The retry count.</param>
    /// <param name="delay">The delay.</param>
    /// <returns>A sequence that retries on error with optional delay.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> onError,
        int retryCount,
        TimeSpan delay)
        where TException : Exception => source.OnErrorRetry(onError, retryCount, delay, Sequencer.Default);

    /// <summary>
    /// When caught exception, do onError action and repeat observable sequence after delay
    /// time(work on delayScheduler) during within retryCount.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TException">The type of the exception.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="retryCount">The retry count.</param>
    /// <param name="delay">The delay.</param>
    /// <param name="delayScheduler">The delay scheduler.</param>
    /// <returns>A sequence that retries on error with optional delay.</returns>
    public static IObservable<TSource> OnErrorRetry<TSource, TException>(
        this IObservable<TSource> source,
        Action<TException> onError,
        int retryCount,
        TimeSpan delay,
        ISequencer delayScheduler)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new RetryWithBackoffObservable<TSource>(
            source,
            new RetryBackoffPolicy(
                MaxRetries: retryCount,
                InitialDelay: delay,
                BackoffFactor: 1.0,
                MaxDelay: null,
                Scheduler: delayScheduler,
                OnError: ex =>
                {
                    if (ex is not TException tex)
                    {
                        return;
                    }

                    onError(tex);
                }));
    }

    /// <summary>
    /// Takes elements until predicate returns true for an element (inclusive) then completes.
    /// </summary>
    /// <typeparam name="TSource">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="predicate">Predicate for completion.</param>
    /// <returns>Sequence that completes when predicate satisfied.</returns>
    public static IObservable<TSource> TakeUntil<TSource>(
        this IObservable<TSource> source,
        Func<TSource, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        return new TakeUntilInclusiveObservable<TSource>(source, predicate);
    }

    /// <summary>
    /// Wraps values with a synchronization disposable that completes when disposed.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <returns>Sequence of (value, sync handle).</returns>
    public static IObservable<(T Value, IDisposable Sync)> SynchronizeSynchronous<T>(this IObservable<T> source) =>
        new SynchronizeAsyncObservable<T>(source);

    /// <summary>
    /// Subscribes to the specified source synchronously.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The on next.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="onCompleted">The on completed.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
    public static IDisposable SubscribeSynchronous<T>(
        this IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        new SubscribeAsyncObservable<T>(source, onNext, onError, onCompleted);

    /// <summary>
    /// Subscribes an element handler and an exception handler to an observable sequence synchronously.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onError">Action to invoke upon exceptional termination of the observable sequence.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
    public static IDisposable SubscribeSynchronous<T>(
        this IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action<Exception> onError) =>
        new SubscribeAsyncObservable<T>(source, onNext, onError);

    /// <summary>
    /// Subscribes an element handler and a completion handler to an observable sequence synchronously.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onCompleted">Action to invoke upon graceful termination of the observable sequence.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> or <paramref name="onCompleted"/> is <c>null</c>.</exception>
    public static IDisposable SubscribeSynchronous<T>(
        this IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action onCompleted) =>
        new SubscribeAsyncObservable<T>(source, onNext, onCompleted: onCompleted);

    /// <summary>
    /// Subscribes an element handler to an observable sequence synchronously.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
    public static IDisposable SubscribeSynchronous<T>(this IObservable<T> source, Func<T, ValueTask> onNext) =>
        new SubscribeAsyncObservable<T>(source, onNext);

    /// <summary>
    /// Provide a fallback observable if the source completes without emitting.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="fallback">The fallback.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> SwitchIfEmpty<T>(
        this IObservable<T> source,
        IObservable<T> fallback) => new SwitchIfEmptyObservable<T>(source, fallback);

    /// <summary>
    /// Synchronizes the asynchronous operations in downstream operations.
    /// Use SubscribeSynchronus instead for a simpler version.
    /// Call Sync.Dispose() to release the lock in the downstream methods.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>An Observable of T and a release mechanism.</returns>
    public static IObservable<(T Value, IDisposable Sync)> SynchronizeAsync<T>(this IObservable<T> source) =>
        new SynchronizeAsyncObservable<T>(source);

    /// <summary>
    /// Subscribes allowing asynchronous operations to be executed without blocking the source.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IDisposable SubscribeAsync<T>(this IObservable<T> source, Func<T, ValueTask> onNext) =>
        new SubscribeAsyncObservable<T>(source, onNext);

    /// <summary>
    /// Subscribes allowing asynchronous operations to be executed without blocking the source.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onCompleted">The on completed.</param>
    /// <returns>
    ///   <see cref="IDisposable" /> object used to unsubscribe from the observable sequence.
    /// </returns>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IDisposable SubscribeAsync<T>(this IObservable<T> source, Func<T, ValueTask> onNext, Action onCompleted) =>
        new SubscribeAsyncObservable<T>(source, onNext, onCompleted: onCompleted);

    /// <summary>
    /// Subscribes allowing asynchronous operations to be executed without blocking the source.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onError">The on error.</param>
    /// <returns>
    ///   <see cref="IDisposable" /> object used to unsubscribe from the observable sequence.
    /// </returns>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IDisposable SubscribeAsync<T>(
        this IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action<Exception> onError) =>
        new SubscribeAsyncObservable<T>(source, onNext, onError);

    /// <summary>
    /// Subscribes allowing asynchronous operations to be executed without blocking the source.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">Observable sequence to subscribe to.</param>
    /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
    /// <param name="onError">The on error.</param>
    /// <param name="onCompleted">The on completed.</param>
    /// <returns>
    ///   <see cref="IDisposable" /> object used to unsubscribe from the observable sequence.
    /// </returns>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IDisposable SubscribeAsync<T>(
        this IObservable<T> source,
        Func<T, ValueTask> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        new SubscribeAsyncObservable<T>(source, onNext, onError, onCompleted);

    /// <summary>
    /// Emits the boolean negation of the source sequence.
    /// </summary>
    /// <param name="source">Boolean source.</param>
    /// <returns>Negated boolean sequence.</returns>
    public static IObservable<bool> Not(this IObservable<bool> source) => new NotObservable(source);

    /// <summary>
    /// Filters to true values only.
    /// </summary>
    /// <param name="source">Boolean source.</param>
    /// <returns>Sequence of true values.</returns>
    public static IObservable<bool> WhereTrue(this IObservable<bool> source) => new WhereTrueObservable(source);

    /// <summary>
    /// Filters to false values only.
    /// </summary>
    /// <param name="source">Boolean source.</param>
    /// <returns>Sequence of false values.</returns>
    public static IObservable<bool> WhereFalse(this IObservable<bool> source) => new WhereFalseObservable(source);

    /// <summary>
    /// Catches any error and returns a fallback value then completes.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="fallback">Fallback value.</param>
    /// <returns>Sequence producing either original values or fallback on error then completing.</returns>
    public static IObservable<T> CatchAndReturn<T>(this IObservable<T> source, T fallback) =>
        new CatchReturnObservable<T>(source, fallback);

    /// <summary>
    /// Catches a specific exception type mapping it to a fallback value.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <typeparam name="TException">Exception type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="fallbackFactory">Factory producing fallback from the exception.</param>
    /// <returns>Recovered sequence.</returns>
    public static IObservable<T> CatchAndReturn<T, TException>(
        this IObservable<T> source,
        Func<TException, T> fallbackFactory)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(fallbackFactory);
        return new CatchAndReturnWithFactoryObservable<T, TException>(source, fallbackFactory);
    }

    /// <summary>
    /// Retries with exponential backoff.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial backoff delay.</param>
    /// <returns>Retried sequence with backoff.</returns>
    public static IObservable<T> RetryWithBackoff<T>(
        this IObservable<T> source,
        int maxRetries,
        TimeSpan initialDelay) =>
        source.RetryWithBackoff(maxRetries, initialDelay, DefaultBackoffFactor, null, null);

    /// <summary>
    /// Retries with exponential backoff.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial backoff delay.</param>
    /// <param name="backoffFactor">Multiplier for each retry (default 2).</param>
    /// <param name="maxDelay">Optional maximum delay.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>Retried sequence with backoff.</returns>
    public static IObservable<T> RetryWithBackoff<T>(
        this IObservable<T> source,
        int maxRetries,
        TimeSpan initialDelay,
        double backoffFactor,
        TimeSpan? maxDelay,
        ISequencer? scheduler) =>
        new RetryWithBackoffObservable<T>(
            source,
            new RetryBackoffPolicy(
                MaxRetries: maxRetries,
                InitialDelay: initialDelay,
                BackoffFactor: backoffFactor,
                MaxDelay: maxDelay,
                Scheduler: scheduler ?? Sequencer.Default,
                OnError: null));

    /// <summary>
    /// Retry with exponential.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="retryCount">The retry count.</param>
    /// <param name="delaySelector">The delay selector.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> RetryWithDelay<T>(
        this IObservable<T> source,
        int retryCount,
        Func<int, TimeSpan> delaySelector) =>
        new RetryWithDelayObservable<T>(source, retryCount, delaySelector);

    /// <summary>
    /// Retries the forever with delay.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="delay">The delay.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> RetryForeverWithDelay<T>(this IObservable<T> source, TimeSpan delay) =>
        new RetryWithDelayObservable<T>(source, int.MaxValue, _ => delay);

    /// <summary>
    /// Retry with fixed backoff.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="retryCount">The retry count.</param>
    /// <param name="delay">The delay.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> RetryWithFixedDelay<T>(
        this IObservable<T> source,
        int retryCount,
        TimeSpan delay)
        => new RetryWithBackoffObservable<T>(
            source,
            new RetryBackoffPolicy(
                MaxRetries: retryCount,
                InitialDelay: delay,
                BackoffFactor: 1.0,
                MaxDelay: null,
                Scheduler: Sequencer.Default,
                OnError: null));

    /// <summary>
    /// Always replay the last value, even if the source hasnt produced one yet.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="initialValue">The initial value.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ReplayLastOnSubscribe<T>(
        this IObservable<T> source,
        T initialValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return new ReplayLastOnSubscribeObservable<T>(source, initialValue);
    }

    /// <summary>
    /// Emits only the first value in each time window.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="window">Time window.</param>
    /// <returns>Throttle-first sequence.</returns>
    public static IObservable<T> ThrottleFirst<T>(
        this IObservable<T> source,
        TimeSpan window) =>
        source.ThrottleFirst(window, null);

    /// <summary>
    /// Emits only the first value in each time window.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="window">Time window.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>Throttle-first sequence.</returns>
    public static IObservable<T> ThrottleFirst<T>(
        this IObservable<T> source,
        TimeSpan window,
        ISequencer? scheduler) =>
        new ThrottleFirstObservable<T>(source, window, scheduler ?? Sequencer.Default);

    /// <summary>
    /// Throttle until a predicate becomes true.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="throttle">The throttle.</param>
    /// <param name="predicate">The predicate.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> ThrottleUntilTrue<T>(
        this IObservable<T> source,
        TimeSpan throttle,
        Func<T, bool> predicate) =>
        new ThrottleUntilTrueObservable<T>(source, throttle, predicate);

    /// <summary>
    /// Throttles the on scheduler.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="timeSpan">The time span.</param>
    /// <param name="scheduler">A scheduler for the operation.</param>
    /// <returns>A observable for the throttle operation.</returns>
    public static IObservable<T> ThrottleOnScheduler<T>(
        this IObservable<T> source,
        TimeSpan timeSpan,
        ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        return new ThrottleObservable<T>(source, timeSpan, scheduler);
    }

    /// <summary>
    /// Builds a current-value subject pair: a read-only observable and the push-side observer.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="initialValue">The initial value.</param>
    /// <returns>A tuple of IObservable and IObserver.</returns>
    public static (IObservable<T> Observable, IObserver<T> Observer) ToReadOnlyBehavior<T>(T initialValue)
    {
        var subject = new CurrentValueSubject<T>(initialValue);
        return (subject.AsObservable(), subject);
    }

    /// <summary>
    /// Convert an observable to a Task that starts immediately.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>A Task of T.</returns>
    public static Task<T> ToHotTask<T>(this IObservable<T> source) => FirstAsTaskHelper.FirstAsTask(source);

    /// <summary>
    /// Convert an observable to a <see cref="ValueTask{T}"/> that starts immediately. Backed by a
    /// pooled <see cref="System.Threading.Tasks.Sources.IValueTaskSource{T}"/> implementation, so
    /// steady-state callers pay no allocations after the per-type pool warms up. Prefer this over
    /// <see cref="ToHotTask{T}"/> when the call site can consume a <see cref="ValueTask{T}"/>
    /// (single await, no caching, no <c>WhenAll</c>).
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>A <see cref="ValueTask{T}"/> that completes with the first value, faults on source error, or faults on empty completion.</returns>
    public static ValueTask<T> ToHotValueTask<T>(this IObservable<T> source) =>
        FirstAsValueTaskHelper<T>.FirstAsValueTask(source);

    /// <summary>
    /// Convert a property getter into an observable that emits on change.
    /// </summary>
    /// <typeparam name="T">The type of the source.</typeparam>
    /// <typeparam name="TProperty">The type of the property.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="propertyExpression">The property expression.</param>
    /// <returns>An IObservable of TProperty.</returns>
    /// <exception cref="ArgumentException">Expression must be a property.</exception>
    public static IObservable<TProperty> ToPropertyObservable<T, TProperty>(
        this T source,
        Expression<Func<T, TProperty>> propertyExpression)
        where T : INotifyPropertyChanged
    {
        ArgumentExceptionHelper.ThrowIfNull(propertyExpression);

        var member = propertyExpression.Body as MemberExpression
                     ?? throw new ArgumentException("Expression must be a property");

        return new PropertyChangedObservable<T, TProperty>(
            source,
            member.Member.Name,
            propertyExpression.Compile());
    }

    /// <summary>
    /// Throttle but only emit when the value actually changes.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="throttle">The throttle.</param>
    /// <returns>A throttled distinct sequence.</returns>
    public static IObservable<T> ThrottleDistinct<T>(
        this IObservable<T> source,
        TimeSpan throttle) =>
        new ThrottleDistinctObservable<T>(source, throttle, Sequencer.Default);

    /// <summary>
    /// Throttle but only emit when the value actually changes.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="throttle">The throttle.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>A throttled distinct sequence.</returns>
    public static IObservable<T> ThrottleDistinct<T>(
        this IObservable<T> source,
        TimeSpan throttle,
        ISequencer scheduler) =>
        new ThrottleDistinctObservable<T>(source, throttle, scheduler);

    /// <summary>
    /// Debounces with an immediate first emission then standard debounce behavior.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="dueTime">Debounce time.</param>
    /// <returns>Debounced sequence.</returns>
    public static IObservable<T> DebounceImmediate<T>(
        this IObservable<T> source,
        TimeSpan dueTime) =>
        source.DebounceImmediate(dueTime, null);

    /// <summary>
    /// Debounces with an immediate first emission then standard debounce behavior.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="dueTime">Debounce time.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>Debounced sequence.</returns>
    public static IObservable<T> DebounceImmediate<T>(
        this IObservable<T> source,
        TimeSpan dueTime,
        ISequencer? scheduler) =>
        new DebounceImmediateObservable<T>(source, dueTime, scheduler ?? Sequencer.Default);

    /// <summary>
    /// Debounce until a condition becomes true.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="debounce">The debounce.</param>
    /// <param name="condition">The condition.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> DebounceUntil<T>(
        this IObservable<T> source,
        TimeSpan debounce,
        Func<T, bool> condition) =>
        source.DebounceUntil(debounce, condition, null);

    /// <summary>
    /// Debounce until a condition becomes true.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="debounce">The debounce.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="scheduler">A scheduler for the operation.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> DebounceUntil<T>(
        this IObservable<T> source,
        TimeSpan debounce,
        Func<T, bool> condition,
        ISequencer? scheduler) =>
        new DebounceUntilObservable<T>(source, debounce, condition, scheduler ?? Sequencer.Default);

    /// <summary>
    /// Maps values to async operations without losing ordering or cancellation semantics.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="asyncSelector">The asynchronous selector.</param>
    /// <returns>An IObservable of TResult.</returns>
    public static IObservable<TResult> SelectAsync<TSource, TResult>(
        this IObservable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> asyncSelector) =>
        new SelectAsyncSequentialObservable<TSource, TResult>(source, x => asyncSelector(x, CancellationToken.None));

    /// <summary>
    /// Maps values to async operations without losing ordering or cancellation semantics.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="asyncSelector">The asynchronous selector.</param>
    /// <returns>An IObservable of TResult.</returns>
    public static IObservable<TResult> SelectAsync<TSource, TResult>(
        this IObservable<TSource> source,
        Func<TSource, Task<TResult>> asyncSelector) =>
        new SelectAsyncSequentialObservable<TSource, TResult>(source, asyncSelector);

    /// <summary>
    /// Projects each element to a task executed sequentially.
    /// </summary>
    /// <typeparam name="TSource">Source element type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="selector">Task selector.</param>
    /// <returns>Sequence of results preserving order.</returns>
    public static IObservable<TResult> SelectAsyncSequential<TSource, TResult>(
        this IObservable<TSource> source,
        Func<TSource, Task<TResult>> selector) =>
        new SelectAsyncSequentialObservable<TSource, TResult>(source, selector);

    /// <summary>
    /// Projects each element to a task but only latest result is emitted.
    /// </summary>
    /// <typeparam name="TSource">Source type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="selector">Task selector.</param>
    /// <returns>Sequence of latest task results.</returns>
    public static IObservable<TResult> SelectLatestAsync<TSource, TResult>(
        this IObservable<TSource> source,
        Func<TSource, Task<TResult>> selector) =>
        new SelectLatestAsyncObservable<TSource, TResult>(source, selector);

    /// <summary>
    /// Projects each element to a task with limited concurrency.
    /// </summary>
    /// <typeparam name="TSource">Source type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="selector">Task selector.</param>
    /// <param name="maxConcurrency">Max concurrency.</param>
    /// <returns>Merged sequence of task results.</returns>
    public static IObservable<TResult> SelectAsyncConcurrent<TSource, TResult>(
        this IObservable<TSource> source,
        Func<TSource, Task<TResult>> selector,
        int maxConcurrency) =>
        new SelectAsyncConcurrentObservable<TSource, TResult>(source, selector, maxConcurrency);

    /// <summary>
    /// Emit (previous, current) pairs.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>An IObservable of (T Previous, T Current).</returns>
    public static IObservable<(T Previous, T Current)> Pairwise<T>(
        this IObservable<T> source) => new PairwiseObservable<T>(source);

    /// <summary>
    /// Partitions a sequence into two based on predicate.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="predicate">Predicate.</param>
    /// <returns>Tuple of (trueSequence, falseSequence).</returns>
    public static (IObservable<T> True, IObservable<T> False) Partition<T>(
        this IObservable<T> source,
        Func<T, bool> predicate)
    {
        var partition = new PartitionObservable<T>(source, predicate);
        return (partition.True, partition.False);
    }

    /// <summary>
    /// Buffers items until inactivity period elapses then emits and resets buffer.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="inactivityPeriod">Inactivity period.</param>
    /// <returns>Sequence of buffered lists.</returns>
    public static IObservable<IList<T>> BufferUntilInactive<T>(
        this IObservable<T> source,
        TimeSpan inactivityPeriod) =>
        source.BufferUntilInactive(inactivityPeriod, null);

    /// <summary>
    /// Buffers items until inactivity period elapses then emits and resets buffer.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="inactivityPeriod">Inactivity period.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Sequence of buffered lists.</returns>
    public static IObservable<IList<T>> BufferUntilInactive<T>(
        this IObservable<T> source,
        TimeSpan inactivityPeriod,
        ISequencer? scheduler) =>
        new BufferUntilIdleObservable<T>(source, inactivityPeriod, scheduler ?? Sequencer.Default);

    /// <summary>
    /// Emits the first element matching predicate then completes.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="predicate">Predicate.</param>
    /// <returns>Sequence with first matching element.</returns>
    public static IObservable<T> WaitUntil<T>(this IObservable<T> source, Func<T, bool> predicate) =>
        new WaitUntilObservable<T>(source, predicate);

    /// <summary>
    /// Drop values when the previous async operation is still running.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="asyncAction">The asynchronous action.</param>
    /// <returns>An IObservable of T.</returns>
    public static IObservable<T> DropIfBusy<T>(
        this IObservable<T> source,
        Func<T, ValueTask> asyncAction) =>
        new DropIfBusyObservable<T>(source, asyncAction);

    /// <summary>
    /// Executes an action at subscription time.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="action">Action to run on subscribe.</param>
    /// <returns>Original sequence with subscribe side-effect.</returns>
    public static IObservable<T> DoOnSubscribe<T>(this IObservable<T> source, Action action) =>
        new DoOnSubscribeObservable<T>(source, action);

    /// <summary>
    /// Executes an action when subscription is disposed.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="source">Source sequence.</param>
    /// <param name="disposeAction">Action to run on dispose.</param>
    /// <returns>Original sequence with dispose side-effect.</returns>
    public static IObservable<T> DoOnDispose<T>(this IObservable<T> source, Action disposeAction) =>
        new DoOnDisposeObservable<T>(source, disposeAction);

    /// <summary>
    /// Fused <c>Where(predicate).Select(selector)</c>. Allocates a single observer
    /// per subscription instead of two, eliminating the intermediate operator that
    /// the equivalent Rx chain would build.
    /// </summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <typeparam name="TOut">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">Filter applied to each source element.</param>
    /// <param name="selector">Projection applied to elements that pass <paramref name="predicate"/>.</param>
    /// <returns>A fused filter-and-project observable.</returns>
    public static IObservable<TOut> WhereSelect<TIn, TOut>(
        this IObservable<TIn> source,
        Func<TIn, bool> predicate,
        Func<TIn, TOut> selector) =>
        new WhereSelectObservable<TIn, TOut>(source, predicate, selector);

    /// <summary>
    /// Swallows any source error by emitting the fallback value followed by completion.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable whose values are forwarded verbatim.</param>
    /// <param name="fallback">The value emitted if the source errors.</param>
    /// <returns>An observable that never produces an error terminal.</returns>
    public static IObservable<T> CatchReturn<T>(this IObservable<T> source, T fallback) =>
        new CatchReturnObservable<T>(source, fallback);

    /// <summary>
    /// Convenience overload: <c>source.CatchReturnUnit()</c> is shorthand for
    /// <c>source.CatchReturn(RxVoid.Default)</c>.
    /// </summary>
    /// <param name="source">The source observable.</param>
    /// <returns>An observable that never produces an error terminal — errors are replaced with a single <see cref="RxVoid.Default"/>.</returns>
    public static IObservable<RxVoid> CatchReturnUnit(this IObservable<RxVoid> source) =>
        new CatchReturnObservable<RxVoid>(source, RxVoid.Default);

    /// <summary>
    /// Projects every source element to a stored constant, avoiding the closure
    /// allocation of <c>.Select(_ =&gt; value)</c>. Common in fire-then-return-value
    /// chains.
    /// </summary>
    /// <typeparam name="TSource">The source element type (ignored).</typeparam>
    /// <typeparam name="TResult">The result element type.</typeparam>
    /// <param name="source">The source observable whose values are ignored.</param>
    /// <param name="constant">The constant value emitted for each source element.</param>
    /// <returns>An observable that emits <paramref name="constant"/> for each source element.</returns>
    public static IObservable<TResult> SelectConstant<TSource, TResult>(
        this IObservable<TSource> source,
        TResult constant) =>
        new SelectConstantObservable<TSource, TResult>(source, constant);

    /// <summary>
    /// Applies <paramref name="selector"/> and emits only non-null results.
    /// Replaces <c>.Select(f).Where(x =&gt; x is not null).Select(x =&gt; x!)</c>
    /// with a single operator allocation.
    /// </summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <typeparam name="TOut">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projection that may return <see langword="null"/>.</param>
    /// <returns>An observable that emits only non-null projected values.</returns>
    public static IObservable<TOut> TrySelect<TIn, TOut>(
        this IObservable<TIn> source,
        Func<TIn, TOut?> selector) =>
        new TrySelectObservable<TIn, TOut>(source, selector);

    /// <summary>
    /// Chains two one-shot <c>SelectMany</c> projections into a single operator.
    /// Replaces <c>.SelectMany(a).SelectMany(b)</c> (2 operator allocations) with 1.
    /// </summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TMid">The intermediate element type.</typeparam>
    /// <typeparam name="TResult">The final result type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="first">First projection: source → intermediate observable.</param>
    /// <param name="second">Second projection: intermediate → result observable.</param>
    /// <returns>A fused two-stage SelectMany observable.</returns>
    public static IObservable<TResult> SelectManyThen<TSource, TMid, TResult>(
        this IObservable<TSource> source,
        Func<TSource, IObservable<TMid>> first,
        Func<TMid, IObservable<TResult>> second) =>
        new SelectManyThenObservable<TSource, TMid, TResult>(source, first, second);

    /// <summary>
    /// Runs a list of one-shot <see cref="IObservable{RxVoid}"/> sequentially and emits
    /// a single <see cref="RxVoid.Default"/> when all have completed. Replaces
    /// <c>.Concat().LastOrDefaultAsync()</c> with a single operator that avoids stack
    /// overflow on inline-completing sources.
    /// </summary>
    /// <param name="sources">The observables to run in order.</param>
    /// <returns>A one-shot observable that completes after all sources.</returns>
    public static IObservable<RxVoid> RunAll(this IReadOnlyList<IObservable<RxVoid>> sources) =>
        new RunAllObservable(sources);

    /// <summary>
    /// Walks a list of candidate keys sequentially, projects each into a one-shot
    /// observable, transforms the raw value, and emits the first transformed value
    /// that satisfies <paramref name="predicate"/>. Errors from individual projections
    /// are swallowed (the candidate is skipped). If no candidate matches, emits
    /// <paramref name="fallback"/>.
    /// </summary>
    /// <typeparam name="TKey">The candidate key type.</typeparam>
    /// <typeparam name="TRaw">The raw element type emitted by the projection.</typeparam>
    /// <typeparam name="TResult">The transformed result type.</typeparam>
    /// <param name="candidates">The ordered list of candidate keys to walk.</param>
    /// <param name="project">Projects a key into a one-shot observable of raw values.</param>
    /// <param name="transform">Transform applied to each raw value to produce the result.</param>
    /// <param name="predicate">Returns <see langword="true"/> for a matching transformed value.</param>
    /// <param name="fallback">Value emitted when no candidate matches.</param>
    /// <returns>An observable emitting the first matching transformed value, or <paramref name="fallback"/>.</returns>
    public static IObservable<TResult> FirstMatchFromCandidates<TKey, TRaw, TResult>(
        this IReadOnlyList<TKey> candidates,
        Func<TKey, IObservable<TRaw>> project,
        Func<TRaw, TResult> transform,
        Func<TResult, bool> predicate,
        TResult fallback) =>
        new FirstMatchFromCandidatesObservable<TKey, TRaw, TResult>(
            candidates,
            project,
            transform,
            predicate,
            fallback);
}
