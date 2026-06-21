// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Operators;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive;
#else
namespace ReactiveUI.Primitives.Extensions;
#endif

/// <summary>Extension methods for Reactive objects.</summary>
[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with \'Async\'", Justification = "Existing API")]
public static class ReactiveExtensions
{
    /// <summary>Default backoff factor for <see cref="RetryWithBackoff{T}(IObservable{T}, int, TimeSpan)"/>: each retry doubles the previous delay.</summary>
    private const double DefaultBackoffFactor = 2.0;

    /// <summary>Default match timeout for regex filters created from string patterns.</summary>
    private static readonly TimeSpan DefaultRegexMatchTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Boolean reduction operators for a set of boolean observable sources.</summary>
    /// <param name="sources">The sources.</param>
    extension(IEnumerable<IObservable<bool>> sources)
    {
        /// <summary>Latest values of each sequence are all false.</summary>
        /// <returns>A sequence that emits true when all latest booleans are false.</returns>
        public IObservable<bool> CombineLatestValuesAreAllFalse() =>
            new BooleanReduceObservable(sources, target: false);

        /// <summary>Latest values of each sequence are all true.</summary>
        /// <returns>A sequence that emits true when all latest booleans are true.</returns>
        public IObservable<bool> CombineLatestValuesAreAllTrue() =>
            new BooleanReduceObservable(sources, target: true);
    }

    /// <summary>Emission operators for an enumerable source.</summary>
    /// <param name="source">Source enumerable.</param>
    /// <typeparam name="T">Element type.</typeparam>
    extension<T>(IEnumerable<T> source)
    {
        /// <summary>Emits each element of an IEnumerable.</summary>
        /// <returns>Observable of elements.</returns>
        public IObservable<T> FromArray() =>
            new FromArrayObservable<T>(source, null);

        /// <summary>Emits each element of an IEnumerable.</summary>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>Observable of elements.</returns>
        public IObservable<T> FromArray(ISequencer? scheduler) =>
            new FromArrayObservable<T>(source, scheduler);
    }

    /// <summary>Concurrency-limiting operators for a set of tasks.</summary>
    /// <param name="taskFunctions">Tasks to execute.</param>
    /// <typeparam name="T">The result type.</typeparam>
    extension<T>(IEnumerable<Task<T>> taskFunctions)
    {
        /// <summary>Executes with limited concurrency.</summary>
        /// <param name="maxConcurrency">Maximum concurrency.</param>
        /// <returns>A sequence of task results.</returns>
        public IObservable<T> WithLimitedConcurrency(int maxConcurrency)
        {
            ArgumentExceptionHelper.ThrowIfNull(taskFunctions);

            return new ConcurrencyLimiter<T>(taskFunctions, maxConcurrency).Observable;
        }
    }

    /// <summary>Flattening operators for an observable source of enumerables.</summary>
    /// <param name="source">Source of enumerables.</param>
    /// <typeparam name="T">Element type.</typeparam>
    extension<T>(IObservable<IEnumerable<T>> source)
    {
        /// <summary>Flattens a sequence of enumerables into individual values.</summary>
        /// <returns>A flattened observable.</returns>
        public IObservable<T> ForEach() =>
            new ForEachObservable<T>(source, null);

        /// <summary>Flattens a sequence of enumerables into individual values.</summary>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>A flattened observable.</returns>
        public IObservable<T> ForEach(ISequencer? scheduler) =>
            new ForEachObservable<T>(source, scheduler);
    }

    /// <summary>Error-recovery operators for an observable source of <see cref="RxVoid"/>.</summary>
    /// <param name="source">The source observable.</param>
    extension(IObservable<RxVoid> source)
    {
        /// <summary>Convenience overload: <c>source.CatchReturnUnit()</c> is shorthand for <c>source.CatchReturn(RxVoid.Default)</c>.</summary>
        /// <returns>An observable that never produces an error terminal — errors are replaced with a single <see cref="RxVoid.Default"/>.</returns>
        public IObservable<RxVoid> CatchReturnUnit() =>
            new CatchReturnObservable<RxVoid>(source, RxVoid.Default);
    }

    /// <summary>General-purpose operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Returns only values that are not null. Converts the nullability.</summary>
        /// <returns>A non nullable version of the observable that only emits valid values.</returns>
        public IObservable<T> WhereIsNotNull() =>
            new WhereIsNotNullObservable<T>(source);

        /// <summary>Change the source observable type to <see cref="RxVoid"/>. This allows us to be notified when the observable emits a value.</summary>
        /// <returns>The signal.</returns>
        public IObservable<RxVoid> AsSignal() =>
            new AsSignalObservable<T>(source);

        /// <summary>Emit a batch when the stream goes quiet.</summary>
        /// <param name="idleTime">The idle time.</param>
        /// <returns>A sequence of buffered lists.</returns>
        public IObservable<IList<T>> BufferUntilIdle(TimeSpan idleTime) =>
            new BufferUntilIdleObservable<T>(source, idleTime, Sequencer.Default);

        /// <summary>Emit a batch when the stream goes quiet.</summary>
        /// <param name="idleTime">The idle time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>A sequence of buffered lists.</returns>
        public IObservable<IList<T>> BufferUntilIdle(TimeSpan idleTime, ISequencer? scheduler) =>
            new BufferUntilIdleObservable<T>(source, idleTime, scheduler ?? Sequencer.Default);

        /// <summary>Catch exception and return Observable.Empty.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="errorAction">The error action.</param>
        /// <returns>A sequence that invokes <paramref name="errorAction"/> on error and completes.</returns>
        public IObservable<T> CatchIgnore<TException>(Action<TException> errorAction)
            where TException : Exception =>
            new CatchIgnoreObservable<T, TException>(source, errorAction);

        /// <summary>Detects when a stream becomes inactive for some period of time.</summary>
        /// <param name="stalenessPeriod">If source stream does not OnNext any update during this period, it is declared stale.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>Observable stale markers or updates.</returns>
        public IObservable<Stale<T>> DetectStale(TimeSpan stalenessPeriod, ISequencer scheduler) =>
            new DetectStaleObservable<T>(source, stalenessPeriod, scheduler);

        /// <summary>
        /// Applies a conflation algorithm to an observable stream. Anytime the stream OnNext twice
        /// below minimumUpdatePeriod, the second update gets delayed to respect the
        /// minimumUpdatePeriod. If more than 2 updates happen, only the last update is pushed.
        /// </summary>
        /// <param name="minimumUpdatePeriod">Minimum delay between two updates.</param>
        /// <param name="scheduler">Scheduler to publish updates.</param>
        /// <returns>The conflated stream.</returns>
        public IObservable<T> Conflate(TimeSpan minimumUpdatePeriod, ISequencer scheduler) =>
            new ConflateObservable<T>(source, minimumUpdatePeriod, scheduler);

        /// <summary>Injects heartbeats in a stream when the source stream becomes quiet.</summary>
        /// <param name="heartbeatPeriod">Period between heartbeats.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <returns>Observable heartbeat values.</returns>
        public IObservable<Heartbeat<T>> Heartbeat(TimeSpan heartbeatPeriod, ISequencer scheduler) =>
            new HeartbeatObservable<T>(source, heartbeatPeriod, scheduler);

        /// <summary>Emit the latest value or a default if none exists.</summary>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>A sequence that emits the latest value or the default.</returns>
        public IObservable<T> LatestOrDefault(T defaultValue) =>
            new LatestOrDefaultObservable<T>(source, defaultValue);

        /// <summary>Logs the errors. Inline error logging without terminating the stream.</summary>
        /// <param name="logger">The logger.</param>
        /// <returns>A sequence that logs errors.</returns>
        public IObservable<T> LogErrors(Action<Exception> logger)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(logger);
            return new LogErrorsObservable<T>(source, logger);
        }

        /// <summary>If the scheduler is not null observes on that scheduler.</summary>
        /// <param name="scheduler">Scheduler to notify observers on (optional).</param>
        /// <returns>The source sequence whose callbacks happen on the specified scheduler.</returns>
        public IObservable<T> ObserveOnSafe(ISequencer? scheduler) =>
            scheduler is null ? source : new ObserveOnObservable<T>(source, scheduler);

        /// <summary>Conditionally switch schedulers.</summary>
        /// <param name="condition">if set to <c>true</c> [condition].</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ObserveOnIf(bool condition, ISequencer scheduler) =>
            condition ? new ObserveOnObservable<T>(source, scheduler) : source;

        /// <summary>Conditionally switch schedulers.</summary>
        /// <param name="condition">if set to <c>true</c> [condition].</param>
        /// <param name="trueScheduler">The true scheduler.</param>
        /// <param name="falseScheduler">The false scheduler.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ObserveOnIf(bool condition, ISequencer trueScheduler, ISequencer falseScheduler) =>
            condition
                ? new ObserveOnObservable<T>(source, trueScheduler)
                : new ObserveOnObservable<T>(source, falseScheduler);

        /// <summary>Conditionally switch schedulers based on a reactive condition.</summary>
        /// <param name="condition">The reactive condition.</param>
        /// <param name="trueScheduler">The scheduler to use when condition is true.</param>
        /// <param name="falseScheduler">The scheduler to use when condition is false.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ObserveOnIf(
            IObservable<bool> condition,
            ISequencer trueScheduler,
            ISequencer falseScheduler) =>
            new ObserveOnIfObservable<T>(source, condition, trueScheduler, falseScheduler);

        /// <summary>Conditionally switch schedulers based on a reactive condition.</summary>
        /// <param name="condition">The reactive condition.</param>
        /// <param name="scheduler">The scheduler to use when condition is true.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ObserveOnIf(IObservable<bool> condition, ISequencer scheduler) =>
            new ObserveOnIfObservable<T>(source, condition, scheduler, Sequencer.Immediate);

        /// <summary>Sample the latest value whenever a trigger fires.</summary>
        /// <param name="trigger">The trigger.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> SampleLatest(IObservable<object> trigger) =>
            new SampleLatestObservable<T>(source, trigger);

        /// <summary>Scan that always emits the initial value first.</summary>
        /// <typeparam name="TAccumulate">The type of the accumulate.</typeparam>
        /// <param name="initial">The initial.</param>
        /// <param name="accumulator">The accumulator.</param>
        /// <returns>An IObservable of TAccumulate.</returns>
        public IObservable<TAccumulate> ScanWithInitial<TAccumulate>(
            TAccumulate initial,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            new ScanWithInitialObservable<T, TAccumulate>(source, initial, accumulator);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime));

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(DateTimeOffset dueTime, ISequencer scheduler) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Absolute(scheduler, dueTime));

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="action">The action.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler, Action<T> action) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime).WithAction(action));

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="action">The action.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(DateTimeOffset dueTime, ISequencer scheduler, Action<T> action) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Absolute(scheduler, dueTime).WithAction(action));

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="function">The function.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(ISequencer scheduler, Func<T, T> function) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Immediate(scheduler).WithTransform(function));

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="function">The function.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler, Func<T, T> function) =>
            new ScheduledSourceObservable<T>(source, ScheduleConfig<T>.Delayed(scheduler, dueTime).WithTransform(function));

        /// <summary>Repeats the source until it terminates successfully (alias of Retry).</summary>
        /// <returns>Retried sequence.</returns>
        public IObservable<T> OnErrorRetry()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            return new RetryForeverObservable<T>(source);
        }

        /// <summary>When caught exception, do onError action and repeat observable sequence.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: int.MaxValue,
                    InitialDelay: TimeSpan.Zero,
                    BackoffFactor: 1.0,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence after delay time.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, TimeSpan delay)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: int.MaxValue,
                    InitialDelay: delay,
                    BackoffFactor: 1.0,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence during within retryCount.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, int retryCount)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: retryCount,
                    InitialDelay: TimeSpan.Zero,
                    BackoffFactor: 1.0,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence after delay time during within retryCount.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, int retryCount, TimeSpan delay)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: retryCount,
                    InitialDelay: delay,
                    BackoffFactor: 1.0,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>
        /// When caught exception, do onError action and repeat observable sequence after delay
        /// time(work on delayScheduler) during within retryCount.
        /// </summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <param name="delayScheduler">The delay scheduler.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(
            Action<TException> onError,
            int retryCount,
            TimeSpan delay,
            ISequencer delayScheduler)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new RetryWithBackoffObservable<T>(
                source,
                new(
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

        /// <summary>Takes elements until predicate returns true for an element (inclusive) then completes.</summary>
        /// <param name="predicate">Predicate for completion.</param>
        /// <returns>Sequence that completes when predicate satisfied.</returns>
        public IObservable<T> TakeUntil(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);
            return new TakeUntilInclusiveObservable<T>(source, predicate);
        }

        /// <summary>Wraps values with a synchronization disposable that completes when disposed.</summary>
        /// <returns>Sequence of (value, sync handle).</returns>
        public IObservable<(T Value, IDisposable Sync)> SynchronizeSynchronous() =>
            new SynchronizeAsyncObservable<T>(source);

        /// <summary>Subscribes to the specified source synchronously.</summary>
        /// <param name="onNext">The on next.</param>
        /// <param name="onError">The on error.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
        public IDisposable SubscribeSynchronous(
            Func<T, ValueTask> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            new SubscribeAsyncObservable<T>(source, onNext, onError, onCompleted);

        /// <summary>Subscribes an element handler and an exception handler to an observable sequence synchronously.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <param name="onError">Action to invoke upon exceptional termination of the observable sequence.</param>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
        public IDisposable SubscribeSynchronous(Func<T, ValueTask> onNext, Action<Exception> onError) =>
            new SubscribeAsyncObservable<T>(source, onNext, onError, null);

        /// <summary>Subscribes an element handler and a completion handler to an observable sequence synchronously.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <param name="onCompleted">Action to invoke upon graceful termination of the observable sequence.</param>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> or <paramref name="onCompleted"/> is <c>null</c>.</exception>
        public IDisposable SubscribeSynchronous(Func<T, ValueTask> onNext, Action onCompleted) =>
            new SubscribeAsyncObservable<T>(source, onNext, null, onCompleted: onCompleted);

        /// <summary>Subscribes an element handler to an observable sequence synchronously.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
        public IDisposable SubscribeSynchronous(Func<T, ValueTask> onNext) =>
            new SubscribeAsyncObservable<T>(source, onNext, null, null);

        /// <summary>Provide a fallback observable if the source completes without emitting.</summary>
        /// <param name="fallback">The fallback.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> SwitchIfEmpty(IObservable<T> fallback) =>
            new SwitchIfEmptyObservable<T>(source, fallback);

        /// <summary>
        /// Synchronizes the asynchronous operations in downstream operations.
        /// Use SubscribeSynchronus instead for a simpler version.
        /// Call Sync.Dispose() to release the lock in the downstream methods.
        /// </summary>
        /// <returns>An Observable of T and a release mechanism.</returns>
        public IObservable<(T Value, IDisposable Sync)> SynchronizeAsync() =>
            new SynchronizeAsyncObservable<T>(source);

        /// <summary>Subscribes allowing asynchronous operations to be executed without blocking the source.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <returns><see cref="IDisposable"/> object used to unsubscribe from the observable sequence.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IDisposable SubscribeAsync(Func<T, ValueTask> onNext) =>
            new SubscribeAsyncObservable<T>(source, onNext, null, null);

        /// <summary>Subscribes allowing asynchronous operations to be executed without blocking the source.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <param name="onCompleted">The on completed.</param>
        /// <returns>
        ///   <see cref="IDisposable" /> object used to unsubscribe from the observable sequence.
        /// </returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IDisposable SubscribeAsync(Func<T, ValueTask> onNext, Action onCompleted) =>
            new SubscribeAsyncObservable<T>(source, onNext, null, onCompleted: onCompleted);

        /// <summary>Subscribes allowing asynchronous operations to be executed without blocking the source.</summary>
        /// <param name="onNext">Action to invoke for each element in the observable sequence.</param>
        /// <param name="onError">The on error.</param>
        /// <returns>
        ///   <see cref="IDisposable" /> object used to unsubscribe from the observable sequence.
        /// </returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IDisposable SubscribeAsync(Func<T, ValueTask> onNext, Action<Exception> onError) =>
            new SubscribeAsyncObservable<T>(source, onNext, onError, null);

        /// <summary>Subscribes allowing asynchronous operations to be executed without blocking the source.</summary>
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
        public IDisposable SubscribeAsync(
            Func<T, ValueTask> onNext,
            Action<Exception> onError,
            Action onCompleted) =>
            new SubscribeAsyncObservable<T>(source, onNext, onError, onCompleted);

        /// <summary>Catches any error and returns a fallback value then completes.</summary>
        /// <param name="fallback">Fallback value.</param>
        /// <returns>Sequence producing either original values or fallback on error then completing.</returns>
        public IObservable<T> CatchAndReturn(T fallback) =>
            new CatchReturnObservable<T>(source, fallback);

        /// <summary>Catches a specific exception type mapping it to a fallback value.</summary>
        /// <typeparam name="TException">Exception type.</typeparam>
        /// <param name="fallbackFactory">Factory producing fallback from the exception.</param>
        /// <returns>Recovered sequence.</returns>
        public IObservable<T> CatchAndReturn<TException>(Func<TException, T> fallbackFactory)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(fallbackFactory);
            return new CatchAndReturnWithFactoryObservable<T, TException>(source, fallbackFactory);
        }

        /// <summary>Retries with exponential backoff.</summary>
        /// <param name="maxRetries">Maximum number of retries.</param>
        /// <param name="initialDelay">Initial backoff delay.</param>
        /// <returns>Retried sequence with backoff.</returns>
        public IObservable<T> RetryWithBackoff(int maxRetries, TimeSpan initialDelay) =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: maxRetries,
                    InitialDelay: initialDelay,
                    BackoffFactor: DefaultBackoffFactor,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: null));

        /// <summary>Retries with exponential backoff.</summary>
        /// <param name="maxRetries">Maximum number of retries.</param>
        /// <param name="initialDelay">Initial backoff delay.</param>
        /// <param name="backoffFactor">Multiplier for each retry (default 2).</param>
        /// <param name="maxDelay">Optional maximum delay.</param>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>Retried sequence with backoff.</returns>
        public IObservable<T> RetryWithBackoff(
            int maxRetries,
            TimeSpan initialDelay,
            double backoffFactor,
            TimeSpan? maxDelay,
            ISequencer? scheduler) =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: maxRetries,
                    InitialDelay: initialDelay,
                    BackoffFactor: backoffFactor,
                    MaxDelay: maxDelay,
                    Scheduler: scheduler ?? Sequencer.Default,
                    OnError: null));

        /// <summary>Retry with exponential.</summary>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delaySelector">The delay selector.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryWithDelay(int retryCount, Func<int, TimeSpan> delaySelector) =>
            new RetryWithDelayObservable<T>(source, retryCount, delaySelector);

        /// <summary>Retries the forever with delay.</summary>
        /// <param name="delay">The delay.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryForeverWithDelay(TimeSpan delay) =>
            new RetryWithDelayObservable<T>(source, int.MaxValue, _ => delay);

        /// <summary>Retry with fixed backoff.</summary>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryWithFixedDelay(int retryCount, TimeSpan delay)
            => new RetryWithBackoffObservable<T>(
                source,
                new(
                    MaxRetries: retryCount,
                    InitialDelay: delay,
                    BackoffFactor: 1.0,
                    MaxDelay: null,
                    Scheduler: Sequencer.Default,
                    OnError: null));

        /// <summary>Always replay the last value, even if the source hasnt produced one yet.</summary>
        /// <param name="initialValue">The initial value.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ReplayLastOnSubscribe(T initialValue)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ReplayLastOnSubscribeObservable<T>(source, initialValue);
        }

        /// <summary>Emits only the first value in each time window.</summary>
        /// <param name="window">Time window.</param>
        /// <returns>Throttle-first sequence.</returns>
        public IObservable<T> ThrottleFirst(TimeSpan window) =>
            new ThrottleFirstObservable<T>(source, window, Sequencer.Default);

        /// <summary>Emits only the first value in each time window.</summary>
        /// <param name="window">Time window.</param>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>Throttle-first sequence.</returns>
        public IObservable<T> ThrottleFirst(TimeSpan window, ISequencer? scheduler) =>
            new ThrottleFirstObservable<T>(source, window, scheduler ?? Sequencer.Default);

        /// <summary>Throttle until a predicate becomes true.</summary>
        /// <param name="throttle">The throttle.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> ThrottleUntilTrue(TimeSpan throttle, Func<T, bool> predicate) =>
            new ThrottleUntilTrueObservable<T>(source, throttle, predicate);

        /// <summary>Throttles the on scheduler.</summary>
        /// <param name="timeSpan">The time span.</param>
        /// <param name="scheduler">A scheduler for the operation.</param>
        /// <returns>A observable for the throttle operation.</returns>
        public IObservable<T> ThrottleOnScheduler(TimeSpan timeSpan, ISequencer scheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);
            return new ThrottleObservable<T>(source, timeSpan, scheduler);
        }

        /// <summary>Convert an observable to a Task that starts immediately.</summary>
        /// <returns>A Task of T.</returns>
        public Task<T> ToHotTask() => FirstAsTaskHelper.FirstAsTask(source);

        /// <summary>
        /// Convert an observable to a <see cref="ValueTask{T}"/> that starts immediately. Backed by a
        /// pooled <see cref="System.Threading.Tasks.Sources.IValueTaskSource{T}"/> implementation, so
        /// steady-state callers pay no allocations after the per-type pool warms up. Prefer this over
        /// <see cref="ToHotTask{T}"/> when the call site can consume a <see cref="ValueTask{T}"/>
        /// (single await, no caching, no <c>WhenAll</c>).
        /// </summary>
        /// <returns>A <see cref="ValueTask{T}"/> that completes with the first value, faults on source error, or faults on empty completion.</returns>
        public ValueTask<T> ToHotValueTask() =>
            FirstAsValueTaskHelper<T>.FirstAsValueTask(source);

        /// <summary>Throttle but only emit when the value actually changes.</summary>
        /// <param name="throttle">The throttle.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservable<T> ThrottleDistinct(TimeSpan throttle) =>
            new ThrottleDistinctObservable<T>(source, throttle, Sequencer.Default);

        /// <summary>Throttle but only emit when the value actually changes.</summary>
        /// <param name="throttle">The throttle.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>A throttled distinct sequence.</returns>
        public IObservable<T> ThrottleDistinct(TimeSpan throttle, ISequencer scheduler) =>
            new ThrottleDistinctObservable<T>(source, throttle, scheduler);

        /// <summary>Debounces with an immediate first emission then standard debounce behavior.</summary>
        /// <param name="dueTime">Debounce time.</param>
        /// <returns>Debounced sequence.</returns>
        public IObservable<T> DebounceImmediate(TimeSpan dueTime) =>
            new DebounceImmediateObservable<T>(source, dueTime, Sequencer.Default);

        /// <summary>Debounces with an immediate first emission then standard debounce behavior.</summary>
        /// <param name="dueTime">Debounce time.</param>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>Debounced sequence.</returns>
        public IObservable<T> DebounceImmediate(TimeSpan dueTime, ISequencer? scheduler) =>
            new DebounceImmediateObservable<T>(source, dueTime, scheduler ?? Sequencer.Default);

        /// <summary>Debounce until a condition becomes true.</summary>
        /// <param name="debounce">The debounce.</param>
        /// <param name="condition">The condition.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> DebounceUntil(TimeSpan debounce, Func<T, bool> condition) =>
            new DebounceUntilObservable<T>(source, debounce, condition, Sequencer.Default);

        /// <summary>Debounce until a condition becomes true.</summary>
        /// <param name="debounce">The debounce.</param>
        /// <param name="condition">The condition.</param>
        /// <param name="scheduler">A scheduler for the operation.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> DebounceUntil(TimeSpan debounce, Func<T, bool> condition, ISequencer? scheduler) =>
            new DebounceUntilObservable<T>(source, debounce, condition, scheduler ?? Sequencer.Default);

        /// <summary>Maps values to async operations without losing ordering or cancellation semantics.</summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="asyncSelector">The asynchronous selector.</param>
        /// <returns>An IObservable of TResult.</returns>
        public IObservable<TResult> SelectAsync<TResult>(
            Func<T, CancellationToken, Task<TResult>> asyncSelector) =>
            new SelectAsyncSequentialObservable<T, TResult>(source, x => asyncSelector(x, CancellationToken.None));

        /// <summary>Maps values to async operations without losing ordering or cancellation semantics.</summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="asyncSelector">The asynchronous selector.</param>
        /// <returns>An IObservable of TResult.</returns>
        public IObservable<TResult> SelectAsync<TResult>(Func<T, Task<TResult>> asyncSelector) =>
            new SelectAsyncSequentialObservable<T, TResult>(source, asyncSelector);

        /// <summary>Projects each element to a task executed sequentially.</summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="selector">Task selector.</param>
        /// <returns>Sequence of results preserving order.</returns>
        public IObservable<TResult> SelectAsyncSequential<TResult>(Func<T, Task<TResult>> selector) =>
            new SelectAsyncSequentialObservable<T, TResult>(source, selector);

        /// <summary>Projects each element to a task but only latest result is emitted.</summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="selector">Task selector.</param>
        /// <returns>Sequence of latest task results.</returns>
        public IObservable<TResult> SelectLatestAsync<TResult>(Func<T, Task<TResult>> selector) =>
            new SelectLatestAsyncObservable<T, TResult>(source, selector);

        /// <summary>Projects each element to a task with limited concurrency.</summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="selector">Task selector.</param>
        /// <param name="maxConcurrency">Max concurrency.</param>
        /// <returns>Merged sequence of task results.</returns>
        public IObservable<TResult> SelectAsyncConcurrent<TResult>(
            Func<T, Task<TResult>> selector,
            int maxConcurrency) =>
            new SelectAsyncConcurrentObservable<T, TResult>(source, selector, maxConcurrency);

        /// <summary>Emit (previous, current) pairs.</summary>
        /// <returns>An IObservable of (T Previous, T Current).</returns>
        public IObservable<(T Previous, T Current)> Pairwise() =>
            new PairwiseObservable<T>(source);

        /// <summary>Partitions a sequence into two based on predicate.</summary>
        /// <param name="predicate">Predicate.</param>
        /// <returns>Tuple of (trueSequence, falseSequence).</returns>
        public (IObservable<T> True, IObservable<T> False) Partition(Func<T, bool> predicate)
        {
            PartitionObservable<T> partition = new(source, predicate);
            return (partition.True, partition.False);
        }

        /// <summary>Buffers items until inactivity period elapses then emits and resets buffer.</summary>
        /// <param name="inactivityPeriod">Inactivity period.</param>
        /// <returns>Sequence of buffered lists.</returns>
        public IObservable<IList<T>> BufferUntilInactive(TimeSpan inactivityPeriod) =>
            new BufferUntilIdleObservable<T>(source, inactivityPeriod, Sequencer.Default);

        /// <summary>Buffers items until inactivity period elapses then emits and resets buffer.</summary>
        /// <param name="inactivityPeriod">Inactivity period.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <returns>Sequence of buffered lists.</returns>
        public IObservable<IList<T>> BufferUntilInactive(TimeSpan inactivityPeriod, ISequencer? scheduler) =>
            new BufferUntilIdleObservable<T>(source, inactivityPeriod, scheduler ?? Sequencer.Default);

        /// <summary>Emits the first element matching predicate then completes.</summary>
        /// <param name="predicate">Predicate.</param>
        /// <returns>Sequence with first matching element.</returns>
        public IObservable<T> WaitUntil(Func<T, bool> predicate) =>
            new WaitUntilObservable<T>(source, predicate);

        /// <summary>Drop values when the previous async operation is still running.</summary>
        /// <param name="asyncAction">The asynchronous action.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> DropIfBusy(Func<T, ValueTask> asyncAction) =>
            new DropIfBusyObservable<T>(source, asyncAction);

        /// <summary>Executes an action at subscription time.</summary>
        /// <param name="action">Action to run on subscribe.</param>
        /// <returns>Original sequence with subscribe side-effect.</returns>
        public IObservable<T> DoOnSubscribe(Action action) =>
            new DoOnSubscribeObservable<T>(source, action);

        /// <summary>Executes an action when subscription is disposed.</summary>
        /// <param name="disposeAction">Action to run on dispose.</param>
        /// <returns>Original sequence with dispose side-effect.</returns>
        public IObservable<T> DoOnDispose(Action disposeAction) =>
            new DoOnDisposeObservable<T>(source, disposeAction);

        /// <summary>
        /// Fused <c>Where(predicate).Select(selector)</c>. Allocates a single observer
        /// per subscription instead of two, eliminating the intermediate operator that
        /// the equivalent Rx chain would build.
        /// </summary>
        /// <typeparam name="TOut">The projected element type.</typeparam>
        /// <param name="predicate">Filter applied to each source element.</param>
        /// <param name="selector">Projection applied to elements that pass <paramref name="predicate"/>.</param>
        /// <returns>A fused filter-and-project observable.</returns>
        public IObservable<TOut> WhereSelect<TOut>(Func<T, bool> predicate, Func<T, TOut> selector) =>
            new WhereSelectObservable<T, TOut>(source, predicate, selector);

        /// <summary>Swallows any source error by emitting the fallback value followed by completion.</summary>
        /// <param name="fallback">The value emitted if the source errors.</param>
        /// <returns>An observable that never produces an error terminal.</returns>
        public IObservable<T> CatchReturn(T fallback) =>
            new CatchReturnObservable<T>(source, fallback);

        /// <summary>
        /// Projects every source element to a stored constant, avoiding the closure
        /// allocation of <c>.Select(_ =&gt; value)</c>. Common in fire-then-return-value
        /// chains.
        /// </summary>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="constant">The constant value emitted for each source element.</param>
        /// <returns>An observable that emits <paramref name="constant"/> for each source element.</returns>
        public IObservable<TResult> SelectConstant<TResult>(TResult constant) =>
            new SelectConstantObservable<T, TResult>(source, constant);

        /// <summary>
        /// Applies <paramref name="selector"/> and emits only non-null results.
        /// Replaces <c>.Select(f).Where(x =&gt; x is not null).Select(x =&gt; x!)</c>
        /// with a single operator allocation.
        /// </summary>
        /// <typeparam name="TOut">The projected element type.</typeparam>
        /// <param name="selector">Projection that may return <see langword="null"/>.</param>
        /// <returns>An observable that emits only non-null projected values.</returns>
        public IObservable<TOut> TrySelect<TOut>(Func<T, TOut?> selector) =>
            new TrySelectObservable<T, TOut>(source, selector);

        /// <summary>
        /// Chains two one-shot <c>SelectMany</c> projections into a single operator.
        /// Replaces <c>.SelectMany(a).SelectMany(b)</c> (2 operator allocations) with 1.
        /// </summary>
        /// <typeparam name="TMid">The intermediate element type.</typeparam>
        /// <typeparam name="TResult">The final result type.</typeparam>
        /// <param name="first">First projection: source → intermediate observable.</param>
        /// <param name="second">Second projection: intermediate → result observable.</param>
        /// <returns>A fused two-stage SelectMany observable.</returns>
        public IObservable<TResult> SelectManyThen<TMid, TResult>(
            Func<T, IObservable<TMid>> first,
            Func<TMid, IObservable<TResult>> second) =>
            new SelectManyThenObservable<T, TMid, TResult>(source, first, second);
    }

    /// <summary>Min/max combination operators for an observable source sequence of comparable values.</summary>
    /// <param name="this">The first observable.</param>
    /// <typeparam name="T">The Value Type.</typeparam>
    extension<T>(IObservable<T> @this)
        where T : struct, IComparable<T>
    {
        /// <summary>Gets the maximum from all sources.</summary>
        /// <param name="sources">Other sources.</param>
        /// <returns>A sequence emitting the maximum of the latest values.</returns>
        public IObservable<T> GetMax(params IObservable<T>[] sources)
        {
            if (sources.Length == 0)
            {
                return @this;
            }

            if (sources.Length == 1)
            {
                return new BinaryMinMaxObservable<T>(@this, sources[0], emitMaximum: true);
            }

            var allSources = new IObservable<T>[sources.Length + 1];
            allSources[0] = @this;
            Array.Copy(sources, 0, allSources, 1, sources.Length);
            return new MinMaxObservable<T>(allSources, emitMaximum: true);
        }

        /// <summary>Gets the minimum from all sources.</summary>
        /// <param name="sources">Other sources.</param>
        /// <returns>A sequence emitting the minimum of the latest values.</returns>
        public IObservable<T> GetMin(params IObservable<T>[] sources)
        {
            if (sources.Length == 0)
            {
                return @this;
            }

            if (sources.Length == 1)
            {
                return new BinaryMinMaxObservable<T>(@this, sources[0], emitMaximum: false);
            }

            var allSources = new IObservable<T>[sources.Length + 1];
            allSources[0] = @this;
            Array.Copy(sources, 0, allSources, 1, sources.Length);
            return new MinMaxObservable<T>(allSources, emitMaximum: false);
        }
    }

    /// <summary>Null-skipping operators for an observable source sequence of reference types.</summary>
    /// <param name="source">The source.</param>
    /// <typeparam name="T">The type.</typeparam>
    extension<T>(IObservable<T> source)
        where T : class
    {
        /// <summary>Skip null values until the first non-null appears.</summary>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> SkipWhileNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            return new SkipWhileNullObservable<T>(source);
        }
    }

    /// <summary>Operators for an observable source sequence that may emit null values.</summary>
    /// <param name="source">The source.</param>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    extension<TSource>(IObservable<TSource?> source)
    {
        /// <summary>Catch exception and return Observable.Empty.</summary>
        /// <returns>A sequence that ignores errors and completes.</returns>
        public IObservable<TSource?> CatchIgnore() =>
            new CatchIgnoreEmptyObservable<TSource?>(source);
    }

    /// <summary>Array-oriented operators for an observable source of arrays.</summary>
    /// <param name="source">Source array sequence.</param>
    /// <typeparam name="T">Array element type.</typeparam>
    extension<T>(IObservable<T[]> source)
    {
        /// <summary>Randomly shuffles arrays emitted by the source.</summary>
        /// <returns>Sequence of shuffled arrays (in-place).</returns>
        public IObservable<T[]> Shuffle() => new ShuffleObservable<T>(source);
    }

    /// <summary>Boolean operators for an observable source of booleans.</summary>
    /// <param name="source">Boolean source.</param>
    extension(IObservable<bool> source)
    {
        /// <summary>Emits the boolean negation of the source sequence.</summary>
        /// <returns>Negated boolean sequence.</returns>
        public IObservable<bool> Not() => new NotObservable(source);

        /// <summary>Filters to true values only.</summary>
        /// <returns>Sequence of true values.</returns>
        public IObservable<bool> WhereTrue() => new WhereTrueObservable(source);

        /// <summary>Filters to false values only.</summary>
        /// <returns>Sequence of false values.</returns>
        public IObservable<bool> WhereFalse() => new WhereFalseObservable(source);
    }

    /// <summary>Character buffering operators for an observable source of characters.</summary>
    /// <param name="this">The source observable of characters.</param>
    extension(IObservable<char> @this)
    {
        /// <summary>Buffers until Start char and End char are found.</summary>
        /// <param name="startsWith">The starting delimiter.</param>
        /// <param name="endsWith">The ending delimiter.</param>
        /// <returns>A sequence of buffered strings including the start and end delimiters.</returns>
        public IObservable<string> BufferUntil(char startsWith, char endsWith) =>
            new BufferUntilObservable(@this, startsWith, endsWith);
    }

    /// <summary>Regex filtering operators for an observable source of strings.</summary>
    /// <param name="source">Source sequence.</param>
    extension(IObservable<string> source)
    {
        /// <summary>Filters strings by regex.</summary>
        /// <param name="regexPattern">Regex pattern.</param>
        /// <returns>Filtered sequence.</returns>
        public IObservable<string> Filter(string regexPattern) =>
            new FilterRegexObservable(source, new Regex(regexPattern, RegexOptions.None, DefaultRegexMatchTimeout));

        /// <summary>Filters strings by regex.</summary>
        /// <param name="regex">Regex.</param>
        /// <returns>Filtered sequence.</returns>
        public IObservable<string> Filter(Regex regex) =>
            new FilterRegexObservable(source, regex);
    }

    /// <summary>Observer push operators for an observer sink.</summary>
    /// <param name="observer">Observer to push to.</param>
    /// <typeparam name="T">Type of value.</typeparam>
    extension<T>(IObserver<T> observer)
    {
        /// <summary>Pushes multiple values to an observer.</summary>
        /// <param name="events">Values to push.</param>
        public void OnNext(params T[] events)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            ArgumentExceptionHelper.ThrowIfNull(events);

            observer.FastForEach(events);
        }
    }

    /// <summary>Sequential-run operators for an ordered list of one-shot signals.</summary>
    /// <param name="sources">The observables to run in order.</param>
    extension(IReadOnlyList<IObservable<RxVoid>> sources)
    {
        /// <summary>
        /// Runs a list of one-shot <see cref="IObservable{RxVoid}"/> sequentially and emits
        /// a single <see cref="RxVoid.Default"/> when all have completed. Replaces
        /// <c>.Concat().LastOrDefaultAsync()</c> with a single operator that avoids stack
        /// overflow on inline-completing sources.
        /// </summary>
        /// <returns>A one-shot observable that completes after all sources.</returns>
        public IObservable<RxVoid> RunAll() =>
            new RunAllObservable(sources);
    }

    /// <summary>Candidate-walking operators for an ordered list of candidate keys.</summary>
    /// <param name="candidates">The ordered list of candidate keys to walk.</param>
    /// <typeparam name="TKey">The candidate key type.</typeparam>
    extension<TKey>(IReadOnlyList<TKey> candidates)
    {
        /// <summary>
        /// Walks a list of candidate keys sequentially, projects each into a one-shot
        /// observable, transforms the raw value, and emits the first transformed value
        /// that satisfies <paramref name="predicate"/>. Errors from individual projections
        /// are swallowed (the candidate is skipped). If no candidate matches, emits
        /// <paramref name="fallback"/>.
        /// </summary>
        /// <typeparam name="TRaw">The raw element type emitted by the projection.</typeparam>
        /// <typeparam name="TResult">The transformed result type.</typeparam>
        /// <param name="project">Projects a key into a one-shot observable of raw values.</param>
        /// <param name="transform">Transform applied to each raw value to produce the result.</param>
        /// <param name="predicate">Returns <see langword="true"/> for a matching transformed value.</param>
        /// <param name="fallback">Value emitted when no candidate matches.</param>
        /// <returns>An observable emitting the first matching transformed value, or <paramref name="fallback"/>.</returns>
        public IObservable<TResult> FirstMatchFromCandidates<TRaw, TResult>(
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

    /// <summary>Safe scheduling operators for an optional scheduler.</summary>
    /// <param name="scheduler">Scheduler.</param>
    extension(ISequencer? scheduler)
    {
        /// <summary>Schedules an action immediately if scheduler null, else on scheduler.</summary>
        /// <param name="action">Action.</param>
        /// <returns>Disposable for the scheduled action.</returns>
        public IDisposable ScheduleSafe(Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(action);

            if (scheduler is not null)
            {
                return scheduler.Schedule(action);
            }

            action();
            return EmptyDisposable.Instance;
        }

        /// <summary>Schedules an action after a due time.</summary>
        /// <param name="dueTime">Delay.</param>
        /// <param name="action">Action.</param>
        /// <returns>Disposable for the scheduled action.</returns>
        public IDisposable ScheduleSafe(TimeSpan dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(action);

            if (scheduler is null)
            {
                Thread.Sleep(dueTime);
                action();
                return EmptyDisposable.Instance;
            }

            return scheduler.Schedule(dueTime, action);
        }
    }

    /// <summary>Resource-scoping operators for a disposable object.</summary>
    /// <param name="obj">Object to use.</param>
    /// <typeparam name="T">Disposable type.</typeparam>
    extension<T>(T obj)
        where T : IDisposable
    {
        /// <summary>Using helper with Action.</summary>
        /// <param name="action">Action to run.</param>
        /// <returns>Completion signal.</returns>
        public IObservable<RxVoid> Using(Action<T>? action) =>
            new UsingActionObservable<T>(obj, action, null);

        /// <summary>Using helper with Action.</summary>
        /// <param name="action">Action to run.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <returns>Completion signal.</returns>
        public IObservable<RxVoid> Using(Action<T>? action, ISequencer? scheduler) =>
            new UsingActionObservable<T>(obj, action, scheduler);

        /// <summary>Using helper with Func.</summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="function">Function to invoke.</param>
        /// <returns>Observable of result.</returns>
        public IObservable<TResult> Using<TResult>(Func<T, TResult> function) =>
            new UsingFuncObservable<T, TResult>(obj, function, null);

        /// <summary>Using helper with Func.</summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="function">Function to invoke.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <returns>Observable of result.</returns>
        public IObservable<TResult> Using<TResult>(Func<T, TResult> function, ISequencer? scheduler) =>
            new UsingFuncObservable<T, TResult>(obj, function, scheduler);
    }

    /// <summary>Change-notification operators for a notifying object.</summary>
    /// <param name="source">The source.</param>
    /// <typeparam name="T">The type of the source.</typeparam>
    extension<T>(T source)
        where T : INotifyPropertyChanged
    {
        /// <summary>Convert a property getter into an observable that emits on change.</summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="propertyExpression">The property expression.</param>
        /// <returns>An IObservable of TProperty.</returns>
        /// <exception cref="ArgumentException">Expression must be a property.</exception>
        public IObservable<TProperty> ToPropertyObservable<TProperty>(
            Expression<Func<T, TProperty>> propertyExpression)
        {
            ArgumentExceptionHelper.ThrowIfNull(propertyExpression);

            var member = (propertyExpression.Body as MemberExpression)
                         ?? throw new ArgumentException("Expression must be a property");

            return new PropertyChangedObservable<T, TProperty>(
                source,
                member.Member.Name,
                propertyExpression.Compile());
        }
    }

    /// <summary>Scheduling operators for a single value.</summary>
    /// <param name="value">The value.</param>
    /// <typeparam name="T">The type.</typeparam>
    extension<T>(T value)
    {
        /// <summary>Schedules a single value after a delay.</summary>
        /// <param name="dueTime">Delay.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <returns>Observable that emits the value.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler) =>
            new ScheduledValueObservable<T>(value, scheduler, dueTime, null, null, null);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(DateTimeOffset dueTime, ISequencer scheduler) =>
            new ScheduledValueObservable<T>(value, scheduler, null, dueTime, null, null);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="action">The action.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler, Action<T> action) =>
            new ScheduledValueObservable<T>(value, scheduler, dueTime, null, null, action);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="action">The action.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(DateTimeOffset dueTime, ISequencer scheduler, Action<T> action) =>
            new ScheduledValueObservable<T>(value, scheduler, null, dueTime, null, action);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="function">The function.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(ISequencer scheduler, Func<T, T> function) =>
            new ScheduledValueObservable<T>(value, scheduler, null, null, function, null);

        /// <summary>Schedules the specified due time.</summary>
        /// <param name="dueTime">The due time.</param>
        /// <param name="scheduler">The scheduler.</param>
        /// <param name="function">The function.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> Schedule(TimeSpan dueTime, ISequencer scheduler, Func<T, T> function) =>
            new ScheduledValueObservable<T>(value, scheduler, dueTime, null, function, null);
    }

    /// <summary>Synchronized timer all instances of this with the same TimeSpan use the same timer.</summary>
    /// <param name="timeSpan">The time span.</param>
    /// <returns>An observable sequence producing the shared DateTime ticks.</returns>
    public static IObservable<DateTime> SyncTimer(TimeSpan timeSpan) =>
        SyncTimerObservable.Get(timeSpan, Sequencer.Default);

    /// <summary>Synchronized timer all instances of this with the same TimeSpan and scheduler use the same timer.</summary>
    /// <param name="timeSpan">The time span.</param>
    /// <param name="scheduler">Scheduler used to emit ticks.</param>
    /// <returns>An observable sequence producing the shared DateTime ticks.</returns>
    public static IObservable<DateTime> SyncTimer(TimeSpan timeSpan, ISequencer scheduler) =>
        SyncTimerObservable.Get(timeSpan, scheduler);

    /// <summary>Invokes the action asynchronously surfacing the result through a RxVoid observable.</summary>
    /// <param name="action">Action to run.</param>
    /// <param name="scheduler">Scheduler (optional).</param>
    /// <returns>A sequence producing RxVoid upon completion.</returns>
    public static IObservable<RxVoid> Start(Action action, ISequencer? scheduler) =>
        new StartActionObservable(action, scheduler);

    /// <summary>Invokes the specified function asynchronously surfacing the result.</summary>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="function">Function to run.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>A sequence producing the function result.</returns>
    public static IObservable<TResult> Start<TResult>(Func<TResult> function, ISequencer? scheduler) =>
        new StartFuncObservable<TResult>(function, scheduler);

    /// <summary>While construct.</summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>Observable representing the loop.</returns>
    public static IObservable<RxVoid> While(Func<bool> condition, Action action) =>
        While(condition, action, null);

    /// <summary>While construct.</summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="action">Action to execute.</param>
    /// <param name="scheduler">Scheduler.</param>
    /// <returns>Observable representing the loop.</returns>
    public static IObservable<RxVoid> While(Func<bool> condition, Action action, ISequencer? scheduler) =>
        new WhileObservable(condition, action, scheduler);

    /// <summary>Builds a current-value subject pair: a read-only observable and the push-side observer.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="initialValue">The initial value.</param>
    /// <returns>A tuple of IObservable and IObserver.</returns>
    public static (IObservable<T> Observable, IObserver<T> Observer) ToReadOnlyBehavior<T>(T initialValue)
    {
        CurrentValueSubject<T> subject = new(initialValue);
        return (subject.AsObservable(), subject);
    }
}
