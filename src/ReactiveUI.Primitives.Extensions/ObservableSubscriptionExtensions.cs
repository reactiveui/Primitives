// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Provides extension methods for subscribing to and handling reactive sequences
/// in a synchronous or blocking manner. These methods offer utility functions
/// to retrieve emitted values, handle completion, and capture errors from observables.
/// </summary>
public static class ObservableSubscriptionExtensions
{
    /// <summary>The default timeout used by the <c>WaitFor*</c> helpers when no override is supplied.</summary>
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Blocking subscribe and wait operators for an observable source sequence.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Subscribes to the source and returns the last value emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The last emitted value, or <see langword="default"/> if no value was emitted.</returns>
        public T? SubscribeGetValue()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var sink = new ValueCaptureWitness<T>();
            using var subscription = source.Subscribe(sink);
            return sink.Value;
        }

        /// <summary>Subscribes to the source and returns any error emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The captured error, or <see langword="null"/> if none.</returns>
        public Exception? SubscribeGetError()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var sink = new ErrorCaptureWitness<T>();
            using var subscription = source.Subscribe(sink);
            return sink.Error;
        }

        /// <summary>Blocks until the source emits a value, errors, or completes (default 30s timeout).</summary>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
        public T? WaitForValue() =>
            WaitForValueCore(source, null, DefaultTimeout);

        /// <summary>Blocks until the source emits a value, errors, or completes, honoring an explicit <paramref name="timeout"/>.</summary>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
        public T? WaitForValue(TimeSpan timeout) =>
            WaitForValueCore(source, null, timeout);

        /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> (default 30s timeout).</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
        public T? WaitForValue(ISequencer scheduler) =>
            WaitForValueCore(source, scheduler, DefaultTimeout);

        /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>.</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
        public T? WaitForValue(ISequencer scheduler, TimeSpan timeout) =>
            WaitForValueCore(source, scheduler, timeout);

        /// <summary>Blocks until the source terminates; returns any captured error (does NOT rethrow). Default 30s timeout.</summary>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        public Exception? WaitForError() =>
            WaitForErrorCore(source, null, DefaultTimeout);

        /// <summary>Blocks until the source terminates with an explicit <paramref name="timeout"/>; returns any captured error (does NOT rethrow).</summary>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        public Exception? WaitForError(TimeSpan timeout) =>
            WaitForErrorCore(source, null, timeout);

        /// <summary>Blocks until the source terminates, routing the subscribe call through <paramref name="scheduler"/>; returns any captured error (does NOT rethrow).</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        public Exception? WaitForError(ISequencer scheduler) =>
            WaitForErrorCore(source, scheduler, DefaultTimeout);

        /// <summary>
        /// Blocks until the source terminates, routing the subscribe call
        /// through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>;
        /// returns any captured error (does NOT rethrow).
        /// </summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        public Exception? WaitForError(ISequencer scheduler, TimeSpan timeout) =>
            WaitForErrorCore(source, scheduler, timeout);
    }

    /// <summary>Blocking subscribe and wait operators for a <see cref="RxVoid"/>-producing source.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    extension(IObservable<RxVoid> source)
    {
        /// <summary>Subscribes to a <see cref="RxVoid"/>-producing observable, discarding the value. Safe only when the sequence terminates synchronously.</summary>
        public void SubscribeAndComplete()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            using var subscription = source.Subscribe(NoopWitness<RxVoid>.Instance);
        }

        /// <summary>Subscribes to the source and returns any error emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The captured error, or <see langword="null"/> if none.</returns>
        public Exception? SubscribeGetError()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var sink = new ErrorCaptureWitness<RxVoid>();
            using var subscription = source.Subscribe(sink);
            return sink.Error;
        }

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes (default 30s timeout); rethrows any captured error.</summary>
        public void WaitForCompletion() =>
            WaitForCompletionCore(source, null, DefaultTimeout);

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, honoring an explicit <paramref name="timeout"/>; rethrows any captured error.</summary>
        /// <param name="timeout">The wait timeout.</param>
        public void WaitForCompletion(TimeSpan timeout) =>
            WaitForCompletionCore(source, null, timeout);

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call through <paramref name="scheduler"/>; rethrows any captured error.</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        public void WaitForCompletion(ISequencer scheduler) =>
            WaitForCompletionCore(source, scheduler, DefaultTimeout);

        /// <summary>
        /// Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call
        /// through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>; rethrows any captured error.
        /// </summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        public void WaitForCompletion(ISequencer scheduler, TimeSpan timeout) =>
            WaitForCompletionCore(source, scheduler, timeout);
    }

    /// <summary>Shared implementation of the <c>WaitForValue</c> operators.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">Optional scheduler for the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/>.</returns>
    private static T? WaitForValueCore<T>(IObservable<T> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        var sink = new BlockingValueWitness<T>(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!done.Wait(timeout))
        {
            throw new TimeoutException(
                $"WaitForValue timed out after {timeout.TotalSeconds}s.");
        }

        return sink.Result;
    }

    /// <summary>Shared implementation of the <c>WaitForCompletion</c> operators.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">Optional scheduler for the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    private static void WaitForCompletionCore(IObservable<RxVoid> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        var sink = new BlockingTerminalWitness<RxVoid>(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!done.Wait(timeout))
        {
            throw new TimeoutException(
                $"WaitForCompletion timed out after {timeout.TotalSeconds}s.");
        }

        if (sink.Error is null)
        {
            return;
        }

        throw sink.Error;
    }

    /// <summary>Shared implementation of the <c>WaitForError</c> operators.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">Optional scheduler for the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The captured error, or <see langword="null"/>.</returns>
    private static Exception? WaitForErrorCore<T>(IObservable<T> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        var sink = new BlockingTerminalWitness<T>(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!done.Wait(timeout))
        {
            throw new TimeoutException(
                $"WaitForError timed out after {timeout.TotalSeconds}s.");
        }

        return sink.Error;
    }

    /// <summary>
    /// Subscribes to the specified <paramref name="source"/> observable using the provided <paramref name="scheduler"/>.
    /// If a scheduler is specified, the subscription is scheduled; otherwise, the subscription occurs immediately.
    /// </summary>
    /// <typeparam name="T">The type of the elements in <paramref name="source"/>.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="observer">The observer to receive notifications from the observable.</param>
    /// <param name="scheduler">
    /// The scheduler on which to execute the subscription logic. If <see langword="null"/>, the subscription occurs directly without scheduling.
    /// </param>
    /// <returns>A disposable representing the subscription.</returns>
    private static IDisposable ScheduledSubscribe<T>(
        IObservable<T> source,
        IObserver<T> observer,
        ISequencer? scheduler)
    {
        if (scheduler is null)
        {
            return source.Subscribe(observer);
        }

        var swap = new SwapDisposable();
        var scheduled = scheduler.Schedule(() => swap.Disposable = source.Subscribe(observer));
        return new DisposableBag(scheduled, swap);
    }

    /// <summary>No-op observer used by <see cref="SubscribeAndComplete"/> to absorb signals without allocating a delegate trio.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    private sealed class NoopWitness<T> : IObserver<T>
    {
        /// <summary>Singleton instance to avoid per-call allocation.</summary>
        public static readonly NoopWitness<T> Instance = new();

        /// <inheritdoc/>
        public void OnNext(T value)
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Observer that captures the last value seen during synchronous subscribe.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    private sealed class ValueCaptureWitness<T> : IObserver<T>
    {
        /// <summary>Gets the captured value, or <see langword="default"/> if none.</summary>
        public T? Value { get; private set; }

        /// <summary>Gets a value indicating whether at least one value was observed.</summary>
        public bool HasValue { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            Value = value;
            HasValue = true;
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Observer that captures the first error seen during synchronous subscribe.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    private sealed class ErrorCaptureWitness<T> : IObserver<T>
    {
        /// <summary>Gets the captured error, or <see langword="null"/> if none.</summary>
        public Exception? Error { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => Error ??= error;

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>
    /// Observer used by the value-returning <c>WaitFor</c> path: captures the last value,
    /// signals the gate on terminal, swallows errors (caller's timeout / default reflects outcome).
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="done">The gate signalled on terminal.</param>
    private sealed class BlockingValueWitness<T>(ManualResetEventSlim done) : IObserver<T>
    {
        /// <summary>Gets the most recent value seen.</summary>
        public T? Result { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Result = value;

        /// <inheritdoc/>
        public void OnError(Exception error) => done.Set();

        /// <inheritdoc/>
        public void OnCompleted() => done.Set();
    }

    /// <summary>Observer used by the completion / error <c>WaitFor</c> paths: captures any terminal error and signals the gate on terminal.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="done">The gate signalled on terminal.</param>
    private sealed class BlockingTerminalWitness<T>(ManualResetEventSlim done) : IObserver<T>
    {
        /// <summary>Gets the captured terminal error, or <see langword="null"/> if completion was normal.</summary>
        public Exception? Error { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            Error = error;
            done.Set();
        }

        /// <inheritdoc/>
        public void OnCompleted() => done.Set();
    }
}
