// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Blocking subscription and terminal-wait operations for <see cref="IObservable{T}"/> sequences.</summary>
public static class BlockingSubscription
{
    /// <summary>The default timeout used by the <c>WaitFor*</c> helpers when no override is supplied.</summary>
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Subscribes to a synchronously terminating sequence and discards its value.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SubscribeAndComplete(IObservable<RxVoid> source) =>
        NoopWitness<RxVoid>.SubscribeAndComplete(source);

    /// <summary>Subscribes to the source and returns the last value emitted during the synchronous subscribe call.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The last emitted value, or <see langword="default"/> if no value was emitted.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? SubscribeGetValue<T>(IObservable<T> source) =>
        ValueCaptureWitness<T>.SubscribeGetValue(source);

    /// <summary>Subscribes to the source and returns any error emitted during the synchronous subscribe call.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The captured error, or <see langword="null"/> if none.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Exception? SubscribeGetError<T>(IObservable<T> source) =>
        ErrorCaptureWitness<T>.SubscribeGetError(source);

    /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes (default 30s timeout); rethrows any captured error.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WaitForCompletion(IObservable<RxVoid> source) =>
        WaitForCompletionCore(source, null, DefaultTimeout);

    /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, honoring an explicit <paramref name="timeout"/>; rethrows any captured error.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="timeout">The wait timeout.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WaitForCompletion(IObservable<RxVoid> source, TimeSpan timeout) =>
        WaitForCompletionCore(source, null, timeout);

    /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call through <paramref name="scheduler"/>; rethrows any captured error.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WaitForCompletion(IObservable<RxVoid> source, ISequencer scheduler) =>
        WaitForCompletionCore(source, scheduler, DefaultTimeout);

    /// <summary>
    /// Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call through
    /// <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>; rethrows any captured error.
    /// </summary>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WaitForCompletion(IObservable<RxVoid> source, ISequencer scheduler, TimeSpan timeout) =>
        WaitForCompletionCore(source, scheduler, timeout);

    /// <summary>Blocks until the source emits a value, errors, or completes (default 30s timeout).</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
    /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? WaitForValue<T>(IObservable<T> source) =>
        WaitForValueCore(source, null, DefaultTimeout);

    /// <summary>Blocks until the source emits a value, errors, or completes, honoring an explicit <paramref name="timeout"/>.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
    /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? WaitForValue<T>(IObservable<T> source, TimeSpan timeout) =>
        WaitForValueCore(source, null, timeout);

    /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> (default 30s timeout).</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
    /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? WaitForValue<T>(IObservable<T> source, ISequencer scheduler) =>
        WaitForValueCore(source, scheduler, DefaultTimeout);

    /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
    /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? WaitForValue<T>(IObservable<T> source, ISequencer scheduler, TimeSpan timeout) =>
        WaitForValueCore(source, scheduler, timeout);

    /// <summary>Blocks until the source terminates or the timeout expires, returning any captured error without throwing it.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Exception? WaitForError<T>(IObservable<T> source) =>
        WaitForErrorCore(source, null, DefaultTimeout);

    /// <summary>Blocks until the source terminates with an explicit <paramref name="timeout"/>; returns any captured error without throwing it.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Exception? WaitForError<T>(IObservable<T> source, TimeSpan timeout) =>
        WaitForErrorCore(source, null, timeout);

    /// <summary>Blocks until the source terminates, routing the subscribe call through <paramref name="scheduler"/>; returns any captured error without throwing it.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Exception? WaitForError<T>(IObservable<T> source, ISequencer scheduler) =>
        WaitForErrorCore(source, scheduler, DefaultTimeout);

    /// <summary>
    /// Blocks until the source terminates, routing the subscribe call through <paramref name="scheduler"/> with an explicit
    /// <paramref name="timeout"/>; returns any captured error without throwing it.
    /// </summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Exception? WaitForError<T>(IObservable<T> source, ISequencer scheduler, TimeSpan timeout) =>
        WaitForErrorCore(source, scheduler, timeout);

    /// <summary>Shared implementation of the <c>WaitForValue</c> operators.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">Optional scheduler for the subscribe call.</param>
    /// <param name="timeout">The wait timeout.</param>
    /// <returns>The last value emitted before terminal, or <see langword="default"/>.</returns>
    /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
    private static T? WaitForValueCore<T>(IObservable<T> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        BlockingValueWitness<T> sink = new(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!WaitForTerminal(done, timeout))
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
    /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
    private static void WaitForCompletionCore(IObservable<RxVoid> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        BlockingTerminalWitness<RxVoid> sink = new(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!WaitForTerminal(done, timeout))
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
    /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
    private static Exception? WaitForErrorCore<T>(IObservable<T> source, ISequencer? scheduler, TimeSpan timeout)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using ManualResetEventSlim done = new();
        BlockingTerminalWitness<T> sink = new(done);
        using var subscription = ScheduledSubscribe(source, sink, scheduler);

        if (!WaitForTerminal(done, timeout))
        {
            throw new TimeoutException(
                $"WaitForError timed out after {timeout.TotalSeconds}s.");
        }

        return sink.Error;
    }

    /// <summary>Blocks until a terminal signal arrives or the timeout expires.</summary>
    /// <param name="done">The terminal signal.</param>
    /// <param name="timeout">The maximum wait duration.</param>
    /// <returns>True when the signal arrives; false on timeout.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool WaitForTerminal(ManualResetEventSlim done, TimeSpan timeout) => done.Wait(timeout);

    /// <summary>Subscribes <paramref name="observer"/> to <paramref name="source"/> on <paramref name="scheduler"/>, or inline when no scheduler is supplied.</summary>
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

        SwapDisposable swap = new();
        var scheduled = scheduler.Schedule(
            (Swap: swap, Source: source, Observer: observer),
            static (_, state) =>
            {
                state.Swap.Disposable = state.Source.Subscribe(state.Observer);
                return EmptyDisposable.Instance;
            });
        return new DisposableBag(scheduled, swap);
    }
}
