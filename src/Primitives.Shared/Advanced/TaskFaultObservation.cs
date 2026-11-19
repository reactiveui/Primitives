// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Task fault-observation and cancellation-handling helpers.</summary>
public static class TaskFaultObservation
{
    /// <summary>Registers fault observation without changing the task's outcome.</summary>
    /// <param name="task">The task that may outlive its consumer.</param>
    [ExcludeFromCodeCoverage]
    public static void Register(Task task)
    {
        ArgumentExceptionHelper.ThrowIfNull(task);

        _ = task.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Awaits <paramref name="source"/> and returns its final value, or <see langword="default"/> when <paramref name="token"/> cancels the wait.</summary>
    /// <typeparam name="TResult">The source element type.</typeparam>
    /// <param name="source">The observable source.</param>
    /// <param name="token">The token that cancels the wait.</param>
    /// <returns>A task carrying the final source value, or <see langword="default"/> on cancellation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<TResult?> HandleCancellationAsync<TResult>(IObservable<TResult> source, CancellationToken token) =>
        HandleCancellationAsync(source, null, token);

    /// <summary>Awaits <paramref name="source"/> and returns its final value; on cancellation invokes <paramref name="action"/> and returns <see langword="default"/>.</summary>
    /// <typeparam name="TResult">The source element type.</typeparam>
    /// <param name="source">The observable source.</param>
    /// <param name="action">Invoked when the wait is cancelled.</param>
    /// <param name="token">The token that cancels the wait.</param>
    /// <returns>A task carrying the final source value, or <see langword="default"/> on cancellation.</returns>
    public static async Task<TResult?> HandleCancellationAsync<TResult>(
        IObservable<TResult> source,
        Action? action,
        CancellationToken token)
    {
        try
        {
            token.ThrowIfCancellationRequested();
            return await AwaitOnTaskPoolAsync(source, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            action?.Invoke();
        }

        return default;
    }

    /// <summary>Awaits <paramref name="asyncTask"/> and completes normally when it is cancelled.</summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task HandleCancellationAsync(Task asyncTask) => HandleCancellationAsync(asyncTask, null);

    /// <summary>Awaits <paramref name="asyncTask"/> and invokes <paramref name="action"/> instead of throwing when it is cancelled.</summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="action">Invoked when the task is cancelled.</param>
    /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="asyncTask"/> is <see langword="null"/>.</exception>
    public static Task HandleCancellationAsync(Task asyncTask, Action? action)
    {
        ArgumentExceptionHelper.ThrowIfNull(asyncTask);
        return HandleCancellationCoreAsync(asyncTask, action);
    }

    /// <summary>Awaits <paramref name="asyncTask"/> and returns its result, or <see langword="default"/> when it is cancelled.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <returns>A task carrying the result, or <see langword="default"/> on cancellation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<TResult?> HandleCancellationAsync<TResult>(Task<TResult> asyncTask) =>
        HandleCancellationAsync(asyncTask, null);

    /// <summary>Awaits <paramref name="asyncTask"/> and returns its result; on cancellation invokes <paramref name="action"/> and returns <see langword="default"/>.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="action">Invoked when the task is cancelled.</param>
    /// <returns>A task carrying the result, or <see langword="default"/> on cancellation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="asyncTask"/> is <see langword="null"/>.</exception>
    public static Task<TResult?> HandleCancellationAsync<TResult>(Task<TResult> asyncTask, Action? action)
    {
        ArgumentExceptionHelper.ThrowIfNull(asyncTask);
        return HandleCancellationCoreAsync(asyncTask, action);
    }

    /// <summary>Awaits whichever completes first, <paramref name="asyncTask"/> or <paramref name="cancellationToken"/>, and reports which one it was.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="cancellationToken">The token raced against the task.</param>
    /// <returns>The task result, and whether the wait ended in cancellation.</returns>
    public static async Task<(TResult Value, bool IsCanceled)> WhenCancelledAsync<TResult>(
        Task<TResult> asyncTask,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(asyncTask);

        TaskCompletionSource<TResult> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.UnsafeRegister(
            static (state, token) => ((TaskCompletionSource<TResult>)state!).TrySetCanceled(token),
            tcs);
        var cancellationTask = tcs.Task;

        try
        {
            var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(false);

            // Faults from work that outlives cancellation are still observed.
            if (readyTask == cancellationTask)
            {
                Register(asyncTask);
            }

            return (await readyTask.ConfigureAwait(false), tcs.Task.IsCanceled || readyTask.IsCanceled);
        }
        finally
        {
#if NET8_0_OR_GREATER
            await registration.DisposeAsync().ConfigureAwait(false);
#else
            registration.Dispose();
#endif
        }
    }

    /// <summary>Starts an observable wait on the task pool, cancelling the wait itself when <paramref name="token"/> fires.</summary>
    /// <typeparam name="TResult">The source value type.</typeparam>
    /// <param name="source">The source to await.</param>
    /// <param name="token">The token that cancels the wait.</param>
    /// <returns>The source's terminal result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<TResult> AwaitOnTaskPoolAsync<TResult>(IObservable<TResult> source, CancellationToken token) =>
        Task.Run(() => Signal.ToTask(source ?? throw new ArgumentNullException(nameof(source)), token), token);

    /// <summary>Awaits <paramref name="asyncTask"/> and invokes <paramref name="action"/> instead of throwing when it is cancelled.</summary>
    /// <param name="asyncTask">The validated asynchronous task.</param>
    /// <param name="action">Invoked when the task is cancelled.</param>
    /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
    private static async Task HandleCancellationCoreAsync(Task asyncTask, Action? action)
    {
        try
        {
            await asyncTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            action?.Invoke();
        }
    }

    /// <summary>Awaits <paramref name="asyncTask"/> and returns its result; on cancellation invokes <paramref name="action"/> and returns <see langword="default"/>.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="asyncTask">The validated asynchronous task.</param>
    /// <param name="action">Invoked when the task is cancelled.</param>
    /// <returns>A task carrying the result, or <see langword="default"/> on cancellation.</returns>
    private static async Task<TResult?> HandleCancellationCoreAsync<TResult>(Task<TResult> asyncTask, Action? action)
    {
        try
        {
            return await asyncTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            action?.Invoke();
        }

        return default;
    }
}
