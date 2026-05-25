// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Signals.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private const int TaskCompleted = 1;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private const int TaskFaulted = 2;

    /// <summary>
    /// Handles Asnyc Tasks with cancellation.
    /// </summary>
    /// <param name="execution">The function to execute.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(Func<CancellationTokenSource, Task<RxVoid>> execution) =>
        FromTask(execution, null, null);

    /// <summary>
    /// Handles Asnyc Tasks with cancellation.
    /// </summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(Func<CancellationTokenSource, Task<RxVoid>> execution, ISequencer? scheduler) =>
        FromTask(execution, scheduler, null);

    /// <summary>
    /// Handles Asnyc Tasks with cancellation.
    /// </summary>
    /// <param name="execution">The function to execute.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>
    /// An ITaskSignal of T.
    /// </returns>
    public static ITaskSignal<RxVoid> FromTask(
        Func<CancellationTokenSource, Task<RxVoid>> execution,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(execution, static _ => true, scheduler, cancellationTokenSource);

    /// <summary>
    /// Froms the asynchronous.
    /// </summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(Func<CancellationTokenSource, Task<TResult>> actionAsync) =>
        FromTask(actionAsync, null, null);

    /// <summary>
    /// Froms the asynchronous.
    /// </summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(Func<CancellationTokenSource, Task<TResult>> actionAsync, ISequencer? scheduler) =>
        FromTask(actionAsync, scheduler, null);

    /// <summary>
    /// Froms the asynchronous.
    /// </summary>
    /// <typeparam name="TResult">The type of the return value.</typeparam>
    /// <param name="actionAsync">The action asynchronous.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>
    /// An TaskSignal of T.
    /// </returns>
    public static ITaskSignal<TResult> FromTask<TResult>(
        Func<CancellationTokenSource, Task<TResult>> actionAsync,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        CreateTaskSignal(actionAsync, static _ => true, scheduler, cancellationTokenSource);

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <returns>A Task.</returns>
    public static Task HandleCancellation(this Task asyncTask) => HandleCancellation(asyncTask, null);

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="action">The action.</param>
    /// <returns>A Task.</returns>
    public static async Task HandleCancellation(this Task asyncTask, Action? action)
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

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <returns>A Task of TResult.</returns>
    public static Task<TResult?> HandleCancellation<TResult>(this Task<TResult> asyncTask) => HandleCancellation(asyncTask, null);

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="action">The action.</param>
    /// <returns>A Task of TResult.</returns>
    public static async Task<TResult?> HandleCancellation<TResult>(this Task<TResult> asyncTask, Action? action)
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

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <typeparam name="TResult">The type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="token">The token.</param>
    /// <returns>
    /// A Task.
    /// </returns>
    public static Task<TResult?> HandleCancellation<TResult>(this IObservable<TResult> asyncTask, CancellationToken token) =>
        HandleCancellation(asyncTask, null, token);

    /// <summary>
    /// Handles the cancellation.
    /// </summary>
    /// <typeparam name="TResult">The type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <param name="action">The action.</param>
    /// <param name="token">The token.</param>
    /// <returns>
    /// A Task.
    /// </returns>
    public static async Task<TResult?> HandleCancellation<TResult>(this IObservable<TResult> asyncTask, Action? action, CancellationToken token)
    {
        try
        {
            token.ThrowIfCancellationRequested();
            return await Task.Run(async () => await asyncTask, token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            action?.Invoke();
        }

        return default;
    }

    /// <summary>
    /// Executes the CreateTaskSignal operation.
    /// </summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="execution">The execution value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    /// <param name="cancellationTokenSource">The cancellationTokenSource value.</param>
    /// <returns>The result.</returns>
    private static ITaskSignal<TResult> CreateTaskSignal<TResult>(
        Func<CancellationTokenSource, Task<TResult>> execution,
        Func<TResult, bool> shouldEmit,
        ISequencer? scheduler,
        CancellationTokenSource? cancellationTokenSource) =>
        TaskSignal.Create<TResult>(
            ao => Defer(() => Create<TResult>(observer => SubscribeTask(ao, execution, shouldEmit, observer))),
            scheduler,
            cancellationTokenSource);

    /// <summary>
    /// Executes the SubscribeTask operation.
    /// </summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="signal">The signal value.</param>
    /// <param name="execution">The execution value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    private static IDisposable SubscribeTask<TResult>(
        ITaskSignal<TResult> signal,
        Func<CancellationTokenSource, Task<TResult>> execution,
        Func<TResult, bool> shouldEmit,
        IObserver<TResult> observer)
    {
        var source = signal.CancellationTokenSource!;
        var token = source.Token;
        token.ThrowIfCancellationRequested();
        var completionState = 0;
        var cancellableTask = Task.Factory
            .StartNew(() => execution(source), token, TaskCreationOptions.None, TaskScheduler.Current)
            .WhenCancelled(token);

        _ = ObserveTask(
            cancellableTask,
            shouldEmit,
            observer,
            () => Volatile.Write(ref completionState, TaskCompleted),
            () => Volatile.Write(ref completionState, TaskFaulted),
            token);

        return Disposable.Create(() =>
        {
            if (Volatile.Read(ref completionState) == TaskCompleted)
            {
                return;
            }

            Cancel(source);
        });
    }

    /// <summary>
    /// Executes the ObserveTask operation.
    /// </summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="cancellableTask">The cancellableTask value.</param>
    /// <param name="shouldEmit">The shouldEmit value.</param>
    /// <param name="observer">The observer value.</param>
    /// <param name="setCompleted">The setCompleted value.</param>
    /// <param name="setFaulted">The setFaulted value.</param>
    /// <param name="token">The token value.</param>
    /// <returns>The result.</returns>
    private static async Task ObserveTask<TResult>(
        Task<(Task<TResult> Task, bool IsCanceled)> cancellableTask,
        Func<TResult, bool> shouldEmit,
        IObserver<TResult> observer,
        Action setCompleted,
        Action setFaulted,
        CancellationToken token)
    {
        try
        {
            var (task, isCanceled) = await cancellableTask.ConfigureAwait(false);
            var result = await task.ConfigureAwait(false);
            if (!isCanceled && !token.IsCancellationRequested && shouldEmit(result))
            {
                observer.OnNext(result);
                setCompleted();
                observer.OnCompleted();
                return;
            }

            setFaulted();
            observer.OnError(new OperationCanceledException());
        }
        catch (Exception error)
        {
            setFaulted();
            observer.OnError(error);
        }
    }

    /// <summary>
    /// Executes the Cancel operation.
    /// </summary>
    /// <param name="source">The source value.</param>
    private static void Cancel(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Another completion path already released the token source.
        }
    }

    /// <summary>
    /// Executes the WhenCancelled operation.
    /// </summary>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    /// <param name="asyncTask">The asyncTask value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    private static async Task<(TResult Value, bool IsCanceled)> WhenCancelled<TResult>(this Task<TResult> asyncTask, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(
            static state => ((TaskCompletionSource<TResult>)state!).TrySetCanceled(),
            tcs,
            useSynchronizationContext: false);
        var cancellationTask = tcs.Task;

        try
        {
            // Create a task that completes when either the async operation completes,
            // or cancellation is requested.
            var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(false);

            // In case of cancellation, register a continuation to observe any unhandled.
            // exceptions from the asynchronous operation (once it completes).
            if (readyTask == cancellationTask)
            {
                await asyncTask.ContinueWith(
                    _ => asyncTask.Exception,
                    cancellationToken,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Current).ConfigureAwait(false);
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
}
