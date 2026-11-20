// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that emits the result of an existing task instance.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskInstanceSignal: Task = {Task}")]
public sealed class TaskInstanceSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="TaskInstanceSignal{T}"/> class.</summary>
    /// <param name="task">The task to observe.</param>
    public TaskInstanceSignal(Task<T> task)
    {
        ArgumentExceptionHelper.ThrowIfNull(task);

        Task = task;
    }

    /// <summary>Gets the task observed by this signal.</summary>
    private Task<T> Task { get; }

    /// <summary>Converts <paramref name="task"/> to a signal, emitting Completed/Faulted/Canceled tasks synchronously on subscription.</summary>
    /// <param name="task">The task to observe.</param>
    /// <returns>A signal that emits the task result or faults with the task error.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "The read is guarded by a RanToCompletion check, so the task is done.")]
    public static IObservable<T> Create(Task<T> task)
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
            ? new ImmediateThrowSignal<T>(task.Exception!.InnerException!)
            : new TaskInstanceSignal<T>(task);
    }

    /// <summary>Validates <paramref name="task"/> and returns it unchanged.</summary>
    /// <param name="task">The task.</param>
    /// <returns>The supplied <paramref name="task"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    public static Task<T> Identity(Task<T> task) =>
        task ?? throw new ArgumentNullException(nameof(task));

    /// <summary>Returns a task that mirrors <paramref name="task"/> or transitions to Canceled when <paramref name="cancellationToken"/> fires first.</summary>
    /// <param name="task">The task to await.</param>
    /// <param name="cancellationToken">The token used to cancel the returned task.</param>
    /// <returns>A task representing the cancellable wait.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    public static Task<T> WithCancellation(Task<T> task, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(task);

        if (!cancellationToken.CanBeCanceled || task.IsCompleted)
        {
            return task;
        }

        return cancellationToken.IsCancellationRequested
            ? System.Threading.Tasks.Task.FromCanceled<T>(cancellationToken)
            : AwaitWithCancellationAsync(task, cancellationToken);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        TaskInstanceSubscription subscription = new();
        _ = ObserveTaskAsync(Task, observer, subscription);
        return subscription;
    }

    /// <summary>Observes the task and forwards the terminal notification while honoring disposal.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscription">The subscription lifetime.</param>
    /// <returns>A task representing the asynchronous observation.</returns>
    internal static async Task ObserveTaskAsync(
        Task<T> task,
        IObserver<T> observer,
        TaskInstanceSubscription subscription)
    {
        try
        {
            var value = await task.ConfigureAwait(false);
            if (!subscription.TryStop())
            {
                return;
            }

            observer.OnNext(value);
            observer.OnCompleted();
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            if (!subscription.TryStop())
            {
                return;
            }

            observer.OnError(task.IsCanceled ? new TaskCanceledException(task) : error);
        }
    }

    /// <summary>Awaits <paramref name="task"/> while observing <paramref name="cancellationToken"/>, registering fault observation if canceled first.</summary>
    /// <param name="task">The task to await.</param>
    /// <param name="cancellationToken">The token used to cancel the wait.</param>
    /// <returns>The task outcome.</returns>
    private static async Task<T> AwaitWithCancellationAsync(Task<T> task, CancellationToken cancellationToken)
    {
        try
        {
            return await task.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            TaskFaultObservation.Register(task);
            throw;
        }
    }
}
