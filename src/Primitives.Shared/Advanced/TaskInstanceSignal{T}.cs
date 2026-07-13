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
    private static async Task ObserveTaskAsync(
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
        catch (Exception error)
        {
            if (!subscription.TryStop())
            {
                return;
            }

            observer.OnError(task.IsCanceled ? new TaskCanceledException(task) : error);
        }
    }
}
