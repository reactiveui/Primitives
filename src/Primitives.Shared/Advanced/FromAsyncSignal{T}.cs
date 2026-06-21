// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Cold task-backed signal that gives each subscription its own cancellable token.</summary>
/// <typeparam name="T">The task result type.</typeparam>
public sealed class FromAsyncSignal<T> : IObservable<T>
{
    /// <summary>The per-subscription task factory.</summary>
    private readonly Func<CancellationToken, Task<T>> _taskFactory;

    /// <summary>The optional external cancellation token linked into each subscription.</summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="FromAsyncSignal{T}"/> class.</summary>
    /// <param name="taskFactory">The factory invoked once for each subscription.</param>
    public FromAsyncSignal(Func<CancellationToken, Task<T>> taskFactory)
        : this(taskFactory, CancellationToken.None)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FromAsyncSignal{T}"/> class.</summary>
    /// <param name="taskFactory">The factory invoked once for each subscription.</param>
    /// <param name="cancellationToken">The external cancellation token linked into each subscription.</param>
    public FromAsyncSignal(Func<CancellationToken, Task<T>> taskFactory, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskFactory);

        _taskFactory = taskFactory;
        _cancellationToken = cancellationToken;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        AsyncSubscriptionLifetime lifetime = new();
        CancellationTokenSource? linkedSource = null;
        var token = lifetime.Token;
        if (_cancellationToken.CanBeCanceled)
        {
            linkedSource = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken, token);
            token = linkedSource.Token;
        }

        Task<T> task;
        try
        {
            task = _taskFactory(token);
            ArgumentExceptionHelper.ThrowIfNull(task);
        }
        catch (Exception error)
        {
            linkedSource?.Dispose();
            lifetime.Complete();
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        if (TryCompleteSynchronously(task, observer, lifetime, linkedSource))
        {
            return EmptyDisposable.Instance;
        }

        _ = ObserveAsync(task, observer, lifetime, linkedSource);
        return lifetime;
    }

    /// <summary>Observes an incomplete task and forwards its terminal result unless the subscription was disposed.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns>A task representing the asynchronous observation.</returns>
    private static async Task ObserveAsync(
        Task<T> task,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        CancellationTokenSource? linkedSource)
    {
        try
        {
            var value = await task.ConfigureAwait(false);
            if (lifetime.IsCancellationRequested)
            {
                return;
            }

            observer.OnNext(value);
            if (!lifetime.IsCancellationRequested)
            {
                observer.OnCompleted();
            }
        }
        catch (Exception) when (lifetime.IsCancellationRequested)
        {
            // Subscription disposal owns this cancellation path and must stay silent downstream.
        }
        catch (Exception error)
        {
            observer.OnError(error);
        }
        finally
        {
            linkedSource?.Dispose();
            lifetime.SetSubscription(EmptyDisposable.Instance);
            lifetime.Complete();
        }
    }

    /// <summary>Forwards a task that has already reached a terminal state.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns><see langword="true"/> when the task was completed synchronously.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4462:Calls to \"async\" methods should not be blocking",
        Justification = "Synchronous read is limited to the already-completed task fast path.")]
    private static bool TryCompleteSynchronously(
        Task<T> task,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        CancellationTokenSource? linkedSource)
    {
        if (task.Status == TaskStatus.RanToCompletion)
        {
            linkedSource?.Dispose();
            lifetime.Complete();
            observer.OnNext(task.Result);
            observer.OnCompleted();
            return true;
        }

        if (task.IsCanceled)
        {
            linkedSource?.Dispose();
            lifetime.Complete();
            observer.OnError(new TaskCanceledException(task));
            return true;
        }

        if (!task.IsFaulted)
        {
            return false;
        }

        linkedSource?.Dispose();
        lifetime.Complete();
        observer.OnError(task.Exception!.InnerException ?? task.Exception);
        return true;
    }
}
