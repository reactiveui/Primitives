// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Subscription that invokes a cold task factory and forwards its terminal result.</summary>
/// <typeparam name="T">The task result type.</typeparam>
public sealed class FromAsyncSubscription<T> : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncSubscription{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="taskFactory">The factory invoked once for this subscription.</param>
    public FromAsyncSubscription(IObserver<T> observer, Func<CancellationToken, Task<T>> taskFactory)
        : this(observer, taskFactory, CancellationToken.None)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FromAsyncSubscription{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="taskFactory">The factory invoked once for this subscription.</param>
    /// <param name="externalCancellationToken">The external cancellation token linked into this subscription.</param>
    public FromAsyncSubscription(
        IObserver<T> observer,
        Func<CancellationToken, Task<T>> taskFactory,
        CancellationToken externalCancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ArgumentExceptionHelper.ThrowIfNull(taskFactory);

        Observer = observer;
        TaskFactory = taskFactory;
        ExternalCancellationToken = externalCancellationToken;
        Lifetime = new();
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the factory invoked once for this subscription.</summary>
    private Func<CancellationToken, Task<T>> TaskFactory { get; }

    /// <summary>Gets the external cancellation token linked into this subscription.</summary>
    private CancellationToken ExternalCancellationToken { get; }

    /// <summary>Gets the lifetime that owns disposal cancellation.</summary>
    private AsyncSubscriptionLifetime Lifetime { get; }

    /// <inheritdoc/>
    public void Dispose() => Lifetime.Dispose();

    /// <summary>Starts the task factory and returns the active subscription lifetime.</summary>
    /// <returns>The active subscription, or an empty disposable when the task completed synchronously.</returns>
    internal IDisposable Start()
    {
        CancellationTokenSource? linkedSource = null;
        var token = Lifetime.Token;
        if (ExternalCancellationToken.CanBeCanceled)
        {
            linkedSource = CancellationTokenSource.CreateLinkedTokenSource(ExternalCancellationToken, token);
            token = linkedSource.Token;
        }

        Task<T> task;
        try
        {
            task = TaskFactory(token);
            ArgumentExceptionHelper.ThrowIfNull(task);
        }
        catch (Exception error)
        {
            linkedSource?.Dispose();
            Lifetime.Complete();
            Observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        if (TryCompleteSynchronously(task, Observer, Lifetime, linkedSource))
        {
            return EmptyDisposable.Instance;
        }

        _ = ObserveAsync(task, Observer, Lifetime, linkedSource);
        return this;
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
        if (task.Exception is { InnerException: { } innerException })
        {
            observer.OnError(innerException);
        }
        else if (task.Exception is { } exception)
        {
            observer.OnError(exception);
        }

        return true;
    }
}
