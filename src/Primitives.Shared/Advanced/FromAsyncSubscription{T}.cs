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
        Lifetime = new();
        ExternalCancellation = new(observer, Lifetime, externalCancellationToken);
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the factory invoked once for this subscription.</summary>
    private Func<CancellationToken, Task<T>> TaskFactory { get; }

    /// <summary>Gets the lifetime that owns disposal cancellation.</summary>
    private AsyncSubscriptionLifetime Lifetime { get; }

    /// <summary>Gets the external cancellation forwarder.</summary>
    private FromAsyncExternalCancellation<T> ExternalCancellation { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        Lifetime.Dispose();
        ExternalCancellation.Dispose();
    }

    /// <summary>Starts the task factory and returns the active subscription lifetime.</summary>
    /// <returns>The active subscription, or an empty disposable when the task completed synchronously.</returns>
    internal IDisposable Start()
    {
        CancellationTokenSource? linkedSource = null;
        var token = Lifetime.Token;
        if (ExternalCancellation.CanBeCanceled)
        {
            linkedSource = ExternalCancellation.CreateLinkedSource(token);
            token = linkedSource.Token;
            if (!ExternalCancellation.Start())
            {
                linkedSource.Dispose();
                ExternalCancellation.Dispose();
                return EmptyDisposable.Instance;
            }
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
            ExternalCancellation.Dispose();
            if (Lifetime.TryComplete())
            {
                Observer.OnError(error);
            }

            return EmptyDisposable.Instance;
        }

        if (TryCompleteSynchronously(task, Observer, Lifetime, ExternalCancellation, linkedSource))
        {
            return EmptyDisposable.Instance;
        }

        FromAsyncTaskObservation<T> observation = new(Observer, Lifetime, ExternalCancellation, linkedSource);
        _ = task.ContinueWith(
            static (completedTask, state) => ((FromAsyncTaskObservation<T>)state!).Observe(completedTask),
            observation,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return this;
    }

    /// <summary>Forwards a task that has already reached a terminal state.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="externalCancellation">The external cancellation forwarder.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns><see langword="true"/> when the task was completed synchronously.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "Synchronous read is limited to the already-completed task fast path.")]
    private static bool TryCompleteSynchronously(
        Task<T> task,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        FromAsyncExternalCancellation<T> externalCancellation,
        CancellationTokenSource? linkedSource)
    {
        if (lifetime.IsCompleted)
        {
            linkedSource?.Dispose();
            externalCancellation.Dispose();
            return true;
        }

        if (task.Status == TaskStatus.RanToCompletion)
        {
            return CompleteSynchronously(task.Result, observer, lifetime, externalCancellation, linkedSource);
        }

        return task.IsCanceled
            ? CancelSynchronously(task, observer, lifetime, externalCancellation, linkedSource)
            : task.IsFaulted && FaultSynchronously(task, observer, lifetime, externalCancellation, linkedSource);
    }

    /// <summary>Forwards an already-successful task result.</summary>
    /// <param name="value">The task result.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="externalCancellation">The external cancellation forwarder.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns><see langword="true"/> because the task was completed synchronously.</returns>
    private static bool CompleteSynchronously(
        T value,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        FromAsyncExternalCancellation<T> externalCancellation,
        CancellationTokenSource? linkedSource)
    {
        linkedSource?.Dispose();
        externalCancellation.Dispose();
        if (!lifetime.TryComplete())
        {
            return true;
        }

        observer.OnNext(value);
        observer.OnCompleted();
        return true;
    }

    /// <summary>Forwards an already-canceled task result.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="externalCancellation">The external cancellation forwarder.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns><see langword="true"/> because the task was completed synchronously.</returns>
    private static bool CancelSynchronously(
        Task<T> task,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        FromAsyncExternalCancellation<T> externalCancellation,
        CancellationTokenSource? linkedSource)
    {
        linkedSource?.Dispose();
        if (externalCancellation.TryForwardCancellation())
        {
            externalCancellation.Dispose();
            return true;
        }

        externalCancellation.Dispose();
        if (!lifetime.TryComplete())
        {
            return true;
        }

        observer.OnError(new TaskCanceledException(task));
        return true;
    }

    /// <summary>Forwards an already-faulted task result.</summary>
    /// <param name="task">The task to observe.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="externalCancellation">The external cancellation forwarder.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    /// <returns><see langword="true"/> because the task was completed synchronously.</returns>
    private static bool FaultSynchronously(
        Task<T> task,
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        FromAsyncExternalCancellation<T> externalCancellation,
        CancellationTokenSource? linkedSource)
    {
        linkedSource?.Dispose();
        externalCancellation.Dispose();
        if (!lifetime.TryComplete())
        {
            return true;
        }

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
