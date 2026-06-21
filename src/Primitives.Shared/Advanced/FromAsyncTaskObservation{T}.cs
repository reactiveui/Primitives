// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observes an incomplete task-backed signal subscription.</summary>
/// <typeparam name="T">The task result type.</typeparam>
internal sealed class FromAsyncTaskObservation<T>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncTaskObservation{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="lifetime">The subscription lifetime.</param>
    /// <param name="externalCancellation">The external cancellation forwarder.</param>
    /// <param name="linkedSource">The linked token source, when an external token was supplied.</param>
    public FromAsyncTaskObservation(
        IObserver<T> observer,
        AsyncSubscriptionLifetime lifetime,
        FromAsyncExternalCancellation<T> externalCancellation,
        CancellationTokenSource? linkedSource)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ArgumentExceptionHelper.ThrowIfNull(lifetime);

        ArgumentExceptionHelper.ThrowIfNull(externalCancellation);

        Observer = observer;
        Lifetime = lifetime;
        ExternalCancellation = externalCancellation;
        LinkedSource = linkedSource;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the subscription lifetime.</summary>
    private AsyncSubscriptionLifetime Lifetime { get; }

    /// <summary>Gets the external cancellation forwarder.</summary>
    private FromAsyncExternalCancellation<T> ExternalCancellation { get; }

    /// <summary>Gets the linked token source, when an external token was supplied.</summary>
    private CancellationTokenSource? LinkedSource { get; }

    /// <summary>Observes the completed task and forwards its terminal result unless the subscription was disposed.</summary>
    /// <param name="task">The task to observe.</param>
    public void Observe(Task<T> task)
    {
        try
        {
            ObserveCore(task);
        }
        catch (Exception) when (Lifetime.IsCancellationRequested)
        {
            // Subscription disposal owns this cancellation path and must stay silent downstream.
        }
        catch (Exception error)
        {
            OnError(error);
        }
        finally
        {
            LinkedSource?.Dispose();
            ExternalCancellation.Dispose();
            Lifetime.SetSubscription(EmptyDisposable.Instance);
            Lifetime.Complete();
        }
    }

    /// <summary>Forwards the task terminal state.</summary>
    /// <param name="task">The task to observe.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4462:Calls to \"async\" methods should not be blocking",
        Justification = "Synchronous result access is limited to the completed task continuation.")]
    private void ObserveCore(Task<T> task)
    {
        if (task.Status == TaskStatus.RanToCompletion)
        {
            OnSuccess(task.Result);
            return;
        }

        if (task.IsCanceled)
        {
            OnError(new TaskCanceledException(task));
            return;
        }

        if (task.Exception is { InnerException: { } innerException })
        {
            OnError(innerException);
        }
        else if (task.Exception is { } exception)
        {
            OnError(exception);
        }
    }

    /// <summary>Forwards a successful task result.</summary>
    /// <param name="value">The task result.</param>
    private void OnSuccess(T value)
    {
        if (Lifetime.IsCancellationRequested || !Lifetime.TryComplete())
        {
            return;
        }

        ExternalCancellation.Dispose();
        LinkedSource?.Dispose();
        Observer.OnNext(value);
        Observer.OnCompleted();
    }

    /// <summary>Forwards a faulted or canceled task result.</summary>
    /// <param name="error">The observed task error.</param>
    private void OnError(Exception error)
    {
        if (Lifetime.IsCancellationRequested || ExternalCancellation.TryForwardCancellation() || !Lifetime.TryComplete())
        {
            return;
        }

        ExternalCancellation.Dispose();
        Observer.OnError(error);
    }
}
