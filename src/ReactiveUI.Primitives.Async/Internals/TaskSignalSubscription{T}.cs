// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Represents an asynchronous subscription that can be cancelled and disposed, managing the lifecycle of an
/// observer and its associated operations.
/// </summary>
/// <remarks>This type provides a base for implementing cancellable, asynchronously disposable
/// subscriptions that coordinate observer notifications and resource cleanup. Disposal cancels any ongoing
/// operations and ensures that all resources are released before completion. Derived classes should implement the
/// core execution logic in <see cref="ExecuteAsyncCore"/>.</remarks>
/// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
/// <param name="observer">The observer that receives notifications for the subscription. Cannot be null.</param>
internal abstract class TaskSignalSubscription<T>(IObserverAsync<T> observer) : IAsyncDisposable
{
    /// <summary>The task completion source used to signal when the subscription's asynchronous operation has finished.</summary>
    private readonly TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token source used to cancel the subscription's asynchronous operation upon disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Flows into the job's notification call chain (across any thread hops) so a reentrant
    /// <see cref="DisposeAsync"/> issued from inside the job — e.g. a downstream operator disposing the
    /// subscription from within its own <c>OnNextAsync</c> — is recognised and skips the self-join on
    /// <see cref="_tcs"/> that would otherwise deadlock. A thread-ID marker only catches the synchronous
    /// same-thread case; once the notification continuation hops threads the ID no longer matches and the
    /// dispose waits for a job that cannot complete until the dispose returns.</summary>
    private readonly AsyncLocal<bool> _executing = new();

    /// <summary>Indicates whether disposal has already been initiated to prevent double-disposal.</summary>
    private int _disposed;

    /// <summary>Starts the operation synchronously using the current cancellation token.</summary>
    /// <remarks>This method initiates the asynchronous operation and does not wait for its completion. To
    /// monitor progress or handle completion, use the asynchronous counterpart directly. The
    /// <see cref="ValueTask"/> returned by <see cref="ExecuteAsync"/> is converted to a <see cref="Task"/>
    /// before being discarded so the fire-and-forget pattern stays compatible with CA2012.</remarks>
    public void Start() => _ = ExecuteAsync(_cts.Token).AsTask();

    /// <summary>Asynchronously releases the resources used by the object and cancels any ongoing operations.</summary>
    /// <remarks>Call this method to ensure that all resources are released and any pending operations
    /// are cancelled before the object is discarded. Await the returned ValueTask to guarantee that disposal has
    /// completed.</remarks>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _cts.CancelAsync().ConfigureAwait(false);
        if (!_executing.Value)
        {
            await _tcs.Task.ConfigureAwait(false);
        }

        _cts.Dispose();
    }

    /// <summary>
    /// Attempts to complete the observer with a failure result. If the observer's completion handler
    /// also throws, the exception is routed to <see cref="UnhandledExceptionHandler"/>.
    /// </summary>
    /// <param name="observer">The observer to complete.</param>
    /// <param name="error">The original exception.</param>
    /// <returns>A <see cref="ValueTask"/> representing the operation.</returns>
    internal static async ValueTask CompleteWithFailureAsync(IObserverAsync<T> observer, Exception error)
    {
        try
        {
            await observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            UnhandledExceptionHandler.ReportUnhandledException(exception);
        }
    }

    /// <summary>Executes the subscription's core logic, handling exceptions by completing the observer with a failure result.</summary>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    internal async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        _executing.Value = true;
        try
        {
            await ExecuteAsyncCore(observer, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await CompleteWithFailureAsync(observer, e).ConfigureAwait(false);
        }
        finally
        {
            _tcs.SetResult(true);
        }
    }

    /// <summary>When overridden in a derived class, executes the core subscription logic asynchronously.</summary>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    protected abstract ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken);
}
