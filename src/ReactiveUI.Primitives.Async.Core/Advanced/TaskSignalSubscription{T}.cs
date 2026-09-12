// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that runs a cancellable asynchronous job feeding a single observer, and joins that job on disposal.</summary>
/// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
/// <param name="observer">The observer that receives notifications for the subscription. Cannot be null.</param>
/// <remarks>Disposal cancels the running job and waits for it to finish before releasing resources;
/// derived classes supply the job body in <see cref="ExecuteAsyncCore"/>.</remarks>
[System.Diagnostics.DebuggerDisplay("TaskSignalSubscription: Disposed = {_disposed}, Completed = {_tcs.Task.IsCompleted}")]
public abstract class TaskSignalSubscription<T>(IObserverAsync<T> observer) : IAsyncDisposable
{
    /// <summary>The task completion source that signals when the subscription's job has finished.</summary>
    private readonly TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token source that cancels the subscription's job on disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Set while the job runs, so a reentrant <see cref="DisposeAsync"/> from inside it skips the self-join.</summary>
    private readonly AsyncLocal<bool> _executing = new();

    /// <summary>Set on the first disposal so later calls are no-ops.</summary>
    private int _disposed;

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    public void Start() => _ = ExecuteAsync(_cts.Token).AsTask();

    /// <summary>Asynchronously releases the resources used by the object and cancels any ongoing operations.</summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    /// <remarks>Joins the in-flight job before returning, except when called from inside that job's own
    /// notification, where joining would deadlock.</remarks>
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

        GC.SuppressFinalize(this);
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
    /// <param name="cancellationToken">A token that cancels the subscription's job.</param>
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
    /// <param name="cancellationToken">A token that cancels the subscription's job.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    protected abstract ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken);
}
