// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Base class for observers that produce a single task-based result value when the observed sequence completes.
/// </summary>
/// <typeparam name="T">The type of elements received from the observable sequence.</typeparam>
/// <typeparam name="TTaskValue">The type of the result value produced by this observer.</typeparam>
/// <param name="cancellationToken">A cancellation token used to cancel the waiting operation.</param>
internal abstract class TaskObserverAsyncBase<T, TTaskValue>(CancellationToken cancellationToken) : ObserverAsync<T>
{
    /// <summary>
    /// The task completion source used to produce the observer's single result value.
    /// </summary>
    private readonly TaskCompletionSource<TTaskValue> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// The cancellation token used to cancel the waiting operation.
    /// </summary>
    private readonly CancellationToken _cancellationToken = cancellationToken;

    /// <summary>
    /// Asynchronously waits for the observer to produce its result value.
    /// </summary>
    /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
    public async ValueTask<TTaskValue> WaitValueAsync()
    {
        try
        {
#if NET8_0_OR_GREATER
            await using var ct = _cancellationToken.Register(
                static x =>
                {
                    var @this = (TaskObserverAsyncBase<T, TTaskValue>)x!;
                    @this._tcs.TrySetException(new OperationCanceledException(@this._cancellationToken));
                },
                this);
#else
            using var ct = _cancellationToken.Register(
                static x =>
                {
                    var @this = (TaskObserverAsyncBase<T, TTaskValue>)x!;
                    @this._tcs.TrySetException(new OperationCanceledException(@this._cancellationToken));
                },
                this);
#endif

            return await _tcs.Task.ConfigureAwait(false);
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Attempts to set the result value and complete the task.
    /// </summary>
    /// <param name="value">The result value to set.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [DebuggerStepThrough]
    protected async ValueTask TrySetCompleted(TTaskValue value)
    {
        try
        {
            _tcs.TrySetResult(value);
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Attempts to set the task to a faulted state with the specified exception.
    /// </summary>
    /// <param name="e">The exception that caused the fault.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected async ValueTask TrySetException(Exception e)
    {
        try
        {
            _tcs.TrySetException(e);
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }
}
