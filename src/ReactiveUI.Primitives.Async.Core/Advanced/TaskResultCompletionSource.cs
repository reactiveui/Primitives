// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates task completion for terminal observers and disposes the owning subscription when complete.</summary>
/// <typeparam name="T">The result value type.</typeparam>
/// <param name="cancellationToken">The cancellation token that can cancel the pending result.</param>
public sealed class TaskResultCompletionSource<T>(CancellationToken cancellationToken)
{
    /// <summary>The task completion source used to publish the terminal result.</summary>
    private readonly TaskCompletionSource<T> _taskSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The cancellation token that cancels the terminal wait.</summary>
    private readonly CancellationToken _cancellationToken = cancellationToken;

    /// <summary>Waits for the terminal result and disposes <paramref name="owner"/> when the wait exits.</summary>
    /// <param name="owner">The owner to dispose when the result wait completes, faults, or is cancelled.</param>
    /// <returns>The terminal result value.</returns>
    public async ValueTask<T> AwaitResultAsync(IAsyncDisposable owner)
    {
        try
        {
#if NET8_0_OR_GREATER
            await using var cancellationRegistration = RegisterCancellation();
#else
            using var cancellationRegistration = RegisterCancellation();
#endif

            return await _taskSource.Task.ConfigureAwait(false);
        }
        finally
        {
            await owner.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Completes the result successfully and disposes <paramref name="owner"/>.</summary>
    /// <param name="value">The result value.</param>
    /// <param name="owner">The owner to dispose after publishing the result. Disposed via the reentrant path
    /// because this runs from within the owner's own in-flight notification.</param>
    /// <returns>A task that completes when the owner has been disposed.</returns>
    public async ValueTask SetResultAndDisposeAsync(T value, IReentrantAsyncDisposable owner)
    {
        try
        {
            _ = _taskSource.TrySetResult(value);
        }
        finally
        {
            await owner.DisposeFromNotificationAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Completes the result with an exception and disposes <paramref name="owner"/>.</summary>
    /// <param name="exception">The exception that faults the result.</param>
    /// <param name="owner">The owner to dispose after publishing the exception. Disposed via the reentrant path
    /// because this runs from within the owner's own in-flight notification.</param>
    /// <returns>A task that completes when the owner has been disposed.</returns>
    public async ValueTask SetExceptionAndDisposeAsync(Exception exception, IReentrantAsyncDisposable owner)
    {
        try
        {
            _ = _taskSource.TrySetException(exception);
        }
        finally
        {
            await owner.DisposeFromNotificationAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Registers cancellation for the pending result task.</summary>
    /// <returns>The cancellation registration.</returns>
    private CancellationTokenRegistration RegisterCancellation() =>
        _cancellationToken.Register(
            static state =>
            {
                var source = (TaskResultCompletionSource<T>)(
                    state ?? throw new InvalidOperationException("The task result state is missing."));
                _ = source._taskSource.TrySetException(new OperationCanceledException(source._cancellationToken));
            },
            this);
}
