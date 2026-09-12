// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Waits for a durable operation using persisted state and live notifications.</summary>
internal static class OperationSynchronizationWaiter
{
    /// <summary>The largest timeout supported by the system timer.</summary>
    private const long MaximumTimeoutMilliseconds = uint.MaxValue - 1;

    /// <summary>Waits without canceling the durable operation.</summary>
    /// <param name="states">The live status source.</param>
    /// <param name="lookup">The persisted status lookup.</param>
    /// <param name="operationId">The operation to observe.</param>
    /// <param name="timeout">The maximum waiting time.</param>
    /// <param name="timeProvider">The timeout clock.</param>
    /// <param name="cancellationToken">The token canceling only this wait.</param>
    /// <returns>The asynchronous waiting task.</returns>
    /// <exception cref="ArgumentNullException">A required dependency is missing.</exception>
    /// <exception cref="ArgumentException">The operation identifier is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is invalid.</exception>
    /// <exception cref="OperationCanceledException">The caller cancels this wait.</exception>
    /// <exception cref="TimeoutException">The waiting time elapses.</exception>
    /// <exception cref="InvalidOperationException">The operation cannot synchronize or its status source closes.</exception>
    internal static async Task WaitAsync(
        IObservable<SyncOperationStatus> states,
        Func<CancellationToken, ValueTask<SyncOperationStatus?>> lookup,
        OperationId operationId,
        TimeSpan timeout,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(states);
        ArgumentExceptionHelper.ThrowIfNull(lookup);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        if (operationId.Value == Guid.Empty)
        {
            throw new ArgumentException("OperationId must be non-empty.", nameof(operationId));
        }

        if (timeout != Timeout.InfiniteTimeSpan && (timeout < TimeSpan.Zero || timeout.TotalMilliseconds > MaximumTimeoutMilliseconds))
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        cancellationToken.ThrowIfCancellationRequested();
        var observer = new StatusObserver(operationId);
        using var subscription = states.Subscribe(observer);
        _ = ObserveLookupAsync(lookup, observer, cancellationToken);
        try
        {
            await observer.Completion.WaitAsync(timeout, timeProvider, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            observer.StopWaiting();
        }
    }

    /// <summary>Observes lookup failures even when a live result completes first.</summary>
    /// <param name="lookup">The persisted status lookup.</param>
    /// <param name="observer">The observer completing this wait.</param>
    /// <param name="cancellationToken">The token canceling the lookup.</param>
    /// <returns>The lookup observation task.</returns>
    private static async Task ObserveLookupAsync(
        Func<CancellationToken, ValueTask<SyncOperationStatus?>> lookup,
        StatusObserver observer,
        CancellationToken cancellationToken)
    {
        try
        {
            observer.CompleteLookup(await lookup(cancellationToken).ConfigureAwait(false));
        }
        catch (Exception exception)
        {
            observer.OnError(exception);
        }
    }

    /// <summary>Combines one durable lookup with the live notification stream.</summary>
    private sealed class StatusObserver : IObserver<SyncOperationStatus>
    {
        /// <summary>The operation being awaited.</summary>
        private readonly OperationId _operationId;

        /// <summary>The completion shared by lookup and notification paths.</summary>
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Indicates that the persisted lookup has returned.</summary>
        private int _lookupFinished;

        /// <summary>Indicates that no more live statuses can arrive.</summary>
        private int _sourceCompleted;

        /// <summary>Initializes a new instance of the <see cref="StatusObserver"/> class.</summary>
        /// <param name="operationId">The awaited operation.</param>
        internal StatusObserver(OperationId operationId) => _operationId = operationId;

        /// <summary>Gets the terminal waiting task.</summary>
        internal Task Completion => _completion.Task;

        /// <inheritdoc/>
        public void OnNext(SyncOperationStatus value)
        {
            if (value.OperationId != _operationId)
            {
                return;
            }

            switch (value.State)
            {
                case SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload
                    or SyncOperationState.Uploading or SyncOperationState.Conflict:
                {
                    return;
                }

                case SyncOperationState.Synchronized:
                {
                    _ = _completion.TrySetResult(true);
                    return;
                }

                default:
                {
                    OnError(new InvalidOperationException($"The operation cannot synchronize from state {value.State}."));
                    return;
                }
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => _ = _completion.TrySetException(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Volatile.Write(ref _sourceCompleted, 1);
            TryCompleteClosed();
        }

        /// <summary>Handles persisted status before recognizing a completed notification source.</summary>
        /// <param name="status">The recovered operation status.</param>
        internal void CompleteLookup(SyncOperationStatus? status)
        {
            if (status is not null)
            {
                if (status.OperationId != _operationId)
                {
                    OnError(new InvalidOperationException("The status lookup returned a different operation."));
                    return;
                }

                OnNext(status);
            }

            Volatile.Write(ref _lookupFinished, 1);
            TryCompleteClosed();
        }

        /// <summary>Retires the private completion so a late lookup failure cannot leave an unobserved faulted task.</summary>
        internal void StopWaiting()
        {
            _ = _completion.TrySetCanceled();
            _ = _completion.Task.Exception;
        }

        /// <summary>Fails a wait once neither lookup nor notifications can provide success.</summary>
        private void TryCompleteClosed()
        {
            if (Volatile.Read(ref _lookupFinished) == 0 || Volatile.Read(ref _sourceCompleted) == 0)
            {
                return;
            }

            OnError(new InvalidOperationException("The operation status source completed before synchronization."));
        }
    }
}
