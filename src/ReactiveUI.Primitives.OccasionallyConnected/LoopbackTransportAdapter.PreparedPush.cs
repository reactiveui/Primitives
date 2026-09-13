// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>In-memory transport adapter for occasionally connected synchronization tests and local loopback flows.</summary>
/// <content>Prepared push handle implementation.</content>
public sealed partial class LoopbackTransportAdapter
{
    /// <summary>Represents one loopback transport session.</summary>
    /// <content>Prepared push handle implementation.</content>
    private sealed partial class LoopbackTransportSession
    {
        /// <summary>Owns one prepared push reservation until send or disposal releases it.</summary>
        /// <param name="session">The owning session.</param>
        /// <param name="validatorOptions">The loopback validator options.</param>
        /// <param name="batch">The validated synchronization batch.</param>
        /// <param name="lease">The admitted push lease.</param>
        private sealed class LoopbackPreparedPush(
            LoopbackTransportSession session,
            LoopbackTransportAdapterOptions validatorOptions,
            SyncBatch batch,
            OperationLease lease) : IPreparedRemotePush
        {
            /// <summary>Synchronizes send and disposal state.</summary>
#if NET9_0_OR_GREATER
            private readonly Lock _gate = new();
#else
            private readonly object _gate = new();
#endif

            /// <summary>The stable disposal task for repeated disposal.</summary>
            private Task? _disposeTask;

            /// <summary>The active send completion task when sending has started.</summary>
            private Task? _sendCompletionTask;

            /// <summary>The batch retained until send or disposal completes.</summary>
            private SyncBatch? _batch = batch;

            /// <summary>Whether send has been started.</summary>
            private bool _sendStarted;

            /// <summary>Whether this handle has been disposed.</summary>
            private bool _disposed;

            /// <summary>Whether the reservation has been released.</summary>
            private int _reservationReleased;

            /// <summary>The exact logical encoded size of the batch in bytes.</summary>
            private long _encodedSizeBytes;

            /// <inheritdoc/>
            public SyncBatch Batch => _batch ?? throw new ObjectDisposedException(GetType().FullName);

            /// <inheritdoc/>
            public long EncodedSizeBytes => _encodedSizeBytes;

            /// <summary>Stores the exact logical encoded size after validation completes.</summary>
            /// <param name="value">The exact logical encoded size in bytes.</param>
            public void SetEncodedSizeBytes(long value) => _encodedSizeBytes = value;

            /// <inheritdoc/>
            /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
            /// <exception cref="ObjectDisposedException">The prepared push is disposed.</exception>
            /// <exception cref="InvalidOperationException">The prepared push has already been sent or the hub returns a malformed response.</exception>
            /// <exception cref="SyncBatchValidationException">The hub result does not exactly match the pushed batch.</exception>
            public ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
            {
                SyncBatch retainedBatch;
                TaskCompletionSource<object?> sendCompletion;
                lock (_gate)
                {
                    ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
                    if (_sendStarted)
                    {
                        throw new InvalidOperationException("The prepared push has already been sent.");
                    }

                    _sendStarted = true;
                    retainedBatch = Batch;
                    sendCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _sendCompletionTask = sendCompletion.Task;
                }

                var task = SendCoreAsync(retainedBatch, sendCompletion, cancellationToken);
                return new(task);
            }

            /// <inheritdoc/>
            public ValueTask DisposeAsync()
            {
                Task? sendCompletionTask = null;
                TaskCompletionSource<object?>? completion = null;
                Task task;
                lock (_gate)
                {
                    if (_disposeTask is null)
                    {
                        _disposed = true;
                        sendCompletionTask = _sendCompletionTask;
                        if (!_sendStarted)
                        {
                            _batch = null;
                        }

                        completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        _disposeTask = completion.Task;
                    }

                    task = _disposeTask;
                }

                if (completion is not null)
                {
                    _ = DisposeCoreAsync(sendCompletionTask, completion);
                }

                return new(task);
            }

            /// <summary>Releases an idle reservation captured by session disposal.</summary>
            public void ReleaseIdleReservationForSessionDispose()
            {
                var shouldRelease = false;
                Task? sendCompletionTask = null;
                TaskCompletionSource<object?>? completion = null;
                lock (_gate)
                {
                    _disposed = true;
                    shouldRelease = !_sendStarted && _disposeTask is null;
                    if (shouldRelease)
                    {
                        _batch = null;
                        _disposeTask = Task.CompletedTask;
                    }
                    else if (_sendStarted && _disposeTask is null)
                    {
                        sendCompletionTask = _sendCompletionTask;
                        completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        _disposeTask = completion.Task;
                    }
                }

                if (shouldRelease)
                {
                    ReleaseReservation();
                    return;
                }

                if (completion is null)
                {
                    return;
                }

                _ = DisposeCoreAsync(sendCompletionTask, completion);
            }

            /// <summary>Releases a reservation for a prepared push that fails validation before being returned.</summary>
            public void ReleaseValidationFailureReservation()
            {
                lock (_gate)
                {
                    _disposed = true;
                    _batch = null;
                    _disposeTask = Task.CompletedTask;
                }

                ReleaseReservation();
            }

            /// <summary>Sends the batch to the loopback hub and releases the reservation exactly once.</summary>
            /// <param name="retainedBatch">The batch retained for this send.</param>
            /// <param name="sendCompletion">The active send completion signal.</param>
            /// <param name="cancellationToken">The caller cancellation token.</param>
            /// <returns>The remote synchronization result.</returns>
            /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
            /// <exception cref="InvalidOperationException">The hub returns a malformed response.</exception>
            /// <exception cref="SyncBatchValidationException">The hub result does not exactly match the pushed batch.</exception>
            private async Task<RemoteSyncResult> SendCoreAsync(
                SyncBatch retainedBatch,
                TaskCompletionSource<object?> sendCompletion,
                CancellationToken cancellationToken)
            {
                CancellationTokenSource? sendCancellation = null;
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var sendToken = lease.Token;
                    if (cancellationToken.CanBeCanceled)
                    {
                        sendCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lease.Token);
                        sendToken = sendCancellation.Token;
                    }

                    sendToken.ThrowIfCancellationRequested();
                    var serverResult = await validatorOptions.Hub.ApplyOperationsAsync(retainedBatch, validatorOptions.AuthenticatedClient, sendToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("The loopback hub returned no synchronization result.");

                    SyncBatchValidator.Validate(retainedBatch, serverResult.Result);
                    return serverResult.Result;
                }
                finally
                {
                    sendCancellation?.Dispose();
                    ClearBatchReference();
                    ReleaseReservation();
                    _ = sendCompletion.TrySetResult(null);
                }
            }

            /// <summary>Waits for any active send and releases the reservation exactly once.</summary>
            /// <param name="sendCompletionTask">The active send completion task.</param>
            /// <param name="completion">The disposal completion signal.</param>
            /// <returns>The disposal task.</returns>
            private async Task DisposeCoreAsync(Task? sendCompletionTask, TaskCompletionSource<object?> completion)
            {
                if (sendCompletionTask is not null)
                {
                    await sendCompletionTask.ConfigureAwait(false);
                }

                ClearBatchReference();
                ReleaseReservation();
                _ = completion.TrySetResult(null);
            }

            /// <summary>Clears the retained batch reference.</summary>
            private void ClearBatchReference()
            {
                lock (_gate)
                {
                    _batch = null;
                }
            }

            /// <summary>Releases this handle's bounded reservation exactly once.</summary>
            private void ReleaseReservation()
            {
                if (Interlocked.Exchange(ref _reservationReleased, 1) != 0)
                {
                    return;
                }

                session.UnregisterPreparedPush(this);
                lease.Dispose();
            }
        }
    }
}
