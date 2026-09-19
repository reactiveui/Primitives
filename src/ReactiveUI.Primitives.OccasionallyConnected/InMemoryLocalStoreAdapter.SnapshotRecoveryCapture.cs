// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Captures process-local snapshot recovery state.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Captures a bounded recovery view synchronously behind the ValueTask interface.</summary>
    /// <param name="request">The capture request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded capture.</returns>
    public ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryAsync(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken) =>
        new(CaptureSnapshotRecoveryCore(request, cancellationToken));

    /// <summary>Captures a bounded recovery view.</summary>
    /// <param name="request">The capture request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded capture.</returns>
    /// <exception cref="InvalidOperationException">The requested stream is inconsistent or quarantined.</exception>
    /// <exception cref="OperationCanceledException">Cancellation was requested.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private LocalSnapshotRecoveryCapture CaptureSnapshotRecoveryCore(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken)
    {
        ValidateSnapshotRecoveryCaptureRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var stream = GetStream(request.StreamId);
            if (stream.SubscriptionId != request.SubscriptionId)
            {
                throw new InvalidOperationException("The recovered subscription identity does not match the requested identity.");
            }

            ThrowIfStreamQuarantined(stream);
            var logicalBytes = GetSnapshotRecoveryCaptureHeaderLogicalBytes(request.StreamId, stream.ServerCursor, stream.Snapshot, request.Limits);
            long visibleOperationCount = 0;
            List<SyncOperation> pending = [];
            List<SyncOperation> replay = [];
            foreach (var pair in _operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                logicalBytes = TryAdmitSnapshotRecoveryCaptureOperation(
                    request.StreamId,
                    pair.Value,
                    request.Limits,
                    logicalBytes,
                    ref visibleOperationCount,
                    pending,
                    replay);
            }

            pending.Sort(OperationSequenceComparison);
            replay.Sort(OperationSequenceComparison);
            var capture = new LocalSnapshotRecoveryCapture
            {
                StreamId = request.StreamId,
                SubscriptionId = request.SubscriptionId,
                ServerCursor = stream.ServerCursor,
                Snapshot = stream.Snapshot,
                NextClientSequence = stream.NextClientSequence,
                PendingOperations = pending,
                ReplayOperations = replay,
            };
            cancellationToken.ThrowIfCancellationRequested();
            return capture;
        }
    }

    /// <summary>Accounts and admits one visible operation without allocating beyond limits.</summary>
    /// <param name="streamId">The requested stream identifier.</param>
    /// <param name="record">The operation record.</param>
    /// <param name="limits">The capture limits.</param>
    /// <param name="logicalBytes">The current logical byte count.</param>
    /// <param name="visibleOperationCount">The visible operation count.</param>
    /// <param name="pending">The pending operation output.</param>
    /// <param name="replay">The replay operation output.</param>
    /// <returns>The updated logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private long TryAdmitSnapshotRecoveryCaptureOperation(
        StreamId streamId,
        OperationRecord record,
        SnapshotRecoveryLimits limits,
        long logicalBytes,
        ref long visibleOperationCount,
        List<SyncOperation> pending,
        List<SyncOperation> replay)
    {
        if (record.Operation.StreamId != streamId || record.Status.State == SyncOperationState.DeadLettered)
        {
            return logicalBytes;
        }

        var included = _includedOperations.Contains(record.Operation.OperationId);
        var pendingVisible = ShouldRecoverPendingOperation(record);
        var replayVisible = ShouldRecoverReplayOperation(record, included);
        if (!pendingVisible && !replayVisible)
        {
            return logicalBytes;
        }

        visibleOperationCount++;
        ThrowIfSnapshotRecoveryCapacityExceeded(
            visibleOperationCount,
            limits.MaximumPendingOperations,
            nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        logicalBytes = checked(logicalBytes + GetSnapshotRecoveryOperationLogicalBytes(record.Operation, limits));
        ThrowIfSnapshotRecoveryCapacityExceeded(
            logicalBytes,
            limits.MaximumLogicalBytes,
            nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        if (pendingVisible)
        {
            pending.Add(record.Operation);
        }

        if (replayVisible)
        {
            replay.Add(record.Operation);
        }

        return logicalBytes;
    }
}
