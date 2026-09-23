// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Queue diagnostic helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Records a durable local commit and wakes upload scheduling for already admitted work.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="snapshot">The durable queue aggregate after the commit.</param>
    /// <param name="receipt">The durable receipt produced by the local commit.</param>
    public void RecordSavedLocalCommit(
        StreamId streamId,
        SyncOperation operation,
        QueueDiagnosticSnapshot snapshot,
        PublishReceipt receipt)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(receipt);
        ValidateQueueSnapshot(snapshot);
        RecordOperationPublished();
        RecordRecoveredQueueAggregate(streamId, snapshot);
        _operationStates.Publish(new(receipt.OperationId, streamId, receipt.State, Attempt: 0, receipt.SavedAtUtc, ReasonCode: null));
        NotifyAdmittedLocalCommitReady(streamId, operation);
    }

    /// <inheritdoc/>
    public void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot)
    {
        ValidateQueueSnapshot(snapshot);
        var delta = default(QueueDiagnosticSnapshot);
        lock (_gate)
        {
            if (_disposed || !_participants.ContainsKey(streamId))
            {
                return;
            }

            if (_queueDiagnosticSnapshots.TryGetValue(streamId, out var current) && current.Revision >= snapshot.Revision)
            {
                return;
            }

            delta = new(
                snapshot.PendingOperations - current.PendingOperations,
                snapshot.PendingBytes - current.PendingBytes,
                snapshot.Revision);
            _queueDiagnosticSnapshots[streamId] = snapshot;
        }

        RecordQueueDiagnostics(delta.PendingOperations, delta.PendingBytes);
        PublishQueueSyncState(streamId);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null) => NotifyLocalWorkReady(streamId, priority, notBeforeUtc);

    /// <summary>Calculates the retained bytes for one raw operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The retained byte count.</returns>
    internal static long GetOperationRetainedBytes(SyncOperation operation)
    {
        var retainedBytes = RetainedEnvelopeOverheadBytes
            + (RetainedGuidBytes * RetainedOperationGuidFieldCount)
            + sizeof(long)
            + sizeof(int)
            + GetRetainedTextBytes(operation.StreamId.Value)
            + GetRetainedTextBytes(operation.BaseVersion)
            + GetRetainedTextBytes(operation.Type.ToString())
            + GetRetainedTextBytes(operation.Policy.DeliveryGuarantee.ToString())
            + GetRetainedTextBytes(operation.Policy.Durability.ToString())
            + GetRetainedTextBytes(operation.Policy.ConflictPolicy.ToString())
            + GetPayloadRetainedBytes(operation.Payload)
            + GetMetadataRetainedBytes(operation.Metadata);
        return Math.Max(UnknownProducerRetainedBytes, retainedBytes);
    }

    /// <summary>Validates retained producer bytes.</summary>
    /// <param name="retainedBytes">The retained bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException">The retained byte count is not positive.</exception>
    private static void ValidateRetainedBytes(long retainedBytes)
    {
        if (retainedBytes > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(retainedBytes), retainedBytes, "Retained bytes must be positive.");
    }

    /// <summary>Records a participant-owned queue snapshot when the participant supplied one.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="snapshot">The participant-owned queue snapshot.</param>
    /// <returns>Whether the participant supplied queue authority.</returns>
    private bool RecordParticipantQueueSnapshot(StreamId streamId, QueueDiagnosticSnapshot? snapshot)
    {
        if (snapshot is not { } value)
        {
            return false;
        }

        RecordRecoveredQueueAggregate(streamId, value);
        return true;
    }

    /// <summary>Nudges upload scheduling after an already admitted durable local commit.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operation">The committed operation.</param>
    private void NotifyAdmittedLocalCommitReady(StreamId streamId, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        lock (_gate)
        {
            if (_disposed || _admissionState is EngineAdmissionState.Disposed)
            {
                return;
            }

            if (!_participants.ContainsKey(streamId))
            {
                return;
            }

            NotifyLocalWorkReadyLocked(streamId, operation.Policy.Priority);
        }
    }

    /// <summary>Nudges upload scheduling after already durable local work is known to be pending.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="priority">The bounded scheduler priority.</param>
    /// <param name="notBeforeUtc">The optional UTC time before which the head is known not to be leaseable.</param>
    private void NotifyLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null)
    {
        lock (_gate)
        {
            if (_disposed
                || _admissionState is EngineAdmissionState.Disposed
                || !_participants.ContainsKey(streamId))
            {
                return;
            }

            // An active attempt already owns upload continuation. Lazy participant recovery can
            // observe that attempt's lease expiry; it must not turn its own lease into retry backoff.
            if (_uploadHeads.TryGetValue(streamId, out var head) && head.Inflight)
            {
                return;
            }

            NotifyLocalWorkReadyLocked(streamId, priority, notBeforeUtc);
        }
    }

    /// <summary>Nudges upload scheduling while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="priority">The bounded scheduler priority.</param>
    /// <param name="notBeforeUtc">The optional UTC time before which the head is known not to be leaseable.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void NotifyLocalWorkReadyLocked(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null) =>
        ScheduleOrDeferStreamLocked(streamId, priority, notBeforeUtc);

    /// <summary>Records a bounded queue aggregate delta for one stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operationDelta">The pending operation delta.</param>
    /// <param name="byteDelta">The pending byte delta.</param>
    private void RecordQueueDelta(StreamId streamId, long operationDelta, long byteDelta)
    {
        var delta = default(QueueDiagnosticSnapshot);
        lock (_gate)
        {
            if (!_participants.ContainsKey(streamId))
            {
                return;
            }

            if (!_queueDiagnosticSnapshots.TryGetValue(streamId, out var current))
            {
                current = default;
            }

            var nextOperations = Math.Max(0L, current.PendingOperations + operationDelta);
            var nextBytes = Math.Max(0L, current.PendingBytes + byteDelta);
            delta = new(nextOperations - current.PendingOperations, nextBytes - current.PendingBytes, current.Revision);
            _queueDiagnosticSnapshots[streamId] = current with { PendingOperations = nextOperations, PendingBytes = nextBytes };
        }

        RecordQueueDiagnostics(delta.PendingOperations, delta.PendingBytes);
        PublishQueueSyncState(streamId);
    }
}
