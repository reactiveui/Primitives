// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics participant wrappers for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Coordinator wrapper that captures a stream facade participant without registering it.</summary>
    /// <param name="inner">The real coordinator.</param>
    private sealed class CapturingStreamCoordinator(IOccasionallyConnectedStreamCoordinator inner) : IOccasionallyConnectedStreamCoordinator
    {
        /// <summary>Stores the captured participant.</summary>
        private IOccasionallyConnectedStreamParticipant? _participant;

        /// <summary>Gets the captured participant.</summary>
        internal IOccasionallyConnectedStreamParticipant Participant =>
            _participant ?? throw new InvalidOperationException("The stream participant was not captured.");

        /// <inheritdoc/>
        public IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant)
        {
            _participant = participant;
            return NoopRegistration.Instance;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.EnsureSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(
            StreamId streamId,
            long retainedBytes,
            CancellationToken cancellationToken) =>
            inner.EnterLocalCommitAsync(streamId, retainedBytes, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteLocalCommit(LocalCommitAdmission admission) => inner.CompleteLocalCommit(admission);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetCapacityReleaseGeneration(StreamId streamId) => inner.GetCapacityReleaseGeneration(streamId);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask WaitForCapacityReleaseAsync(
            StreamId streamId,
            long observedGeneration,
            long retainedBytes,
            CancellationToken cancellationToken) =>
            inner.WaitForCapacityReleaseAsync(streamId, observedGeneration, retainedBytes, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken) =>
            inner.StartStreamAsync(streamId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken) =>
            inner.StopStreamAsync(streamId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot) =>
            inner.RecordRecoveredQueueAggregate(streamId, snapshot);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null) =>
            inner.NotifyRecoveredLocalWorkReady(streamId, priority, notBeforeUtc);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordSavedLocalCommit(
            StreamId streamId,
            SyncOperation operation,
            QueueDiagnosticSnapshot snapshot,
            PublishReceipt receipt) =>
            inner.RecordSavedLocalCommit(streamId, operation, snapshot, receipt);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation) =>
            inner.NotifyLocalCommitReady(streamId, operation);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyCapacityReleased(StreamId streamId) => inner.NotifyCapacityReleased(streamId);
    }

    /// <summary>Participant wrapper that pauses after the typed facade has durably applied an upload result.</summary>
    /// <param name="inner">The captured typed stream participant.</param>
    private sealed class PausingApplySyncResultParticipant(IOccasionallyConnectedStreamParticipant inner) : IOccasionallyConnectedStreamParticipant
    {
        /// <summary>Stores the signal that releases the paused apply call.</summary>
        private readonly TaskCompletionSource _releaseApplyCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public StreamId StreamId => inner.StreamId;

        /// <summary>Gets the signal set after the inner participant applies the upload result.</summary>
        internal TaskCompletionSource ApplyCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken) =>
            inner.PrepareReceiveAsync(cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken) =>
            inner.CommitSerializedAsync(operation, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(
            SyncBatch batch,
            RemoteSyncResult result,
            CancellationToken cancellationToken)
        {
            ParticipantQueueTransitionResult transition;
            try
            {
                transition = await inner.ApplySyncResultAsync(batch, result, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _ = ApplyCompleted.TrySetException(exception);
                throw;
            }

            _ = ApplyCompleted.TrySetResult();
            await _releaseApplyCompletion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return transition;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantSnapshotRecoveryTransitionResult> RecoverSnapshotAsync(
            IRemoteSnapshotRecoverySession session,
            string? expiredCursor,
            SnapshotRecoveryLimits limits,
            CancellationToken cancellationToken) =>
            inner.RecoverSnapshotAsync(session, expiredCursor, limits, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken);

        /// <summary>Releases the paused apply call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseApply() => _ = _releaseApplyCompletion.TrySetResult();
    }

    /// <summary>Participant wrapper that pauses after the typed facade has durably dead-lettered an operation.</summary>
    /// <param name="inner">The captured typed stream participant.</param>
    private sealed class PausingDeadLetterParticipant(IOccasionallyConnectedStreamParticipant inner) : IOccasionallyConnectedStreamParticipant
    {
        /// <summary>Stores the signal that releases the paused dead-letter call.</summary>
        private readonly TaskCompletionSource _releaseDeadLetterCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public StreamId StreamId => inner.StreamId;

        /// <summary>Gets the signal set after the inner participant durably dead-letters the operation.</summary>
        internal TaskCompletionSource DeadLetterCompleted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken) =>
            inner.PrepareReceiveAsync(cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken) =>
            inner.CommitSerializedAsync(operation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(
            SyncBatch batch,
            RemoteSyncResult result,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(batch, result, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantSnapshotRecoveryTransitionResult> RecoverSnapshotAsync(
            IRemoteSnapshotRecoverySession session,
            string? expiredCursor,
            SnapshotRecoveryLimits limits,
            CancellationToken cancellationToken) =>
            inner.RecoverSnapshotAsync(session, expiredCursor, limits, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            CancellationToken cancellationToken)
        {
            var transition = await inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken).ConfigureAwait(false);
            _ = DeadLetterCompleted.TrySetResult();
            await _releaseDeadLetterCompletion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return transition;
        }

        /// <summary>Releases the paused dead-letter call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseDeadLetter() => _ = _releaseDeadLetterCompletion.TrySetResult();
    }
}
