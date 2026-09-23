// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Exposes serialized stream-owned mutation paths to the context engine.</summary>
internal interface IOccasionallyConnectedStreamParticipant
{
    /// <summary>Gets the participant stream identity.</summary>
    StreamId StreamId { get; }

    /// <summary>Prepares durable remote receive metadata for this participant.</summary>
    /// <param name="cancellationToken">The cancellation token observed before subscription starts.</param>
    /// <returns>The subscription metadata, or <see langword="null"/> when this stream has no remote subscription.</returns>
    ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken);

    /// <summary>Commits a caller-supplied serialized operation through the stream mutation lane.</summary>
    /// <param name="operation">The serialized operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken);

    /// <summary>Reconciles a remote upload result through the stream mutation lane.</summary>
    /// <param name="batch">The exact synchronization batch sent to the remote peer.</param>
    /// <param name="result">The remote result to apply durably.</param>
    /// <param name="cancellationToken">The cancellation token observed before local reconciliation commits.</param>
    /// <returns>The durable participant queue transition result.</returns>
    ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(
        SyncBatch batch,
        RemoteSyncResult result,
        CancellationToken cancellationToken);

    /// <summary>Applies a remote event batch through the stream mutation lane.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <param name="cancellationToken">The cancellation token observed before the local receive transaction commits.</param>
    /// <returns>The durable remote apply receipt and participant queue transition result.</returns>
    ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, CancellationToken cancellationToken);

    /// <summary>Moves one oversized leased operation to the durable dead-letter set through the stream mutation lane.</summary>
    /// <param name="leaseId">The lease that owns the operation.</param>
    /// <param name="operationId">The operation to dead-letter.</param>
    /// <param name="reasonCode">The stable local reason code.</param>
    /// <param name="cancellationToken">The cancellation token observed before local reconciliation commits.</param>
    /// <returns>The durable participant queue transition result.</returns>
    ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        CancellationToken cancellationToken);
}
