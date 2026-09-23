// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates context-owned lifecycle, identity, and scheduling hooks for a stream facade.</summary>
internal interface IOccasionallyConnectedStreamCoordinator
{
    /// <summary>Registers a stream participant without performing I/O.</summary>
    /// <param name="participant">The stream participant.</param>
    /// <returns>The registration handle.</returns>
    IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant);

    /// <summary>Gets or creates the durable subscription identity after context-owned store initialization.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="preferredId">The optional caller-supplied subscription identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable subscription identity.</returns>
    ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken);

    /// <summary>Admits a new durable local commit against the current context lifecycle.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="retainedBytes">The bytes retained while the caller waits for admission.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The admission reservation.</returns>
    ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(StreamId streamId, long retainedBytes, CancellationToken cancellationToken);

    /// <summary>Completes one previously admitted durable local commit.</summary>
    /// <param name="admission">The admission reservation to release.</param>
    void CompleteLocalCommit(LocalCommitAdmission admission);

    /// <summary>Gets the capacity-release generation before an admission attempt.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The current capacity-release generation.</returns>
    long GetCapacityReleaseGeneration(StreamId streamId);

    /// <summary>Waits for a capacity-release generation newer than the observed value.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="observedGeneration">The generation captured before admission was attempted.</param>
    /// <param name="retainedBytes">The bytes retained while waiting.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The wait operation.</returns>
    ValueTask WaitForCapacityReleaseAsync(
        StreamId streamId,
        long observedGeneration,
        long retainedBytes,
        CancellationToken cancellationToken);

    /// <summary>Starts context-owned stream work without implying remote connectivity.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The start operation.</returns>
    ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken);

    /// <summary>Stops remote work for one stream without closing local publication admission or completing public observers.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stop operation.</returns>
    ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken);

    /// <summary>Records the bounded queue aggregate recovered for one stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="snapshot">The recovered queue aggregate.</param>
    void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot);

    /// <summary>Nudges upload scheduling after durable pending work is recovered.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="priority">The bounded head priority recovered from pending work.</param>
    /// <param name="notBeforeUtc">The optional UTC time before which the recovered head is known not to be leaseable.</param>
    void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null);

    /// <summary>Records and nudges an already saved typed local commit.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="snapshot">The bounded queue aggregate after the commit.</param>
    /// <param name="receipt">The durable receipt produced by the local commit.</param>
    void RecordSavedLocalCommit(
        StreamId streamId,
        SyncOperation operation,
        QueueDiagnosticSnapshot snapshot,
        PublishReceipt receipt);

    /// <summary>Nudges context-owned scheduling after a durable local commit.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operation">The committed operation.</param>
    void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation);

    /// <summary>Notifies blocked local producers after durable capacity is released.</summary>
    /// <param name="streamId">The stream identity.</param>
    void NotifyCapacityReleased(StreamId streamId);
}
