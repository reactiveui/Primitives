// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Persists local occasionally-connected stream state using transactional workflow operations.</summary>
public interface ILocalStoreAdapter : IAsyncDisposable
{
    /// <summary>Gets the local store capabilities.</summary>
    LocalStoreCapabilities Capabilities { get; }

    /// <summary>Initializes the store for use by one synchronization engine.</summary>
    /// <param name="initialization">The initialization requirements.</param>
    /// <param name="cancellationToken">The token used to cancel initialization.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken);

    /// <summary>Gets or creates the durable subscription identifier assigned to a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">
    /// The preferred durable subscription identifier, or <see langword="null"/> to use the stored or generated
    /// identifier.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel identity lookup before persistence commits.</param>
    /// <returns>The durable subscription identifier stored for the initialized store partition and stream.</returns>
    /// <remarks>
    /// The mapping is scoped by the initialized <see cref="LocalStoreInitialization.StoreIdentity"/> and
    /// <paramref name="streamId"/>. If a mapping already exists, stores return it when
    /// <paramref name="preferredId"/> is omitted or matches the stored identifier. If an existing mapping differs from
    /// an explicit <paramref name="preferredId"/>, stores throw and leave the mapping unchanged. When no mapping
    /// exists, stores persist <paramref name="preferredId"/> when supplied, otherwise persist one newly generated
    /// <see cref="SubscriptionId"/>. Concurrent compatible calls must be linearizable and return the same committed
    /// identifier. Concurrent incompatible calls use first committed write wins semantics; the losing explicit mismatch
    /// throws. Successful initialization is required before lookup. Cancellation observed before commit leaves the
    /// mapping unchanged by that call; it must not remove an existing mapping or one committed by another caller.
    /// Cancellation requested after commit returns the committed identifier. Stores must reject a default <paramref name="streamId"/> and a
    /// <paramref name="preferredId"/> whose <see cref="SubscriptionId.Value"/> is <see cref="Guid.Empty"/>. Tenant
    /// hints must not be trusted for partitioning; the initialized store identity defines the partition.
    /// </remarks>
    ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken);

    /// <summary>Recovers a durable stream and its pending work.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="cancellationToken">The token used to cancel recovery.</param>
    /// <returns>The recovered stream state.</returns>
    /// <remarks>
    /// <see cref="RecoveredStream.PendingOperations"/> contains operations whose upload result is still unresolved and
    /// must continue lease correlation. <see cref="RecoveredStream.ReplayOperations"/> contains operations that must be
    /// replayed into the local projection during recovery. An accepted upload result removes an operation from pending;
    /// an authoritative receive completion from the same initialized client removes it from replay.
    /// </remarks>
    ValueTask<RecoveredStream> RecoverStreamAsync(StreamId streamId, SubscriptionId subscriptionId, CancellationToken cancellationToken);

    /// <summary>Atomically commits a local operation and optimistic snapshot mutation.</summary>
    /// <param name="operation">The local operation to commit.</param>
    /// <param name="snapshotMutation">The snapshot mutation to commit.</param>
    /// <param name="cancellationToken">The token used to cancel the commit.</param>
    /// <returns>The local commit result.</returns>
    /// <remarks>
    /// The operation and mutation must identify the same stream. The store verifies the next client sequence and
    /// <see cref="SnapshotMutation.ExpectedRevision"/> before atomically updating the sequence, outbox, and snapshot.
    /// Cancellation observed before commit leaves all three unchanged. After commit the method returns the committed
    /// result even if cancellation is subsequently requested, so callers never mistake a durable publish for a rollback.
    /// </remarks>
    ValueTask<LocalCommitResult> CommitLocalOperationAsync(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken);

    /// <summary>Leases pending operations for upload.</summary>
    /// <param name="request">The lease request.</param>
    /// <param name="cancellationToken">The token used to cancel lease enumeration.</param>
    /// <returns>The leased operation batches.</returns>
    IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(OutboxLeaseRequest request, CancellationToken cancellationToken);

    /// <summary>Applies a remote synchronization result to leased operations.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="result">The remote synchronization result.</param>
    /// <param name="cancellationToken">The token used to cancel result application.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// The lease must still own every referenced operation. Before changing any operation, the store validates the
    /// complete result against the exact leased batch, including duplicate, unknown, and omitted operation identifiers.
    /// Validation or ownership failure leaves all operation states unchanged.
    /// </remarks>
    ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken);

    /// <summary>Atomically applies upload results and the optimistic snapshots rebuilt after rejection.</summary>
    /// <param name="leaseId">The active lease owning every result operation.</param>
    /// <param name="result">The complete result matching the leased batch exactly.</param>
    /// <param name="snapshotMutations">One replacement for each stream losing rejected optimistic work.</param>
    /// <param name="cancellationToken">The token observed before transaction commit.</param>
    /// <returns>The snapshots committed with the operation results.</returns>
    /// <remarks>
    /// Stores capture the mutation collection within finite count and byte budgets before validating every lease,
    /// operation identifier, and snapshot revision. Missing, duplicate, or unrelated replacements fail atomically.
    /// Each replacement requires an existing authoritative checkpoint and increments the current snapshot revision.
    /// An omitted authoritative payload preserves that checkpoint; an explicitly supplied payload must match it.
    /// Upload results never advance the receive cursor or establish authoritative operation inclusion. Accepted
    /// operations stop uploading but remain replay-visible until authoritative receive inclusion. A rejection that
    /// contradicts existing receive inclusion fails closed. Cancellation before commit leaves state unchanged;
    /// cancellation after commit returns the committed snapshots. Status-only result application must reject a
    /// rejection requiring a snapshot rebuild when the stored authoritative checkpoint is known.
    /// </remarks>
    ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        CancellationToken cancellationToken);

    /// <summary>Atomically dead-letters one leased local operation and commits the rebuilt optimistic snapshot.</summary>
    /// <param name="leaseId">The active lease that owns <paramref name="operationId"/>.</param>
    /// <param name="operationId">The leased operation to move to the dead-letter set.</param>
    /// <param name="reasonCode">The stable local reason code.</param>
    /// <param name="snapshotMutation">The optimistic replacement snapshot after excluding the operation.</param>
    /// <param name="cancellationToken">The token observed before transaction commit.</param>
    /// <returns>The snapshot committed with the dead-letter transition.</returns>
    /// <remarks>
    /// Stores must validate the active lease owns the target operation, the target has not been authoritatively
    /// included, and the replacement snapshot matches the current stream revision. The operation payload remains
    /// retained for <see cref="RecoveredStream.DeadLetters"/> and is removed from pending and replay recovery. Only
    /// the target is removed from the lease; remaining lease members keep their ownership. Cancellation before commit
    /// leaves status, lease membership, and snapshot unchanged. Cancellation requested after commit returns the
    /// committed receipt.
    /// </remarks>
    ValueTask<LocalSnapshot> DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken);

    /// <summary>Returns remote event identifiers that have not yet been durably applied for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventIds">The candidate remote event identifiers in received order.</param>
    /// <param name="cancellationToken">The token used to cancel inbox lookup.</param>
    /// <returns>The candidate event identifiers that are not present in the durable inbox.</returns>
    /// <remarks>
    /// Engines use this method before projection so a mixed batch of duplicate and new events cannot compute a
    /// snapshot from already-applied events. Store adapters must provide read-your-writes visibility for this lookup.
    /// </remarks>
    ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
        StreamId streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken);

    /// <summary>Atomically applies a remote event batch and snapshot mutation.</summary>
    /// <param name="batch">The remote event batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation to commit.</param>
    /// <param name="cancellationToken">The token used to cancel result application.</param>
    /// <returns>The remote apply result.</returns>
    /// <remarks>
    /// The batch may contain both new and previously applied remote events. Stores deduplicate inbox identifiers inside
    /// the same transaction, report new and duplicate counts in the returned <see cref="RemoteApplyResult"/>, advance the
    /// server cursor, and replace the snapshot only when the cursor and <see cref="SnapshotMutation.ExpectedRevision"/>
    /// fence match durable state. Completion declarations are bounded before lookup allocation. A completion whose
    /// origin is the initialized local client may remove the matching local operation from recovery replay only when the
    /// batch carries an authoritative snapshot mutation; stores must reject a mismatched revision or cursor atomically
    /// with no inbox, cursor, snapshot, or completion-inclusion effects.
    /// </remarks>
    ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken);

    /// <summary>Gets the latest durable status recorded for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="cancellationToken">The token used to cancel status lookup.</param>
    /// <returns>The operation status, or <see langword="null"/> when the store has no record.</returns>
    /// <remarks>
    /// Engines use this method to complete awaiters after restart when an operation became terminal before the current
    /// process observed its state transition.
    /// </remarks>
    ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken);

    /// <summary>Gets durable retry state recorded for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="cancellationToken">The token used to cancel retry lookup.</param>
    /// <returns>The retry state, or <see langword="null"/> when no retry state is recorded.</returns>
    ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken);

    /// <summary>Records a durable attempt barrier before any network I/O for an operation.</summary>
    /// <param name="leaseId">The lease identifier that currently owns the operation.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nextAttempt">The attempt number about to be sent.</param>
    /// <param name="cancellationToken">The token used to cancel barrier recording.</param>
    /// <returns>The barrier decision.</returns>
    /// <remarks>
    /// Engines must call this method before sending a leased operation to the remote peer. The store must verify that
    /// <paramref name="leaseId"/> still owns <paramref name="operationId"/> before committing the barrier. Once the
    /// barrier commits, an ambiguous transport outcome is restart-visible. Stores must deny a later send for
    /// at-most-once operations whose previous barrier became ambiguous, so ambiguity never turns into an implicit retry.
    /// </remarks>
    ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        CancellationToken cancellationToken);

    /// <summary>Saves durable retry state for an operation after a retryable decision or ambiguous attempt.</summary>
    /// <param name="operationId">The operation identifier that owns the retry state.</param>
    /// <param name="retryState">The retry state to save.</param>
    /// <param name="cancellationToken">The token used to cancel retry state persistence.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken);

    /// <summary>Extends an outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="extension">The lease extension duration.</param>
    /// <param name="cancellationToken">The token used to cancel renewal.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken);

    /// <summary>Releases an outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="cancellationToken">The token used to cancel release.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken);

    /// <summary>Compacts terminal records and stale storage data.</summary>
    /// <param name="request">The compaction request.</param>
    /// <param name="cancellationToken">The token used to cancel compaction.</param>
    /// <returns>The compaction result.</returns>
    ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken);
}
