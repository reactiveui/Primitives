// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="ILocalStoreAdapter"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class ILocalStoreAdapterExtensions
{
    /// <summary>Convenience overloads for a local store adapter.</summary>
    /// <param name="adapter">The local store adapter.</param>
    extension(ILocalStoreAdapter adapter)
    {
        /// <summary>Initializes the store for use by one synchronization engine.</summary>
        /// <param name="initialization">The initialization requirements.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization) =>
            adapter.InitializeAsync(initialization, CancellationToken.None);

        /// <summary>Gets or creates the durable subscription identifier assigned to a stream.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="preferredId">
        /// The preferred durable subscription identifier, or <see langword="null"/> to use the stored or generated
        /// identifier.
        /// </param>
        /// <returns>The durable subscription identifier stored for the initialized store partition and stream.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId) =>
            adapter.GetOrCreateSubscriptionIdAsync(streamId, preferredId, CancellationToken.None);

        /// <summary>Recovers a durable stream and its pending work.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="subscriptionId">The durable subscription identifier.</param>
        /// <returns>The recovered stream state.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(StreamId streamId, SubscriptionId subscriptionId) =>
            adapter.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None);

        /// <summary>Atomically commits a local operation and optimistic snapshot mutation.</summary>
        /// <param name="operation">The local operation to commit.</param>
        /// <param name="snapshotMutation">The snapshot mutation to commit.</param>
        /// <returns>The local commit result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation) =>
            adapter.CommitLocalOperationAsync(operation, snapshotMutation, CancellationToken.None);

        /// <summary>Leases pending operations for upload.</summary>
        /// <param name="request">The lease request.</param>
        /// <returns>The leased operation batches.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(OutboxLeaseRequest request) =>
            adapter.LeasePendingOperationsAsync(request, CancellationToken.None);

        /// <summary>Applies a remote synchronization result to leased operations.</summary>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="result">The remote synchronization result.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result) =>
            adapter.ApplySyncResultAsync(leaseId, result, CancellationToken.None);

        /// <summary>Returns remote event identifiers that have not yet been durably applied for a stream.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="eventIds">The candidate remote event identifiers in received order.</param>
        /// <returns>The candidate event identifiers that are not present in the durable inbox.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds) =>
            adapter.GetUnappliedEventIdsAsync(streamId, eventIds, CancellationToken.None);

        /// <summary>Atomically applies a remote event batch and snapshot mutation.</summary>
        /// <param name="batch">The remote event batch.</param>
        /// <param name="snapshotMutation">The snapshot mutation to commit.</param>
        /// <returns>The remote apply result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation) =>
            adapter.ApplyRemoteBatchAsync(batch, snapshotMutation, CancellationToken.None);

        /// <summary>Gets the latest durable status recorded for an operation.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <returns>The operation status, or <see langword="null"/> when the store has no record.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId) =>
            adapter.GetOperationStatusAsync(operationId, CancellationToken.None);

        /// <summary>Gets durable retry state recorded for an operation.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <returns>The retry state, or <see langword="null"/> when no retry state is recorded.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId) =>
            adapter.GetRetryStateAsync(operationId, CancellationToken.None);

        /// <summary>Records a durable attempt barrier before network I/O for an operation.</summary>
        /// <param name="leaseId">The lease identifier that currently owns the operation.</param>
        /// <param name="operationId">The operation identifier.</param>
        /// <param name="nextAttempt">The attempt number about to be sent.</param>
        /// <returns>The barrier decision.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(Guid leaseId, OperationId operationId, int nextAttempt) =>
            adapter.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, CancellationToken.None);

        /// <summary>Saves durable retry state for an operation.</summary>
        /// <param name="operationId">The operation identifier that owns the retry state.</param>
        /// <param name="retryState">The retry state to save.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState) =>
            adapter.SaveRetryStateAsync(operationId, retryState, CancellationToken.None);

        /// <summary>Extends an outbox lease.</summary>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="extension">The lease extension duration.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension) =>
            adapter.RenewLeaseAsync(leaseId, extension, CancellationToken.None);

        /// <summary>Releases an outbox lease.</summary>
        /// <param name="leaseId">The lease identifier.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId) =>
            adapter.ReleaseLeaseAsync(leaseId, CancellationToken.None);

        /// <summary>Compacts terminal records and stale storage data.</summary>
        /// <param name="request">The compaction request.</param>
        /// <returns>The compaction result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request) =>
            adapter.CompactAsync(request, CancellationToken.None);
    }
}
