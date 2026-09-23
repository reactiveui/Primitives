// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Durable result status-read failure tests for <see cref="OccasionallyConnectedStream{TState, TInput}"/>.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The injected status-read failure text that must stay out of public diagnostics.</summary>
    private const string StatusReadFailureMessage = "The durable status read failed.";

    /// <summary>Verifies a failed status read does not undo a durable dead-letter and reports the observation fault.</summary>
    /// <param name="throwOnRead">Whether the status read throws rather than returning no status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DeadLetterOperationAsyncPreservesCommitWhenStatusReadFails(bool throwOnRead)
    {
        await using var durableStore = await CreateInitializedStoreAsync();
        var store = new FaultingOperationStatusStore(durableStore);
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var statusSubscription = stream.OperationStates.Subscribe(statuses);
        using var faultSubscription = stream.Faults.Subscribe(faults);
        var lease = await LeaseStreamResultAsync(durableStore, 1);
        store.FailNextStatusRead = true;
        store.ThrowOnStatusRead = throwOnRead;

        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        var transition = await participant.DeadLetterOperationAsync(
            lease.LeaseId,
            receipt.OperationId,
            "cannot-upload",
            CancellationToken.None);
        scheduler.RunAll();
        var durableStatus = await durableStore.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);

        await Assert.That(transition.QueueSnapshot?.PendingOperations).IsEqualTo(0);
        await Assert.That(durableStatus?.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(statuses.Values).IsEmpty();
        await Assert.That(faults.Values.Count).IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.OperationStatus");
        await Assert.That(faults.Values[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(faults.Values[0].Exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(faults.Values[0].Exception?.Message).Contains(throwOnRead ? nameof(IOException) : nameof(InvalidOperationException));
        await Assert.That(faults.Values[0].Exception?.Message.Contains(StatusReadFailureMessage) ?? false).IsFalse();
    }

    /// <summary>Verifies a failed status read does not undo accepted upload reconciliation.</summary>
    /// <param name="throwOnRead">Whether the status read throws rather than returning no status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ApplySyncResultAsyncPreservesCommitWhenStatusReadFails(bool throwOnRead)
    {
        await using var durableStore = await CreateInitializedStoreAsync();
        var store = new FaultingOperationStatusStore(durableStore);
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var statusSubscription = stream.OperationStates.Subscribe(statuses);
        using var faultSubscription = stream.Faults.Subscribe(faults);
        var lease = await LeaseStreamResultAsync(durableStore, 1);
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(receipt.OperationId, OperationResultKind.Accepted, null, null)],
            null,
            null);
        store.FailNextStatusRead = true;
        store.ThrowOnStatusRead = throwOnRead;

        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        var transition = await participant.ApplySyncResultAsync(new(lease.LeaseId, lease.Operations), result, CancellationToken.None);
        scheduler.RunAll();
        var durableStatus = await durableStore.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);

        await Assert.That(transition.QueueSnapshot?.PendingOperations).IsEqualTo(0);
        await Assert.That(durableStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(statuses.Values).IsEmpty();
        await Assert.That(faults.Values.Count).IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.OperationStatus");
        await Assert.That(faults.Values[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(faults.Values[0].Exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(faults.Values[0].Exception?.Message).Contains(throwOnRead ? nameof(IOException) : nameof(InvalidOperationException));
        await Assert.That(faults.Values[0].Exception?.Message.Contains(StatusReadFailureMessage) ?? false).IsFalse();
    }

    /// <summary>Forwards durable work while injecting one post-commit operation-status read failure.</summary>
    /// <param name="inner">The real durable store.</param>
    private sealed class FaultingOperationStatusStore(SqliteLocalStoreAdapter inner) : ILocalStoreAdapter
    {
        /// <inheritdoc />
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <summary>Gets or sets whether the next status read fails.</summary>
        public bool FailNextStatusRead { get; set; }

        /// <summary>Gets or sets whether the failure throws instead of returning no status.</summary>
        public bool ThrowOnStatusRead { get; set; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            inner.InitializeAsync(initialization, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(StreamId streamId, SubscriptionId? preferredId, CancellationToken cancellationToken) =>
            inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(StreamId streamId, SubscriptionId subscriptionId, CancellationToken cancellationToken) =>
            inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(SyncOperation operation, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(OutboxLeaseRequest request, CancellationToken cancellationToken) =>
            inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(Guid leaseId, OperationId operationId, string reasonCode, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(StreamId streamId, IReadOnlyList<Guid> eventIds, CancellationToken cancellationToken) =>
            inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            if (!FailNextStatusRead)
            {
                return inner.GetOperationStatusAsync(operationId, cancellationToken);
            }

            FailNextStatusRead = false;
            return ThrowOnStatusRead
                ? ValueTask.FromException<SyncOperationStatus?>(new IOException(StatusReadFailureMessage))
                : ValueTask.FromResult<SyncOperationStatus?>(null);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(Guid leaseId, OperationId operationId, int nextAttempt, CancellationToken cancellationToken) =>
            inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);
    }
}
