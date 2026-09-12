// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies malformed requests cannot mutate store state.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies complete lease enumeration yields one batch and safely permits retrying an ordinary operation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task LeaseEnumerationCompletesAndRecoveryRemainsStreamScoped()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, OperationPayloadText);
        _ = await CommitOperationAsync(store, OtherStream, 1, OperationPayloadText);
        var count = 0;
        await foreach (var lease in store.LeasePendingOperationsAsync(new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)), CancellationToken.None))
        {
            count++;
            _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
            await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None);
            await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        }

        await Assert.That(count).IsEqualTo(1);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)))).Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies an operation larger than total record capacity cannot consume snapshot state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task OperationExceedingTotalRecordCapacityLeavesSnapshotEmpty()
    {
        await using var store = await CreateInitializedStoreAsync(maximumRecordCount: ExpectedPendingOperationCount);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        Func<Task> commit = async () => _ = await store.CommitLocalOperationAsync(CreateOperation(1), CreateSnapshotMutation(0), CancellationToken.None);
        await Assert.That(commit).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That((await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None)).Snapshot).IsNull();
    }

    /// <summary>Verifies invalid lease and compaction requests leave the current operation available.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InvalidLeaseRequestsPreservePendingWork()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        Func<Task> emptyLease = async () => await store.ReleaseLeaseAsync(Guid.Empty, CancellationToken.None);
        Func<Task> zeroAttempt = async () => _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 0, CancellationToken.None);
        Func<Task> zeroRenewal = async () => await store.RenewLeaseAsync(lease.LeaseId, TimeSpan.Zero, CancellationToken.None);
        Func<Task> infiniteRenewal = async () => await store.RenewLeaseAsync(lease.LeaseId, TimeSpan.MaxValue, CancellationToken.None);
        Func<Task> zeroDuration = async () => _ = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.Zero));
        Func<Task> negativeTarget = async () => _ = await store.CompactAsync(new(Stream, DateTimeOffset.MaxValue, -1), CancellationToken.None);
        await Assert.That(emptyLease).ThrowsExactly<ArgumentException>();
        await Assert.That(zeroAttempt).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(zeroRenewal).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(infiniteRenewal).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(zeroDuration).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(negativeTarget).ThrowsExactly<ArgumentOutOfRangeException>();
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        await Assert.That(RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)))).Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies lease timestamp overflow does not consume ownership or change status.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task LeaseTimestampOverflowPreservesPendingOperation()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, OperationPayloadText);
        Func<Task> overflow = async () => _ = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromTicks(1)));
        await Assert.That(overflow).ThrowsExactly<ArgumentException>();
        await Assert.That((await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None))?.State).IsEqualTo(SyncOperationState.SavedLocally);
    }

    /// <summary>Verifies unknown state and invalid event identities cannot create records.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UnknownRecordsAndEmptyIdentitiesAreRejected()
    {
        await using var store = await CreateInitializedStoreAsync();
        Func<Task> emptyIdentity = async () => await store.InitializeAsync(new(" ", SchemaVersion, false), CancellationToken.None);
        Func<Task> unknownStream = async () => _ = await store.RecoverStreamAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        Func<Task> emptyEvent = async () => _ = await store.GetUnappliedEventIdsAsync(Stream, [Guid.Empty], CancellationToken.None);
        Func<Task> emptyOperation = async () => _ = await store.GetOperationStatusAsync(default, CancellationToken.None);
        var retry = new RetryState(DateTimeOffset.UnixEpoch, null, null, 0, RetryAuthenticationState.None, null);
        Func<Task> missingOperation = async () => await store.SaveRetryStateAsync(OperationId.New(), retry, CancellationToken.None);
        await Assert.That(emptyIdentity).ThrowsExactly<ArgumentException>();
        await Assert.That(unknownStream).ThrowsExactly<InvalidOperationException>();
        await Assert.That(emptyEvent).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyOperation).ThrowsExactly<ArgumentException>();
        await Assert.That(missingOperation).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies stale projection revisions are rejected for local and remote commits.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task StaleSnapshotProjectionCannotReplaceNewerState()
    {
        await using var store = await CreateInitializedStoreAsync();
        _ = await CommitOperationAsync(store, Stream, 1, OperationPayloadText);
        Func<Task> local = async () => _ = await store.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var remote = CreateRemoteBatch(null, RemoteCursor, [CreateRemoteEvent(RemoteCursor)]);
        Func<Task> receive = async () => _ = await store.ApplyRemoteBatchAsync(remote, CreateSnapshotMutation(0), CancellationToken.None);
        await Assert.That(local).ThrowsExactly<InvalidOperationException>();
        await Assert.That(receive).ThrowsExactly<InvalidOperationException>();
        Func<Task> outOfOrder = async () => _ = await store.CommitLocalOperationAsync(CreateOperation(ThirdClientSequence), CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That(outOfOrder).ThrowsExactly<InvalidOperationException>();
        await Assert.That((await store.GetUnappliedEventIdsAsync(Stream, [remote.Events[0].EventId], CancellationToken.None)).Count).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate receive events are rejected even when the cursor chain is valid.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task PreviouslyAppliedEventCannotBeCommittedAgain()
    {
        await using var store = await CreateInitializedStoreAsync();
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);
        _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(null, RemoteCursor, [remoteEvent]), CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> duplicate = async () => _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(RemoteCursor, "cursor-2", [remoteEvent]), CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That(duplicate).ThrowsExactly<InvalidOperationException>();
        await Assert.That((await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None)).Count).IsEqualTo(0);
    }
}
