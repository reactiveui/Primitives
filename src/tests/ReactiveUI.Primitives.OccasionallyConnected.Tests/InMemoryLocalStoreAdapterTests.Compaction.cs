// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies compaction preserves required local intent and honors the retained budget.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The record capacity used by explicit compaction retention fixtures.</summary>
    private const int CompactionStoreRecordCapacity = 100;

    /// <summary>The encoded byte capacity used by explicit compaction retention fixtures.</summary>
    private const int CompactionStoreEncodedByteCapacity = 4096;

    /// <summary>The terminal outbox retention in days for explicit compaction fixtures.</summary>
    private const int CompactionOutboxRetentionDays = 1;

    /// <summary>Verifies compaction orders multiple terminal records while preserving the last snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionRemovesMultipleEligibleOperations()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var first = await CommitOperationAsync(store, Stream, 1, "first");
        await SetServerResultAsync(store, first, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromTicks(1));
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        await SetServerResultAsync(store, second, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None)).IsNull();
        await Assert.That(await store.GetOperationStatusAsync(second.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies an already satisfied byte target does not delete retained terminal history.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionDoesNotDeleteWhenAlreadyBelowTarget()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        await SetServerResultAsync(store, operation, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), long.MaxValue), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNotNull();
    }

    /// <summary>Verifies conflicts protect the stream's retained operation history.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionPreservesHistoryWhileStreamHasUnresolvedConflict()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var first = await CommitOperationAsync(store, Stream, 1, "first");
        await SetServerResultAsync(store, first, OperationResultKind.Accepted);
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        await SetServerResultAsync(store, second, OperationResultKind.Conflict);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 1), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None)).IsNotNull();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies a zero target reclaims eligible records while preserving the current snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ZeroCompactionTargetPreservesSnapshotAndSequence()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        await SetServerResultAsync(store, operation, OperationResultKind.Rejected);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        var after = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(after.Snapshot).IsEqualTo(before.Snapshot);
        await Assert.That(after.NextClientSequence).IsEqualTo(before.NextClientSequence);
        var again = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(again.RecordsRemoved).IsEqualTo(0);
    }

    /// <summary>Verifies compaction preserves an included operation while its upload lease remains active.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionPreservesIncludedOperationWhileUploadLeaseIsActive()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateCompactionBoundStoreAsync(clock);
        var operation = await CommitOperationAsync(
            store,
            Stream,
            FirstClientSequence,
            OperationPayloadText,
            OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce });
        var retry = RetryState.Start(clock.GetUtcNow());
        await store.SaveRetryStateAsync(operation.OperationId, retry, CancellationToken.None);
        var leaseDuration = TimeSpan.FromDays(CompactionAdvanceDays + 1);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, leaseDuration)));
        var queuedStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var retryDuringLease = await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        var attempt = await store.TryBeginRemoteAttemptAsync(
            lease.LeaseId,
            operation.OperationId,
            FirstClientSequence,
            CancellationToken.None);
        var includedRecovery = await ApplyAuthoritativeInclusionAsync(store, operation);
        var uploadingStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(queuedStatus?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(retryDuringLease).IsEqualTo(retry);
        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(uploadingStatus?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(uploadingStatus?.Attempt).IsEqualTo(FirstClientSequence);
        await Assert.That(includedRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(includedRecovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(includedRecovery.ReplayOperations.Count).IsEqualTo(0);

        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);

        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var retryAfterCompaction = await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        var activeLeaseRetry = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(status).IsNotNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(status?.Attempt).IsEqualTo(FirstClientSequence);
        await Assert.That(retryAfterCompaction).IsEqualTo(retry);
        await Assert.That(activeLeaseRetry).IsNull();

        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var afterRelease = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        var releasedStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(afterRelease.RecordsRemoved).IsEqualTo(0);
        await Assert.That(releasedStatus).IsNotNull();
        await Assert.That(releasedStatus?.State).IsEqualTo(SyncOperationState.Uploading);
    }

    /// <summary>Verifies compaction preserves an included queued operation after lease ownership ends.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionPreservesIncludedQueuedOperationAfterLeaseRelease()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateCompactionBoundStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        var leaseDuration = TimeSpan.FromDays(CompactionOutboxRetentionDays);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, leaseDuration)));
        var includedRecovery = await ApplyAuthoritativeInclusionAsync(store, operation);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var queuedStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(queuedStatus?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(includedRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(includedRecovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(includedRecovery.ReplayOperations.Count).IsEqualTo(0);

        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);

        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(status).IsNotNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies compaction preserves included conflict state until reconciliation resolves the stream.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionPreservesIncludedConflictOperation()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateCompactionBoundStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        await SetServerResultAsync(store, operation, OperationResultKind.Conflict);
        var includedRecovery = await ApplyAuthoritativeInclusionAsync(store, operation);
        var conflictStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(conflictStatus?.State).IsEqualTo(SyncOperationState.Conflict);
        await Assert.That(includedRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(includedRecovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(includedRecovery.ReplayOperations.Count).IsEqualTo(0);

        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);

        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(status).IsNotNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Conflict);
    }

    /// <summary>Creates a bound in-memory store with explicit compaction retention.</summary>
    /// <param name="timeProvider">The time provider.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateCompactionBoundStoreAsync(TimeProvider timeProvider)
    {
        var retention = new RetentionOptions { OutboxTerminalRetention = TimeSpan.FromDays(CompactionOutboxRetentionDays) };
        var store = new InMemoryLocalStoreAdapter(
            timeProvider,
            CompactionStoreRecordCapacity,
            CompactionStoreEncodedByteCapacity,
            retention);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        return store;
    }

    /// <summary>Applies an authoritative receive inclusion for a local operation and recovers the stream.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operation">The included operation.</param>
    /// <returns>The recovered stream after inclusion is recorded.</returns>
    private static async Task<RecoveredStream> ApplyAuthoritativeInclusionAsync(
        InMemoryLocalStoreAdapter store,
        SyncOperation operation)
    {
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };
        _ = await store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        return await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
    }

    /// <summary>Applies a correlated server outcome to one pending operation.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="kind">The outcome.</param>
    /// <returns>The asynchronous test setup.</returns>
    private static async Task SetServerResultAsync(InMemoryLocalStoreAdapter store, SyncOperation operation, OperationResultKind kind)
    {
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(operation.StreamId, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(lease.LeaseId, new(lease.LeaseId, [new(operation.OperationId, kind, null, null)], null, null), CancellationToken.None);
    }
}
