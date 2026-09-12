// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The schema version used by tests.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The second client sequence.</summary>
    private const int SecondClientSequence = 2;

    /// <summary>The third client sequence.</summary>
    private const int ThirdClientSequence = 3;

    /// <summary>The expected pending operation count after two commits.</summary>
    private const int ExpectedPendingOperationCount = 2;

    /// <summary>The expected leased operation count after two same-stream commits.</summary>
    private const int ExpectedLeasedOperationCount = 2;

    /// <summary>The first client sequence.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The lease operation limit used for multi-operation lease tests.</summary>
    private const int LeaseOperationLimit = 3;

    /// <summary>The small lease byte limit that still admits the first two operations.</summary>
    private const int TightLeaseBytes = 5;

    /// <summary>The default lease byte limit.</summary>
    private const long DefaultLeaseBytes = 128;

    /// <summary>The byte capacity used by bounded tests.</summary>
    private const long StoreByteCapacity = 12;

    /// <summary>The store byte capacity used for metadata accounting tests.</summary>
    private const int MetadataStoreByteCapacity = 512;

    /// <summary>The multiplier used to make metadata exceed the encoded byte capacity.</summary>
    private const int MetadataCapacityOverflowMultiplier = 2;

    /// <summary>The record capacity that admits one committed operation but not a lease record.</summary>
    private const int LeaseStoreRecordCapacity = 5;

    /// <summary>The record capacity that admits one committed operation but not a second operation.</summary>
    private const int OneCommitRecordCapacity = 6;

    /// <summary>The default store identity.</summary>
    private const string StoreIdentity = "client-alpha";

    /// <summary>The alternate store identity.</summary>
    private const string OtherStoreIdentity = "client-beta";

    /// <summary>The default operation payload text.</summary>
    private const string OperationPayloadText = "operation";

    /// <summary>The default snapshot payload text.</summary>
    private const string SnapshotPayloadText = "snapshot";

    /// <summary>The second at-most-once send attempt.</summary>
    private const int AtMostOnceSecondAttempt = 2;

    /// <summary>The compaction clock advance in days.</summary>
    private const int CompactionAdvanceDays = 2;

    /// <summary>The retry reason code.</summary>
    private const string RetryReasonCode = "OC.Retry";

    /// <summary>The remote cursor used by tests.</summary>
    private const string RemoteCursor = "cursor-1";

    /// <summary>The remote payload text used by tests.</summary>
    private const string RemotePayloadText = "remote";

    /// <summary>The accepted server version.</summary>
    private const string ServerVersion = "server-b";

    /// <summary>A test stream identifier.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>A second test stream identifier.</summary>
    private static readonly StreamId OtherStream = new("sensor/humidity");

    /// <summary>Verifies local commits recover pending work and replay original receipts exactly.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLocalOperationIsCommittedAndReplayed_ThenSnapshotPendingWorkAndOriginalReceiptRecover()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var firstOperation = CreateOperation(clientSequence: FirstClientSequence);
        var firstSnapshot = CreateSnapshotMutation(expectedRevision: 0);
        var first = await store.CommitLocalOperationAsync(firstOperation, firstSnapshot, CancellationToken.None);
        var secondOperation = CreateOperation(clientSequence: SecondClientSequence);
        var secondSnapshot = new SnapshotMutation(Stream, CreatePayload("second-snapshot"), FormatVersion: 1, ExpectedRevision: 1);
        var second = await store.CommitLocalOperationAsync(secondOperation, secondSnapshot, CancellationToken.None);

        var replay = await store.CommitLocalOperationAsync(firstOperation, firstSnapshot, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(replay.CommittedAtUtc).IsEqualTo(first.CommittedAtUtc);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(ThirdClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(firstOperation.OperationId);
        await Assert.That(recovery.PendingOperations[1].OperationId).IsEqualTo(secondOperation.OperationId);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(second.SnapshotRevision);
        await Assert.That(recovery.Snapshot?.State.Payload.ToArray().SequenceEqual(secondSnapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies local and remote transactions fail without partial side effects.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLocalOrRemoteCompareAndSwapIsStale_ThenStateIsUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(CreateOperation(clientSequence: FirstClientSequence), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);

        Func<Task> staleLocal = async () => await store.CommitLocalOperationAsync(
            CreateOperation(clientSequence: SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Func<Task> staleRemote = async () => await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch("wrong-cursor", RemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload(RemotePayloadText), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);

        await Assert.That(staleLocal).ThrowsExactly<InvalidOperationException>();
        await Assert.That(staleRemote).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies leases enforce stream order, ownership, result membership, and result batch correlation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeasedBatchIsCompleted_ThenStatusesAndPendingWorkReflectResultAtomically()
    {
        await using var store = await CreateInitializedStoreAsync();
        var first = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, "aa");
        var second = await CommitOperationAsync(store, Stream, clientSequence: SecondClientSequence, "bbb");
        _ = await CommitOperationAsync(store, OtherStream, clientSequence: FirstClientSequence, "zz");
        var batch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, TightLeaseBytes, TimeSpan.FromMinutes(1))));
        var mismatched = new RemoteSyncResult(
            Guid.NewGuid(),
            [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(second.OperationId, OperationResultKind.Accepted, null, ServerVersion)],
            null,
            null);

        Func<Task> wrongBatchId = async () => await store.ApplySyncResultAsync(batch.LeaseId, mismatched, CancellationToken.None);
        await Assert.That(batch.Operations.Count).IsEqualTo(ExpectedLeasedOperationCount);
        await Assert.That(batch.Operations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(batch.Operations[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(wrongBatchId).ThrowsExactly<SyncBatchValidationException>();

        var accepted = new RemoteSyncResult(
            batch.LeaseId,
            [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(second.OperationId, OperationResultKind.Accepted, null, ServerVersion)],
            null,
            null);
        await store.ApplySyncResultAsync(batch.LeaseId, accepted, CancellationToken.None);
        var firstStatus = await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        var secondStatus = await store.GetOperationStatusAsync(second.OperationId, CancellationToken.None);
        var after = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(after).IsNull();
    }

    /// <summary>Verifies lease expiry reclaims ownership while stale owners cannot mutate the batch.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpires_ThenNewOwnerCanReclaimAndOldOwnerCannotRenewReleaseOrBeginAttempts()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, OperationPayloadText);
        var first = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        clock.Advance(TimeSpan.FromMinutes(AtMostOnceSecondAttempt));

        var second = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        Func<Task> renewOld = async () => await store.RenewLeaseAsync(first.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        Func<Task> releaseOld = async () => await store.ReleaseLeaseAsync(first.LeaseId, CancellationToken.None);
        Func<Task> attemptOld = async () => await store.TryBeginRemoteAttemptAsync(first.LeaseId, operation.OperationId, 1, CancellationToken.None);

        await Assert.That(second.LeaseId).IsNotEqualTo(first.LeaseId);
        await Assert.That(second.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(renewOld).ThrowsExactly<InvalidOperationException>();
        await Assert.That(releaseOld).ThrowsExactly<InvalidOperationException>();
        await Assert.That(attemptOld).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies at-most-once barriers become terminal after the first attempt and are not leased again.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAtMostOnceAttemptBegins_ThenLaterAttemptsAreDeniedAndOperationIsNotResent()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(
            store,
            Stream,
            clientSequence: 1,
            OperationPayloadText,
            OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce });
        var batch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        var first = await store.TryBeginRemoteAttemptAsync(batch.LeaseId, operation.OperationId, 1, CancellationToken.None);
        var second = await store.TryBeginRemoteAttemptAsync(batch.LeaseId, operation.OperationId, AtMostOnceSecondAttempt, CancellationToken.None);
        await store.ReleaseLeaseAsync(batch.LeaseId, CancellationToken.None);
        var next = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(first.MaySend).IsTrue();
        await Assert.That(first.Attempt).IsEqualTo(1);
        await Assert.That(second.MaySend).IsFalse();
        await Assert.That(second.ReasonCode).IsEqualTo("OC.AtMostOnceAttemptAlreadyRecorded");
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(status?.Attempt).IsEqualTo(1);
        await Assert.That(next).IsNull();
    }

    /// <summary>Verifies retryable results and explicit retry state survive in the instance and are leaseable later.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateIsSaved_ThenStatusRetryStateAndLeaseEligibilityRemainVisible()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, OperationPayloadText);
        var batch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = new RemoteSyncResult(
            batch.LeaseId,
            [new(operation.OperationId, OperationResultKind.Retryable, RetryReasonCode, null)],
            null,
            TimeSpan.FromSeconds(1));
        var retryState = RetryState.Start(DateTimeOffset.UnixEpoch) with
        {
            DueUtc = DateTimeOffset.UnixEpoch.AddSeconds(1),
            PreviousDelay = TimeSpan.FromSeconds(1),
            TransientAttemptCount = 1,
        };

        await store.ApplySyncResultAsync(batch.LeaseId, result, CancellationToken.None);
        await store.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var recoveredRetry = await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        var retryBatch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ReasonCode).IsEqualTo(RetryReasonCode);
        await Assert.That(recoveredRetry).IsEqualTo(retryState);
        await Assert.That(retryBatch.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies store bounds count retained records and encoded payload bytes.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStoreCapacityWouldBeExceeded_ThenCommitIsRejectedWithoutMutation()
    {
        await using var recordBounded = await CreateInitializedStoreAsync(maximumRecordCount: OneCommitRecordCapacity);
        var recordSubscriptionId = await recordBounded.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = await recordBounded.CommitLocalOperationAsync(
            CreateOperation(clientSequence: 1, payloadText: "abcd"),
            CreateSnapshotMutation(expectedRevision: 0, payloadText: "efgh"),
            CancellationToken.None);

        Func<Task> tooManyRecords = async () => await recordBounded.CommitLocalOperationAsync(
            CreateOperation(clientSequence: SecondClientSequence, payloadText: "i"),
            CreateSnapshotMutation(expectedRevision: 1, payloadText: "j"),
            CancellationToken.None);
        await Assert.That(tooManyRecords).ThrowsExactly<QueueCapacityExceededException>();
        var recordRecovery = await recordBounded.RecoverStreamAsync(Stream, recordSubscriptionId, CancellationToken.None);
        await Assert.That(recordRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recordRecovery.PendingOperations.Count).IsEqualTo(1);

        await using var byteBounded = await CreateInitializedStoreAsync(maximumEncodedBytes: MetadataStoreByteCapacity);
        var byteSubscriptionId = await byteBounded.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = await byteBounded.CommitLocalOperationAsync(CreateOperation(clientSequence: 1, payloadText: "a"), CreateSnapshotMutation(expectedRevision: 0, payloadText: "b"), CancellationToken.None);
        Func<Task> tooManyBytes = async () => await byteBounded.CommitLocalOperationAsync(
            CreateOperation(clientSequence: SecondClientSequence, payloadText: "123456"),
            CreateSnapshotMutation(expectedRevision: 1, payloadText: "789"),
            CancellationToken.None);

        await Assert.That(tooManyBytes).ThrowsExactly<QueueCapacityExceededException>();
        var byteRecovery = await byteBounded.RecoverStreamAsync(Stream, byteSubscriptionId, CancellationToken.None);
        await Assert.That(byteRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(byteRecovery.PendingOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies stream identity records are counted before new stream registration mutates state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStreamRecordCapacityWouldBeExceeded_ThenSubscriptionRegistrationIsRejectedWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync(maximumRecordCount: ExpectedPendingOperationCount);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);

        Func<Task> tooManyStreams = async () => await store.GetOrCreateSubscriptionIdAsync(OtherStream, SubscriptionId.New(), CancellationToken.None);

        await Assert.That(tooManyStreams).ThrowsExactly<QueueCapacityExceededException>();
        var otherSubscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(otherSubscription.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies operation metadata contributes to encoded data accounting.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationMetadataWouldExceedEncodedCapacity_ThenCommitIsRejectedWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync(maximumEncodedBytes: MetadataStoreByteCapacity);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: FirstClientSequence, payloadText: string.Empty) with
        {
            BaseVersion = null,
            Metadata = CreateOverflowMetadata(),
        };

        Func<Task> tooMuchMetadata = async () => await store.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(expectedRevision: 0, payloadText: string.Empty),
            CancellationToken.None);

        await Assert.That(tooMuchMetadata).ThrowsExactly<QueueCapacityExceededException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies inbox deduplication keys are counted before remote apply mutates state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenInboxRecordCapacityWouldBeExceeded_ThenRemoteApplyIsRejectedWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync(maximumRecordCount: LeaseOperationLimit);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);

        Func<Task> tooManyInboxRecords = async () => await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload(string.Empty), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);

        await Assert.That(tooManyInboxRecords).ThrowsExactly<QueueCapacityExceededException>();
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies lease records are counted before pending operation ownership mutates state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseRecordCapacityWouldBeExceeded_ThenLeaseIsRejectedWithoutChangingOperationStatus()
    {
        await using var store = await CreateInitializedStoreAsync(maximumRecordCount: LeaseStoreRecordCapacity);
        var operation = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, OperationPayloadText);

        Func<Task> tooManyLeaseRecords = async () => _ = await LeaseSingleBatchAsync(
            store,
            new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(tooManyLeaseRecords).ThrowsExactly<QueueCapacityExceededException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.SavedLocally);
    }

    /// <summary>Verifies compaction removes only old terminal outbox records and leaves inbox deduplication intact.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenCompactionRuns_ThenOldTerminalOutboxRecordsAreRemovedButInboxDeduplicationRemains()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, null, ServerVersion)], null, null),
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);
        _ = await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload(RemotePayloadText), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));

        var compacted = await store.CompactAsync(new(null, clock.GetUtcNow().AddDays(-1), 0), CancellationToken.None);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(compacted.RecordsRemoved).IsEqualTo(FirstClientSequence);
        await Assert.That(status).IsNull();
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Verifies compaction reclaims the encoded bytes retained by compacted operation records.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenCompactionRemovesTerminalOperation_ThenEncodedOperationBytesAreReclaimed()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, clientSequence: FirstClientSequence, string.Empty);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, null, ServerVersion)], null, null),
            CancellationToken.None);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));

        var compacted = await store.CompactAsync(new(null, clock.GetUtcNow().AddDays(-1), 0), CancellationToken.None);

        await Assert.That(compacted.RecordsRemoved).IsEqualTo(1);
        await Assert.That(compacted.BytesReclaimed).IsGreaterThan(0);
    }

    /// <summary>Verifies initialization rejects unsupported claims and conflicting identities.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenInitializationRequestsUnsupportedFeaturesOrConflictingIdentity_ThenItFailsClosed()
    {
        await using var encrypted = new InMemoryLocalStoreAdapter();
        await using var initialized = await CreateInitializedStoreAsync();

        Func<Task> encryption = async () => await encrypted.InitializeAsync(new(StoreIdentity, SchemaVersion, true), CancellationToken.None);
        Func<Task> conflict = async () => await initialized.InitializeAsync(new(OtherStoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(encryption).ThrowsExactly<NotSupportedException>();
        await Assert.That(conflict).ThrowsExactly<InvalidOperationException>();
        await Assert.That((initialized.Capabilities & LocalStoreCapabilities.MultiProcessCoordination) == LocalStoreCapabilities.MultiProcessCoordination).IsFalse();
        await Assert.That((initialized.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) == LocalStoreCapabilities.AuthenticatedEncryptionAtRest).IsFalse();
    }

    /// <summary>Creates an initialized store.</summary>
    /// <param name="timeProvider">The time provider.</param>
    /// <param name="maximumRecordCount">The maximum record count.</param>
    /// <param name="maximumEncodedBytes">The maximum encoded bytes.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateInitializedStoreAsync(
        TimeProvider? timeProvider = null,
        int maximumRecordCount = 100,
        long maximumEncodedBytes = 4096)
    {
        var store = timeProvider is null
            ? new InMemoryLocalStoreAdapter(maximumRecordCount, maximumEncodedBytes)
            : new InMemoryLocalStoreAdapter(timeProvider, maximumRecordCount, maximumEncodedBytes, new RetentionOptions());
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        return store;
    }

    /// <summary>Creates a representative operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        long clientSequence,
        string payloadText = OperationPayloadText,
        OperationPolicy? policy = null) => new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = clientSequence,
            TimestampUtc = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
            BaseVersion = "server-a",
            Type = SyncOperationType.Update,
            Payload = CreatePayload(payloadText),
            Policy = policy ?? OperationPolicy.Default,
            Metadata = new Dictionary<string, string> { ["origin"] = "unit-test" },
        };

    /// <summary>Creates metadata that exceeds the metadata byte capacity test budget.</summary>
    /// <returns>The oversized metadata.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Dictionary<string, string> CreateOverflowMetadata() =>
        new Dictionary<string, string> { ["padding"] = new('x', MetadataStoreByteCapacity * MetadataCapacityOverflowMultiplier) };

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision, string payloadText = SnapshotPayloadText) =>
        new(Stream, CreatePayload(payloadText), FormatVersion: 1, expectedRevision);

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text}");

    /// <summary>Commits one operation for tests.</summary>
    /// <param name="store">The store.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The operation payload text.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The committed operation.</returns>
    private static async Task<SyncOperation> CommitOperationAsync(
        InMemoryLocalStoreAdapter store,
        StreamId streamId,
        long clientSequence,
        string payloadText,
        OperationPolicy? policy = null)
    {
        _ = await store.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence, payloadText, policy) with { StreamId = streamId };
        var snapshot = new SnapshotMutation(streamId, CreatePayload($"snapshot-{payloadText}"), FormatVersion: 1, ExpectedRevision: clientSequence - 1);
        _ = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        return operation;
    }

    /// <summary>Leases a single batch.</summary>
    /// <param name="store">The store.</param>
    /// <param name="request">The request.</param>
    /// <returns>The batch, if present.</returns>
    private static async Task<LeasedOperationBatch?> LeaseSingleBatchAsync(
        InMemoryLocalStoreAdapter store,
        OutboxLeaseRequest request)
    {
        LeasedOperationBatch? leased = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(request, CancellationToken.None))
        {
            leased = batch;
        }

        return leased;
    }

    /// <summary>Requires a leased batch to be present.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The required lease batch.</returns>
    /// <exception cref="InvalidOperationException">The batch was not present.</exception>
    private static LeasedOperationBatch RequireBatch(LeasedOperationBatch? batch) =>
        batch ?? throw new InvalidOperationException("Expected a leased operation batch.");

    /// <summary>Creates a remote batch.</summary>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="events">The events.</param>
    /// <returns>The batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(string? previousCursor, string nextCursor, IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, events);

    /// <summary>Creates a remote event.</summary>
    /// <param name="serverCursor">The server cursor.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(string serverCursor) =>
        new(Guid.NewGuid(), Stream, serverCursor, new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), null, CreatePayload(RemotePayloadText), new Dictionary<string, string>());

    /// <summary>Manual clock for lease tests.</summary>
    /// <param name="timestamp">The starting timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>The current timestamp.</summary>
        private DateTimeOffset _timestamp = timestamp;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _timestamp;

        /// <summary>Advances the current timestamp.</summary>
        /// <param name="duration">The duration.</param>
        public void Advance(TimeSpan duration) => _timestamp = _timestamp.Add(duration);
    }
}
