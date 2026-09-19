// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The authoritative checkpoint text used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryAuthoritativeText = "snapshot-recovery-authoritative";

    /// <summary>The optimistic recovery text used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryOptimisticText = "snapshot-recovery-optimistic";

    /// <summary>The recovery frontier cursor used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryCursor = "snapshot-recovery-cursor";

    /// <summary>The record capacity that admits one committed leased operation but not a second commit.</summary>
    private const int LeaseReclaimObservationRecordCapacity = 8;

    /// <summary>The clock advance that expires a one-minute snapshot recovery test lease.</summary>
    private const int SnapshotRecoveryLeaseExpiryAdvanceMinutes = 2;

    /// <summary>The payload text used for recovery capacity admission probes.</summary>
    private const string SnapshotRecoveryCapacityProbeText = "capacity-proof";

    /// <summary>The disposition count that exceeds the default planning record reservation.</summary>
    private const int SnapshotRecoveryRecordReservationOverflowCount = 25;

    /// <summary>The disposition count that exceeds the bounded planning byte reservation.</summary>
    private const int SnapshotRecoveryByteReservationOverflowCount = 7;

    /// <summary>Verifies the in-memory adapter advertises and implements process-local atomic snapshot recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task SnapshotRecoveryCapabilityIsBackedByInterface()
    {
        await using var store = await CreateInitializedStoreAsync();
        object candidate = store;

        await Assert.That(candidate is ILocalSnapshotRecoveryStore).IsTrue();
        await Assert.That((store.Capabilities & LocalStoreCapabilities.AtomicSnapshotRecovery) != 0).IsTrue();
    }

    /// <summary>Verifies included and unknown dispositions commit as one local transaction.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncCommitsCheckpointAndPreservesUnknownWorkAtomically()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var included = await CommitOperationAsync(store, Stream, FirstClientSequence, "included");
        var preserved = await CommitOperationAsync(store, Stream, SecondClientSequence, "preserved");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                IncludedSnapshotDisposition(included.OperationId, OperationResultKind.Accepted),
                UnknownSnapshotDisposition(preserved.OperationId),
            ]);

        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
        await Assert.That(SamePayload(recovered.Snapshot?.State, CreatePayload(SnapshotRecoveryOptimisticText))).IsTrue();
        await Assert.That(SamePayload(recovered.Snapshot?.AuthoritativeState, CreatePayload(SnapshotRecoveryAuthoritativeText))).IsTrue();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(preserved.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(preserved.OperationId);
        await Assert.That((await store.GetOperationStatusAsync(included.OperationId, CancellationToken.None))?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies local recovery rejects exact disposition mismatch without changing local state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsDispositionMismatchWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "first");
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(first.OperationId)]);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies stale recovery fences fail without changing cursor, snapshot, or pending work.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsStaleRevisionWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "pending");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: 0,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies terminal rejected dispositions remove pending work and record a durable terminal status.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncCommitsTerminalRejectedDisposition()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var rejected = await CommitOperationAsync(store, Stream, FirstClientSequence, "rejected");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [RejectedSnapshotDisposition(rejected.OperationId)]);

        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(rejected.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.TerminalOperationCount).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Rejected);
        await Assert.That(status?.ReasonCode).IsEqualTo("OC.Rejected");
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies an active lease blocks recovery without stealing ownership.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsActiveLeaseWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "leased");
        _ = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)))).IsNull();
    }

    /// <summary>Verifies failed recovery does not secretly reclaim an expired lease before rejecting.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsStaleRevisionWithoutReclaimingExpiredLease()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock, LeaseReclaimObservationRecordCapacity);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "expired-lease");
        _ = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        clock.Advance(TimeSpan.FromMinutes(SnapshotRecoveryLeaseExpiryAdvanceMinutes));
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: 0,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();
        Func<Task> commitSecond = () => store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence, "still-full"),
            CreateSnapshotMutation(FirstClientSequence, "still-full"),
            CancellationToken.None).AsTask();

        await Assert.That(commitSecond).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        await Assert.That(commitSecond).ThrowsExactly<QueueCapacityExceededException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies successful recovery reclaims expired lease capacity before later lease APIs run.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncReclaimsExpiredLeaseCapacityDuringSuccessfulCommit()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock, LeaseReclaimObservationRecordCapacity);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "expired-lease");
        _ = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var leasedStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(SnapshotRecoveryLeaseExpiryAdvanceMinutes));
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);
        Func<Task> commitSecond = () => store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence, SnapshotRecoveryCapacityProbeText),
            CreateSnapshotMutation(FirstClientSequence, SnapshotRecoveryCapacityProbeText),
            CancellationToken.None).AsTask();

        await Assert.That(commitSecond).ThrowsExactly<QueueCapacityExceededException>();
        var recoveryResult = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var second = await store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence, SnapshotRecoveryCapacityProbeText),
            CreateSnapshotMutation(recoveryResult.Snapshot.Revision, SnapshotRecoveryCapacityProbeText),
            CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var recoveredStatus = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(recoveryResult.Snapshot.Revision).IsEqualTo(SecondClientSequence);
        await Assert.That(second.ClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(recoveredStatus).IsEqualTo(leasedStatus);
    }

    /// <summary>Verifies recovery capacity rejection preserves the previous process-local state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsCapacityOverflowWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync(maximumEncodedBytes: MetadataStoreByteCapacity);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, string.Empty);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)])
            with
            {
                OptimisticState = CreatePayload(new('x', MetadataStoreByteCapacity * MetadataCapacityOverflowMultiplier)),
            };

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<QueueCapacityExceededException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies accepted replay-only work can be proven included without replaying after recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncIncludesAcceptedReplayOnlyOperationWithoutReplayingIt()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "replay-only");
        await CompleteUploadAsync(store, operation.OperationId, OperationResultKind.Accepted);
        var before = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Accepted)]);

        await Assert.That(before.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(before.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(before.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(0);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies replay-only work requires accepted proof before recovery can remove it from replay.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsUnprovenReplayOnlyOperationWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "replay-only-unknown");
        await CompleteUploadAsync(store, operation.OperationId, OperationResultKind.Accepted);
        var before = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        await Assert.That(before.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(before.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(before.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies conflict proof preserves pending ownership while still preventing replay after restart.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncCountsConflictProofAsPreservedWork()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "conflict");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Conflict)]);

        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(0);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Conflict);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies checkpoint identity mismatches are rejected before store mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsCheckpointIdentityMismatches()
    {
        await using var streamMismatchStore = await CreateInitializedStoreAsync();
        var streamSubscriptionId = await streamMismatchStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var streamOperation = await CommitOperationAsync(streamMismatchStore, Stream, FirstClientSequence, "checkpoint-stream");
        var streamMismatch = CreateSnapshotRecoveryMutation(
            streamSubscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(streamOperation.OperationId)]);
        streamMismatch = streamMismatch with { Checkpoint = streamMismatch.Checkpoint with { StreamId = OtherStream } };

        await using var subscriptionMismatchStore = await CreateInitializedStoreAsync();
        var localSubscriptionId = await subscriptionMismatchStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var wrongSubscriptionId = SubscriptionId.New();
        var subscriptionMismatch = CreateSnapshotRecoveryMutation(localSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                SubscriptionId = wrongSubscriptionId,
                Checkpoint = CreateSnapshotRecoveryCheckpoint(wrongSubscriptionId),
            };

        await Assert.That(RecoveryApplyAction(streamMismatchStore, streamMismatch)).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(subscriptionMismatchStore, subscriptionMismatch)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies checkpoint cursor and server version are required.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsIncompleteCheckpointFrontier()
    {
        await using var missingCursorStore = await CreateInitializedStoreAsync();
        var cursorSubscriptionId = await missingCursorStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var missingCursor = CreateSnapshotRecoveryMutation(cursorSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                Checkpoint = CreateSnapshotRecoveryCheckpoint(cursorSubscriptionId) with { FrontierCursor = string.Empty },
            };

        await using var missingVersionStore = await CreateInitializedStoreAsync();
        var versionSubscriptionId = await missingVersionStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var missingVersion = CreateSnapshotRecoveryMutation(versionSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                Checkpoint = CreateSnapshotRecoveryCheckpoint(versionSubscriptionId) with { ServerVersion = " " },
            };

        await Assert.That(RecoveryApplyAction(missingCursorStore, missingCursor)).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(missingVersionStore, missingVersion)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed recovery revisions and snapshot format versions are rejected.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsMalformedSnapshotRecoveryVersions()
    {
        await using var revisionStore = await CreateInitializedStoreAsync();
        var revisionSubscriptionId = await revisionStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var negativeRevision = CreateSnapshotRecoveryMutation(revisionSubscriptionId, expectedRevision: -1, expectedCursor: null, []);

        await using var mutationFormatStore = await CreateInitializedStoreAsync();
        var mutationFormatSubscriptionId = await mutationFormatStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var mutationFormat = CreateSnapshotRecoveryMutation(mutationFormatSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                SnapshotFormatVersion = 0,
            };

        await using var checkpointFormatStore = await CreateInitializedStoreAsync();
        var checkpointFormatSubscriptionId = await checkpointFormatStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var checkpointFormat = CreateSnapshotRecoveryMutation(checkpointFormatSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                Checkpoint = CreateSnapshotRecoveryCheckpoint(checkpointFormatSubscriptionId) with { SnapshotFormatVersion = 0 },
            };

        await Assert.That(RecoveryApplyAction(revisionStore, negativeRevision)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(RecoveryApplyAction(mutationFormatStore, mutationFormat)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(RecoveryApplyAction(checkpointFormatStore, checkpointFormat)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies snapshot recovery payload envelopes require complete metadata.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsIncompletePayloadMetadata()
    {
        await using var optimisticStore = await CreateInitializedStoreAsync();
        var optimisticSubscriptionId = await optimisticStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var optimistic = CreateSnapshotRecoveryMutation(optimisticSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                OptimisticState = CreateIncompleteSnapshotRecoveryPayload(),
            };

        await using var checkpointStore = await CreateInitializedStoreAsync();
        var checkpointSubscriptionId = await checkpointStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var checkpoint = CreateSnapshotRecoveryMutation(checkpointSubscriptionId, expectedRevision: 0, expectedCursor: null, [])
            with
            {
                Checkpoint = CreateSnapshotRecoveryCheckpoint(checkpointSubscriptionId) with
                {
                    ClientState = CreateIncompleteSnapshotRecoveryPayload(),
                },
            };

        await Assert.That(RecoveryApplyAction(optimisticStore, optimistic)).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(checkpointStore, checkpoint)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed disposition proof is rejected before store mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsMalformedDispositionProof()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "proof");
        var unsupported = new SnapshotOperationDisposition { OperationId = operation.OperationId, Kind = (SnapshotOperationDispositionKind)int.MaxValue };
        var unknownWithResult = UnknownSnapshotDisposition(operation.OperationId) with
        {
            Result = new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion),
        };
        var includedRejected = IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Rejected);
        var terminalAccepted = new SnapshotOperationDisposition
        {
            OperationId = operation.OperationId,
            Kind = SnapshotOperationDispositionKind.TerminalRejected,
            Result = new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion),
        };
        var missingResult = new SnapshotOperationDisposition { OperationId = operation.OperationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted };
        var mismatchedResult = new SnapshotOperationDisposition
        {
            OperationId = operation.OperationId,
            Kind = SnapshotOperationDispositionKind.IncludedAccepted,
            Result = new(OperationId.New(), OperationResultKind.Accepted, null, ServerVersion),
        };

        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [unsupported]))).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [unknownWithResult]))).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [includedRejected]))).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [terminalAccepted]))).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [missingResult]))).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(store, CreateSnapshotRecoveryMutation(subscriptionId, FirstClientSequence, null, [mismatchedResult]))).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies duplicate and out-of-order dispositions are rejected.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsDuplicateAndOutOfOrderDispositions()
    {
        await using var duplicateStore = await CreateInitializedStoreAsync();
        var duplicateSubscriptionId = await duplicateStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var duplicateFirst = await CommitOperationAsync(duplicateStore, Stream, FirstClientSequence, "duplicate-first");
        _ = await CommitOperationAsync(duplicateStore, Stream, SecondClientSequence, "duplicate-second");
        var duplicate = CreateSnapshotRecoveryMutation(
            duplicateSubscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(duplicateFirst.OperationId), UnknownSnapshotDisposition(duplicateFirst.OperationId)]);

        await using var orderStore = await CreateInitializedStoreAsync();
        var orderSubscriptionId = await orderStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var orderFirst = await CommitOperationAsync(orderStore, Stream, FirstClientSequence, "order-first");
        var orderSecond = await CommitOperationAsync(orderStore, Stream, SecondClientSequence, "order-second");
        var outOfOrder = CreateSnapshotRecoveryMutation(
            orderSubscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(orderSecond.OperationId), UnknownSnapshotDisposition(orderFirst.OperationId)]);

        await Assert.That(RecoveryApplyAction(duplicateStore, duplicate)).ThrowsExactly<ArgumentException>();
        await Assert.That(RecoveryApplyAction(orderStore, outOfOrder)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies cursor fences use exact optional cursor equality.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsMismatchedCursorFences()
    {
        await using var missingLocalCursorStore = await CreateInitializedStoreAsync();
        var missingLocalCursorSubscriptionId = await missingLocalCursorStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var missingLocalCursor = CreateSnapshotRecoveryMutation(
            missingLocalCursorSubscriptionId,
            expectedRevision: 0,
            expectedCursor: RemoteCursor,
            []);

        await using var mismatchedCursorStore = await CreateInitializedStoreAsync();
        var mismatchedCursorSubscriptionId = await mismatchedCursorStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var initialRecovery = CreateSnapshotRecoveryMutation(
            mismatchedCursorSubscriptionId,
            expectedRevision: 0,
            expectedCursor: null,
            []);

        await Assert.That(RecoveryApplyAction(missingLocalCursorStore, missingLocalCursor)).ThrowsExactly<InvalidOperationException>();
        _ = await RequireSnapshotRecoveryStore(mismatchedCursorStore).ApplySnapshotRecoveryAsync(initialRecovery, CancellationToken.None);
        var unexpectedNullCursor = CreateSnapshotRecoveryMutation(
            mismatchedCursorSubscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            []);
        await Assert.That(RecoveryApplyAction(mismatchedCursorStore, unexpectedNullCursor)).ThrowsExactly<InvalidOperationException>();

        var mismatchedCursor = CreateSnapshotRecoveryMutation(
            mismatchedCursorSubscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: RemoteCursor,
            []);

        await Assert.That(RecoveryApplyAction(mismatchedCursorStore, mismatchedCursor)).ThrowsExactly<InvalidOperationException>();
        var matchingCursor = CreateSnapshotRecoveryMutation(
            mismatchedCursorSubscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: SnapshotRecoveryCursor,
            []);

        var result = await RequireSnapshotRecoveryStore(mismatchedCursorStore).ApplySnapshotRecoveryAsync(matchingCursor, CancellationToken.None);
        await Assert.That(result.Snapshot.Revision).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies expired lease reclamation de-duplicates a multi-operation lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncReclaimsOneExpiredLeaseForMultipleOperations()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "lease-first");
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "lease-second");
        _ = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, ExpectedLeasedOperationCount, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        clock.Advance(TimeSpan.FromMinutes(SnapshotRecoveryLeaseExpiryAdvanceMinutes));
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(first.OperationId), UnknownSnapshotDisposition(second.OperationId)]);

        _ = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, ExpectedLeasedOperationCount, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(lease.Operations.Count).IsEqualTo(ExpectedLeasedOperationCount);
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(lease.Operations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies recovery planning reservations fail before mutation when request bounds are exceeded.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsPlanningReservationOverflowWithoutMutation()
    {
        await using var recordStore = await CreateInitializedStoreAsync();
        var recordSubscriptionId = await recordStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var recordOverflow = CreateSnapshotRecoveryMutation(
            recordSubscriptionId,
            expectedRevision: 0,
            expectedCursor: null,
            CreateUnknownSnapshotDispositions(SnapshotRecoveryRecordReservationOverflowCount));

        await using var byteStore = await CreateInitializedStoreAsync(maximumEncodedBytes: MetadataStoreByteCapacity);
        var byteSubscriptionId = await byteStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var byteOverflow = CreateSnapshotRecoveryMutation(
            byteSubscriptionId,
            expectedRevision: 0,
            expectedCursor: null,
            CreateUnknownSnapshotDispositions(SnapshotRecoveryByteReservationOverflowCount));

        await Assert.That(RecoveryApplyAction(recordStore, recordOverflow)).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(RecoveryApplyAction(byteStore, byteOverflow)).ThrowsExactly<QueueCapacityExceededException>();
        var recovered = await recordStore.RecoverStreamAsync(Stream, recordSubscriptionId, CancellationToken.None);
        await Assert.That(recovered.Snapshot).IsNull();
    }

    /// <summary>Verifies quarantined streams reject recovery without changing the marker.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsQuarantinedStreamWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "quarantined");
        var quarantine = await store.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var stored = await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(stored?.QuarantineId).IsEqualTo(quarantine.Record.QuarantineId);
    }

    /// <summary>Creates a local recovery mutation bound to current in-memory fixtures.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="expectedCursor">The expected cursor.</param>
    /// <param name="dispositions">The exact operation dispositions.</param>
    /// <returns>The local recovery mutation.</returns>
    private static LocalSnapshotRecoveryMutation CreateSnapshotRecoveryMutation(
        SubscriptionId subscriptionId,
        long expectedRevision,
        string? expectedCursor,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            ExpectedRevision = expectedRevision,
            ExpectedPreviousCursor = expectedCursor,
            Checkpoint = new()
            {
                StreamId = Stream,
                SubscriptionId = subscriptionId,
                FrontierCursor = SnapshotRecoveryCursor,
                ServerVersion = ServerVersion,
                SnapshotFormatVersion = SchemaVersion,
                ClientState = CreatePayload(SnapshotRecoveryAuthoritativeText),
                ObservedAtUtc = DateTimeOffset.UnixEpoch,
            },
            OptimisticState = CreatePayload(SnapshotRecoveryOptimisticText),
            SnapshotFormatVersion = SchemaVersion,
            OperationDispositions = dispositions,
        };

    /// <summary>Creates a checkpoint bound to the shared stream fixture.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The checkpoint.</returns>
    private static RemoteSnapshotCheckpoint CreateSnapshotRecoveryCheckpoint(SubscriptionId subscriptionId) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            FrontierCursor = SnapshotRecoveryCursor,
            ServerVersion = ServerVersion,
            SnapshotFormatVersion = SchemaVersion,
            ClientState = CreatePayload(SnapshotRecoveryAuthoritativeText),
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };

    /// <summary>Creates disposition placeholders for reservation-boundary tests.</summary>
    /// <param name="count">The disposition count.</param>
    /// <returns>The disposition list.</returns>
    private static SnapshotOperationDisposition[] CreateUnknownSnapshotDispositions(int count)
    {
        var dispositions = new SnapshotOperationDisposition[count];
        for (var index = 0; index < dispositions.Length; index++)
        {
            dispositions[index] = UnknownSnapshotDisposition(OperationId.New());
        }

        return dispositions;
    }

    /// <summary>Creates an included snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="kind">The server result kind.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition IncludedSnapshotDisposition(OperationId operationId, OperationResultKind kind) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted, Result = new(operationId, kind, null, ServerVersion) };

    /// <summary>Creates a terminal rejected snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition RejectedSnapshotDisposition(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.TerminalRejected, Result = new(operationId, OperationResultKind.Rejected, "OC.Rejected", ServerVersion) };

    /// <summary>Creates an unknown snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition UnknownSnapshotDisposition(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown };

    /// <summary>Completes one leased operation upload without recording authoritative receive inclusion.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="kind">The result kind.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task CompleteUploadAsync(
        InMemoryLocalStoreAdapter store,
        OperationId operationId,
        OperationResultKind kind)
    {
        var batch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = new RemoteSyncResult(
            batch.LeaseId,
            [new(operationId, kind, null, ServerVersion)],
            null,
            null);
        await store.ApplySyncResultAsync(batch.LeaseId, result, CancellationToken.None);
    }

    /// <summary>Compares optional payload envelopes by content.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The second payload.</param>
    /// <returns>Whether the payloads have matching content.</returns>
    private static bool SamePayload(PayloadEnvelope? left, PayloadEnvelope right) =>
        left is not null
        && string.Equals(left.ContractId, right.ContractId, StringComparison.Ordinal)
        && left.SchemaVersion == right.SchemaVersion
        && string.Equals(left.ContentType, right.ContentType, StringComparison.Ordinal)
        && CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(left.PayloadHash), Encoding.UTF8.GetBytes(right.PayloadHash))
        && left.Payload.ToArray().SequenceEqual(right.Payload.ToArray());

    /// <summary>Creates a payload envelope that fails recovery metadata validation.</summary>
    /// <returns>The malformed payload envelope.</returns>
    private static PayloadEnvelope CreateIncompleteSnapshotRecoveryPayload() =>
        new(string.Empty, SchemaVersion, "application/json", "invalid"u8.ToArray(), "hash-invalid");

    /// <summary>Creates an asynchronous recovery action for exception assertions.</summary>
    /// <param name="store">The store.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <returns>The asynchronous action.</returns>
    private static Func<Task> RecoveryApplyAction(
        InMemoryLocalStoreAdapter store,
        LocalSnapshotRecoveryMutation mutation) =>
        () => RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

    /// <summary>Requires the process-local snapshot recovery interface without depending on the adapter declaration.</summary>
    /// <param name="candidate">The candidate store.</param>
    /// <returns>The snapshot recovery store.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not implemented the interface.</exception>
    private static ILocalSnapshotRecoveryStore RequireSnapshotRecoveryStore(object candidate) =>
        candidate as ILocalSnapshotRecoveryStore
        ?? throw new InvalidOperationException("Expected the in-memory local store to implement snapshot recovery.");
}
