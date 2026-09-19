// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery replay-union tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The expected operation count for mixed replay union assertions.</summary>
    private const int ExpectedReplayUnionOperationCount = 2;

    /// <summary>The expected operation count for tiny advertised frontier rejection assertions.</summary>
    private const int ExpectedTinyAdvertisedFrontierOperationCount = 3;

    /// <summary>Verifies recovery accepts pending work followed by accepted replay-only proof.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncAcceptsPendingThenAcceptedReplayOnlyUnion()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "union-replay");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var pending = await CommitOperationAsync(store, Stream, SecondClientSequence, "union-pending");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(pending.OperationId),
                IncludedSnapshotDisposition(replayOnly.OperationId, OperationResultKind.Accepted),
            ]);

        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pending.OperationId);
    }

    /// <summary>Verifies recovery rejects replay proof before pending proof without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsReplayBeforePendingUnionOrder()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "order-replay");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var pending = await CommitOperationAsync(store, Stream, SecondClientSequence, "order-pending");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                IncludedSnapshotDisposition(replayOnly.OperationId, OperationResultKind.Accepted),
                UnknownSnapshotDisposition(pending.OperationId),
            ]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnly.OperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(pending.OperationId);
    }

    /// <summary>Verifies recovery rejects a missing accepted replay-only proof without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsMissingReplayOnlyUnionMember()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "missing-replay");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var pending = await CommitOperationAsync(store, Stream, SecondClientSequence, "missing-pending");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(pending.OperationId)]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnly.OperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(pending.OperationId);
    }

    /// <summary>Verifies replay-only conflict proof fails closed and leaves replay visible.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsReplayOnlyConflictProofWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "replay-conflict");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(replayOnly.OperationId, OperationResultKind.Conflict)]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(replayOnly.OperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnly.OperationId);
    }

    /// <summary>Verifies replay-only rejected proof fails closed and leaves replay visible.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsReplayOnlyRejectedProofWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "replay-rejected");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [RejectedSnapshotDisposition(replayOnly.OperationId)]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(replayOnly.OperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnly.OperationId);
    }

    /// <summary>Verifies a tiny advertised disposition set rejects a larger local frontier before mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsSmallAdvertisedUnionBeforeMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "tiny-first");
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "tiny-second");
        var third = await CommitOperationAsync(store, Stream, ThirdClientSequence, "tiny-third");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: ThirdClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(first.OperationId)]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedTinyAdvertisedFrontierOperationCount);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(recovered.PendingOperations[2].OperationId).IsEqualTo(third.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedTinyAdvertisedFrontierOperationCount);
    }

    /// <summary>Verifies surplus recovery proof rejects atomically when fewer local records exist.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsSurplusReplayProofWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var pending = await CommitOperationAsync(store, Stream, FirstClientSequence, "surplus-pending");
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(pending.OperationId),
                IncludedSnapshotDisposition(OperationId.New(), OperationResultKind.Accepted),
            ]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pending.OperationId);
    }

    /// <summary>Verifies recovery rejects reversed accepted replay-only proof order without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncRejectsReversedAcceptedReplayOnlyProofOrder()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "reversed-first");
        await CompleteUploadAsync(store, first.OperationId, OperationResultKind.Accepted);
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "reversed-second");
        await CompleteUploadAsync(store, second.OperationId, OperationResultKind.Accepted);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                IncludedSnapshotDisposition(second.OperationId, OperationResultKind.Accepted),
                IncludedSnapshotDisposition(first.OperationId, OperationResultKind.Accepted),
            ]);

        await Assert.That(RecoveryApplyAction(store, mutation)).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies recovery accepts pending-first work followed by a later accepted replay-only operation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncAcceptsPendingFirstThenAcceptedReplayOnlyLater()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var pending = await CommitOperationAsync(store, Stream, FirstClientSequence, "pending-first");
        var acceptedLater = await CommitOperationAsync(store, Stream, SecondClientSequence, "accepted-later");
        var batch = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, SecondClientSequence, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var uploadResult = new RemoteSyncResult(
            batch.LeaseId,
            [
                new(pending.OperationId, OperationResultKind.Retryable, null, null),
                new(acceptedLater.OperationId, OperationResultKind.Accepted, null, ServerVersion),
            ],
            null,
            null);
        await store.ApplySyncResultAsync(batch.LeaseId, uploadResult, CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(pending.OperationId),
                IncludedSnapshotDisposition(acceptedLater.OperationId, OperationResultKind.Accepted),
            ]);

        var result = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pending.OperationId);
    }

    /// <summary>Verifies replay-only inclusion preserves the complete synchronized status.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ApplySnapshotRecoveryAsyncPreservesReplayOnlyAcceptedStatusFacts()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var replayOnly = await CommitOperationAsync(store, Stream, FirstClientSequence, "status-preserved");
        await CompleteUploadAsync(store, replayOnly.OperationId, OperationResultKind.Accepted);
        var beforeStatus = await store.GetOperationStatusAsync(replayOnly.OperationId, CancellationToken.None);
        await Assert.That(beforeStatus).IsNotNull();
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(replayOnly.OperationId, OperationResultKind.Accepted)]);

        clock.Advance(TimeSpan.FromMinutes(1));
        _ = await RequireSnapshotRecoveryStore(store).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var afterStatus = await store.GetOperationStatusAsync(replayOnly.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(afterStatus).IsEqualTo(beforeStatus);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }
}
