// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
/// <content>Upload result reconciliation tests.</content>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The permanent rejection reason returned by the test peer.</summary>
    private const string ResultRejectedReason = "rejected";

    /// <summary>Verifies a late rejection cannot contradict an already committed authoritative completion.</summary>
    /// <param name="replaceSnapshot">Whether the caller supplies an optimistic replacement.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenRejectionContradictsReceiveInclusion_ThenTheAuthoritativeStateIsPreserved(bool replaceSnapshot)
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var completion = CreateRemoteBatch(null, RemoteCursor, []) with { CompletedOperations = [new(new(ClientId, operation.OperationId), [])] };
        _ = await store.ApplyRemoteBatchAsync(
            completion,
            CreateSnapshotMutation(1, AuthoritativeRemoteText) with { AuthoritativeState = CreatePayload(AuthoritativeRemoteText) },
            CancellationToken.None);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);
        Func<Task> apply = async () =>
        {
            if (replaceSnapshot)
            {
                _ = await store.ApplySyncResultAsync(lease.LeaseId, rejected, [CreateSnapshotMutation(SecondClientSequence)], CancellationToken.None);
            }
            else
            {
                await store.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None);
            }
        };

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(AuthoritativeRemoteText);
        await Assert.That(recovered.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies invalid snapshot transactions leave the original lease and optimistic state available.</summary>
    /// <param name="failure">The invalid mutation condition.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments("missing")]
    [Arguments("duplicate")]
    [Arguments("unrelated")]
    [Arguments("stale")]
    [Arguments("authoritative")]
    [Arguments("unknown-base")]
    public async Task WhenResultSnapshotValidationFails_ThenNoOperationOrSnapshotChanges(string failure)
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var initial = CreateSnapshotMutation(0, OptimisticInitialText) with
        {
            AuthoritativeState = failure == "unknown-base" ? null : CreatePayload(AuthoritativeInitialText),
        };
        _ = await store.CommitLocalOperationAsync(operation, initial, CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var replacement = CreateSnapshotMutation(1, AuthoritativeInitialText);
        SnapshotMutation[] replacements = failure switch
        {
            "missing" => [],
            "duplicate" => [replacement, replacement],
            "unrelated" => [replacement with { StreamId = new("different-stream") }],
            "stale" => [replacement with { ExpectedRevision = 0 }],
            "authoritative" => [replacement with { AuthoritativeState = CreatePayload(AuthoritativeChangedText) }],
            _ => [replacement],
        };
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = async () => await store.ApplySyncResultAsync(lease.LeaseId, rejected, replacements, CancellationToken.None);

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        var accepted = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], RemoteCursor, null);
        var unchangedSnapshots = await store.ApplySyncResultAsync(lease.LeaseId, accepted, [], CancellationToken.None);
        await Assert.That(unchangedSnapshots.Count).IsEqualTo(0);
        recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ServerCursor).IsNull();
    }

    /// <summary>Verifies mixed upload decisions replace optimistic state while retaining accepted work until receive inclusion.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenMixedUploadResultsCommit_ThenSnapshotAndReplayMembershipChangeTogether()
    {
        const long ReconciledRevision = 3;
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence);
        var second = CreateOperation(SecondClientSequence);
        _ = await store.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, OptimisticInitialText) with { AuthoritativeState = CreatePayload(AuthoritativeInitialText) },
            CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, OptimisticLocalText), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, SecondClientSequence, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(second.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)],
            RemoteCursor,
            null);

        var snapshots = await store.ApplySyncResultAsync(
            lease.LeaseId,
            result,
            [CreateSnapshotMutation(SecondClientSequence, OptimisticInitialText)],
            CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(snapshots.Count).IsEqualTo(1);
        await Assert.That(snapshots[0]).IsEqualTo(recovered.Snapshot);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(ReconciledRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        var firstStatus = await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        var secondStatus = await store.GetOperationStatusAsync(second.OperationId, CancellationToken.None);
        await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Rejected);
    }

    /// <summary>Verifies rejection cannot discard replay work while leaving its optimistic snapshot committed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRejectionRequiresSnapshotRebuild_ThenStatusOnlyResultLeavesTheLeaseAndStateUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var mutation = CreateSnapshotMutation(0, OptimisticInitialText) with { AuthoritativeState = CreatePayload(AuthoritativeInitialText) };
        _ = await store.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = async () => await store.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None);

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
    }
}
