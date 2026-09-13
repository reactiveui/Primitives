// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests reconciliation with pending noninvertible application mutations.</summary>
/// <content>Upload result projection tests.</content>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The maximum lease byte count for result tests.</summary>
    private const int ResultMaximumLeaseBytes = 4096;

    /// <summary>The number of oversized result entries used to trip the committer bound.</summary>
    private const int OversizedResultCount = 10_001;

    /// <summary>The reason used for permanently rejected result operations.</summary>
    private const string ResultRejectedReasonCode = "invalid-edit";

    /// <summary>The expected attempts after a failed store call and retry.</summary>
    private const int ExpectedRetriedResultApplyCount = 2;

    /// <summary>The revision after two local commits and one result reconciliation.</summary>
    private const int ReconciledResultRevision = 3;

    /// <summary>Verifies rejection restores the prior replacement edit across recovery.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRebuildsReplacementStateAcrossRecovery()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new ReplacementProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);
        var batch = new SyncBatch(lease.LeaseId, lease.Operations);
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(first.Operation.OperationId, OperationResultKind.Accepted, null, null), new(second.Operation.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null)],
            null,
            null);

        var state = await committer.ApplySyncResultAsync(batch, result, CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(state.Revision).IsEqualTo(second.State.Revision + 1);
        await Assert.That(state.ServerCursor).IsNull();
        var reopened = CreateLocalCommitter(options);
        var recovered = await reopened.RecoverAsync(CancellationToken.None);
        await Assert.That(recovered.State.Sum).IsEqualTo(FirstReadingValue);
        var persisted = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(persisted.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(persisted.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies accepted work remains replay-visible while rejected additive work is removed from optimistic state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRebuildsAdditiveStateAndKeepsAcceptedReplay()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new SumProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);

        var state = await committer.ApplySyncResultAsync(
            new(lease.LeaseId, lease.Operations),
            CreateRejectedSecondResult(lease.LeaseId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.Operation.OperationId);
        var acceptedStatus = await store.GetOperationStatusAsync(first.Operation.OperationId, CancellationToken.None);
        var rejectedStatus = await store.GetOperationStatusAsync(second.Operation.OperationId, CancellationToken.None);
        await Assert.That(acceptedStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(rejectedStatus?.State).IsEqualTo(SyncOperationState.Rejected);
    }

    /// <summary>Verifies mutable projection code runs against isolated replay state during result reconciliation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRebuildsStateWithMutatingProjection()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new MutatingProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var before = committer.Current;
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);

        var state = await committer.ApplySyncResultAsync(
            new(lease.LeaseId, lease.Operations),
            CreateRejectedSecondResult(lease.LeaseId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None);

        await Assert.That(before.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(state.State).IsNotSameReferenceAs(before.State);
    }

    /// <summary>Verifies retryable work remains pending while a rejected later edit is removed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncKeepsRetryableOperationPendingAfterRejectedLaterEdit()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new SumProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(first.Operation.OperationId, OperationResultKind.Retryable, "later", null), new(second.Operation.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null)],
            null,
            TimeSpan.FromSeconds(1));

        var state = await committer.ApplySyncResultAsync(new(lease.LeaseId, lease.Operations), result, CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.Operation.OperationId);
        var status = await store.GetOperationStatusAsync(first.Operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies accepted-only results persist statuses without replacing optimistic state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncAcceptedOnlyResultLeavesVisibleStateUnchanged()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batchId = Guid.NewGuid();
        var before = committer.Current;

        var state = await committer.ApplySyncResultAsync(
            new(batchId, [committed.Operation]),
            new(batchId, [new(committed.Operation.OperationId, OperationResultKind.Accepted, null, null)], null, null),
            CancellationToken.None);

        await Assert.That(state).IsSameReferenceAs(before);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies malformed accepted-only receipts poison before later use.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncAcceptedOnlyMalformedReceiptPoisonsCommitter()
    {
        var store = new ScriptedLocalStore { ReturnNullResultSnapshots = true };
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batchId = Guid.NewGuid();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            new(batchId, [committed.Operation]),
            new(batchId, [new(committed.Operation.OperationId, OperationResultKind.Accepted, null, null)], null, null),
            CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
    }

    /// <summary>Verifies a later receive after result reconciliation retires accepted replay without restoring a rejected edit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncAllowsRestartThenLaterReceiveToRetireAcceptedReplay()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new ReplacementProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);
        _ = await committer.ApplySyncResultAsync(
            new(lease.LeaseId, lease.Operations),
            CreateRejectedSecondResult(lease.LeaseId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None);
        var restarted = CreateLocalCommitter(options);
        _ = await restarted.RecoverAsync(CancellationToken.None);
        var echoed = CreateRemoteEvent(FirstRemoteValue, causedByOperationId: first.Operation.OperationId) with
        {
            Origin = new(ReconciliationClientId, first.Operation.OperationId),
        };
        var receive = CreateRemoteBatch(null, NextRemoteCursor, [echoed]) with
        {
            CompletedOperations = [new(new(ReconciliationClientId, first.Operation.OperationId), [echoed.EventId])],
        };

        var received = await restarted.ApplyRemoteBatchAsync(receive, CancellationToken.None);

        await Assert.That(received.State.State.Sum).IsEqualTo(FirstRemoteValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies replay decode failures do not reach the store result transaction.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsWrongDecodedReplayInputBeforeStore()
    {
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        serializer.DeserializeInputAsState = true;
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(
                batch,
                CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
                CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies projection failures during replay do not reach the store result transaction.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncProjectionFailureLeavesCommittedStateUnchanged()
    {
        var projection = new ThrowingProjection();
        var store = new ScriptedLocalStore();
        var options = CreateOptions(store, new()) with { Dependencies = CreateDependencies(store, new(), new SequenceOperationIdSource()) with { Projection = projection } };
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        projection.ThrowOnLocalValue = FirstReadingValue;
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(
                batch,
                CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
                CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies store failures leave the visible result state unchanged and retryable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncStoreFailureLeavesStateAndCanRetry()
    {
        var failure = new IOException("result transaction failed");
        var store = new ScriptedLocalStore { ResultCommitException = failure };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);
        var result = CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId);
        var previous = committer.Current;

        var thrown = await Assert.ThrowsExactlyAsync<IOException>(() => committer.ApplySyncResultAsync(batch, result, CancellationToken.None).AsTask());
        await Assert.That(thrown).IsSameReferenceAs(failure);
        await Assert.That(committer.Current).IsSameReferenceAs(previous);

        store.ResultCommitException = null;
        var state = await committer.ApplySyncResultAsync(batch, result, CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(ExpectedRetriedResultApplyCount);
    }

    /// <summary>Verifies cancellation before the store result transaction leaves no visible result state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncCancellationBeforeStoreLeavesStateUnchanged()
    {
        using CancellationTokenSource source = new();
        var serializer = new ScriptedPayloadSerializer { CancelAfterStateSerialization = source };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var previous = committer.Current;
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.ApplySyncResultAsync(
                batch,
                CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
                source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current).IsSameReferenceAs(previous);
    }

    /// <summary>Verifies cancellation after a successful result transaction still returns the committed state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncCancellationAfterStoreCommitReturnsState()
    {
        using CancellationTokenSource source = new();
        var store = new ScriptedLocalStore { CancelAfterSuccessfulResultApply = source };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        var state = await committer.ApplySyncResultAsync(
            batch,
            CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
            source.Token);

        await Assert.That(source.IsCancellationRequested).IsTrue();
        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(committer.Current.Revision).IsEqualTo(ReconciledResultRevision);
    }

    /// <summary>Verifies malformed result batches are rejected before the store is called.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsOversizedResultBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var operations = new OperationSyncResult[OversizedResultCount];
        Array.Fill(operations, new(committed.Operation.OperationId, OperationResultKind.Accepted, null, null));

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            new(Guid.NewGuid(), [committed.Operation]),
            new(Guid.NewGuid(), operations, null, null),
            CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies batches for other streams are rejected before the store is called.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsForeignStreamBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var operation = committed.Operation with { StreamId = new("foreign-stream") };
        var batchId = Guid.NewGuid();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            new(batchId, [operation]),
            new(batchId, [new(operation.OperationId, OperationResultKind.Accepted, null, null)], null, null),
            CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies rejection requires a known authoritative base.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsUnknownAuthoritativeBaseBeforeStore()
    {
        var snapshot = await CreateSnapshotAsync(new(FirstReadingValue));
        var pending = CreatePendingOperation(FirstClientSequence, FirstReadingValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [pending], RecoveredNextSequence) };
        var committer = await CreateRecoveredCommitterAsync(store);
        var batchId = Guid.NewGuid();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            new(batchId, [pending]),
            new(batchId, [new(pending.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null)], null, null),
            CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
    }

    /// <summary>Verifies changed recovery metadata fails before projection or store mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsMismatchedRecoveryBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var changedRecovery = new RecoveredStream(
            store.Recovery.SubscriptionId,
            store.Recovery.ServerCursor,
            store.Recovery.Snapshot,
            store.Recovery.PendingOperations,
            store.Recovery.DeadLetters,
            store.Recovery.NextClientSequence + 1);
        store.Recovery = changedRecovery with { ReplayOperations = store.Recovery.ReplayOperations };
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            batch,
            CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies malformed replay operations fail before result storage.</summary>
    /// <param name="fault">The malformed replay condition.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("missing")]
    [Arguments("foreign")]
    [Arguments("order")]
    public async Task ApplySyncResultAsyncRejectsMalformedReplayRecoveryBeforeStore(string fault)
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var replay = fault switch
        {
            "missing" => new SyncOperation[1],
            "foreign" => [first.Operation with { StreamId = new("foreign-stream") }, second.Operation],
            _ => [second.Operation, first.Operation],
        };
        store.Recovery = ReplaceRecoveredReplay(store.Recovery, replay);
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            batch,
            CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None).AsTask());

        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies malformed result receipts poison the committer before later use.</summary>
    /// <param name="fault">The malformed receipt field.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("null")]
    [Arguments("empty")]
    [Arguments("extra")]
    [Arguments("revision")]
    [Arguments("payload")]
    [Arguments("authoritative")]
    [Arguments("missing-authoritative")]
    [Arguments("stream")]
    [Arguments("format")]
    [Arguments("cursor")]
    public async Task ApplySyncResultAsyncMalformedReceiptPoisonsCommitter(string fault)
    {
        var store = new ScriptedLocalStore
        {
            ReturnNullResultSnapshots = string.Equals(fault, "null", StringComparison.Ordinal),
            ResultSnapshotRevisionOffset = string.Equals(fault, "revision", StringComparison.Ordinal) ? 1 : 0,
            TransformResultSnapshots = snapshots => fault switch
            {
                "empty" => [],
                "extra" => [snapshots[0], snapshots[0]],
                "payload" => [snapshots[0] with { State = CreateRemotePayload(InputContract, InputSchemaVersion) }],
                "authoritative" => [snapshots[0] with { AuthoritativeState = CreateRemotePayload(InputContract, InputSchemaVersion) }],
                "missing-authoritative" => [snapshots[0] with { AuthoritativeState = null }],
                "stream" => [snapshots[0] with { StreamId = new("other-result-stream") }],
                "format" => [snapshots[0] with { FormatVersion = 0 }],
                "cursor" => [snapshots[0] with { ServerCursor = "unexpected-result-cursor" }],
                _ => snapshots,
            },
        };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplySyncResultAsync(
            batch,
            CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
    }

    /// <summary>Verifies result reconciliation shares the same exclusive lane as recovery and local commits.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncPendingCommitExcludesOtherTransactions()
    {
        TaskCompletionSource enteredStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedLocalStore { BeforeResultCommitAsync = PauseAfterSignal(enteredStore, releaseStore) };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = CreateResultBatch(Guid.NewGuid(), first.Operation, second.Operation);
        var pending = committer.ApplySyncResultAsync(
            batch,
            CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId),
            CancellationToken.None).AsTask();
        try
        {
            await enteredStore.Task.WaitAsync(TimeSpan.FromSeconds(StoreStartWaitSeconds));
            await Assert.That(pending.IsCompleted).IsFalse();
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.RecoverAsync(CancellationToken.None).AsTask());
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.ApplySyncResultAsync(batch, CreateRejectedSecondResult(batch.BatchId, first.Operation.OperationId, second.Operation.OperationId), CancellationToken.None).AsTask());
        }
        finally
        {
            _ = releaseStore.TrySetResult();
            _ = await pending;
        }

        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(1);
    }

    /// <summary>Initializes a real in-memory result store with the reconciliation client binding.</summary>
    /// <param name="store">The store.</param>
    /// <returns>The subscription identity.</returns>
    private static async ValueTask<SubscriptionId> InitializeResultStoreAsync(ILocalStoreAdapter store)
    {
        await store.InitializeAsync(new(ReconciliationClientId, 1, false) { ClientId = ReconciliationClientId }, CancellationToken.None);
        return await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
    }

    /// <summary>Creates options for real-store result tests.</summary>
    /// <param name="store">The real store.</param>
    /// <param name="subscription">The subscription identity.</param>
    /// <param name="projection">The projection.</param>
    /// <returns>The committer options.</returns>
    private static LocalStreamCommitterOptions<ReadingState, MutableReading> CreateResultOptions(
        ILocalStoreAdapter store,
        SubscriptionId subscription,
        ILocalProjection<ReadingState, MutableReading> projection)
    {
        var template = CreateOptions(new(), new());
        return template with
        {
            SubscriptionId = subscription,
            Dependencies = template.Dependencies with { Store = store, Projection = projection },
        };
    }

    /// <summary>Leases pending result operations from a real store.</summary>
    /// <param name="store">The real store.</param>
    /// <param name="maximumOperations">The maximum operation count.</param>
    /// <returns>The leased batch.</returns>
    /// <exception cref="InvalidOperationException">The store returned no pending lease.</exception>
    private static async ValueTask<LeasedOperationBatch> LeaseResultBatchAsync(
        ILocalStoreAdapter store,
        int maximumOperations)
    {
        await using var leases = store
            .LeasePendingOperationsAsync(new(Stream, maximumOperations, ResultMaximumLeaseBytes, TimeSpan.FromMinutes(1)), CancellationToken.None)
            .GetAsyncEnumerator();
        if (await leases.MoveNextAsync())
        {
            return leases.Current;
        }

        throw new InvalidOperationException("The result test store did not lease any pending operations.");
    }

    /// <summary>Creates a two-operation result batch.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <returns>The sync batch.</returns>
    private static SyncBatch CreateResultBatch(Guid batchId, SyncOperation first, SyncOperation second) => new(batchId, [first, second]);

    /// <summary>Creates an upload result that accepts the first operation and rejects the second.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="first">The accepted operation identifier.</param>
    /// <param name="second">The rejected operation identifier.</param>
    /// <returns>The remote result.</returns>
    private static RemoteSyncResult CreateRejectedSecondResult(Guid batchId, OperationId first, OperationId second) =>
        new(batchId, [new(first, OperationResultKind.Accepted, null, null), new(second, OperationResultKind.Rejected, ResultRejectedReasonCode, null)], null, null);

    /// <summary>Replaces recovered replay operations while preserving the recovered stream metadata.</summary>
    /// <param name="recovery">The original recovery payload.</param>
    /// <param name="replay">The replacement replay operations.</param>
    /// <returns>The updated recovery payload.</returns>
    private static RecoveredStream ReplaceRecoveredReplay(RecoveredStream recovery, IReadOnlyList<SyncOperation> replay)
    {
        var replacement = new RecoveredStream(
            recovery.SubscriptionId,
            recovery.ServerCursor,
            recovery.Snapshot,
            recovery.PendingOperations,
            recovery.DeadLetters,
            recovery.NextClientSequence);
        return replacement with { ReplayOperations = replay };
    }

    /// <summary>Models a projection that can fail during result replay.</summary>
    private sealed class ThrowingProjection : ILocalProjection<ReadingState, MutableReading>
    {
        /// <inheritdoc/>
        public ReadingState InitialState { get; } = new(InitialSum);

        /// <summary>Gets or sets the local value that should fail.</summary>
        public int ThrowOnLocalValue { get; set; } = int.MinValue;

        /// <inheritdoc/>
        public ReadingState ApplyLocal(ReadingState state, MutableReading input, SyncOperation operation)
        {
            if (input.Value == ThrowOnLocalValue)
            {
                throw new InvalidOperationException("projection failed");
            }

            return new(state.Sum + input.Value);
        }

        /// <inheritdoc/>
        public ReadingState ApplyRemote(ReadingState state, MutableReading input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState Reconcile(ReadingState state, ConflictResolutionResult result) => state;
    }
}
