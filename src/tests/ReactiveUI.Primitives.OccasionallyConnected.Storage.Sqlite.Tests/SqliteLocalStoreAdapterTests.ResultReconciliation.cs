// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Upload result reconciliation tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The initial authoritative payload used by result reconciliation tests.</summary>
    private const string ResultAuthoritativeInitialText = "authoritative-initial";

    /// <summary>The replacement authoritative payload used to test preservation checks.</summary>
    private const string ResultAuthoritativeChangedText = "authoritative-changed";

    /// <summary>The first optimistic payload used by result reconciliation tests.</summary>
    private const string ResultOptimisticInitialText = "optimistic-initial";

    /// <summary>The second optimistic payload used by result reconciliation tests.</summary>
    private const string ResultOptimisticLocalText = "optimistic-local";

    /// <summary>The permanent rejection reason returned by the test peer.</summary>
    private const string ResultRejectedReason = "rejected";

    /// <summary>The snapshot revision after two local commits and one reconciliation rebuild.</summary>
    private const int ResultReconciledRevision = 3;

    /// <summary>The maximum result snapshot mutation count admitted before SQLite reconciliation.</summary>
    private const int ResultMaximumSnapshotMutations = 128;

    /// <summary>The mutation count used when the first mutation must stop later reads.</summary>
    private const int ResultTwoSnapshotMutations = 2;

    /// <summary>Verifies mixed upload decisions replace optimistic state while retaining accepted work until receive inclusion.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenMixedUploadResultsCommit_ThenSnapshotAndReplayMembershipChangeTogether()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence) with { Payload = CreatePayload("first-operation") };
        var second = CreateOperation(SecondClientSequence) with { Payload = CreatePayload("second-operation") };
        _ = await adapter.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, ResultOptimisticLocalText), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(second.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)],
            RemoteCursor,
            null);

        var snapshots = await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            result,
            [CreateSnapshotMutation(SecondClientSequence, ResultOptimisticInitialText)],
            CancellationToken.None);

        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var firstStatus = await adapter.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        var secondStatus = await adapter.GetOperationStatusAsync(second.OperationId, CancellationToken.None);

        await Assert.That(snapshots.Count).IsEqualTo(1);
        await AssertSnapshotMatchesAsync(recovered.Snapshot, snapshots[0]);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(ResultReconciledRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(ResultAuthoritativeInitialText);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Rejected);
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
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var initial = CreateSnapshotMutation(0, ResultOptimisticInitialText) with
        {
            AuthoritativeState = failure == "unknown-base" ? null : CreatePayload(ResultAuthoritativeInitialText),
        };
        _ = await adapter.CommitLocalOperationAsync(operation, initial, CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var replacement = CreateSnapshotMutation(1, ResultAuthoritativeInitialText);
        SnapshotMutation[] replacements = failure switch
        {
            "missing" => [],
            "duplicate" => [replacement, replacement],
            "unrelated" => [replacement with { StreamId = new("sensor/unrelated") }],
            "stale" => [replacement with { ExpectedRevision = 0 }],
            "authoritative" => [replacement with { AuthoritativeState = CreatePayload(ResultAuthoritativeChangedText) }],
            _ => [replacement],
        };
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, rejected, replacements, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);

        var accepted = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], RemoteCursor, null);
        var unchangedSnapshots = await adapter.ApplySyncResultAsync(lease.LeaseId, accepted, [], CancellationToken.None);
        await Assert.That(unchangedSnapshots.Count).IsEqualTo(0);
    }

    /// <summary>Verifies rejection cannot discard replay work while leaving its optimistic snapshot committed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRejectionRequiresSnapshotRebuild_ThenStatusOnlyResultLeavesTheLeaseAndStateUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(ResultAuthoritativeInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);

        var accepted = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);
        await adapter.ApplySyncResultAsync(lease.LeaseId, accepted, CancellationToken.None);
    }

    /// <summary>Verifies a late rejection cannot contradict an already committed authoritative completion.</summary>
    /// <param name="replaceSnapshot">Whether the caller supplies an optimistic replacement.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenRejectionContradictsReceiveInclusion_ThenTheAuthoritativeStateIsPreserved(bool replaceSnapshot)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var completion = CreateRemoteBatch(null, RemoteCursor, []) with { CompletedOperations = [new(new(ClientId, operation.OperationId), [])] };
        _ = await adapter.ApplyRemoteBatchAsync(
            completion,
            CreateSnapshotMutation(1, AuthoritativePayloadText) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);
        Func<Task> apply = async () =>
        {
            if (replaceSnapshot)
            {
                _ = await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, [CreateSnapshotMutation(SecondClientSequence)], CancellationToken.None);
                return;
            }

            await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None);
        };

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativePayloadText);
        await Assert.That(recovered.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies SQLite aborts roll back status, lease, and snapshot changes together.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultCommitFailsAfterStatusUpdate_ThenStatusLeaseAndSnapshotRollBack()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        CreateResultStatusRollbackTrigger(database.Path);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(
            lease.LeaseId,
            rejected,
            [CreateSnapshotMutation(1, ResultAuthoritativeInitialText)],
            CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<SqliteException>();
        DropResultStatusRollbackTrigger(database.Path);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);

        var accepted = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);
        await adapter.ApplySyncResultAsync(lease.LeaseId, accepted, CancellationToken.None);
    }

    /// <summary>Verifies committed result reconciliation survives disposal and reopen.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultReconciliationCommitsAndStoreReopens_ThenSnapshotAndReplayRecover()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscription;
        OperationId acceptedOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var accepted = CreateOperation(FirstClientSequence) with { Payload = CreatePayload("accepted-operation") };
            var rejected = CreateOperation(SecondClientSequence) with { Payload = CreatePayload("rejected-operation") };
            acceptedOperationId = accepted.OperationId;
            _ = await adapter.CommitLocalOperationAsync(
                accepted,
                CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
                CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(rejected, CreateSnapshotMutation(1, ResultOptimisticLocalText), CancellationToken.None);
            var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            var result = new RemoteSyncResult(
                lease.LeaseId,
                [new(accepted.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(rejected.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)],
                null,
                null);
            _ = await adapter.ApplySyncResultAsync(lease.LeaseId, result, [CreateSnapshotMutation(SecondClientSequence, ResultOptimisticInitialText)], CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(acceptedOperationId);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(ResultAuthoritativeInitialText);
    }

    /// <summary>Verifies mixed-stream lease corruption is rejected without changing status or releasing the lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeasedResultContainsMixedStreams_ThenValidationRejectsWithoutMutation()
    {
        var otherStream = new StreamId("sensor/humidity");
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(otherStream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence);
        var second = CreateOperation(FirstClientSequence) with { StreamId = otherStream };
        _ = await adapter.CommitLocalOperationAsync(first, CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(second, new(otherStream, CreatePayload("other-snapshot"), FormatVersion: 1, ExpectedRevision: 0), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        AddOperationToLease(database.Path, lease.LeaseId, second);
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(second.OperationId, OperationResultKind.Accepted, null, ServerVersion)],
            null,
            null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, result, [], CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(apply);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        await Assert.That(exception?.Error).IsEqualTo(SyncBatchValidationError.MixedStreams);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);

        var accepted = new RemoteSyncResult(lease.LeaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);
        Func<Task> retryOriginalLease = () => adapter.ApplySyncResultAsync(lease.LeaseId, accepted, [], CancellationToken.None).AsTask();
        await Assert.That(retryOriginalLease).ThrowsExactly<SyncBatchValidationException>();
    }

    /// <summary>Verifies cancellation before the result transaction starts leaves durable state unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultReconciliationIsCanceledBeforeCommit_ThenStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(
            lease.LeaseId,
            rejected,
            [CreateSnapshotMutation(1, ResultAuthoritativeInitialText)],
            cancellation.Token).AsTask();

        await Assert.That(apply).ThrowsExactly<OperationCanceledException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies oversized result counts fail before lease lookup and reconciliation validation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultOperationCountExceedsBounds_ThenCapacityRejectsBeforeLeaseLookup()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = BoundedRemoteApplyBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var results = Enumerable
            .Range(0, (int)BoundedRemoteApplyBytes + 1)
            .Select(static _ => new OperationSyncResult(OperationId.New(), OperationResultKind.Accepted, null, null))
            .ToArray();
        var result = new RemoteSyncResult(Guid.NewGuid(), results, null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(result.BatchId, result, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(apply);
        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
    }

    /// <summary>Verifies negative caller mutation counts fail before allocation or indexing.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultMutationCountIsNegative_ThenInputIsNotIndexed()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var result = new RemoteSyncResult(Guid.NewGuid(), [], null, null);
        var mutations = new ResultMutationList(-1, static () => CreateSnapshotMutation(0));

        Func<Task> apply = () => adapter.ApplySyncResultAsync(result.BatchId, result, mutations, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(mutations.ReadCount).IsEqualTo(0);
    }

    /// <summary>Verifies snapshot mutation counts use a finite entry bound before allocation or indexing.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultMutationCountExceedsFiniteBound_ThenInputIsNotIndexed()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var result = new RemoteSyncResult(Guid.NewGuid(), [], null, null);
        var mutations = new ResultMutationList(ResultMaximumSnapshotMutations + 1, static () => CreateSnapshotMutation(0));

        Func<Task> apply = () => adapter.ApplySyncResultAsync(result.BatchId, result, mutations, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(apply);
        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(mutations.ReadCount).IsEqualTo(0);
    }

    /// <summary>Verifies mutation sizing happens before retaining and reading subsequent caller-owned mutations.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultMutationExceedsBounds_ThenLaterMutationsAreNotRead()
    {
        using var database = TempDatabase.Create();
        await using var setup = CreateAdapter(database.Path);
        await setup.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await setup.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await setup.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(setup, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);
        await setup.DisposeAsync();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = BoundedRemoteApplyBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        SnapshotMutation oversized = new(Stream, CreatePayload(new('x', OversizedPayloadLength)), FormatVersion: 1, ExpectedRevision: 1);
        var readIndex = 0;
        SnapshotMutation ReadMutation()
        {
            var currentIndex = readIndex;
            readIndex++;
            if (currentIndex == 0)
            {
                return oversized;
            }

            throw new InvalidOperationException("The second mutation should not be read.");
        }

        var mutations = new ResultMutationList(ResultTwoSnapshotMutations, ReadMutation);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, result, mutations, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(apply);
        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(mutations.ReadCount).IsEqualTo(1);
    }

    /// <summary>Verifies expired leases are rejected before snapshot replacement mutates durable state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultLeaseExpiresBeforeSnapshotReplacement_ThenStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        ExpireLease(database.Path, lease.LeaseId);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(
            lease.LeaseId,
            rejected,
            [CreateSnapshotMutation(1, ResultAuthoritativeInitialText)],
            CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies deleted operation states are rejected before status-only or snapshot replacement reconciliation.</summary>
    /// <param name="replaceSnapshot">Whether the new snapshot replacement overload is used.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenRejectedResultOperationStateIsMissing_ThenReconciliationFails(bool replaceSnapshot)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        DeleteOperationState(database.Path, operation.OperationId);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = async () =>
        {
            if (replaceSnapshot)
            {
                _ = await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, [CreateSnapshotMutation(1)], CancellationToken.None);
                return;
            }

            await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None);
        };

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies deleted snapshots are rejected before status-only or snapshot replacement reconciliation.</summary>
    /// <param name="replaceSnapshot">Whether the new snapshot replacement overload is used.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenRejectedResultSnapshotIsMissing_ThenReconciliationFails(bool replaceSnapshot)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        DeleteSnapshot(database.Path, Stream);
        var rejected = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);

        Func<Task> apply = async () =>
        {
            if (replaceSnapshot)
            {
                _ = await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, [CreateSnapshotMutation(1)], CancellationToken.None);
                return;
            }

            await adapter.ApplySyncResultAsync(lease.LeaseId, rejected, CancellationToken.None);
        };

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the result-count preflight reports omitted operation results before shared validation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultOmitsLeasedOperation_ThenLeaseAndStateRemainUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence);
        var second = CreateOperation(SecondClientSequence);
        _ = await adapter.CommitLocalOperationAsync(first, CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(second, CreateSnapshotMutation(1), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(lease.LeaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, result, [], CancellationToken.None).AsTask();

        var exception = await Assert.That(apply).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception?.Error).IsEqualTo(SyncBatchValidationError.OmittedOperationResult);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(TwoWorkerCommands);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(TwoWorkerCommands);
    }

    /// <summary>Verifies the result-count preflight reports unknown operation results before shared validation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultAddsUnknownOperation_ThenLeaseAndStateRemainUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion), new(OperationId.New(), OperationResultKind.Accepted, null, ServerVersion)],
            null,
            null);

        Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, result, [], CancellationToken.None).AsTask();

        var exception = await Assert.That(apply).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception?.Error).IsEqualTo(SyncBatchValidationError.UnknownOperationResult);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies blocked caller capture reserves capacity without preventing independent store reads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultMutationCaptureBlocks_ThenOtherCapturesAreBoundedAndReadsRemainAvailable()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var mutations = new ResultMutationList(
            () =>
        {
            entered.Set();
            _ = release.Wait(GuardTimeout);
            return 1;
        },
            static () => CreateSnapshotMutation(1, ResultAuthoritativeInitialText));
        var commit = Task.Run(async () => await adapter.ApplySyncResultAsync(lease.LeaseId, result, mutations, CancellationToken.None));
        try
        {
            await Assert.That(entered.Wait(GuardTimeout)).IsTrue();
            var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
            var competing = new ResultMutationList(1, static () => CreateSnapshotMutation(1));
            Func<Task> apply = () => adapter.ApplySyncResultAsync(lease.LeaseId, result, competing, CancellationToken.None).AsTask();
            var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(apply);
            await Assert.That(exception?.CanFitWhenEmpty).IsTrue();
            await Assert.That(competing.ReadCount).IsEqualTo(0);
        }
        finally
        {
            release.Set();
            _ = await commit.WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision, string payloadText) =>
        new(Stream, CreatePayload(payloadText), FormatVersion: 1, expectedRevision);

    /// <summary>Reads payload text from a nullable payload.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The payload text, or null.</returns>
    private static string? PayloadText(PayloadEnvelope? payload) =>
        payload is null ? null : System.Text.Encoding.UTF8.GetString(payload.Payload.ToArray());

    /// <summary>Asserts that two snapshots have the same persisted identity and payload values.</summary>
    /// <param name="actual">The recovered snapshot.</param>
    /// <param name="expected">The returned snapshot.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSnapshotMatchesAsync(LocalSnapshot? actual, LocalSnapshot expected)
    {
        await Assert.That(actual?.StreamId).IsEqualTo(expected.StreamId);
        await Assert.That(actual?.FormatVersion).IsEqualTo(expected.FormatVersion);
        await Assert.That(actual?.ServerCursor).IsEqualTo(expected.ServerCursor);
        await Assert.That(actual?.Revision).IsEqualTo(expected.Revision);
        await Assert.That(PayloadText(actual?.State)).IsEqualTo(PayloadText(expected.State));
        await Assert.That(PayloadText(actual?.AuthoritativeState)).IsEqualTo(PayloadText(expected.AuthoritativeState));
    }

    /// <summary>Creates a trigger that aborts result status updates.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateResultStatusRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_result_status_abort
            AFTER UPDATE OF operation_state ON oc_outbox_operation_states
            WHEN NEW.operation_state = 5
            BEGIN
                SELECT RAISE(ABORT, 'rollback result status');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the result status rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropResultStatusRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_result_status_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Expires an existing lease.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    private static void ExpireLease(string path, Guid leaseId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_expires_at_utc = '1970-01-01T00:00:00.0000000+00:00'
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes an operation state row.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void DeleteOperationState(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_outbox_operation_states
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes a stream snapshot row.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="streamId">The stream identifier.</param>
    private static void DeleteSnapshot(string path, StreamId streamId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Adds another operation to an existing lease to simulate historical mixed-stream corruption.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operation">The added operation.</param>
    private static void AddOperationToLease(string path, Guid leaseId, SyncOperation operation)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO oc_outbox_leases
                (store_identity, lease_id, operation_id, stream_id, client_sequence, lease_expires_at_utc, lease_member_count)
            SELECT store_identity, lease_id, $operationId, $streamId, $clientSequence, lease_expires_at_utc, 2
            FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId
            LIMIT 1;
            UPDATE oc_outbox_leases
            SET lease_member_count = 2
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue("$operationId", operation.OperationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(StreamIdParameter, operation.StreamId.Value);
        _ = command.Parameters.AddWithValue("$clientSequence", operation.ClientSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>A caller-owned mutation list with observable indexing callbacks.</summary>
    private sealed class ResultMutationList : IReadOnlyList<SnapshotMutation>
    {
        /// <summary>The fixed advertised count.</summary>
        private readonly int _countValue;

        /// <summary>The optional advertised count callback.</summary>
        private readonly Func<int>? _count;

        /// <summary>The callback supplying each mutation.</summary>
        private readonly Func<SnapshotMutation> _read;

        /// <summary>Initializes a new instance of the <see cref="ResultMutationList"/> class.</summary>
        /// <param name="count">The advertised count.</param>
        /// <param name="read">The callback supplying each mutation.</param>
        internal ResultMutationList(int count, Func<SnapshotMutation> read)
        {
            _countValue = count;
            _read = read;
        }

        /// <summary>Initializes a new instance of the <see cref="ResultMutationList"/> class.</summary>
        /// <param name="count">The advertised count callback.</param>
        /// <param name="read">The callback supplying each mutation.</param>
        internal ResultMutationList(Func<int> count, Func<SnapshotMutation> read)
        {
            _count = count;
            _read = read;
        }

        /// <inheritdoc/>
        public int Count => _count?.Invoke() ?? _countValue;

        /// <summary>Gets the number of indexer calls.</summary>
        public int ReadCount { get; private set; }

        /// <inheritdoc/>
        public SnapshotMutation this[int index]
        {
            get
            {
                ReadCount++;
                return _read();
            }
        }

        /// <inheritdoc/>
        public IEnumerator<SnapshotMutation> GetEnumerator()
        {
            for (var index = 0; index < Count; index++)
            {
                yield return this[index];
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
