// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>SQLite snapshot recovery replay-union tests.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The expected operation count after preserving one replay-only and one pending operation.</summary>
    private const int ExpectedReplayUnionOperationCount = 2;

    /// <summary>Verifies SQLite recovery accepts pending work followed by accepted replay-only proof.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryIncludesPendingAndReplayOnly_ThenUnionCommitsInNormalizedOrder()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        OperationId pendingOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            var pending = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            pendingOperationId = pending.OperationId;
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: SecondClientSequence,
                expectedCursor: null,
                [
                    UnknownSnapshotDisposition(pendingOperationId),
                    IncludedSnapshotDisposition(replayOnlyOperationId, OperationResultKind.Accepted),
                ]);

            var result = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);

            await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
            await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
            await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pendingOperationId);
    }

    /// <summary>Verifies SQLite recovery rejects replay proof before pending proof without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryReplayProofPrecedesPendingProof_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        OperationId pendingOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            var pending = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            pendingOperationId = pending.OperationId;
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: SecondClientSequence,
                expectedCursor: null,
                [
                    IncludedSnapshotDisposition(replayOnlyOperationId, OperationResultKind.Accepted),
                    UnknownSnapshotDisposition(pendingOperationId),
                ]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnlyOperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(pendingOperationId);
    }

    /// <summary>Verifies SQLite recovery rejects a missing replay-only proof without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryOmitsReplayOnlyProof_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        OperationId pendingOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            var pending = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            pendingOperationId = pending.OperationId;
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: SecondClientSequence,
                expectedCursor: null,
                [UnknownSnapshotDisposition(pendingOperationId)]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnlyOperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(pendingOperationId);
    }

    /// <summary>Verifies SQLite replay-only conflict proof fails closed and survives restart unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryReplayOnlyProofIsConflict_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: FirstClientSequence,
                expectedCursor: null,
                [IncludedSnapshotDisposition(replayOnlyOperationId, OperationResultKind.Conflict)]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(replayOnlyOperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnlyOperationId);
    }

    /// <summary>Verifies SQLite replay-only rejected proof fails closed and survives restart unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryReplayOnlyProofIsRejected_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: FirstClientSequence,
                expectedCursor: null,
                [RejectedSnapshotDisposition(replayOnlyOperationId)]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(replayOnlyOperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(replayOnlyOperationId);
    }

    /// <summary>Verifies the internal replay-only scan rejects non-positive row bounds.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryReplayOnlyScanBoundIsInvalid_ThenItIsRejected()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(CancellationToken.None);

        Action scan = () => SqliteLocalCommitSql.ReadSnapshotRecoveryReplayOnlyOperationIds(
            connection,
            transaction,
            StoreIdentity,
            Stream,
            0,
            CancellationToken.None);

        await Assert.That(scan).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies SQLite surplus recovery proof rejects atomically when fewer local records exist.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryHasSurplusReplayProof_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId pendingOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var pending = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            pendingOperationId = pending.OperationId;
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: FirstClientSequence,
                expectedCursor: null,
                [
                    UnknownSnapshotDisposition(pendingOperationId),
                    IncludedSnapshotDisposition(OperationId.New(), OperationResultKind.Accepted),
                ]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pendingOperationId);
    }

    /// <summary>Verifies SQLite rejects reversed accepted replay-only proof order without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryAcceptedReplayProofIsReversed_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId firstOperationId;
        OperationId secondOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var first = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            firstOperationId = first.OperationId;
            await CompleteUploadAsync(adapter, firstOperationId, OperationResultKind.Accepted);
            var second = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            secondOperationId = second.OperationId;
            await CompleteUploadAsync(adapter, secondOperationId, OperationResultKind.Accepted);
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: SecondClientSequence,
                expectedCursor: null,
                [
                    IncludedSnapshotDisposition(secondOperationId, OperationResultKind.Accepted),
                    IncludedSnapshotDisposition(firstOperationId, OperationResultKind.Accepted),
                ]);

            Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedReplayUnionOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(firstOperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(secondOperationId);
    }

    /// <summary>Verifies SQLite accepts pending-first work followed by a later accepted replay-only operation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryIncludesPendingFirstAndAcceptedLater_ThenUnionCommitsInNormalizedOrder()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId pendingOperationId;
        OperationId acceptedLaterOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var pending = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            pendingOperationId = pending.OperationId;
            var acceptedLater = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            acceptedLaterOperationId = acceptedLater.OperationId;
            var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            var uploadResult = new RemoteSyncResult(
                lease.LeaseId,
                [
                    new(pendingOperationId, OperationResultKind.Retryable, null, null),
                    new(acceptedLaterOperationId, OperationResultKind.Accepted, null, ServerVersion),
                ],
                null,
                null);
            await adapter.ApplySyncResultAsync(lease.LeaseId, uploadResult, CancellationToken.None);
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: SecondClientSequence,
                expectedCursor: null,
                [
                    UnknownSnapshotDisposition(pendingOperationId),
                    IncludedSnapshotDisposition(acceptedLaterOperationId, OperationResultKind.Accepted),
                ]);

            var result = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);

            await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
            await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
            await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(pendingOperationId);
    }

    /// <summary>Verifies accepted replay-only recovery prevents repeated replay across multiple restarts.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAcceptedReplayOnlyRecoveryRestartsTwice_ThenOperationDoesNotReplayAgain()
    {
        using var database = TempDatabase.Create();
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        SubscriptionId subscriptionId;
        OperationId replayOnlyOperationId;
        SyncOperationStatus? beforeStatus;
        await using (var adapter = CreateAdapter(database.Path, clock))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var replayOnly = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            replayOnlyOperationId = replayOnly.OperationId;
            await CompleteUploadAsync(adapter, replayOnlyOperationId, OperationResultKind.Accepted);
            beforeStatus = await adapter.GetOperationStatusAsync(replayOnlyOperationId, CancellationToken.None);
            await Assert.That(beforeStatus).IsNotNull();
        }

        clock.Advance(TimeSpan.FromMinutes(1));
        await using (var reopened = CreateAdapter(database.Path, clock))
        {
            await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: FirstClientSequence,
                expectedCursor: null,
                [IncludedSnapshotDisposition(replayOnlyOperationId, OperationResultKind.Accepted)]);

            _ = await RequireSnapshotRecoveryStore(reopened).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
            var afterStatus = await reopened.GetOperationStatusAsync(replayOnlyOperationId, CancellationToken.None);

            await Assert.That(afterStatus).IsEqualTo(beforeStatus);
            await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
            await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
            await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
            await Assert.That(SamePayload(recovered.Snapshot?.State, CreatePayload(SnapshotRecoveryOptimisticText))).IsTrue();
        }

        await using var restartedAgain = CreateAdapter(database.Path, clock);
        await restartedAgain.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recoveredAgain = await restartedAgain.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await restartedAgain.GetOperationStatusAsync(replayOnlyOperationId, CancellationToken.None);

        await Assert.That(status).IsEqualTo(beforeStatus);
        await Assert.That(recoveredAgain.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
        await Assert.That(recoveredAgain.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recoveredAgain.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(SamePayload(recoveredAgain.Snapshot?.State, CreatePayload(SnapshotRecoveryOptimisticText))).IsTrue();
    }
}
