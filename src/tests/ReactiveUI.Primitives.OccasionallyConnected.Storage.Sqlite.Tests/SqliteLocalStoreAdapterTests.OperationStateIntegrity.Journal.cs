// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Protected operation state proof and journal lifecycle tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Time elapsed before dead-letter history becomes eligible for compaction.</summary>
    private const int JournalCompactionElapsedDays = 32;

    /// <summary>Rotating more than one proof batch preserves every queued operation after the old key is retired.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenProtectedHistorySpansProofBatches_ThenKeyRotationPreservesRecovery()
    {
        using var database = TempDatabase.Create();
        const int operationCount = 129;
        SubscriptionId subscription;
        await using (var original = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await original.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            subscription = await original.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            for (var sequence = 1; sequence <= operationCount; sequence++)
            {
                _ = await original.CommitLocalOperationAsync(
                    CreateOperation(sequence),
                    CreateSnapshotMutation(sequence - 1),
                    CancellationToken.None);
            }
        }

        var oldKey = CreateTestKey(FirstKeyId, FirstKeyFill);
        var newKey = CreateTestKey(SecondKeyId, SecondKeyFill);
        await using (var rotating = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(newKey, [oldKey])))
        {
            await rotating.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            await Assert.That(await rotating.RotateEncryptionKeyAsync(CancellationToken.None)).IsGreaterThan(0);
        }

        await using var retired = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(newKey));
        await retired.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var recovery = await retired.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(operationCount);
        await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[^1].ClientSequence).IsEqualTo(operationCount);
    }

    /// <summary>Deleting a state alone leaves a proof that must block recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenProtectedStateIsDeletedButProofRemains_ThenRecoveryRejectsOrphan()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("""
                PRAGMA foreign_keys = OFF;
                DELETE FROM oc_outbox_operation_states
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """);
            _ = command.Execute();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>Removing the set manifest must block recovery even when row proofs still authenticate.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenProtectedStateManifestIsDeleted_ThenRecoveryRejectsMissingManifest()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("DELETE FROM oc_metadata WHERE key = 'rxui.localstore.operation_state_manifest';");
            _ = command.Execute();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>Compaction removes old proofs through the write journal and leaves an authentic empty set.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenProtectedDeadLettersAreCompacted_ThenJournalRemovesTheirProofs()
    {
        using var database = TempDatabase.Create();
        var clock = new MutableTimeProvider(new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var options = new SqliteLocalStoreAdapterOptions { KeyProvider = CreateFirstKeyProvider(), TimeProvider = clock };
        SubscriptionId subscription;
        OperationId deadLetterId;
        OperationId currentId;
        await using (var adapter = new SqliteLocalStoreAdapter(database.Path, options))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var deadLetter = CreateOperation(1);
            deadLetterId = deadLetter.OperationId;
            var initialSnapshot = CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(EncryptedSnapshotSentinel) };
            _ = await adapter.CommitLocalOperationAsync(deadLetter, initialSnapshot, CancellationToken.None);
            var lease = await ReadSingleLeaseAsync(adapter, new(Stream, 1, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            _ = await adapter.DeadLetterOperationAsync(
                lease.LeaseId,
                deadLetterId,
                "permanent-rejection",
                CreateSnapshotMutation(1),
                CancellationToken.None);
            var current = CreateOperation(SecondClientSequence);
            currentId = current.OperationId;
            _ = await adapter.CommitLocalOperationAsync(current, CreateSnapshotMutation(TwoWorkerCommands), CancellationToken.None);
            var currentLease = await ReadSingleLeaseAsync(adapter, new(Stream, 1, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            _ = await adapter.DeadLetterOperationAsync(
                currentLease.LeaseId,
                currentId,
                "permanent-rejection",
                CreateSnapshotMutation(EncryptedSecondCommitRevision),
                CancellationToken.None);

            clock.Advance(TimeSpan.FromDays(JournalCompactionElapsedDays));
            var result = await adapter.CompactAsync(new(Stream, clock.GetUtcNow(), TargetBytes: 0), CancellationToken.None);
            await Assert.That(result.RecordsRemoved).IsEqualTo(TwoWorkerCommands);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.DeadLetters.Count).IsEqualTo(0);
        await Assert.That(await reopened.GetOperationStatusAsync(deadLetterId, CancellationToken.None)).IsNull();
        await Assert.That(await reopened.GetOperationStatusAsync(currentId, CancellationToken.None)).IsNull();
    }
}
