// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for the durable local commit store.</summary>
/// <content>Maintained outbox accounting and its legacy migration.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The retained history size used to compare indexed counter reads.</summary>
    private const int AccountingHistoryCount = 128;

    /// <summary>The proof test encryption key id.</summary>
    private const string AccountingKeyId = "accounting";

    /// <summary>Changes both the byte charge and aggregate while leaving the proof intact.</summary>
    private const int ModifiedChargeBytes = 0;

    /// <summary>Removes the immutable charge proof.</summary>
    private const int MissingChargeProof = 1;

    /// <summary>Replaces the proof with a BLOB above its strict materialization bound.</summary>
    private const int OversizedChargeProof = 2;

    /// <summary>Checks legacy plaintext and protected rows backfill exact plaintext bytes once.</summary>
    /// <param name="protect">Whether the stored rows are encrypted.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LegacyOutboxAccountingBackfillsOwnedBytesAndPreservesDuplicates(bool protect)
    {
        using var database = TempDatabase.Create();
        var key = LocalStoreKey.CreateRandom(AccountingKeyId);
        var operation = CreateOperation(clientSequence: 1) with { Metadata = new Dictionary<string, string> { ["clé"] = "🙂" } };
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = GetOutboxOperationBytes(operation), MaximumBlockedPublishers = 1 };
        LocalCommitResult receipt;
        using (var original = CreateAccountingStore(database.Path, protect, key, outbox))
        {
            _ = original.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
            receipt = original.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        }

        RemoveCapacityExtension(database.Path);
        using var reopened = CreateAccountingStore(database.Path, protect, key, outbox);
        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, outbox.MaxBytes));
        await Assert.That(reopened.CommitLocalOperation(operation, snapshot, CancellationToken.None)).IsEqualTo(receipt);
        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, outbox.MaxBytes));
        Action overflow = () => reopened.CommitLocalOperation(
            CreateOperation(clientSequence: SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 1),
            CancellationToken.None);
        await Assert.That(overflow).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, outbox.MaxBytes));
    }

    /// <summary>Checks every terminal transition, resurrection, rollback, and cascade delete updates usage.</summary>
    /// <param name="terminalState">The definitive terminal state.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncOperationState.Synchronized)]
    [Arguments(SyncOperationState.Rejected)]
    [Arguments(SyncOperationState.DeadLettered)]
    public async Task OutboxAccountingTracksTerminalResurrectionAndTransactionalRollback(SyncOperationState terminalState)
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(clientSequence: 1);
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var bytes = GetOutboxOperationBytes(operation);
        using var connection = OpenRawConnection(database.Path);
        using (var transaction = connection.BeginTransaction())
        {
            SetAccountingState(connection, terminalState);
            await Assert.That(SqliteOutboxCapacitySql.ReadUsage(connection, transaction, StoreIdentity)).IsEqualTo((0L, 0L));
            SetAccountingState(connection, SyncOperationState.QueuedForUpload);
            await Assert.That(SqliteOutboxCapacitySql.ReadUsage(connection, transaction, StoreIdentity)).IsEqualTo((1L, bytes));
            SetAccountingState(connection, terminalState);
        }

        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, bytes));
        using (var transaction = connection.BeginTransaction())
        {
            using var delete = connection.CreateStatement();
            delete.SetSql("DELETE FROM oc_outbox;");
            _ = delete.Execute();
            await Assert.That(SqliteOutboxCapacitySql.ReadUsage(connection, transaction, StoreIdentity)).IsEqualTo((0L, 0L));
        }

        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, bytes));
    }

    /// <summary>Checks inconsistent derived usage fails closed on reopen and before a bounded warm admission.</summary>
    /// <param name="removeRow">Whether the usage row is removed instead of modified.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OutboxAccountingRejectsExternalMissingOrModifiedUsage(bool removeRow)
    {
        using var database = TempDatabase.Create();
        var outbox = new OutboxOptions { MaxOperations = DoubleOperationCapacity, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var store = CreateInitializedStore(database.Path, outbox);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        using (var connection = OpenRawConnection(database.Path))
        {
            using var command = connection.CreateStatement();
            command.SetSql(removeRow ? "DELETE FROM oc_outbox_capacity_usage;" : "UPDATE oc_outbox_capacity_usage SET encoded_bytes = 0;");
            _ = command.Execute();
        }

        Action admission = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 1),
            CancellationToken.None);
        await Assert.That(admission).ThrowsExactly<InvalidOperationException>();
        Action reopen = () => { using var ignored = CreateInitializedStore(database.Path, outbox); };
        await Assert.That(reopen).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks an attacker cannot alter both the charge and aggregate in a protected database.</summary>
    /// <param name="mutation">The charge or proof corruption.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(ModifiedChargeBytes)]
    [Arguments(MissingChargeProof)]
    [Arguments(OversizedChargeProof)]
    public async Task OutboxAccountingAuthenticatesProtectedImmutableCharge(int mutation)
    {
        using var database = TempDatabase.Create();
        var key = LocalStoreKey.CreateRandom(AccountingKeyId);
        var outbox = new OutboxOptions { MaxOperations = DoubleOperationCapacity, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var store = CreateAccountingStore(database.Path, true, key, outbox);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        using (var connection = OpenRawConnection(database.Path))
        {
            using var command = connection.CreateStatement();
            command.SetSql(mutation switch
            {
                MissingChargeProof => "UPDATE oc_outbox_capacity_charges SET proof = NULL;",
                OversizedChargeProof => "UPDATE oc_outbox_capacity_charges SET proof = zeroblob($proofBytes);",
                _ => "UPDATE oc_outbox_capacity_charges SET encoded_bytes = 0; UPDATE oc_outbox_capacity_usage SET encoded_bytes = 0;",
            });
            if (mutation == OversizedChargeProof)
            {
                _ = command.Bind("$proofBytes", OutboxCapacityMetadataLength);
            }

            _ = command.Execute();
        }

        Action admission = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 1),
            CancellationToken.None);
        await Assert.That(admission).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Checks warm usage reads execute identical native instruction counts with retained history.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxAccountingReadCostDoesNotGrowWithRetainedHistory()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var firstSteps = MeasureAccountingReadSteps(database.Path);
        for (var sequence = SecondClientSequence; sequence <= AccountingHistoryCount; sequence++)
        {
            _ = store.CommitLocalOperation(CreateOperation(clientSequence: sequence), CreateSnapshotMutation(expectedRevision: sequence - 1), CancellationToken.None);
        }

        await Assert.That(ReadAccountingUsage(database.Path).Count).IsEqualTo((long)AccountingHistoryCount);
        var retainedHistorySteps = MeasureAccountingReadSteps(database.Path);
        await Assert.That(retainedHistorySteps).IsEqualTo(firstSteps);
        await TestContext.Current!.OutputWriter.WriteLineAsync($"Indexed usage VM steps: one operation = {firstSteps}; {AccountingHistoryCount} operations = {retainedHistorySteps}.");
    }

    /// <summary>Creates a bounded plaintext or protected store for accounting checks.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="protect">Whether protection is enabled.</param>
    /// <param name="key">The protection key.</param>
    /// <param name="outbox">The capacity limits.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteLocalCommitStore CreateAccountingStore(string path, bool protect, LocalStoreKey key, OutboxOptions outbox)
    {
        var protection = protect ? SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(key)) : null;
        var store = new SqliteLocalCommitStore(path, TimeProvider.System, OutboxCapacityBytes, NoOpSqliteCommitFaultPoint.Instance, protection);
        try
        {
            store.Initialize(new(StoreIdentity, SchemaVersion, protect) { Outbox = outbox }, CancellationToken.None);
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }
    }

    /// <summary>Removes only the new extension and records the authentic legacy schema checksum.</summary>
    /// <param name="path">The test database.</param>
    private static void RemoveCapacityExtension(string path)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateStatement();
        command.SetSql("""
            DROP TRIGGER oc_capacity_state_insert;
            DROP TRIGGER oc_capacity_state_update;
            DROP TRIGGER oc_capacity_state_delete;
            DROP TRIGGER oc_capacity_outbox_delete;
            DROP TABLE oc_outbox_capacity_charges;
            DROP TABLE oc_outbox_capacity_usage;
            """);
        _ = command.Execute();
        _ = SqliteSchemaChecksum.Record(connection, transaction);
        transaction.Commit();
    }

    /// <summary>Reads exact maintained usage under a read transaction.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The maintained count and bytes.</returns>
    private static (long Count, long Bytes) ReadAccountingUsage(string path)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction(deferred: true);
        return SqliteOutboxCapacitySql.ReadUsage(connection, transaction, StoreIdentity);
    }

    /// <summary>Changes only the operation state to exercise SQLite trigger accounting.</summary>
    /// <param name="connection">The active test connection.</param>
    /// <param name="state">The requested state.</param>
    private static void SetAccountingState(SqliteDatabase connection, SyncOperationState state)
    {
        using var command = connection.CreateStatement();
        command.SetSql("UPDATE oc_outbox_operation_states SET operation_state = $state;");
        _ = command.Bind("$state", (int)state);
        _ = command.Execute();
    }

    /// <summary>Measures native VM steps for the same primary-key lookup as production usage reads.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The native VM instruction count.</returns>
    private static int MeasureAccountingReadSteps(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateStatement();
        command.SetSql("SELECT operation_count, encoded_bytes FROM oc_outbox_capacity_usage WHERE store_identity = $identity;");
        _ = command.Bind("$identity", StoreIdentity);
        using var rows = command.Query();
        _ = rows.Read();
        _ = rows.Read();
        return raw.sqlite3_stmt_status(command.Handle, raw.SQLITE_STMTSTATUS_VM_STEP, 0);
    }
}
