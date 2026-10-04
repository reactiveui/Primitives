// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Final operation state storage and write journal integrity cases.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The manifest's record protection column.</summary>
    private const string IntegrityManifestColumn = "operation_state_manifest";

    /// <summary>An ignored proof or manifest write cannot advance the trusted state set.</summary>
    /// <param name="manifest">Whether to reject the manifest rather than the row proof.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IgnoredProofMutationRollsBackProtectedCommit(bool manifest)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(1), CreateSnapshotMutation(0), CancellationToken.None);
        using (var connection = OpenRawConnection(database.Path))
        {
            connection.Execute(manifest
                ? """
                  CREATE TRIGGER reject_manifest BEFORE UPDATE ON oc_metadata
                  WHEN NEW.key = 'rxui.localstore.operation_state_manifest'
                  BEGIN SELECT RAISE(IGNORE); END;
                  """
                : """
                  CREATE TRIGGER reject_proof BEFORE INSERT ON oc_operation_state_proofs
                  BEGIN SELECT RAISE(IGNORE); END;
                  """);
        }

        var operation = CreateOperation(SecondClientSequence);
        await Assert.That(async () => await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(1), CancellationToken.None))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>A proof trigger cannot change canonical state after signing and still extend cached global trust.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProofTriggerSideEffectsRollBackProtectedCommit()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        using (var connection = OpenRawConnection(database.Path))
        {
            connection.Execute("""
                CREATE TRIGGER proof_changes_state AFTER INSERT ON oc_operation_state_proofs BEGIN
                    UPDATE oc_outbox_operation_states SET attempt_count = attempt_count + 1
                    WHERE operation_id = NEW.operation_id;
                END;
                """);
        }

        var operation = CreateOperation(1);
        await Assert.That(async () => await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        await Assert.That(await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>A journal update refuses to sign a row whose original proof has disappeared.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenJournaledStateHasNoOriginalProof_ThenWriteRejectsIt()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        using var connection = OpenProtectedIntegrityConnection(database.Path);
        SqliteOperationStateIntegrity.InstallJournal(connection);
        using var transaction = connection.BeginTransaction();
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("""
                DELETE FROM oc_operation_state_proofs
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                UPDATE oc_outbox_operation_states SET attempt_count = attempt_count + 1
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """);
            _ = command.Execute();
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
        await Assert.That(write).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>The write journal rejects authenticated manifest text with an invalid count or digest.</summary>
    /// <param name="invalidManifest">The plaintext manifest to authenticate.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments("0")]
    [Arguments("0:0")]
    [Arguments("/:0000000000000000000000000000000000000000000000000000000000000000")]
    [Arguments("x:0000000000000000000000000000000000000000000000000000000000000000")]
    [Arguments("9223372036854775808:00000000000000000000000000000000"
               + "00000000000000000000000000000000")]
    [Arguments("0:000000000000000000000000000000000000000000000000000000000000000Z")]
    public async Task WhenJournalManifestHasInvalidPlaintext_ThenWriteRejectsIt(string invalidManifest)
    {
        using var database = TempDatabase.Create();
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        }

        using var connection = OpenProtectedIntegrityConnection(database.Path);
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), string.Empty);
        var stored = cipher.ProtectText(invalidManifest, SqliteRecordContext.KeyCheck(), IntegrityManifestColumn);
        using var transaction = connection.BeginTransaction();
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("UPDATE oc_metadata SET value = $value WHERE key = 'rxui.localstore.operation_state_manifest';");
            _ = command.Bind("$value", stored);
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
        await Assert.That(write).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>A missing manifest is rejected before the journal signs any changes.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenJournalManifestIsMissing_ThenWriteRejectsIt()
    {
        using var database = TempDatabase.Create();
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        }

        using var connection = OpenProtectedIntegrityConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("DELETE FROM oc_metadata WHERE key = 'rxui.localstore.operation_state_manifest';");
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
        await Assert.That(write).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>A valid digest remains readable when its hexadecimal letters use lowercase.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenJournalManifestUsesLowercaseHex_ThenWriteAcceptsIt()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        using var connection = OpenProtectedIntegrityConnection(database.Path);
        SqliteOperationStateIntegrity.InstallJournal(connection);
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), string.Empty);
        using var transaction = connection.BeginTransaction();
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("SELECT value FROM oc_metadata WHERE key = 'rxui.localstore.operation_state_manifest';");
            var stored = command.Scalar() as string;
            await Assert.That(stored).IsNotNull();
            var plaintext = cipher.UnprotectText(stored!, SqliteRecordContext.KeyCheck(), IntegrityManifestColumn);
            command.SetSql("UPDATE oc_metadata SET value = $value WHERE key = 'rxui.localstore.operation_state_manifest';");
            _ = command.Bind(
                "$value",
                cipher.ProtectText(plaintext.ToLowerInvariant(), SqliteRecordContext.KeyCheck(), IntegrityManifestColumn));
            await Assert.That(command.Execute()).IsEqualTo(1);
            command.ClearBindings();
            command.SetSql("""
                UPDATE oc_outbox_operation_states SET attempt_count = attempt_count + 1
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """);
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
        transaction.Commit();

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
    }

    /// <summary>Plaintext connections have no journal proof work to perform.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenPlaintextConnectionWritesJournal_ThenItReturnsWithoutProofWork()
    {
        using var database = TempDatabase.Create();
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
        await Assert.That(transaction.Connection).IsNotNull();
    }

    /// <summary>Opens a protected connection with the same database scoped cipher as the adapter.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The opened connection.</returns>
    private static SqliteDatabase OpenProtectedIntegrityConnection(string path)
    {
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), StoreIdentity);
        return SqliteLocalCommitConnection.OpenConnection(path, cipher);
    }
}
