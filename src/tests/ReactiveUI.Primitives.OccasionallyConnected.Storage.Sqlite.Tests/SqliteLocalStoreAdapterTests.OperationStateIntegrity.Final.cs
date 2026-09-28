// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Final operation state storage and write journal integrity cases.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The manifest's record protection column.</summary>
    private const string IntegrityManifestColumn = "operation_state_manifest";

    /// <summary>A journal update refuses to sign a row whose original proof has disappeared.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenJournaledStateHasNoOriginalProof_ThenWriteRejectsIt()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using var connection = OpenProtectedIntegrityConnection(database.Path);
        SqliteOperationStateIntegrity.InstallJournal(connection);
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = (SqliteTransaction)transaction;
            command.CommandText = """
                DELETE FROM oc_operation_state_proofs
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                UPDATE oc_outbox_operation_states SET attempt_count = attempt_count + 1
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """;
            _ = await command.ExecuteNonQueryAsync();
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, (SqliteTransaction)transaction);
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

        await using var connection = OpenProtectedIntegrityConnection(database.Path);
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), string.Empty);
        var stored = cipher.ProtectText(invalidManifest, SqliteRecordContext.KeyCheck(), IntegrityManifestColumn);
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = (SqliteTransaction)transaction;
            command.CommandText = "UPDATE oc_metadata SET value = $value WHERE key = 'rxui.localstore.operation_state_manifest';";
            _ = command.Parameters.AddWithValue("$value", stored);
            await Assert.That(await command.ExecuteNonQueryAsync()).IsEqualTo(1);
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, (SqliteTransaction)transaction);
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

        await using var connection = OpenProtectedIntegrityConnection(database.Path);
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = (SqliteTransaction)transaction;
            command.CommandText = "DELETE FROM oc_metadata WHERE key = 'rxui.localstore.operation_state_manifest';";
            await Assert.That(await command.ExecuteNonQueryAsync()).IsEqualTo(1);
        }

        Action write = () => SqliteOperationStateIntegrity.WriteChanges(connection, (SqliteTransaction)transaction);
        await Assert.That(write).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>A valid digest remains readable when its hexadecimal letters use lowercase.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenJournalManifestUsesLowercaseHex_ThenWriteAcceptsIt()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using var connection = OpenProtectedIntegrityConnection(database.Path);
        SqliteOperationStateIntegrity.InstallJournal(connection);
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), string.Empty);
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = (SqliteTransaction)transaction;
            command.CommandText = "SELECT value FROM oc_metadata WHERE key = 'rxui.localstore.operation_state_manifest';";
            var stored = await command.ExecuteScalarAsync() as string;
            await Assert.That(stored).IsNotNull();
            var plaintext = cipher.UnprotectText(stored!, SqliteRecordContext.KeyCheck(), IntegrityManifestColumn);
            command.CommandText = "UPDATE oc_metadata SET value = $value WHERE key = 'rxui.localstore.operation_state_manifest';";
            _ = command.Parameters.AddWithValue(
                "$value",
                cipher.ProtectText(plaintext.ToLowerInvariant(), SqliteRecordContext.KeyCheck(), IntegrityManifestColumn));
            await Assert.That(await command.ExecuteNonQueryAsync()).IsEqualTo(1);
            command.Parameters.Clear();
            command.CommandText = """
                UPDATE oc_outbox_operation_states SET attempt_count = attempt_count + 1
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """;
            await Assert.That(await command.ExecuteNonQueryAsync()).IsEqualTo(1);
        }

        SqliteOperationStateIntegrity.WriteChanges(connection, (SqliteTransaction)transaction);
        await transaction.CommitAsync();

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
    }

    /// <summary>Plaintext connections have no journal proof work to perform.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenPlaintextConnectionWritesJournal_ThenItReturnsWithoutProofWork()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = await connection.BeginTransactionAsync();
        SqliteOperationStateIntegrity.WriteChanges(connection, (SqliteTransaction)transaction);
        await Assert.That(transaction.Connection).IsNotNull();
    }

    /// <summary>Opens a protected connection with the same database scoped cipher as the adapter.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The opened connection.</returns>
    private static SqliteProtectedConnection OpenProtectedIntegrityConnection(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), StoreIdentity);
        var connection = new SqliteProtectedConnection(connectionString, cipher);
        connection.Open();
        return connection;
    }
}
