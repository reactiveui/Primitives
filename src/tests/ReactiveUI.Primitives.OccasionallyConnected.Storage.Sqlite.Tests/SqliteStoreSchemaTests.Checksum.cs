// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests recorded schema checksums.</summary>
public sealed partial class SqliteStoreSchemaTests
{
    /// <summary>The identity used by checksum initialization tests.</summary>
    private const string ChecksumStoreIdentity = "checksum-store";

    /// <summary>Verifies initialization records a checksum and reopening accepts the intact schema.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitStoreReopens_ThenRecordedChecksumIsVerified()
    {
        using var database = TempDatabase.Create();
        LocalStoreInitialization initialization = new(ChecksumStoreIdentity, SqliteStoreSchema.LocalCommitSchemaVersion, false);
        using (var store = new SqliteLocalCommitStore(database.Path))
        {
            store.Initialize(initialization, CancellationToken.None);
        }

        await using (var connection = OpenRawConnection(database.Path))
        await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
        {
            await Assert.That(SqliteSchemaChecksum.TrySelect(connection, transaction)).StartsWith(SqliteSchemaChecksum.AlgorithmPrefix);
            SqliteSchemaChecksum.Verify(connection, transaction);
        }

        using var reopened = new SqliteLocalCommitStore(database.Path);
        reopened.Initialize(initialization, CancellationToken.None);
    }

    /// <summary>Verifies a changed schema cannot be reopened after a checksum was recorded.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaChangesOutsideStore_ThenReopenFailsClosed()
    {
        using var database = TempDatabase.Create();
        LocalStoreInitialization initialization = new(ChecksumStoreIdentity, SqliteStoreSchema.LocalCommitSchemaVersion, false);
        using (var store = new SqliteLocalCommitStore(database.Path))
        {
            store.Initialize(initialization, CancellationToken.None);
        }

        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE INDEX external_stream_index ON oc_streams (stream_id);";
            _ = await command.ExecuteNonQueryAsync();
        }

        using var reopened = new SqliteLocalCommitStore(database.Path);
        Action action = () => reopened.Initialize(initialization, CancellationToken.None);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies recording an unchanged schema keeps the original checksum.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaChecksumIsRecordedTwice_ThenSecondRecordDoesNotWrite()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateIdentitySchema(connection, transaction);

        await Assert.That(SqliteSchemaChecksum.Record(connection, transaction)).IsTrue();
        await Assert.That(SqliteSchemaChecksum.Record(connection, transaction)).IsFalse();
        SqliteSchemaChecksum.Verify(connection, transaction);
    }

    /// <summary>Verifies a shortened recorded checksum fails validation before the store can reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecordedChecksumIsTruncated_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateIdentitySchema(connection, transaction);
        _ = SqliteSchemaChecksum.Record(connection, transaction);
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "UPDATE oc_metadata SET value = 'sha256:' WHERE key = 'schema_checksum';";
            _ = await command.ExecuteNonQueryAsync();
        }

        Action action = () => SqliteSchemaChecksum.Verify(connection, transaction);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }
}
