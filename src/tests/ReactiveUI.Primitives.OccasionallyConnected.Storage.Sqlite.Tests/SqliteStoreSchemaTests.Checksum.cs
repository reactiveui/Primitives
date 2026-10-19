// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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

        using (var connection = OpenRawConnection(database.Path))
        using (var transaction = connection.BeginTransaction())
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

        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("CREATE INDEX external_stream_index ON oc_streams (stream_id);");
            _ = command.Execute();
        }

        using var reopened = new SqliteLocalCommitStore(database.Path);
        Action action = () => reopened.Initialize(initialization, CancellationToken.None);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a complete V1 database without its checksum is rejected without repair.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExistingVersionOneSchemaHasNoChecksum_ThenReopenFailsWithoutWriting()
    {
        using var database = TempDatabase.Create();
        LocalStoreInitialization initialization = new(ChecksumStoreIdentity, SqliteStoreSchema.LocalCommitSchemaVersion, false);
        using (var store = new SqliteLocalCommitStore(database.Path))
        {
            store.Initialize(initialization, CancellationToken.None);
        }

        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("DELETE FROM oc_metadata WHERE key = 'schema_checksum';");
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        using var reopened = new SqliteLocalCommitStore(database.Path);
        Action initialize = () => reopened.Initialize(initialization, CancellationToken.None);
        await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();

        using var inspection = OpenRawConnection(database.Path);
        using var transaction = inspection.BeginTransaction();
        await Assert.That(SelectUserVersion(inspection, transaction)).IsEqualTo(SqliteStoreSchema.LocalCommitSchemaVersion);
        await Assert.That(SqliteSchemaChecksum.TrySelect(inspection, transaction)).IsNull();
        SqliteStoreSchema.ValidateLocalCommitSchema(inspection, transaction);
    }

    /// <summary>Verifies recording an unchanged schema keeps the original checksum.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaChecksumIsRecordedTwice_ThenSecondRecordDoesNotWrite()
    {
        using var database = TempDatabase.Create();
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);

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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        _ = SqliteSchemaChecksum.Record(connection, transaction);
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("UPDATE oc_metadata SET value = 'sha256:' WHERE key = 'schema_checksum';");
            _ = command.Execute();
        }

        Action action = () => SqliteSchemaChecksum.Verify(connection, transaction);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }
}
