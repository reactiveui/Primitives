// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteStoreSchema"/>.</summary>
public sealed partial class SqliteStoreSchemaTests
{
    /// <summary>Verifies local commit schema validation rejects stale metadata version drift.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.IdentitySchemaVersion);

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies legacy local commit schema validation rejects current metadata version drift.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLegacyLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        CreateLegacyLocalCommitSchema(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.LocalCommitSchemaVersion);

        Action action = () => SqliteStoreSchema.ValidateLegacyLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing table SQL metadata is rejected as schema corruption.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTableDefinitionIsMissingSqlText_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        ClearTableDefinition(connection, transaction, SqliteStoreSchema.MetadataTableName);

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies migration fails closed when the metadata version row disappears during update.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMigrationMetadataVersionUpdateAffectsNoRows_ThenMigrationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
        {
            SqliteStoreSchema.CreateIdentitySchema(connection, transaction);
            await transaction.CommitAsync();
        }

        CreateMetadataUpdateIgnoreTrigger(connection);
        await using var migrationTransaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        Action action = () => SqliteStoreSchema.MigrateIdentityToLocalCommit(connection, migrationTransaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema version six validates and migrates by adding receive inclusion sidecars.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeLocalCommitSchemaMigrates_ThenReceiveInclusionTableIsCreated()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SchemaSixFixture.Create(connection, transaction);

        _ = AssertNoThrow(() => SqliteStoreSchema.ValidateExistingSchemaForLocalCommit(
            connection,
            transaction,
            SqliteStoreSchema.AuthoritativeLocalCommitSchemaVersion));
        SqliteStoreSchema.MigrateAuthoritativeLocalCommitToCurrent(connection, transaction);

        await Assert.That(SelectUserVersion(connection, transaction)).IsEqualTo(SqliteStoreSchema.LocalCommitSchemaVersion);
        await Assert.That(SqliteStoreSchema.SelectMetadata(connection, transaction, SqliteStoreSchema.SchemaVersionKey))
            .IsEqualTo(SqliteStoreSchema.LocalCommitSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Assert.That(TableExists(connection, transaction, SqliteStoreSchema.OutboxReceiveInclusionsTableName)).IsTrue();
        SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
    }

    /// <summary>Verifies schema version six validation rejects mismatched metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SchemaSixFixture.Create(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.LocalCommitSchemaVersion);

        Action action = () => SqliteStoreSchema.ValidateAuthoritativeLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema version seven validates and migrates by adding durable payload quarantine markers.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPreQuarantineLocalCommitSchemaMigrates_ThenPayloadQuarantineTableIsCreated()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        CreatePreQuarantineLocalCommitSchema(connection, transaction);

        _ = AssertNoThrow(() => SqliteStoreSchema.ValidateExistingSchemaForLocalCommit(
            connection,
            transaction,
            SqliteStoreSchema.PreQuarantineLocalCommitSchemaVersion));
        _ = AssertNoThrow(() => SqliteStoreSchema.ValidateExistingSchemaForIdentityFacade(
            connection,
            transaction,
            SqliteStoreSchema.PreQuarantineLocalCommitSchemaVersion));
        SqliteStoreSchema.MigratePreQuarantineLocalCommitToCurrent(connection, transaction);

        await Assert.That(SelectUserVersion(connection, transaction)).IsEqualTo(SqliteStoreSchema.LocalCommitSchemaVersion);
        await Assert.That(SqliteStoreSchema.SelectMetadata(connection, transaction, SqliteStoreSchema.SchemaVersionKey))
            .IsEqualTo(SqliteStoreSchema.LocalCommitSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Assert.That(TableExists(connection, transaction, SqliteStoreSchema.PayloadQuarantineTableName)).IsTrue();
        SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
    }

    /// <summary>Verifies schema version seven validation rejects mismatched metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPreQuarantineLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        CreatePreQuarantineLocalCommitSchema(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.LocalCommitSchemaVersion);

        Action action = () => SqliteStoreSchema.ValidatePreQuarantineLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a schema version seven local commit schema from the current schema definitions.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void CreatePreQuarantineLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        DropPayloadQuarantineTable(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.PreQuarantineLocalCommitSchemaVersion);
        SetPreQuarantineUserVersion(connection, transaction);
    }

    /// <summary>Executes an action and returns true when it does not throw.</summary>
    /// <param name="action">The action.</param>
    /// <returns>True when the action completes.</returns>
    private static bool AssertNoThrow(Action action)
    {
        action();
        return true;
    }

    /// <summary>Sets the stored metadata schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="schemaVersion">The schema version.</param>
    private static void SetMetadataVersion(SqliteConnection connection, SqliteTransaction transaction, int schemaVersion)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE oc_metadata SET value = $value WHERE key = 'schema_version';";
        _ = command.Parameters.AddWithValue("$value", schemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the payload quarantine table from a schema fixture.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void DropPayloadQuarantineTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DROP TABLE oc_payload_quarantine;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the SQLite user version for the pre-quarantine schema fixture.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void SetPreQuarantineUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version = 7;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Removes a table definition from SQLite metadata to simulate catalog corruption.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    private static void ClearTableDefinition(SqliteConnection connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            PRAGMA writable_schema = ON;
            UPDATE sqlite_master SET sql = NULL WHERE type = 'table' AND name = $tableName;
            PRAGMA writable_schema = OFF;
            """;
        _ = command.Parameters.AddWithValue("$tableName", tableName);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that makes the metadata version update affect no rows.</summary>
    /// <param name="connection">The connection.</param>
    private static void CreateMetadataUpdateIgnoreTrigger(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_metadata_version_update_ignore
            BEFORE UPDATE OF value ON oc_metadata
            WHEN OLD.key = 'schema_version'
            BEGIN
                DELETE FROM oc_metadata WHERE key = OLD.key;
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Selects the current SQLite user version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected user version.</exception>
    private static long SelectUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version;";
        return command.ExecuteScalar() is long value
            ? value
            : throw new InvalidOperationException("SQLite user_version returned an unexpected value.");
    }

    /// <summary>Returns whether a user table exists.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>Whether the table exists.</returns>
    private static bool TableExists(SqliteConnection connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $tableName;";
        _ = command.Parameters.AddWithValue("$tableName", tableName);
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>Opens a raw SQLite connection with pooling disabled.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    private static SqliteConnection OpenRawConnection(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    /// <summary>Temporary database file helper.</summary>
    private sealed class TempDatabase : IDisposable
    {
        /// <summary>The temporary directory path.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="TempDatabase"/> class.</summary>
        /// <param name="directory">The temporary directory path.</param>
        private TempDatabase(string directory)
        {
            _directory = directory;
            Path = System.IO.Path.Combine(directory, "local.db");
        }

        /// <summary>Gets the SQLite database path.</summary>
        public string Path { get; }

        /// <summary>Creates a new temporary database helper.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase Create()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite-local", Guid.NewGuid().ToString("N"));
            _ = System.IO.Directory.CreateDirectory(directory);
            return new(directory);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (!System.IO.Directory.Exists(_directory))
            {
                return;
            }

            System.IO.Directory.Delete(_directory, true);
        }
    }
}
