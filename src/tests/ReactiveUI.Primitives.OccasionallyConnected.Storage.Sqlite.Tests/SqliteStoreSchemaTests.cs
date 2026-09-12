// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteStoreSchema"/>.</summary>
public sealed class SqliteStoreSchemaTests
{
    /// <summary>Verifies local commit schema validation rejects stale metadata version drift.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = connection.BeginTransaction();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        SetMetadataVersion(connection, transaction, SqliteStoreSchema.IdentitySchemaVersion);

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing table SQL metadata is rejected as schema corruption.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTableDefinitionIsMissingSqlText_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = connection.BeginTransaction();
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
        await using (var transaction = connection.BeginTransaction())
        {
            SqliteStoreSchema.CreateIdentitySchema(connection, transaction);
            transaction.Commit();
        }

        CreateMetadataUpdateIgnoreTrigger(connection);
        await using var migrationTransaction = connection.BeginTransaction();
        Action action = () => SqliteStoreSchema.MigrateIdentityToLocalCommit(connection, migrationTransaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
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
