// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteStoreSchema"/>.</summary>
public sealed partial class SqliteStoreSchemaTests
{
    /// <summary>An unsupported version used to test V1 metadata validation.</summary>
    private const int UnsupportedSchemaVersion = 2;

    /// <summary>Verifies the first release creates the complete local commit schema as V1.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitSchemaIsCreated_ThenAllStateTablesStartAtVersionOne()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);

        await Assert.That(SelectUserVersion(connection, transaction)).IsEqualTo(1);
        await Assert.That(SqliteStoreSchema.SelectMetadata(connection, transaction, SqliteStoreSchema.SchemaVersionKey)).IsEqualTo("1");
        await Assert.That(TableExists(connection, transaction, SqliteStoreSchema.OutboxReceiveInclusionsTableName)).IsTrue();
        await Assert.That(TableExists(connection, transaction, SqliteStoreSchema.PayloadQuarantineTableName)).IsTrue();
        await Assert.That(TableExists(connection, transaction, "oc_operation_state_proofs")).IsTrue();
        SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
    }

    /// <summary>Verifies V1 validation rejects metadata version drift.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitMetadataVersionDrifts_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        SetMetadataVersion(connection, transaction, UnsupportedSchemaVersion);

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies V1 validation rejects missing table SQL metadata.</summary>
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

    /// <summary>Verifies a missing owned table is rejected during schema validation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnedTableIsMissing_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "DROP TABLE oc_payload_quarantine;";
            _ = await command.ExecuteNonQueryAsync();
        }

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a changed owned table definition is rejected even when its name remains intact.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnedTableDefinitionChanges_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET sql = sql || ' CHECK (1)' WHERE type = 'table' AND name = 'oc_metadata';
                PRAGMA writable_schema = OFF;
                """;
            _ = await command.ExecuteNonQueryAsync();
        }

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
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
