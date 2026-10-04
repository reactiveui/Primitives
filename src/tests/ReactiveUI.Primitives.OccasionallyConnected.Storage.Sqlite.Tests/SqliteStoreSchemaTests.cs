// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("DROP TABLE oc_payload_quarantine;");
            _ = command.Execute();
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
        using var connection = OpenRawConnection(database.Path);
        using var transaction = connection.BeginTransaction();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("""
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master SET sql = sql || ' CHECK (1)' WHERE type = 'table' AND name = 'oc_metadata';
                PRAGMA writable_schema = OFF;
                """);
            _ = command.Execute();
        }

        Action action = () => SqliteStoreSchema.ValidateLocalCommitSchema(connection, transaction);
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Sets the stored metadata schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="schemaVersion">The schema version.</param>
    private static void SetMetadataVersion(SqliteDatabase connection, SqliteTransaction transaction, int schemaVersion)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("UPDATE oc_metadata SET value = $value WHERE key = 'schema_version';");
        _ = command.Bind("$value", schemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        _ = command.Execute();
    }

    /// <summary>Removes a table definition from SQLite metadata to simulate catalog corruption.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    private static void ClearTableDefinition(SqliteDatabase connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            PRAGMA writable_schema = ON;
            UPDATE sqlite_master SET sql = NULL WHERE type = 'table' AND name = $tableName;
            PRAGMA writable_schema = OFF;
            """);
        _ = command.Bind("$tableName", tableName);
        _ = command.Execute();
    }

    /// <summary>Selects the current SQLite user version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected user version.</exception>
    private static long SelectUserVersion(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA user_version;");
        return command.Scalar() is long value
            ? value
            : throw new InvalidOperationException("SQLite user_version returned an unexpected value.");
    }

    /// <summary>Returns whether a user table exists.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>Whether the table exists.</returns>
    private static bool TableExists(SqliteDatabase connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $tableName;");
        _ = command.Bind("$tableName", tableName);
        return Convert.ToInt64(command.Scalar(), System.Globalization.CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>Opens a raw SQLite connection with pooling disabled.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    private static SqliteDatabase OpenRawConnection(string path)
    {
        var connection = new SqliteDatabase(path);

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
            var directory = System.IO.Path.Combine(PhysicalTempDirectory.GetRoot(), "rxui-oc-sqlite-local", Guid.NewGuid().ToString("N"));
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
