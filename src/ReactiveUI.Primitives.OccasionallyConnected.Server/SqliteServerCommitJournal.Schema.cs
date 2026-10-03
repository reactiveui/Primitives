// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides SQLite schema and connection helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Sets the current SQLite user version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void SetUserVersion(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA user_version = 1;");
        _ = command.Execute();
    }

    /// <summary>Creates the metadata table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateMetadataTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(MetadataTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the stream table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateStreamsTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(StreamsTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the ledger table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateLedgerTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(LedgerTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the conflicts table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateConflictsTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(ConflictsTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the events table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateEventsTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(EventsTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the event metadata table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateEventMetadataTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(EventMetadataTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the subscription acknowledgement table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSubscriptionsTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(SubscriptionsTableSql);
        _ = command.Execute();
    }

    /// <summary>Creates the subscription offer table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSubscriptionOffersTable(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(SubscriptionOffersTableSql);
        _ = command.Execute();
    }

    /// <summary>Reads the retained event count.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The event count.</returns>
    private static int ReadEventCount(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM oc_server_journal_events;");
        return ReadCount(command.Scalar(), "The SQLite server journal count is invalid.");
    }

    /// <summary>Returns whether user tables exist.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether user tables exist.</returns>
    private static bool HasUserTables(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';");
        return ReadCount(command.Scalar(), "The SQLite server journal count is invalid.") > 0;
    }

    /// <summary>Gets the SQLite schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static long GetUserVersion(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA user_version;");
        return ReadStorage<long>(command.Scalar(), "The SQLite server journal schema version could not be read.");
    }

    /// <summary>Validates the exact owned user table set.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="expectedNames">The expected table names.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateUserTableNames(SqliteDatabase connection, SqliteTransaction transaction, string[] expectedNames)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name;");
        using var reader = command.Query();
        var found = 0;
        while (reader.Read())
        {
            if (found >= expectedNames.Length)
            {
                throw new InvalidOperationException(InvalidSchemaMessage);
            }

            if (ReadString(reader, 0, InvalidSchemaMessage) != expectedNames[found])
            {
                throw new InvalidOperationException(InvalidSchemaMessage);
            }

            found++;
        }

        if (found == expectedNames.Length)
        {
            return;
        }

        throw new InvalidOperationException(InvalidSchemaMessage);
    }

    /// <summary>Validates that a table uses the expected SQL definition.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="expectedSql">The expected SQL definition.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateTableDefinition(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string tableName,
        string expectedSql) =>
        _ = TextEqualsOrdinalIgnoreCase(ReadTableDefinition(connection, transaction, tableName), NormalizeCreateTableSql(expectedSql))
            ? true
            : throw new InvalidOperationException(InvalidSchemaMessage);

    /// <summary>Compares schema text without a content-dependent early return.</summary>
    /// <param name="left">The first normalized definition.</param>
    /// <param name="right">The second normalized definition.</param>
    /// <returns>Whether the definitions match, ignoring ordinal case.</returns>
    private static bool TextEqualsOrdinalIgnoreCase(string left, string right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var result = 0;
        for (var index = 0; index < left.Length; index++)
        {
            result |= char.ToUpperInvariant(left[index]) ^ char.ToUpperInvariant(right[index]);
        }

        return result == 0;
    }

    /// <summary>Reads a table definition from SQLite metadata.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The normalized table definition.</returns>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static string ReadTableDefinition(SqliteDatabase connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = $name;");
        _ = command.Bind("$name", tableName);
        return NormalizeCreateTableSql(ReadStorage<string>(command.Scalar(), InvalidSchemaMessage));
    }

    /// <summary>Normalizes create-table SQL for schema comparison.</summary>
    /// <param name="sql">The SQL text.</param>
    /// <returns>The normalized SQL text.</returns>
    private static string NormalizeCreateTableSql(string sql)
    {
        var builder = new StringBuilder(sql.Length);
        var pendingSpace = false;
        foreach (var character in sql)
        {
            if (char.IsWhiteSpace(character))
            {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace)
            {
                _ = builder.Append(' ');
                pendingSpace = false;
            }

            _ = builder.Append(character);
        }

        return builder.ToString().TrimEnd(';');
    }

    /// <summary>Applies the connection busy timeout.</summary>
    /// <param name="connection">The open connection.</param>
    private static void ConfigureBusyTimeout(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        connection.SetBusyTimeout(SqliteDatabase.DefaultBusyTimeoutMilliseconds);
    }

    /// <summary>Applies per-connection settings required before operational transactions.</summary>
    /// <param name="connection">The open connection.</param>
    private static void ConfigureOperationalConnection(SqliteDatabase connection)
    {
        using (var foreignKeysCommand = connection.CreateStatement())
        {
            foreignKeysCommand.SetSql("PRAGMA foreign_keys = ON;");
            _ = foreignKeysCommand.Execute();
        }

        using (var synchronousCommand = connection.CreateStatement())
        {
            synchronousCommand.SetSql("PRAGMA synchronous = FULL;");
            _ = synchronousCommand.Execute();
        }

        using (var verifyForeignKeysCommand = connection.CreateStatement())
        {
            verifyForeignKeysCommand.SetSql("PRAGMA foreign_keys;");
            VerifyForeignKeys(verifyForeignKeysCommand.Scalar());
        }

        using var verifySynchronousCommand = connection.CreateStatement();
        verifySynchronousCommand.SetSql("PRAGMA synchronous;");
        VerifyFullSynchronous(verifySynchronousCommand.Scalar());
    }

    /// <summary>Applies durability pragmas after schema validation.</summary>
    /// <param name="connection">The open connection.</param>
    private static void ConfigureDurability(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        command.SetSql("PRAGMA journal_mode = WAL;");
        VerifyWalJournalMode(command.Scalar());
    }

    /// <summary>Verifies SQLite enabled foreign key enforcement for the current connection.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void VerifyForeignKeys(object? value) =>
        ThrowIfFalse(
            ReadStorage<long>(value, "SQLite did not enable foreign key enforcement for the server journal.") == 1,
            "SQLite did not enable foreign key enforcement for the server journal.");

    /// <summary>Verifies SQLite accepted WAL journaling.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void VerifyWalJournalMode(object? value) =>
        ThrowIfFalse(
            string.Equals(ReadStorage<string>(value, "SQLite did not enable WAL journaling for the server journal."), "wal", StringComparison.OrdinalIgnoreCase),
            "SQLite did not enable WAL journaling for the server journal.");

    /// <summary>Verifies SQLite accepted FULL synchronous writes.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void VerifyFullSynchronous(object? value) =>
        ThrowIfFalse(
            ReadStorage<long>(value, "SQLite did not enable FULL synchronous writes for the server journal.") == SqliteFullSynchronous,
            "SQLite did not enable FULL synchronous writes for the server journal.");

    /// <summary>Gets the directory that must exist before opening a database file.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>The directory to create.</returns>
    private static string GetDirectoryForCreate(string databasePath) => Path.GetDirectoryName(databasePath)!;

    /// <summary>Rejects unsupported non-file SQLite path forms.</summary>
    /// <param name="databasePath">The requested database path.</param>
    /// <exception cref="ArgumentException">Thrown when the database path is unsupported.</exception>
    private static void ThrowIfUnsupportedPath(string databasePath)
    {
        if (!string.Equals(databasePath, ":memory:", StringComparison.OrdinalIgnoreCase)
            && !databasePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new ArgumentException("The SQLite server journal database path must identify a real file.", nameof(databasePath));
    }

    /// <summary>Rejects blank text.</summary>
    /// <param name="value">The value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">Thrown when the argument is invalid.</exception>
    private static void ThrowIfBlank(string value, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            _ = TextEncoding.GetByteCount(value);
            return;
        }

        throw new ArgumentException("The SQLite server journal database path cannot be empty.", parameterName);
    }
}
