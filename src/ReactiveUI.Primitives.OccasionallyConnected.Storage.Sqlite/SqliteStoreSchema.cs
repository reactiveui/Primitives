// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Owns exact SQLite schema definitions shared by local store components.</summary>
internal static class SqliteStoreSchema
{
    /// <summary>The identity-only schema version.</summary>
    internal const int IdentitySchemaVersion = 1;

    /// <summary>The legacy local commit schema version without remote inbox rows.</summary>
    internal const int LegacyLocalCommitSchemaVersion = 2;

    /// <summary>The local commit schema version with remote inbox rows.</summary>
    internal const int RemoteApplySchemaVersion = 3;

    /// <summary>The local commit schema version.</summary>
    internal const int LocalCommitSchemaVersion = 4;

    /// <summary>The metadata key for the schema version.</summary>
    internal const string SchemaVersionKey = "schema_version";

    /// <summary>The metadata table name.</summary>
    internal const string MetadataTableName = "oc_metadata";

    /// <summary>The subscription identity table name.</summary>
    internal const string SubscriptionIdentitiesTableName = "oc_subscription_identities";

    /// <summary>The stream table name.</summary>
    internal const string StreamsTableName = "oc_streams";

    /// <summary>The snapshot table name.</summary>
    internal const string SnapshotsTableName = "oc_snapshots";

    /// <summary>The outbox table name.</summary>
    internal const string OutboxTableName = "oc_outbox";

    /// <summary>The outbox metadata table name.</summary>
    internal const string OutboxMetadataTableName = "oc_outbox_metadata";

    /// <summary>The remote inbox table name.</summary>
    internal const string InboxTableName = "oc_inbox";

    /// <summary>The outbox leases table name.</summary>
    internal const string OutboxLeasesTableName = "oc_outbox_leases";

    /// <summary>The invalid schema exception message.</summary>
    private const string InvalidSchemaMessage = "The SQLite identity schema is invalid.";

    /// <summary>The unsupported metadata schema version exception message.</summary>
    private const string UnsupportedMetadataSchemaVersionMessage = "The SQLite identity metadata schema version is not supported.";

    /// <summary>The SQL definition for the metadata table.</summary>
    private const string MetadataTableSql = "CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";

    /// <summary>The SQL definition for the subscription identity table.</summary>
    private const string SubscriptionIdentitiesTableSql = """
        CREATE TABLE oc_subscription_identities (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id));
        """;

    /// <summary>The SQL definition for the stream table.</summary>
    private const string StreamsTableSql = """
        CREATE TABLE oc_streams (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            next_client_sequence INTEGER NOT NULL,
            server_cursor TEXT NULL,
            PRIMARY KEY (store_identity, stream_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_subscription_identities (store_identity, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the snapshot table.</summary>
    private const string SnapshotsTableSql = """
        CREATE TABLE oc_snapshots (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            format_version INTEGER NOT NULL,
            server_cursor TEXT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            revision INTEGER NOT NULL,
            saved_at_utc TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the outbox table.</summary>
    private const string OutboxTableSql = """
        CREATE TABLE oc_outbox (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_sequence INTEGER NOT NULL,
            timestamp_utc TEXT NOT NULL,
            base_version TEXT NULL,
            operation_type INTEGER NOT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            policy_delivery_guarantee INTEGER NOT NULL,
            policy_durability INTEGER NOT NULL,
            policy_priority INTEGER NOT NULL,
            policy_conflict INTEGER NOT NULL,
            snapshot_revision INTEGER NOT NULL,
            committed_at_utc TEXT NOT NULL,
            commit_fingerprint BLOB NOT NULL,
            PRIMARY KEY (store_identity, operation_id),
            UNIQUE (store_identity, stream_id, client_sequence),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the outbox metadata table.</summary>
    private const string OutboxMetadataTableSql = """
        CREATE TABLE oc_outbox_metadata (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (store_identity, operation_id, key),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the remote inbox table.</summary>
    private const string InboxTableSql = """
        CREATE TABLE oc_inbox (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            server_cursor TEXT NOT NULL,
            committed_at_utc TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id, event_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the outbox leases table.</summary>
    private const string OutboxLeasesTableSql = """
        CREATE TABLE oc_outbox_leases (
            store_identity TEXT NOT NULL,
            lease_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_sequence INTEGER NOT NULL,
            lease_expires_at_utc TEXT NOT NULL,
            lease_member_count INTEGER NOT NULL,
            PRIMARY KEY (store_identity, lease_id, operation_id),
            UNIQUE (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON DELETE CASCADE,
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>Creates schema version one.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void CreateIdentitySchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        SetIdentityUserVersion(connection, transaction);
        CreateMetadataTable(connection, transaction);
        CreateSubscriptionIdentitiesTable(connection, transaction);
        InsertMetadata(connection, transaction, SchemaVersionKey, IdentitySchemaVersion.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Creates schema version three.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void CreateLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        SetLocalCommitUserVersion(connection, transaction);
        CreateMetadataTable(connection, transaction);
        CreateSubscriptionIdentitiesTable(connection, transaction);
        CreateLegacyLocalCommitTables(connection, transaction);
        CreateInboxTable(connection, transaction);
        CreateOutboxLeasesTable(connection, transaction);
        InsertMetadata(connection, transaction, SchemaVersionKey, LocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Migrates an exact identity schema to schema version three.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void MigrateIdentityToLocalCommit(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateIdentitySchema(connection, transaction);
        CreateLegacyLocalCommitTables(connection, transaction);
        CreateInboxTable(connection, transaction);
        CreateOutboxLeasesTable(connection, transaction);
        BackfillStreamsFromIdentities(connection, transaction);
        UpdateMetadata(connection, transaction, SchemaVersionKey, LocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture));
        SetLocalCommitUserVersion(connection, transaction);
    }

    /// <summary>Migrates an exact schema version two database to schema version three.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void MigrateLegacyLocalCommitToCurrent(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateLegacyLocalCommitSchema(connection, transaction);
        CreateInboxTable(connection, transaction);
        CreateOutboxLeasesTable(connection, transaction);
        UpdateMetadata(connection, transaction, SchemaVersionKey, LocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture));
        SetLocalCommitUserVersion(connection, transaction);
    }

    /// <summary>Migrates an exact schema version three database to schema version four.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void MigrateRemoteApplyToCurrent(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateRemoteApplySchema(connection, transaction);
        CreateOutboxLeasesTable(connection, transaction);
        UpdateMetadata(connection, transaction, SchemaVersionKey, LocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture));
        SetLocalCommitUserVersion(connection, transaction);
    }

    /// <summary>Validates an existing schema for the identity facade.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="userVersion">The SQLite user version.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void ValidateExistingSchemaForIdentityFacade(SqliteConnection connection, SqliteTransaction transaction, long userVersion)
    {
        if (userVersion == IdentitySchemaVersion)
        {
            ValidateIdentitySchema(connection, transaction);
            return;
        }

        if (userVersion == LegacyLocalCommitSchemaVersion)
        {
            ValidateLegacyLocalCommitSchema(connection, transaction);
            return;
        }

        if (userVersion == RemoteApplySchemaVersion)
        {
            ValidateRemoteApplySchema(connection, transaction);
            return;
        }

        if (userVersion == LocalCommitSchemaVersion)
        {
            ValidateLocalCommitSchema(connection, transaction);
            return;
        }

        throw new InvalidOperationException("The SQLite identity schema version is not supported.");
    }

    /// <summary>Validates an existing schema for the local commit kernel.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="userVersion">The SQLite user version.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ValidateExistingSchemaForLocalCommit(SqliteConnection connection, SqliteTransaction transaction, long userVersion) =>
        ValidateExistingSchemaForIdentityFacade(connection, transaction, userVersion);

    /// <summary>Validates an exact schema version three database.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void ValidateRemoteApplySchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [InboxTableName, MetadataTableName, OutboxTableName, OutboxMetadataTableName, SnapshotsTableName, StreamsTableName, SubscriptionIdentitiesTableName]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        var schemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (schemaVersion != RemoteApplySchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
        }

        ValidateTableDefinition(connection, transaction, SubscriptionIdentitiesTableName, SubscriptionIdentitiesTableSql);
        ValidateTableDefinition(connection, transaction, StreamsTableName, StreamsTableSql);
        ValidateTableDefinition(connection, transaction, SnapshotsTableName, SnapshotsTableSql);
        ValidateTableDefinition(connection, transaction, OutboxTableName, OutboxTableSql);
        ValidateTableDefinition(connection, transaction, OutboxMetadataTableName, OutboxMetadataTableSql);
        ValidateTableDefinition(connection, transaction, InboxTableName, InboxTableSql);
    }

    /// <summary>Validates an exact identity schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void ValidateIdentitySchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(connection, transaction, [MetadataTableName, SubscriptionIdentitiesTableName]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        var schemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (schemaVersion != IdentitySchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
        }

        ValidateTableDefinition(connection, transaction, SubscriptionIdentitiesTableName, SubscriptionIdentitiesTableSql);
    }

    /// <summary>Validates an exact legacy local commit schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void ValidateLegacyLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [MetadataTableName, OutboxTableName, OutboxMetadataTableName, SnapshotsTableName, StreamsTableName, SubscriptionIdentitiesTableName]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        var schemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (schemaVersion != LegacyLocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
        }

        ValidateTableDefinition(connection, transaction, SubscriptionIdentitiesTableName, SubscriptionIdentitiesTableSql);
        ValidateTableDefinition(connection, transaction, StreamsTableName, StreamsTableSql);
        ValidateTableDefinition(connection, transaction, SnapshotsTableName, SnapshotsTableSql);
        ValidateTableDefinition(connection, transaction, OutboxTableName, OutboxTableSql);
        ValidateTableDefinition(connection, transaction, OutboxMetadataTableName, OutboxMetadataTableSql);
    }

    /// <summary>Validates an exact local commit schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    internal static void ValidateLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [InboxTableName, MetadataTableName, OutboxTableName, OutboxLeasesTableName, OutboxMetadataTableName, SnapshotsTableName, StreamsTableName, SubscriptionIdentitiesTableName]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        var schemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (schemaVersion != LocalCommitSchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
        }

        ValidateTableDefinition(connection, transaction, SubscriptionIdentitiesTableName, SubscriptionIdentitiesTableSql);
        ValidateTableDefinition(connection, transaction, StreamsTableName, StreamsTableSql);
        ValidateTableDefinition(connection, transaction, SnapshotsTableName, SnapshotsTableSql);
        ValidateTableDefinition(connection, transaction, OutboxTableName, OutboxTableSql);
        ValidateTableDefinition(connection, transaction, OutboxLeasesTableName, OutboxLeasesTableSql);
        ValidateTableDefinition(connection, transaction, OutboxMetadataTableName, OutboxMetadataTableSql);
        ValidateTableDefinition(connection, transaction, InboxTableName, InboxTableSql);
    }

    /// <summary>Selects a metadata value.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The metadata value.</returns>
    internal static string SelectMetadata(SqliteConnection connection, SqliteTransaction transaction, string key)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        return SqliteIdentityStoreData.ReadMetadataValue(command.ExecuteScalar());
    }

    /// <summary>Creates local commit tables after identity tables already exist.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    private static void CreateLegacyLocalCommitTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        CreateStreamsTable(connection, transaction);
        CreateSnapshotsTable(connection, transaction);
        CreateOutboxTable(connection, transaction);
        CreateOutboxMetadataTable(connection, transaction);
    }

    /// <summary>Validates the exact owned user table set.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="expectedNames">The expected table names.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    private static void ValidateUserTableNames(SqliteConnection connection, SqliteTransaction transaction, string[] expectedNames)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name;";
        using var reader = command.ExecuteReader();
        var found = 0;
        while (reader.Read())
        {
            if (found >= expectedNames.Length || reader.GetString(0) != expectedNames[found])
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
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="expectedSql">The expected SQL definition.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    private static void ValidateTableDefinition(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        string expectedSql)
    {
        var actualSql = ReadTableDefinition(connection, transaction, tableName);
        if (TextEqualsOrdinalIgnoreCase(actualSql, NormalizeCreateTableSql(expectedSql)))
        {
            return;
        }

        throw new InvalidOperationException(InvalidSchemaMessage);
    }

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
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The normalized table definition.</returns>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    private static string ReadTableDefinition(SqliteConnection connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = $name;";
        _ = command.Parameters.AddWithValue("$name", tableName);
        if (command.ExecuteScalar() is string tableSql)
        {
            return NormalizeCreateTableSql(tableSql);
        }

        throw new InvalidOperationException(InvalidSchemaMessage);
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

    /// <summary>Inserts a metadata entry.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    private static void InsertMetadata(SqliteConnection connection, SqliteTransaction transaction, string key, string value)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ($key, $value);";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue("$value", value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Updates a metadata entry.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    /// <exception cref="InvalidOperationException">The SQLite schema state is invalid.</exception>
    private static void UpdateMetadata(SqliteConnection connection, SqliteTransaction transaction, string key, string value)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE oc_metadata SET value = $value WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue("$value", value);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite identity metadata is incomplete.");
    }

    /// <summary>Sets schema version one.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void SetIdentityUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version = 1;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets schema version four.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void SetLocalCommitUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version = 4;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the metadata table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateMetadataTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = MetadataTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the subscription identity table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSubscriptionIdentitiesTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SubscriptionIdentitiesTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the streams table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateStreamsTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = StreamsTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the snapshots table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSnapshotsTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SnapshotsTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the outbox table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateOutboxTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = OutboxTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the outbox metadata table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateOutboxMetadataTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = OutboxMetadataTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the inbox table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateInboxTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = InboxTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the outbox leases table.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateOutboxLeasesTable(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = OutboxLeasesTableSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Backfills stream rows from existing subscription identities.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void BackfillStreamsFromIdentities(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_streams
                (store_identity, stream_id, subscription_id, next_client_sequence, server_cursor)
            SELECT store_identity, stream_id, subscription_id, 1, NULL
            FROM oc_subscription_identities;
            """;
        _ = command.ExecuteNonQuery();
    }
}
