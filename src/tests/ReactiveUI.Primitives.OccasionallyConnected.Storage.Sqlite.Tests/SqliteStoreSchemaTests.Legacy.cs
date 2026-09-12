// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteStoreSchema"/>.</summary>
/// <content>Frozen historical schema fixtures.</content>
public sealed partial class SqliteStoreSchemaTests
{
    /// <summary>The exact version-two schema, independent from current production schema definitions.</summary>
    private const string LegacySchemaSql = """
        -- Frozen schema v2 from d9f69d3f1d57854ab9e73833adc4a08820cd26e8.

        -- Keep this fixture independent from current schema construction.

        PRAGMA user_version = 2;

        CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);

        CREATE TABLE oc_subscription_identities (
                    store_identity TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    subscription_id TEXT NOT NULL,
                    PRIMARY KEY (store_identity, stream_id));

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

        CREATE TABLE oc_outbox_metadata (
                    store_identity TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (store_identity, operation_id, key),
                    FOREIGN KEY (store_identity, operation_id)
                        REFERENCES oc_outbox (store_identity, operation_id)
                        ON DELETE CASCADE);

        INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '2');
        """;

    /// <summary>The exact version-three schema, independent from current production schema definitions.</summary>
    private const string RemoteApplySchemaSql = """
        -- Frozen schema v3 from remote-apply stage.

        -- Keep this fixture independent from current schema construction.

        PRAGMA user_version = 3;

        CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);

        CREATE TABLE oc_subscription_identities (
                    store_identity TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    subscription_id TEXT NOT NULL,
                    PRIMARY KEY (store_identity, stream_id));

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

        CREATE TABLE oc_outbox_metadata (
                    store_identity TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (store_identity, operation_id, key),
                    FOREIGN KEY (store_identity, operation_id)
                        REFERENCES oc_outbox (store_identity, operation_id)
                        ON DELETE CASCADE);

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

        INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '3');
        """;

    /// <summary>The exact version-four schema, independent from current production schema definitions.</summary>
    private const string LeaseSchemaSql = """
        -- Frozen schema v4 from lease-capable local commit stage.

        -- Keep this fixture independent from current schema construction.

        PRAGMA user_version = 4;

        CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);

        CREATE TABLE oc_subscription_identities (
                    store_identity TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    subscription_id TEXT NOT NULL,
                    PRIMARY KEY (store_identity, stream_id));

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

        CREATE TABLE oc_outbox_metadata (
                    store_identity TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (store_identity, operation_id, key),
                    FOREIGN KEY (store_identity, operation_id)
                        REFERENCES oc_outbox (store_identity, operation_id)
                        ON DELETE CASCADE);

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

        INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '4');
        """;

    /// <summary>Creates the frozen schema from the version-two implementation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void CreateLegacyLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = LegacySchemaSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the frozen schema from the version-three implementation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void CreateRemoteApplySchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = RemoteApplySchemaSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates the frozen schema from the version-four implementation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void CreateLeaseSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = LeaseSchemaSql;
        _ = command.ExecuteNonQuery();
    }
}
