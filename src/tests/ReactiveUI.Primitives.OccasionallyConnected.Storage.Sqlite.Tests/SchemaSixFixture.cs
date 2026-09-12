// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Creates the frozen version-six SQLite schema used by migration tests.</summary>
internal static class SchemaSixFixture
{
    /// <summary>Frozen schema SQL from 6de8a9d:SqliteStoreSchema.cs, before receive-inclusion sidecars existed.</summary>
    private const string SchemaSql = """
        PRAGMA user_version = 6;
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
        CREATE TABLE oc_outbox_operation_states (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            operation_state INTEGER NOT NULL,
            attempt_count INTEGER NOT NULL,
            changed_at_utc TEXT NOT NULL,
            reason_code TEXT NULL,
            retry_started_utc TEXT NULL,
            retry_due_utc TEXT NULL,
            retry_previous_delay_ticks INTEGER NULL,
            retry_transient_attempt_count INTEGER NULL,
            retry_authentication_state INTEGER NULL,
            retry_credentials_version TEXT NULL,
            PRIMARY KEY (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
        CREATE TABLE oc_outbox_authoritative_mutations (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON DELETE CASCADE);
        CREATE TABLE oc_snapshot_authoritative_states (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_snapshots (store_identity, stream_id)
                ON DELETE CASCADE);
        INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '6');
        """;

    /// <summary>Creates the historical schema inside the supplied transaction.</summary>
    /// <param name="connection">The open SQLite connection.</param>
    /// <param name="transaction">The transaction to populate.</param>
    internal static void Create(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SchemaSql;
        _ = command.ExecuteNonQuery();
    }
}
