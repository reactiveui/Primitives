// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Backfills immutable plaintext charges once when an existing version-one store is opened.</summary>
internal static class SqliteOutboxCapacityMigration
{
    /// <summary>Upgrades legacy stores after their schema checksum and record protection have been verified.</summary>
    /// <param name="connection">The initialized connection.</param>
    /// <param name="transaction">The initialization write transaction.</param>
    /// <param name="maximumPayloadBytes">The configured payload materialization bound.</param>
    /// <param name="cancellationToken">The initialization cancellation token.</param>
    internal static void EnsureInitialized(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        long maximumPayloadBytes,
        CancellationToken cancellationToken)
    {
        if (SqliteStoreSchema.HasOutboxCapacitySchema(connection, transaction))
        {
            return;
        }

        SqliteStoreSchema.CreateOutboxCapacitySchema(connection, transaction);
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT outbox.operation_id, outbox.client_sequence, outbox.timestamp_utc, outbox.base_version, outbox.operation_type,
                   outbox.payload_contract_id, outbox.payload_schema_version, outbox.payload_content_type, outbox.payload, outbox.payload_hash,
                   outbox.policy_delivery_guarantee, outbox.policy_durability, outbox.policy_priority, outbox.policy_conflict,
                   state.operation_state, outbox.rowid,
                   typeof(outbox.payload_contract_id), length(CAST(outbox.payload_contract_id AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_contract_id AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload_schema_version), outbox.payload_schema_version,
                   typeof(outbox.payload_content_type), length(CAST(outbox.payload_content_type AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_content_type AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload), length(CAST(outbox.payload AS BLOB)), IFNULL(substr(CAST(outbox.payload AS BLOB), 1, 4096), x''),
                   typeof(outbox.payload_hash), length(CAST(outbox.payload_hash AS BLOB)), IFNULL(substr(CAST(outbox.payload_hash AS BLOB), 1, 4100), x''),
                   outbox.store_identity, outbox.stream_id
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity AND state.operation_id = outbox.operation_id;
            """);
        using var reader = command.Query();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            const int StoreIdentityIndex = 30;
            const int StreamIdIndex = 31;
            var identity = reader.GetString(StoreIdentityIndex);
            var operation = SqliteLocalCommitSql.ReadPendingOperation(
                connection,
                transaction,
                identity,
                new(reader.GetString(StreamIdIndex)),
                reader,
                maximumPayloadBytes);
            SqliteOutboxCapacitySql.InsertCharge(connection, transaction, identity, operation);
        }
    }
}
