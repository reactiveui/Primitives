// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Checks global unresolved outbox capacity inside the local commit write transaction.</summary>
internal static class SqliteOutboxCapacitySql
{
    /// <summary>The fixed bytes in the operation id, sequence, timestamp, type, payload lengths, and policy.</summary>
    private const int FixedOperationEnvelopeBytes = 68;

    /// <summary>Rejects a local commit that would exceed global unresolved outbox capacity.</summary>
    /// <param name="connection">The active SQLite connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="storeIdentity">The store partition identity.</param>
    /// <param name="operation">The proposed operation.</param>
    /// <param name="options">The optional capacity limits.</param>
    /// <exception cref="QueueCapacityExceededException">The operation cannot be admitted.</exception>
    internal static void EnsureCapacityFor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        OutboxOptions? options)
    {
        if (options is null)
        {
            return;
        }

        var candidateBytes = GetEncodedOperationBytes(operation);
        var (unresolvedCount, unresolvedBytes) = ReadUsage(connection, transaction, storeIdentity);
        if (unresolvedCount < options.MaxOperations && candidateBytes <= options.MaxBytes - unresolvedBytes)
        {
            return;
        }

        throw new QueueCapacityExceededException(
            "The unresolved outbox capacity would be exceeded.",
            candidateBytes <= options.MaxBytes);
    }

    /// <summary>Calculates the immutable operation envelope and metadata bytes.</summary>
    /// <param name="operation">The proposed operation.</param>
    /// <returns>The encoded byte count.</returns>
    private static long GetEncodedOperationBytes(SyncOperation operation)
    {
        var payload = operation.Payload;
        var bytes = checked((long)FixedOperationEnvelopeBytes
            + Encoding.UTF8.GetByteCount(operation.StreamId.Value)
            + Encoding.UTF8.GetByteCount(operation.BaseVersion ?? string.Empty)
            + Encoding.UTF8.GetByteCount(payload.ContractId)
            + Encoding.UTF8.GetByteCount(payload.ContentType)
            + payload.PayloadLength
            + Encoding.UTF8.GetByteCount(payload.PayloadHash));
        foreach (var pair in operation.Metadata)
        {
            bytes = checked(bytes + Encoding.UTF8.GetByteCount(pair.Key) + Encoding.UTF8.GetByteCount(pair.Value));
        }

        return bytes;
    }

    /// <summary>Reads unresolved operation count and bytes from the current write transaction.</summary>
    /// <param name="connection">The active SQLite connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="storeIdentity">The store partition identity.</param>
    /// <returns>The current unresolved operation count and bytes.</returns>
    private static (long Count, long Bytes) ReadUsage(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT COUNT(*), COALESCE(SUM(
                $fixedEnvelopeBytes
                + length(CAST(outbox.stream_id AS BLOB))
                + COALESCE(length(CAST(outbox.base_version AS BLOB)), 0)
                + length(CAST(outbox.payload_contract_id AS BLOB))
                + length(CAST(outbox.payload_content_type AS BLOB))
                + length(CAST(outbox.payload AS BLOB))
                + length(CAST(outbox.payload_hash AS BLOB))
                + (SELECT COALESCE(SUM(
                    length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB))), 0)
                   FROM oc_outbox_metadata AS metadata
                   WHERE metadata.store_identity = outbox.store_identity
                     AND metadata.operation_id = outbox.operation_id)), 0)
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
               AND state.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity
              AND (state.operation_state IS NULL OR state.operation_state NOT IN ($synchronized, $rejected, $deadLettered));
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        _ = command.Parameters.AddWithValue("$fixedEnvelopeBytes", FixedOperationEnvelopeBytes);
        _ = command.Parameters.AddWithValue("$synchronized", (int)SyncOperationState.Synchronized);
        _ = command.Parameters.AddWithValue("$rejected", (int)SyncOperationState.Rejected);
        _ = command.Parameters.AddWithValue("$deadLettered", (int)SyncOperationState.DeadLettered);
        using var reader = command.ExecuteReader();
        _ = reader.Read();
        return (reader.GetInt64(0), reader.GetInt64(1));
    }
}
