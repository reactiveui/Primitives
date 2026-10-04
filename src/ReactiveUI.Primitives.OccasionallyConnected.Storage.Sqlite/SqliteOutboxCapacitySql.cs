// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

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
        SqliteDatabase connection,
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
        SqliteOutboxCapacityIntegrity.VerifyWhenChanged(connection, transaction);
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
    internal static long GetEncodedOperationBytes(SyncOperation operation)
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
    internal static (long Count, long Bytes) ReadUsage(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT operation_count, encoded_bytes FROM oc_outbox_capacity_usage
            WHERE store_identity = $storeIdentity;
            """);
        _ = command.Bind("$storeIdentity", storeIdentity);
        using var reader = command.Query();
        return reader.Read() ? (reader.GetInt64(0), reader.GetInt64(1)) : (0, 0);
    }

    /// <summary>Records the plaintext immutable charge in the same transaction as the operation.</summary>
    /// <param name="connection">The active connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <param name="storeIdentity">The store partition.</param>
    /// <param name="operation">The immutable operation.</param>
    internal static void InsertCharge(SqliteDatabase connection, SqliteTransaction transaction, string storeIdentity, SyncOperation operation)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_outbox_capacity_charges (store_identity, operation_id, encoded_bytes, proof, unresolved)
            VALUES ($storeIdentity, $operationId, $bytes, $proof, COALESCE(
                (SELECT operation_state NOT IN (4, 5, 6) FROM oc_outbox_operation_states
                 WHERE store_identity = $storeIdentity AND operation_id = $operationId), 1));
            """);
        _ = command.Bind("$storeIdentity", storeIdentity);
        _ = command.Bind("$operationId", operation.OperationId.Value.ToString("D"));
        var bytes = GetEncodedOperationBytes(operation);
        _ = command.Bind("$bytes", bytes);
        _ = command.Bind("$proof", SqliteOutboxCapacityIntegrity.CreateProof(connection, operation.OperationId, bytes));
        _ = command.Execute();
    }
}
