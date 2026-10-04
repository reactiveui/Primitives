// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes local SQLite commit SQL.</summary>
/// <content>Reads metadata for precisely the selected recovery operation set in one query.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Identifies a selected recovery operation set.</summary>
    private enum MetadataSelection
    {
        /// <summary>Unresolved upload operations.</summary>
        Pending = 0,

        /// <summary>Operations not yet included in received authoritative state.</summary>
        Replay = 1,

        /// <summary>Dead-lettered operations.</summary>
        DeadLetters = 2,
    }

    /// <summary>Reads only metadata belonging to the selected stream and recovery state set.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The partition identity.</param>
    /// <param name="streamId">The selected stream.</param>
    /// <param name="selection">The selected operation set.</param>
    /// <returns>Metadata keyed by selected operation id.</returns>
    /// <exception cref="InvalidOperationException">A stored metadata value is invalid.</exception>
    private static Dictionary<OperationId, Dictionary<string, string>> ReadSelectedOperationMetadata(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        MetadataSelection selection)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT metadata.operation_id, metadata.key, metadata.value
            FROM oc_outbox AS outbox
            JOIN oc_outbox_metadata AS metadata
                ON metadata.store_identity = outbox.store_identity AND metadata.operation_id = outbox.operation_id
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity AND state.operation_id = outbox.operation_id
            LEFT JOIN oc_outbox_receive_inclusions AS inclusion
                ON inclusion.store_identity = outbox.store_identity AND inclusion.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity AND outbox.stream_id = $streamId AND (
                ($selection = 0 AND (state.operation_state IS NULL OR state.operation_state NOT IN (4, 5, 6))) OR
                ($selection = 1 AND inclusion.operation_id IS NULL AND (state.operation_state IS NULL OR state.operation_state NOT IN (5, 6))) OR
                ($selection = 2 AND state.operation_state = 6))
            ORDER BY outbox.client_sequence, metadata.key;
            """);
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Bind("$selection", (int)selection);
        using var reader = command.Query();
        var result = new Dictionary<OperationId, Dictionary<string, string>>();
        while (reader.Read())
        {
            ReadSelectedMetadataRow(connection, reader, result);
        }

        return result;
    }

    /// <summary>Authenticates one metadata row with its original operation-specific context.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The selected metadata row.</param>
    /// <param name="result">The accumulated metadata.</param>
    /// <exception cref="InvalidOperationException">A stored metadata value is invalid.</exception>
    private static void ReadSelectedMetadataRow(SqliteDatabase connection, SqliteRows reader, Dictionary<OperationId, Dictionary<string, string>> result)
    {
        const int ValueIndex = 2;
        var operationId = ReadOperationId(reader, 0);
        var key = ReadString(reader, 1, "The SQLite metadata key is invalid.");
        var value = ReadProtectedNullableText(
            connection,
            reader,
            ValueIndex,
            SqliteRecordContext.OutboxMetadata(operationId, key),
            SqliteRecordContext.ValueColumn,
            operationId) ?? throw new InvalidOperationException("The SQLite metadata value is invalid.");
#if NET8_0_OR_GREATER
        ref var metadata = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(result, operationId, out _);
        metadata ??= [with(StringComparer.Ordinal)];
#else
        if (!result.TryGetValue(operationId, out var metadata))
        {
            metadata = [with(StringComparer.Ordinal)];
            result.Add(operationId, metadata);
        }
#endif

        metadata.Add(key, value);
    }

    /// <summary>Uses preloaded metadata when a recovery set is already selected.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="identity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="selected">The optional selected metadata.</param>
    /// <returns>The selected or point-read metadata.</returns>
    private static Dictionary<string, string> GetSelectedMetadata(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string identity,
        OperationId operationId,
        Dictionary<OperationId, Dictionary<string, string>>? selected)
    {
        if (selected is null)
        {
            return ReadMetadata(connection, transaction, identity, operationId);
        }

        return selected.TryGetValue(operationId, out var metadata) ? metadata : [with(StringComparer.Ordinal)];
    }

    /// <summary>Reads metadata only for the already bounded persisted lease membership.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="identity">The partition identity.</param>
    /// <param name="leaseId">The selected lease.</param>
    /// <returns>The selected lease metadata.</returns>
    private static Dictionary<OperationId, Dictionary<string, string>> ReadLeasedOperationMetadata(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string identity,
        Guid leaseId)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT metadata.operation_id, metadata.key, metadata.value
            FROM oc_outbox_leases AS lease
            JOIN oc_outbox AS outbox
                ON outbox.store_identity = lease.store_identity AND outbox.operation_id = lease.operation_id
            JOIN oc_outbox_metadata AS metadata
                ON metadata.store_identity = lease.store_identity AND metadata.operation_id = lease.operation_id
            WHERE lease.store_identity = $storeIdentity AND lease.lease_id = $leaseId
            ORDER BY lease.client_sequence, metadata.key;
            """);
        AddLeaseParameters(command, identity, leaseId);
        using var reader = command.Query();
        var result = new Dictionary<OperationId, Dictionary<string, string>>();
        while (reader.Read())
        {
            ReadSelectedMetadataRow(connection, reader, result);
        }

        return result;
    }
}
