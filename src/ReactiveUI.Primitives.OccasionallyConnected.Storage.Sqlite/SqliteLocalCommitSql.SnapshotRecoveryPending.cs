// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes SQLite statements for snapshot recovery pending operation scans.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The canonical operation id text length.</summary>
    private const int SnapshotRecoveryOperationIdTextLength = 36;

    /// <summary>The invalid operation identifier message.</summary>
    private const string InvalidSnapshotRecoveryOperationIdMessage = "The SQLite operation id is invalid.";

    /// <summary>Reads pending operation identifiers in client sequence order for snapshot recovery.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="maximumRows">The maximum rows to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The pending operation identifiers.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The row bound is not positive.</exception>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled while scanning rows.</exception>
    internal static List<SqliteSnapshotRecoveryPendingOperation> ReadSnapshotRecoveryPendingOperations(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        int maximumRows,
        CancellationToken cancellationToken)
    {
        if (maximumRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRows), maximumRows, "The snapshot recovery pending row bound must be positive.");
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT state.operation_state,
                   typeof(outbox.operation_id), length(CAST(outbox.operation_id AS BLOB)),
                   IFNULL(substr(CAST(outbox.operation_id AS BLOB), 1, $operationIdTextLength), x'')
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity
                AND outbox.stream_id = $streamId
                AND (state.operation_state IS NULL OR state.operation_state NOT IN (4, 5, 6))
            ORDER BY outbox.client_sequence ASC
            LIMIT $maximumRows;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Parameters.AddWithValue("$maximumRows", maximumRows);
        _ = command.Parameters.AddWithValue("$operationIdTextLength", SnapshotRecoveryOperationIdTextLength);
        using var reader = command.ExecuteReader();
        var operations = new List<SqliteSnapshotRecoveryPendingOperation>();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            const int OperationStateIndex = 0;
            const int OperationIdTypeIndex = 1;
            const int OperationIdLengthIndex = 2;
            const int OperationIdIndex = 3;
            if (!reader.IsDBNull(OperationStateIndex))
            {
                _ = ReadSnapshotRecoveryOperationState(reader, OperationStateIndex);
            }

            operations.Add(new(ReadSnapshotRecoveryOperationId(reader, OperationIdIndex, OperationIdTypeIndex, OperationIdLengthIndex)));
        }

        return operations;
    }

    /// <summary>Reads synchronized operation identifiers that still need receive inclusion in client sequence order.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="maximumRows">The maximum rows to read.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The replay-only operation identifiers.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The row bound is not positive.</exception>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled while scanning rows.</exception>
    internal static List<OperationId> ReadSnapshotRecoveryReplayOnlyOperationIds(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        int maximumRows,
        CancellationToken cancellationToken)
    {
        if (maximumRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRows), maximumRows, "The snapshot recovery replay row bound must be positive.");
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT typeof(outbox.operation_id), length(CAST(outbox.operation_id AS BLOB)),
                   IFNULL(substr(CAST(outbox.operation_id AS BLOB), 1, $operationIdTextLength), x'')
            FROM oc_outbox AS outbox
            INNER JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            LEFT JOIN oc_outbox_receive_inclusions AS inclusion
                ON inclusion.store_identity = outbox.store_identity
                AND inclusion.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity
                AND outbox.stream_id = $streamId
                AND state.operation_state = 4
                AND inclusion.operation_id IS NULL
            ORDER BY outbox.client_sequence ASC
            LIMIT $maximumRows;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Parameters.AddWithValue("$maximumRows", maximumRows);
        _ = command.Parameters.AddWithValue("$operationIdTextLength", SnapshotRecoveryOperationIdTextLength);
        using var reader = command.ExecuteReader();
        var operations = new List<OperationId>();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            const int OperationIdTypeIndex = 0;
            const int OperationIdLengthIndex = 1;
            const int OperationIdIndex = 2;
            operations.Add(ReadSnapshotRecoveryOperationId(reader, OperationIdIndex, OperationIdTypeIndex, OperationIdLengthIndex));
        }

        return operations;
    }

    /// <summary>Reads and validates a bounded operation identifier for snapshot recovery scans.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="valueIndex">The bounded value column index.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="lengthIndex">The storage byte length column index.</param>
    /// <returns>The operation identifier.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static OperationId ReadSnapshotRecoveryOperationId(
        SqliteDataReader reader,
        int valueIndex,
        int typeIndex,
        int lengthIndex)
    {
        var storageType = ReadString(reader, typeIndex, InvalidSnapshotRecoveryOperationIdMessage);
        var byteLength = ReadInt(reader, lengthIndex, InvalidSnapshotRecoveryOperationIdMessage);
        if (!string.Equals(storageType, "text", StringComparison.Ordinal) || byteLength != SnapshotRecoveryOperationIdTextLength)
        {
            throw new InvalidOperationException(InvalidSnapshotRecoveryOperationIdMessage);
        }

        var text = ReadString(reader, valueIndex, InvalidSnapshotRecoveryOperationIdMessage);
        return Guid.TryParse(text, out var value) && value != Guid.Empty
            ? new(value)
            : throw new InvalidOperationException(InvalidSnapshotRecoveryOperationIdMessage);
    }

    /// <summary>Reads and validates an operation state enum for snapshot recovery scans.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation state.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static SyncOperationState ReadSnapshotRecoveryOperationState(SqliteDataReader reader, int index)
    {
        var state = (SyncOperationState)ReadInt(reader, index, "The SQLite operation state is invalid.");
        return IsSnapshotRecoveryPendingOperationState(state)
            ? state
            : throw new InvalidOperationException("The SQLite operation state is invalid.");
    }

    /// <summary>Determines whether an operation state can appear in the pending recovery scan.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the operation state can appear in the pending recovery scan.</returns>
    private static bool IsSnapshotRecoveryPendingOperationState(SyncOperationState state) =>
        state is SyncOperationState.SavedLocally
            or SyncOperationState.QueuedForUpload
            or SyncOperationState.Uploading
            or SyncOperationState.Conflict
            or SyncOperationState.Ambiguous
            or SyncOperationState.GuaranteeExpired;
}
