// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes atomic upload result reconciliation statements.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Rejects status-only results that require an optimistic snapshot replacement.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leasedOperations">The validated leased operations.</param>
    /// <param name="result">The validated result.</param>
    /// <exception cref="InvalidOperationException">A rejection contradicts inclusion or requires a snapshot rebuild.</exception>
    internal static void ValidateStatusOnlyReconciliation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        IReadOnlyList<SyncOperation> leasedOperations,
        RemoteSyncResult result)
    {
        var operationStreams = GetLeasedOperationStreams(leasedOperations);
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var operation = result.Operations[index];
            if (operation.Kind != OperationResultKind.Rejected)
            {
                continue;
            }

            if (IsOperationIncluded(connection, transaction, storeIdentity, operation.OperationId))
            {
                throw new InvalidOperationException("A rejection contradicts authoritative operation inclusion.");
            }

            var snapshot = ReadSnapshot(connection, transaction, storeIdentity, operationStreams[operation.OperationId])
                ?? throw new InvalidOperationException("The SQLite snapshot is missing.");
            if (snapshot.AuthoritativeState is not null)
            {
                throw new InvalidOperationException("Removing an optimistic operation requires an atomic snapshot replacement.");
            }
        }
    }

    /// <summary>Creates committed replacement snapshots after validating the complete result reconciliation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leasedOperations">The validated leased operations.</param>
    /// <param name="result">The validated result.</param>
    /// <param name="snapshotMutations">The replacement mutations.</param>
    /// <param name="savedAtUtc">The snapshot save timestamp.</param>
    /// <returns>The committed snapshots.</returns>
    /// <exception cref="InvalidOperationException">The replacement set or revision fence is invalid.</exception>
    internal static List<LocalSnapshot> CreateResultReconciliationSnapshots(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        IReadOnlyList<SyncOperation> leasedOperations,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        DateTimeOffset savedAtUtc)
    {
        var operationStreams = GetLeasedOperationStreams(leasedOperations);
        var requiredStreams = GetResultReconciliationStreams(connection, transaction, storeIdentity, operationStreams, result);
        if (snapshotMutations.Count != requiredStreams.Count)
        {
            throw new InvalidOperationException("Each affected stream requires exactly one snapshot replacement.");
        }

        List<LocalSnapshot> snapshots = [with(capacity: snapshotMutations.Count)];
        for (var index = 0; index < snapshotMutations.Count; index++)
        {
            var mutation = snapshotMutations[index];
            SqliteLocalCommitValidation.ValidateSnapshotMutation(mutation);
            if (!requiredStreams.Remove(mutation.StreamId))
            {
                throw new InvalidOperationException("A snapshot replacement is duplicated or unrelated to this result.");
            }

            var stream = ReadStreamState(connection, transaction, storeIdentity, mutation.StreamId);
            var current = ReadSnapshot(connection, transaction, storeIdentity, mutation.StreamId)
                ?? throw new InvalidOperationException("The SQLite snapshot is missing.");
            var authoritative = current.AuthoritativeState
                ?? throw new InvalidOperationException("The stream requires an authoritative checkpoint before reconciliation.");
            if (current.Revision != mutation.ExpectedRevision)
            {
                throw new InvalidOperationException("The optimistic snapshot changed before result reconciliation.");
            }

            if (mutation.AuthoritativeState is not null && !PayloadEquals(authoritative, mutation.AuthoritativeState))
            {
                throw new InvalidOperationException("An upload result cannot replace the authoritative checkpoint.");
            }

            snapshots.Add(new(
                mutation.StreamId,
                mutation.FormatVersion,
                stream.ServerCursor,
                mutation.State,
                checked(current.Revision + 1),
                savedAtUtc) { AuthoritativeState = authoritative });
        }

        return snapshots;
    }

    /// <summary>Identifies streams whose optimistic replay membership will shrink.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationStreams">The validated operation stream lookup.</param>
    /// <param name="result">The validated result.</param>
    /// <returns>The affected streams.</returns>
    /// <exception cref="InvalidOperationException">The result contradicts authoritative inclusion.</exception>
    private static HashSet<StreamId> GetResultReconciliationStreams(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Dictionary<OperationId, StreamId> operationStreams,
        RemoteSyncResult result)
    {
        HashSet<StreamId> streams = [];
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var operation = result.Operations[index];
            if (operation.Kind != OperationResultKind.Rejected)
            {
                continue;
            }

            if (IsOperationIncluded(connection, transaction, storeIdentity, operation.OperationId))
            {
                throw new InvalidOperationException("A rejection contradicts authoritative operation inclusion.");
            }

            _ = streams.Add(operationStreams[operation.OperationId]);
        }

        return streams;
    }

    /// <summary>Creates a stream lookup for validated leased operations.</summary>
    /// <param name="leasedOperations">The leased operations.</param>
    /// <returns>The operation stream lookup.</returns>
    private static Dictionary<OperationId, StreamId> GetLeasedOperationStreams(IReadOnlyList<SyncOperation> leasedOperations)
    {
        Dictionary<OperationId, StreamId> streams = [];
        for (var index = 0; index < leasedOperations.Count; index++)
        {
            var operation = leasedOperations[index];
            streams.Add(operation.OperationId, operation.StreamId);
        }

        return streams;
    }

    /// <summary>Returns whether a local operation is already covered by authoritative receive proof.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>Whether an inclusion row exists.</returns>
    private static bool IsOperationIncluded(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT 1
            FROM oc_outbox_receive_inclusions
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        return command.ExecuteScalar() is not null;
    }
}
