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
    /// <summary>The missing snapshot message.</summary>
    private const string MissingSnapshotMessage = "The SQLite snapshot is missing.";

    /// <summary>Rejects status-only results that require an optimistic snapshot replacement.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leasedOperations">The validated leased operations.</param>
    /// <param name="result">The validated result.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <exception cref="InvalidOperationException">A rejection contradicts inclusion or requires a snapshot rebuild.</exception>
    internal static void ValidateStatusOnlyReconciliation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        IReadOnlyList<SyncOperation> leasedOperations,
        RemoteSyncResult result,
        long maximumPayloadBytes)
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

            var snapshot = ReadSnapshot(connection, transaction, storeIdentity, operationStreams[operation.OperationId], maximumPayloadBytes)
                ?? throw new InvalidOperationException(MissingSnapshotMessage);
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
    /// <param name="plan">The snapshot reconciliation plan.</param>
    /// <returns>The committed snapshots.</returns>
    /// <exception cref="InvalidOperationException">The replacement set or revision fence is invalid.</exception>
    internal static List<LocalSnapshot> CreateResultReconciliationSnapshots(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        IReadOnlyList<SyncOperation> leasedOperations,
        SqliteResultReconciliationPlan plan)
    {
        var operationStreams = GetLeasedOperationStreams(leasedOperations);
        var requiredStreams = GetResultReconciliationStreams(connection, transaction, storeIdentity, operationStreams, plan.Result);
        if (plan.SnapshotMutations.Count != requiredStreams.Count)
        {
            throw new InvalidOperationException("Each affected stream requires exactly one snapshot replacement.");
        }

        List<LocalSnapshot> snapshots = [with(capacity: plan.SnapshotMutations.Count)];
        for (var index = 0; index < plan.SnapshotMutations.Count; index++)
        {
            var mutation = plan.SnapshotMutations[index];
            SqliteLocalCommitValidation.ValidateSnapshotMutation(mutation);
            if (!requiredStreams.Remove(mutation.StreamId))
            {
                throw new InvalidOperationException("A snapshot replacement is duplicated or unrelated to this result.");
            }

            var stream = ReadStreamState(connection, transaction, storeIdentity, mutation.StreamId);
            var current = ReadSnapshot(connection, transaction, storeIdentity, mutation.StreamId, plan.MaximumPayloadBytes)
                ?? throw new InvalidOperationException(MissingSnapshotMessage);
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
                plan.SavedAtUtc) { AuthoritativeState = authoritative });
        }

        return snapshots;
    }

    /// <summary>Creates the committed snapshot for a local dead-letter transition after validating all fences.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The leased operation to dead-letter.</param>
    /// <param name="mutation">The replacement mutation.</param>
    /// <param name="savedAtUtc">The snapshot save timestamp.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The committed replacement snapshot.</returns>
    /// <exception cref="InvalidOperationException">The operation is already included, terminal, or the snapshot is stale.</exception>
    internal static LocalSnapshot CreateDeadLetterSnapshot(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        SnapshotMutation mutation,
        DateTimeOffset savedAtUtc,
        long maximumPayloadBytes)
    {
        SqliteLocalCommitValidation.ValidateSnapshotMutation(mutation);
        ValidateDeadLetterOperationTarget(connection, transaction, storeIdentity, operation, mutation);
        ValidateDeadLetterOperationStatus(connection, transaction, storeIdentity, operation.OperationId);

        var stream = ReadStreamState(connection, transaction, storeIdentity, mutation.StreamId);
        var current = ReadSnapshot(connection, transaction, storeIdentity, mutation.StreamId, maximumPayloadBytes)
            ?? throw new InvalidOperationException(MissingSnapshotMessage);
        var authoritative = current.AuthoritativeState
            ?? throw new InvalidOperationException("The stream requires an authoritative checkpoint before dead-letter reconciliation.");
        if (current.Revision != mutation.ExpectedRevision)
        {
            throw new InvalidOperationException("The optimistic snapshot changed before dead-letter reconciliation.");
        }

        if (mutation.AuthoritativeState is not null && !PayloadEquals(authoritative, mutation.AuthoritativeState))
        {
            throw new InvalidOperationException("A dead-letter transition cannot replace the authoritative checkpoint.");
        }

        return new(
            mutation.StreamId,
            mutation.FormatVersion,
            stream.ServerCursor,
            mutation.State,
            checked(current.Revision + 1),
            savedAtUtc) { AuthoritativeState = authoritative };
    }

    /// <summary>Validates dead-letter operation identity and authoritative inclusion fences.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <param name="mutation">The replacement mutation.</param>
    /// <exception cref="InvalidOperationException">The mutation targets another stream or contradicts inclusion.</exception>
    private static void ValidateDeadLetterOperationTarget(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        SnapshotMutation mutation)
    {
        ValidateDeadLetterStream(operation.StreamId, mutation.StreamId);
        ValidateDeadLetterInclusion(connection, transaction, storeIdentity, operation.OperationId);
    }

    /// <summary>Validates that the replacement mutation targets the leased operation stream.</summary>
    /// <param name="operationStreamId">The leased operation stream.</param>
    /// <param name="mutationStreamId">The mutation stream.</param>
    /// <exception cref="InvalidOperationException">The mutation targets another stream.</exception>
    private static void ValidateDeadLetterStream(StreamId operationStreamId, StreamId mutationStreamId) =>
        _ = operationStreamId == mutationStreamId
            || ThrowInvalidOperation("The dead-letter snapshot targets a different stream.");

    /// <summary>Validates that authoritative receive processing has not already included the operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="InvalidOperationException">The operation is already included.</exception>
    private static void ValidateDeadLetterInclusion(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId) =>
        _ = !IsOperationIncluded(connection, transaction, storeIdentity, operationId)
            || ThrowInvalidOperation("A dead-letter transition contradicts authoritative operation inclusion.");

    /// <summary>Validates operation state evidence before local dead-lettering.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="InvalidOperationException">The operation is terminal or has prior upload evidence.</exception>
    private static void ValidateDeadLetterOperationStatus(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        var status = ReadOperationStatus(connection, transaction, storeIdentity, operationId)
            ?? throw new InvalidOperationException("The SQLite operation state is missing.");
        ValidateDeadLetterTerminalState(status.State);
        ValidateDeadLetterAttemptEvidence(status);
    }

    /// <summary>Validates that an operation state is eligible for local dead-lettering.</summary>
    /// <param name="state">The operation state.</param>
    /// <exception cref="InvalidOperationException">The operation state is terminal.</exception>
    private static void ValidateDeadLetterTerminalState(SyncOperationState state) =>
        _ = !IsTerminalForDeadLetter(state)
            || ThrowInvalidOperation("The SQLite operation state is terminal.");

    /// <summary>Validates that an operation has no prior upload attempt evidence.</summary>
    /// <param name="status">The operation status.</param>
    /// <exception cref="InvalidOperationException">The operation might already have reached the remote service.</exception>
    private static void ValidateDeadLetterAttemptEvidence(SyncOperationStatus status) =>
        _ = !HasPriorUploadAttemptEvidence(status)
            || ThrowInvalidOperation("The SQLite operation has prior upload attempt evidence.");

    /// <summary>Throws an invalid operation exception from expression guards.</summary>
    /// <param name="message">The exception message.</param>
    /// <returns>This method never returns.</returns>
    /// <exception cref="InvalidOperationException">Always thrown.</exception>
    private static bool ThrowInvalidOperation(string message) =>
        throw new InvalidOperationException(message);

    /// <summary>Determines whether an operation status carries remote-attempt evidence.</summary>
    /// <param name="status">The operation status.</param>
    /// <returns>Whether the operation might already have reached the remote service.</returns>
    private static bool HasPriorUploadAttemptEvidence(SyncOperationStatus status) =>
        status.State != SyncOperationState.QueuedForUpload || status.Attempt != 0;

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

    /// <summary>Determines whether a state rejects local dead-letter transition.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the state is terminal for dead-letter reconciliation.</returns>
    private static bool IsTerminalForDeadLetter(SyncOperationState state) =>
        state is SyncOperationState.Conflict
            or SyncOperationState.Synchronized
            or SyncOperationState.Rejected
            or SyncOperationState.DeadLettered
            or SyncOperationState.Ambiguous
            or SyncOperationState.GuaranteeExpired;

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
