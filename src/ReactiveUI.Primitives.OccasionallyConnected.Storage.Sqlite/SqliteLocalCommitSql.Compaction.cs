// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes transactional compaction statements.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The maximum number of rows selected into memory for one compaction batch.</summary>
    private const int CompactionBatchSize = 64;

    /// <summary>The operation id column index for compaction rows.</summary>
    private const int CompactionOperationIdIndex = 0;

    /// <summary>The changed-at column index for compaction rows.</summary>
    private const int CompactionChangedAtIndex = 1;

    /// <summary>The byte-count column index for compaction rows.</summary>
    private const int CompactionBytesIndex = 2;

    /// <summary>The inbox row id column index for compaction rows.</summary>
    private const int CompactionInboxRowIdIndex = 0;

    /// <summary>The inbox committed-at column index for compaction rows.</summary>
    private const int CompactionInboxCommittedAtIndex = 1;

    /// <summary>The SQL that selects outbox-backed compaction candidates.</summary>
    private const string SelectOperationCompactionCandidatesSql = """
        SELECT outbox.operation_id, state.changed_at_utc,
               length(outbox.payload) + COALESCE((
                   SELECT length(authoritative.payload)
                   FROM oc_outbox_authoritative_mutations AS authoritative
                   WHERE authoritative.store_identity = outbox.store_identity
                       AND authoritative.operation_id = outbox.operation_id), 0) + COALESCE((
                   SELECT SUM(length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB)))
                   FROM oc_outbox_metadata AS metadata
                   WHERE metadata.store_identity = outbox.store_identity
                       AND metadata.operation_id = outbox.operation_id), 0)
        FROM oc_outbox AS outbox
        INNER JOIN oc_outbox_operation_states AS state
            ON state.store_identity = outbox.store_identity
            AND state.operation_id = outbox.operation_id
        LEFT JOIN oc_outbox_receive_inclusions AS inclusion
            ON inclusion.store_identity = outbox.store_identity
            AND inclusion.operation_id = outbox.operation_id
        WHERE outbox.store_identity = $storeIdentity
            AND ($streamId IS NULL OR outbox.stream_id = $streamId)
            AND (state.operation_state = $firstState
                OR ($secondState IS NOT NULL AND state.operation_state = $secondState))
            AND (state.operation_state <> 4 OR inclusion.operation_id IS NOT NULL)
            AND state.changed_at_utc < $cutoffUtc
            AND outbox.snapshot_revision < COALESCE((
                SELECT snapshot.revision
                FROM oc_snapshots AS snapshot
                WHERE snapshot.store_identity = outbox.store_identity
                    AND snapshot.stream_id = outbox.stream_id), 0)
            AND NOT EXISTS (
                SELECT 1
                FROM oc_outbox_leases AS lease
                WHERE lease.store_identity = outbox.store_identity
                    AND lease.operation_id = outbox.operation_id)
            AND NOT EXISTS (
                SELECT 1
                FROM oc_outbox AS unresolved
                LEFT JOIN oc_outbox_operation_states AS unresolved_state
                    ON unresolved_state.store_identity = unresolved.store_identity
                    AND unresolved_state.operation_id = unresolved.operation_id
                LEFT JOIN oc_outbox_receive_inclusions AS unresolved_inclusion
                    ON unresolved_inclusion.store_identity = unresolved.store_identity
                    AND unresolved_inclusion.operation_id = unresolved.operation_id
                WHERE unresolved.store_identity = outbox.store_identity
                    AND unresolved.stream_id = outbox.stream_id
                    AND (unresolved_state.operation_id IS NULL
                        OR unresolved_state.operation_state NOT IN (4, 5, 6)
                        OR (unresolved_state.operation_state = 4 AND unresolved_inclusion.operation_id IS NULL)))
        ORDER BY state.changed_at_utc ASC, outbox.stream_id ASC, outbox.client_sequence ASC
        LIMIT $limit;
        """;

    /// <summary>Compacts eligible SQLite local commit records in a single caller-owned transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The compaction request.</param>
    /// <param name="retention">The retention policy.</param>
    /// <param name="nowUtc">The sampled UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The compaction result.</returns>
    /// <exception cref="OperationCanceledException">Compaction was canceled before commit.</exception>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    /// <exception cref="SqliteException">SQLite rejects a compaction statement.</exception>
    internal static CompactionResult Compact(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        CompactionRequest request,
        RetentionOptions retention,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var cutoffs = CreateCompactionCutoffs(request, retention, nowUtc);
        var retainedBytes = ReadScopedRetainedBytes(connection, transaction, storeIdentity, request.StreamId);
        var result = new CompactionAccumulator(retainedBytes, request.TargetBytes);
        var terminalFilter = new OperationCompactionFilter(
            request.StreamId,
            SyncOperationState.Synchronized,
            SyncOperationState.Rejected,
            cutoffs.OutboxTerminalCutoffUtc);
        var deadLetterFilter = new OperationCompactionFilter(
            request.StreamId,
            SyncOperationState.DeadLettered,
            null,
            cutoffs.DeadLetterCutoffUtc);
        while (result.CanDeleteMore)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var removed = DeleteNextOperationBatch(
                connection,
                transaction,
                storeIdentity,
                in terminalFilter,
                result,
                cancellationToken);
            removed += DeleteNextOperationBatch(
                connection,
                transaction,
                storeIdentity,
                in deadLetterFilter,
                result,
                cancellationToken);
            if (removed == 0)
            {
                break;
            }
        }

        while (DeleteNextInboxBatch(
            connection,
            transaction,
            storeIdentity,
            request.StreamId,
            cutoffs.InboxCutoffUtc,
            result,
            cancellationToken) != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }

        return new(result.RecordsRemoved, result.BytesReclaimed);
    }

    /// <summary>Reads the scoped logical encoded bytes retained by compaction-managed outbox rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The optional stream filter.</param>
    /// <returns>The scoped retained payload and metadata byte count; inbox rows are counted as zero and SQLite file size is not measured.</returns>
    private static long ReadScopedRetainedBytes(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId? streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT COALESCE(SUM(
                length(outbox.payload) + COALESCE((
                    SELECT length(authoritative.payload)
                    FROM oc_outbox_authoritative_mutations AS authoritative
                    WHERE authoritative.store_identity = outbox.store_identity
                        AND authoritative.operation_id = outbox.operation_id), 0) + COALESCE((
                    SELECT SUM(length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB)))
                    FROM oc_outbox_metadata AS metadata
                    WHERE metadata.store_identity = outbox.store_identity
                        AND metadata.operation_id = outbox.operation_id), 0)), 0)
            FROM oc_outbox AS outbox
            WHERE outbox.store_identity = $storeIdentity
                AND ($streamId IS NULL OR outbox.stream_id = $streamId);
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, (object?)streamId?.Value ?? DBNull.Value);
        return ReadNonNegativeLong(command.ExecuteScalar(), "The SQLite compaction byte count is invalid.");
    }

    /// <summary>Deletes one bounded batch of eligible outbox-backed rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="filter">The outbox-backed row filter.</param>
    /// <param name="result">The accumulated result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of rows removed.</returns>
    private static long DeleteNextOperationBatch(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        in OperationCompactionFilter filter,
        CompactionAccumulator result,
        CancellationToken cancellationToken)
    {
        var candidates = SelectOperationCompactionCandidates(connection, transaction, storeIdentity, in filter);
        var removed = 0L;
        for (var index = 0; index < candidates.Count && result.CanDeleteMore; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DeleteOutboxOperationForCompaction(connection, transaction, storeIdentity, candidates[index].OperationId);
            result.RecordDeleted(candidates[index].Bytes);
            removed++;
        }

        return removed;
    }

    /// <summary>Deletes one bounded batch of eligible inbox rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The optional stream filter.</param>
    /// <param name="cutoffUtc">The retention cutoff.</param>
    /// <param name="result">The accumulated result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of rows removed.</returns>
    private static long DeleteNextInboxBatch(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId? streamId,
        DateTimeOffset cutoffUtc,
        CompactionAccumulator result,
        CancellationToken cancellationToken)
    {
        var candidates = SelectInboxCompactionCandidates(connection, transaction, storeIdentity, streamId, cutoffUtc);
        var removed = 0L;
        for (var index = 0; index < candidates.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DeleteInboxRowForCompaction(connection, transaction, candidates[index].RowId);
            result.RecordDeleted(0);
            removed++;
        }

        return removed;
    }

    /// <summary>Selects bounded outbox-backed compaction candidates.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="filter">The outbox-backed row filter.</param>
    /// <returns>The candidate rows.</returns>
    private static List<OperationCompactionCandidate> SelectOperationCompactionCandidates(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        in OperationCompactionFilter filter)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SelectOperationCompactionCandidatesSql;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue("$firstState", (int)filter.FirstState);
        _ = command.Parameters.AddWithValue("$secondState", filter.SecondState.HasValue ? (int)filter.SecondState.GetValueOrDefault() : DBNull.Value);
        _ = command.Parameters.AddWithValue("$cutoffUtc", FormatDateTimeOffset(filter.CutoffUtc));
        _ = command.Parameters.AddWithValue("$limit", CompactionBatchSize);
        _ = command.Parameters.AddWithValue(StreamIdParameter, (object?)filter.StreamId?.Value ?? DBNull.Value);

        using var reader = command.ExecuteReader();
        return ReadOperationCompactionCandidates(reader);
    }

    /// <summary>Reads outbox-backed compaction candidates from the current reader.</summary>
    /// <param name="reader">The reader.</param>
    /// <returns>The candidate rows.</returns>
    private static List<OperationCompactionCandidate> ReadOperationCompactionCandidates(SqliteDataReader reader)
    {
        List<OperationCompactionCandidate> candidates = [];
        while (reader.Read())
        {
            var operationId = ReadOperationId(reader, CompactionOperationIdIndex);
            _ = ReadDateTimeOffset(reader, CompactionChangedAtIndex, "The SQLite operation state timestamp is invalid.");
            var bytes = ReadNonNegativeLong(reader, CompactionBytesIndex, "The SQLite compaction byte count is invalid.");
            candidates.Add(new(operationId, bytes));
        }

        return candidates;
    }

    /// <summary>Selects bounded inbox compaction candidates.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The optional stream filter.</param>
    /// <param name="cutoffUtc">The cutoff timestamp.</param>
    /// <returns>The candidate rows.</returns>
    private static List<InboxCompactionCandidate> SelectInboxCompactionCandidates(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId? streamId,
        DateTimeOffset cutoffUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT inbox.rowid, inbox.committed_at_utc
            FROM oc_inbox AS inbox
            WHERE inbox.store_identity = $storeIdentity
                AND ($streamId IS NULL OR inbox.stream_id = $streamId)
                AND inbox.committed_at_utc < $cutoffUtc
                AND NOT EXISTS (
                    SELECT 1
                    FROM oc_outbox AS unresolved
                    LEFT JOIN oc_outbox_operation_states AS unresolved_state
                        ON unresolved_state.store_identity = unresolved.store_identity
                        AND unresolved_state.operation_id = unresolved.operation_id
                    LEFT JOIN oc_outbox_receive_inclusions AS unresolved_inclusion
                        ON unresolved_inclusion.store_identity = unresolved.store_identity
                        AND unresolved_inclusion.operation_id = unresolved.operation_id
                    WHERE unresolved.store_identity = inbox.store_identity
                        AND unresolved.stream_id = inbox.stream_id
                        AND (unresolved_state.operation_id IS NULL
                            OR unresolved_state.operation_state NOT IN (4, 5, 6)
                            OR (unresolved_state.operation_state = 4 AND unresolved_inclusion.operation_id IS NULL)))
            ORDER BY inbox.committed_at_utc ASC, inbox.stream_id ASC, inbox.event_id ASC
            LIMIT $limit;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue("$cutoffUtc", FormatDateTimeOffset(cutoffUtc));
        _ = command.Parameters.AddWithValue("$limit", CompactionBatchSize);
        _ = command.Parameters.AddWithValue(StreamIdParameter, (object?)streamId?.Value ?? DBNull.Value);

        using var reader = command.ExecuteReader();
        List<InboxCompactionCandidate> candidates = [];
        while (reader.Read())
        {
            var rowId = ReadPositiveLong(reader, CompactionInboxRowIdIndex, "The SQLite inbox row id is invalid.");
            _ = ReadDateTimeOffset(reader, CompactionInboxCommittedAtIndex, "The SQLite remote event timestamp is invalid.");
            candidates.Add(new(rowId));
        }

        return candidates;
    }

    /// <summary>Deletes one outbox row selected for compaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation id.</param>
    /// <exception cref="InvalidOperationException">The row disappeared before deletion.</exception>
    private static void DeleteOutboxOperationForCompaction(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_outbox
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite compaction candidate disappeared before deletion.");
    }

    /// <summary>Deletes one inbox row selected for compaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="rowId">The SQLite row id.</param>
    /// <exception cref="InvalidOperationException">The row disappeared before deletion.</exception>
    private static void DeleteInboxRowForCompaction(SqliteConnection connection, SqliteTransaction transaction, long rowId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM oc_inbox WHERE rowid = $rowId;";
        _ = command.Parameters.AddWithValue("$rowId", rowId);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite inbox compaction candidate disappeared before deletion.");
    }

    /// <summary>Creates effective compaction cutoffs.</summary>
    /// <param name="request">The compaction request.</param>
    /// <param name="retention">The retention policy.</param>
    /// <param name="nowUtc">The sampled UTC timestamp.</param>
    /// <returns>The effective cutoffs.</returns>
    private static CompactionCutoffs CreateCompactionCutoffs(CompactionRequest request, RetentionOptions retention, DateTimeOffset nowUtc)
    {
        var requestCutoffUtc = request.RetainTerminalRecordsAfter.ToUniversalTime();
        var outboxRetentionCutoffUtc = SubtractRetention(nowUtc, retention.OutboxTerminalRetention);
        var deadLetterRetentionCutoffUtc = SubtractRetention(nowUtc, retention.DeadLetterRetention);
        var inboxRetentionCutoffUtc = SubtractRetention(nowUtc, retention.InboxDeduplicationRetention);
        return new(
            Earlier(requestCutoffUtc, outboxRetentionCutoffUtc),
            Earlier(requestCutoffUtc, deadLetterRetentionCutoffUtc),
            inboxRetentionCutoffUtc);
    }

    /// <summary>Subtracts a retention interval while preserving a no-delete lower bound on underflow.</summary>
    /// <param name="nowUtc">The sampled UTC timestamp.</param>
    /// <param name="retention">The retention interval.</param>
    /// <returns>The retention cutoff.</returns>
    private static DateTimeOffset SubtractRetention(DateTimeOffset nowUtc, TimeSpan retention)
    {
        try
        {
            return nowUtc.Subtract(retention).ToUniversalTime();
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MinValue;
        }
    }

    /// <summary>Returns the earlier timestamp.</summary>
    /// <param name="first">The first timestamp.</param>
    /// <param name="second">The second timestamp.</param>
    /// <returns>The earlier timestamp.</returns>
    private static DateTimeOffset Earlier(DateTimeOffset first, DateTimeOffset second) =>
        first <= second ? first : second;

    /// <summary>Effective compaction cutoffs.</summary>
    /// <param name="OutboxTerminalCutoffUtc">The synchronized/rejected cutoff.</param>
    /// <param name="DeadLetterCutoffUtc">The dead-letter cutoff.</param>
    /// <param name="InboxCutoffUtc">The inbox cutoff.</param>
    private readonly record struct CompactionCutoffs(
        DateTimeOffset OutboxTerminalCutoffUtc,
        DateTimeOffset DeadLetterCutoffUtc,
        DateTimeOffset InboxCutoffUtc);

    /// <summary>An outbox-backed compaction candidate.</summary>
    /// <param name="OperationId">The operation id.</param>
    /// <param name="Bytes">The encoded payload and metadata bytes.</param>
    private readonly record struct OperationCompactionCandidate(OperationId OperationId, long Bytes);

    /// <summary>An outbox-backed compaction filter.</summary>
    /// <param name="StreamId">The optional stream filter.</param>
    /// <param name="FirstState">The first eligible state.</param>
    /// <param name="SecondState">The optional second eligible state.</param>
    /// <param name="CutoffUtc">The cutoff timestamp.</param>
    private readonly record struct OperationCompactionFilter(
        StreamId? StreamId,
        SyncOperationState FirstState,
        SyncOperationState? SecondState,
        DateTimeOffset CutoffUtc);

    /// <summary>An inbox compaction candidate.</summary>
    /// <param name="RowId">The SQLite row id.</param>
    private readonly record struct InboxCompactionCandidate(long RowId);

    /// <summary>Accumulates compaction counts and enforces the advisory retained-byte target.</summary>
    /// <param name="initialRetainedBytes">The scoped retained bytes before compaction.</param>
    /// <param name="targetBytes">The advisory byte target.</param>
    private sealed class CompactionAccumulator(long initialRetainedBytes, long targetBytes)
    {
        /// <summary>Gets the removed record count.</summary>
        public long RecordsRemoved { get; private set; }

        /// <summary>Gets the deleted logical encoded payload and metadata byte count; this is not physical SQLite file shrinkage.</summary>
        public long BytesReclaimed { get; private set; }

        /// <summary>Gets whether another row can be deleted.</summary>
        public bool CanDeleteMore => targetBytes == 0 || initialRetainedBytes - BytesReclaimed > targetBytes;

        /// <summary>Records one deleted row.</summary>
        /// <param name="bytes">The deleted encoded bytes.</param>
        public void RecordDeleted(long bytes)
        {
            RecordsRemoved++;
            BytesReclaimed += bytes;
        }
    }
}
