// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Selects only the receive groups needed by one bounded page.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>The resolved cursor group sequence column.</summary>
    private const int CursorGroupSequenceColumn = 2;

    /// <summary>The cursor's within-operation event index column.</summary>
    private const int CursorEventIndexColumn = 3;

    /// <summary>The final within-operation event index column.</summary>
    private const int CursorFinalEventIndexColumn = 4;

    /// <summary>Finds a final event cursor with an indexed lookup and a bounded group-completion check.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="cursor">The caller cursor.</param>
    /// <param name="keys">The optional anchor keys.</param>
    /// <param name="sequence">The resolved group sequence.</param>
    /// <returns>Whether the cursor is retained.</returns>
    /// <exception cref="ArgumentException">The cursor does not complete an operation group.</exception>
    private static bool TryReadCursorGroup(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        string cursor,
        List<ServerOperationKey> keys,
        out long sequence)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT e.client_id, e.operation_id, l.group_sequence, e.event_index,
                (SELECT MAX(last.event_index) FROM oc_server_journal_events last
                 WHERE last.tenant_id = e.tenant_id AND last.stream_id = e.stream_id
                    AND last.client_id = e.client_id AND last.operation_id = e.operation_id)
            FROM oc_server_journal_events e JOIN oc_server_journal_ledger l
                ON l.tenant_id = e.tenant_id AND l.stream_id = e.stream_id
                    AND l.client_id = e.client_id AND l.operation_id = e.operation_id
            WHERE e.tenant_id = $tenantId AND e.stream_id = $streamId
                AND e.server_cursor = $cursor AND l.group_sequence IS NOT NULL;
            """);
        AddStreamParameters(command, streamKey);
        _ = command.Bind(CursorParameterName, cursor);
        using var reader = command.Query();
        sequence = 0;
        if (!reader.Read())
        {
            return false;
        }

        if (ReadNonNegativeLong(reader, CursorEventIndexColumn, InvalidEventSequenceMessage)
            != ReadNonNegativeLong(reader, CursorFinalEventIndexColumn, InvalidEventSequenceMessage))
        {
            throw new ArgumentException("The receive cursor does not identify a complete operation group.", nameof(cursor));
        }

        keys.Add(ReadOperationKey(reader, 0, 1));
        sequence = ReadNonNegativeLong(reader, CursorGroupSequenceColumn, InvalidGroupSequenceMessage);
        return true;
    }

    /// <summary>Clamps an independent receive boundary at the minimum supported timestamp.</summary>
    /// <param name="utcNow">The sampled clock.</param>
    /// <param name="retention">The receive retention.</param>
    /// <returns>The inclusive retained boundary.</returns>
    private static DateTimeOffset GetReceiveBoundary(DateTimeOffset utcNow, TimeSpan retention) =>
        utcNow - DateTimeOffset.MinValue < retention ? DateTimeOffset.MinValue : utcNow - retention;

    /// <summary>Reads a stream header and marks independent receive expiry without deleting replay proofs.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="utcNow">The receive clock.</param>
    /// <returns>The stream header.</returns>
    private ServerCommitStreamRecord? ReadReceiveHeader(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        DateTimeOffset utcNow)
    {
        var stream = ReadStreamHeader(connection, transaction, streamKey);
        if (stream is null || _options.ReceiveHistoryRetention is not { } retention)
        {
            return stream;
        }

        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT EXISTS (SELECT 1 FROM oc_server_journal_ledger
                WHERE tenant_id = $tenantId AND stream_id = $streamId
                    AND group_sequence IS NOT NULL AND committed_at_utc < $receiveBoundary);
            """);
        AddStreamParameters(command, streamKey);
        _ = command.Bind("$receiveBoundary", FormatDateTimeOffset(GetReceiveBoundary(utcNow, retention)));
        stream.HasReceiveHistoryGap |= ReadCount(command.Scalar(), InvalidGroupSequenceMessage) != 0;
        return stream;
    }

    /// <summary>Reads a bounded page and its optional retained event-cursor anchor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The bounded page request.</param>
    /// <param name="stream">The stream header.</param>
    /// <param name="utcNow">The receive clock.</param>
    /// <returns>The receive page.</returns>
    private ServerReceivePageResult ReadSelectedReceivePage(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerReceivePageRequest request,
        ServerCommitStreamRecord? stream,
        DateTimeOffset utcNow)
    {
        if (stream is null)
        {
            return ServerReceivePageOperations.Create(request, null);
        }

        var keys = new List<ServerOperationKey>();
        var sequence = 0L;
        if (request.Cursor is not null)
        {
            if (ServerReceiveGroupCursor.IsGroupCursor(request.Cursor))
            {
                sequence = ServerReceiveGroupCursor.Parse(request.StreamKey, request.Cursor);
            }
            else if (!TryReadCursorGroup(connection, transaction, request.StreamKey, request.Cursor, keys, out sequence))
            {
                return new(ServerReceivePageStatus.RetentionGap, null, 0, stream.LastGroupSequence);
            }
        }

        using (var command = connection.CreateStatement())
        {
            command.UseTransaction(transaction);
            command.SetSql("""
                SELECT client_id, operation_id FROM oc_server_journal_ledger
                WHERE tenant_id = $tenantId AND stream_id = $streamId AND group_sequence > $afterGroup
                ORDER BY group_sequence LIMIT $groupLimit;
                """);
            AddStreamParameters(command, request.StreamKey);
            _ = command.Bind("$afterGroup", sequence);
            _ = command.Bind("$groupLimit", Math.Min(request.MaximumGroups, _options.MaximumLedgerEntries));
            using var reader = command.Query();
            while (reader.Read())
            {
                keys.Add(ReadOperationKey(reader, 0, 1));
            }
        }

        stream.Ledger.Clear();
        stream.Groups.Clear();
        stream.Events.Clear();
        ReadLedger(connection, transaction, request.StreamKey, stream, keys);
        ServerReceivePageOperations.ExpireReceiveHistory(stream, _options, utcNow);
        return ServerReceivePageOperations.Create(request, stream);
    }
}
