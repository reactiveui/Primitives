// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Checks candidate event uniqueness with bounded indexed key reads.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Reads only event identities that can conflict with the prepared plan.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="commit">The prepared plan.</param>
    /// <param name="stream">The selected stream record.</param>
    private static void ReadCommitEventConflicts(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerCommitValidationResult commit,
        ServerCommitStreamRecord stream)
    {
        var events = new List<RemoteEvent>(Math.Min(MaximumSelectionKeys, commit.EventCount));
        foreach (var entry in commit.Entries)
        {
            foreach (var remoteEvent in entry.Events)
            {
                events.Add(remoteEvent);
                if (events.Count != MaximumSelectionKeys)
                {
                    continue;
                }

                ReadConflictingEventKeys(connection, transaction, commit.StreamKey, stream, events);
                events.Clear();
            }
        }

        if (events.Count > 0)
        {
            ReadConflictingEventKeys(connection, transaction, commit.StreamKey, stream, events);
        }
    }

    /// <summary>Uses the event-id and cursor uniqueness indexes for one bounded key chunk.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The selected record.</param>
    /// <param name="events">The candidate event keys.</param>
    private static void ReadConflictingEventKeys(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream,
        List<RemoteEvent> events)
    {
        var ids = new string[events.Count];
        var cursors = new string[events.Count];
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        for (var index = 0; index < events.Count; index++)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);
            ids[index] = $"$candidateEvent{suffix}";
            cursors[index] = $"$candidateCursor{suffix}";
            _ = command.Bind(ids[index], events[index].EventId.ToString("D"));
            _ = command.Bind(cursors[index], events[index].ServerCursor);
        }

        command.SetSql($"""
            SELECT event_id, server_cursor FROM oc_server_journal_events
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND event_id IN ({string.Join(",", ids)})
            UNION
            SELECT event_id, server_cursor FROM oc_server_journal_events
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND server_cursor IN ({string.Join(",", cursors)});
            """);
        AddStreamParameters(command, streamKey);
        using var reader = command.Query();
        while (reader.Read())
        {
            _ = stream.EventIds.Add(ReadGuid(reader, 0, "The SQLite server journal event id is invalid."));
            _ = stream.Cursors.Add(ReadCursor(reader, 1, "The SQLite server journal event cursor is invalid."));
        }
    }
}
