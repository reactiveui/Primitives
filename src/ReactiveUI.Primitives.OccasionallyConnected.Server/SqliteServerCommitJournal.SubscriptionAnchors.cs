// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides subscription acknowledgement storage helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Resolves the effective first-read cursor for a subscription.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="clientCursor">The caller-supplied cursor.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="observedUtc">The sampled timestamp.</param>
    /// <returns>The resolved read-cursor decision.</returns>
    private (bool HasReadCursor, string? ReadCursor, ServerReceivePageResult PendingResult) ResolveInitialReadCursor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        string? clientCursor,
        ServerCommitStreamRecord? stream,
        DateTimeOffset observedUtc)
    {
        var pendingResult = new ServerReceivePageResult(ServerReceivePageStatus.EndOfStream, null, 0, 0);
        if (clientCursor is not null)
        {
            return (true, clientCursor, pendingResult);
        }

        if (record.InitialAnchorResolved)
        {
            return (true, ServerSubscriptionStartPositionOperations.GetInitialReadCursor(record), pendingResult);
        }

        var resolution = TryResolveInitialAnchor(connection, transaction, record, stream, out var anchor);
        if (resolution == ServerSubscriptionAnchorResolution.Resolved)
        {
            ApplyInitialAnchor(connection, transaction, record, anchor, observedUtc);
            return (true, ServerSubscriptionStartPositionOperations.GetInitialReadCursor(record), pendingResult);
        }

        var lastGroupSequence = stream?.LastGroupSequence ?? 0;
        var status = resolution == ServerSubscriptionAnchorResolution.RetentionGap
            ? ServerReceivePageStatus.RetentionGap
            : ServerReceivePageStatus.EndOfStream;
        return (false, null, new(status, null, lastGroupSequence, lastGroupSequence));
    }

    /// <summary>Persists a resolved initial anchor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <param name="observedUtc">The sampled timestamp.</param>
    /// <exception cref="QueueCapacityExceededException">The anchor exceeds the retained byte limit.</exception>
    private void ApplyInitialAnchor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        ServerSubscriptionInitialAnchor anchor,
        DateTimeOffset observedUtc)
    {
        var updatedUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
        var logicalBytesDelta = ServerSubscriptionJournalOperations.GetInitialAnchorCursorDelta(record.InitialAnchorCursor, anchor.Cursor);
        if (!HasSubscriptionCapacity(connection, transaction, 0, 0, logicalBytesDelta, _options))
        {
            throw new QueueCapacityExceededException("The server subscription anchor exceeds the journal byte limit.", canFitWhenEmpty: false);
        }

        UpdateInitialAnchor(connection, transaction, record.Identity.SubscriptionId, anchor, updatedUtc, logicalBytesDelta);
        WriteLatestUtc(connection, transaction, updatedUtc);
        record.InitialAnchorCursor = anchor.Cursor;
        record.InitialAnchorGroupSequence = anchor.GroupSequence;
        record.InitialAnchorResolved = true;
        record.UpdatedAtUtc = updatedUtc;
        record.LastTouchedUtc = updatedUtc;
    }
}
