// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Keeps unchanged subscription polls entirely read-only.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Creates a subscription state using an indexed count instead of loading offer payloads.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription header.</param>
    /// <returns>The retained subscription state.</returns>
    private static ServerSubscriptionState ReadSubscriptionState(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM oc_server_journal_subscription_offers WHERE subscription_id = $subscriptionId;");
        AddSubscriptionIdParameter(command, record.Identity.SubscriptionId);
        return ServerSubscriptionJournalOperations.CreateState(record) with
        {
            OfferCount = ReadCount(command.Scalar(), InvalidLogicalBytesMessage),
        };
    }

    /// <summary>Reads an initial-position header and at most two anchor groups.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="position">The requested initial position.</param>
    /// <returns>The bounded anchor view.</returns>
    private static ServerCommitStreamRecord? ReadInitialAnchorStream(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        StartPosition position)
    {
        var stream = ReadStreamHeader(connection, transaction, streamKey);
        if (stream is null)
        {
            return null;
        }

        var keys = new List<ServerOperationKey>();
        if (position.Kind == StartPositionKind.FromTimestamp)
        {
            ReadTimestampAnchorKeys(connection, transaction, streamKey, position.Timestamp.GetValueOrDefault(), keys);
        }
        else if (position.Kind == StartPositionKind.FromCursor && !ServerReceiveGroupCursor.IsGroupCursor(position.Cursor!))
        {
            _ = TryReadCursorGroup(connection, transaction, streamKey, position.Cursor!, keys, out _);
        }

        ReadLedger(connection, transaction, streamKey, stream, keys);
        return stream;
    }

    /// <summary>Reads the earliest inclusive timestamp group and its retained predecessor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="timestamp">The inclusive timestamp.</param>
    /// <param name="keys">The selected anchor keys.</param>
    private static void ReadTimestampAnchorKeys(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        DateTimeOffset timestamp,
        List<ServerOperationKey> keys)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT client_id, operation_id FROM oc_server_journal_ledger
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND group_sequence <=
                (SELECT group_sequence FROM oc_server_journal_ledger
                 WHERE tenant_id = $tenantId AND stream_id = $streamId
                    AND group_sequence IS NOT NULL AND committed_at_utc >= $timestamp
                 ORDER BY committed_at_utc, group_sequence LIMIT 1)
            ORDER BY group_sequence DESC LIMIT 2;
            """);
        AddStreamParameters(command, streamKey);
        _ = command.Bind("$timestamp", FormatDateTimeOffset(timestamp));
        using var reader = command.Query();
        while (reader.Read())
        {
            keys.Add(ReadOperationKey(reader, 0, 1));
        }
    }

    /// <summary>Returns an unchanged empty offer without acquiring a write reservation.</summary>
    /// <param name="request">The requested page.</param>
    /// <param name="observedUtc">The sampled clock.</param>
    /// <returns>The empty result, or null when durable state must change.</returns>
    private ServerReceivePageResult? TryReadEmptyOffer(ServerSubscriptionPageRequest request, DateTimeOffset observedUtc)
    {
        lock (_connectionGate)
        {
            ThrowIfDisposed();
            var connection = _connection;
            using var transaction = connection.BeginTransaction(deferred: true);
            ValidateExistingSchema(connection, transaction);
            ValidateReadCapacity(connection, transaction);
            var record = ReadRegisteredSubscription(connection, transaction, request.Identity, readOffers: false);
            if (request.ExpectedGeneration.HasValue && request.ExpectedGeneration.Value != record.Generation)
            {
                transaction.Commit();
                return new(ServerReceivePageStatus.RetentionGap, null, 0, 0);
            }

            var stream = ReadSubscriptionAnchorStream(connection, transaction, record, observedUtc);
            var cursor = request.Cursor;
            if (cursor is null)
            {
                if (!record.InitialAnchorResolved)
                {
                    var resolution = TryResolveInitialAnchor(connection, transaction, record, stream, out _);
                    if (resolution == ServerSubscriptionAnchorResolution.Resolved)
                    {
                        return null;
                    }

                    var sequence = stream?.LastGroupSequence ?? 0;
                    var status = resolution == ServerSubscriptionAnchorResolution.RetentionGap
                        ? ServerReceivePageStatus.RetentionGap
                        : ServerReceivePageStatus.EndOfStream;
                    transaction.Commit();
                    return new(status, null, sequence, sequence);
                }

                cursor = ServerSubscriptionStartPositionOperations.GetInitialReadCursor(record);
            }

            var result = ReadSelectedReceivePage(
                connection,
                transaction,
                ServerSubscriptionJournalOperations.CreateReceiveRequest(request with { Cursor = cursor }),
                stream,
                observedUtc);
            transaction.Commit();
            return result.Batch is null ? result : null;
        }
    }

    /// <summary>Reads only the groups needed to resolve a deferred initial position.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="observedUtc">The receive clock.</param>
    /// <returns>The receive header with optional anchor groups.</returns>
    private ServerCommitStreamRecord? ReadSubscriptionAnchorStream(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        DateTimeOffset observedUtc)
    {
        var stream = record.InitialAnchorResolved
            ? ReadReceiveHeader(connection, transaction, record.Identity.StreamKey, observedUtc)
            : ReadInitialAnchorStream(connection, transaction, record.Identity.StreamKey, record.InitialStartPosition);
        ServerReceivePageOperations.ExpireReceiveHistory(stream, _options, observedUtc);
        return stream;
    }
}
