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
    /// <summary>The subscription identifier column index.</summary>
    private const int SubscriptionIdColumn = 0;

    /// <summary>The subscription tenant column index.</summary>
    private const int SubscriptionTenantColumn = 1;

    /// <summary>The subscription stream column index.</summary>
    private const int SubscriptionStreamColumn = 2;

    /// <summary>The subscription client column index.</summary>
    private const int SubscriptionClientColumn = 3;

    /// <summary>The subscription initial position kind column index.</summary>
    private const int SubscriptionInitialPositionKindColumn = 4;

    /// <summary>The subscription initial sequence column index.</summary>
    private const int SubscriptionInitialSequenceColumn = 5;

    /// <summary>The subscription initial timestamp column index.</summary>
    private const int SubscriptionInitialTimestampColumn = 6;

    /// <summary>The subscription initial cursor column index.</summary>
    private const int SubscriptionInitialCursorColumn = 7;

    /// <summary>The subscription initial anchor cursor column index.</summary>
    private const int SubscriptionInitialAnchorCursorColumn = 8;

    /// <summary>The subscription initial anchor group sequence column index.</summary>
    private const int SubscriptionInitialAnchorSequenceColumn = 9;

    /// <summary>The subscription initial anchor resolved column index.</summary>
    private const int SubscriptionInitialAnchorResolvedColumn = 10;

    /// <summary>The subscription acknowledged cursor column index.</summary>
    private const int SubscriptionAcknowledgedCursorColumn = 11;

    /// <summary>The subscription acknowledged group sequence column index.</summary>
    private const int SubscriptionAcknowledgedSequenceColumn = 12;

    /// <summary>The subscription latest offered cursor column index.</summary>
    private const int SubscriptionLatestOfferedCursorColumn = 13;

    /// <summary>The subscription latest offered group sequence column index.</summary>
    private const int SubscriptionLatestOfferedSequenceColumn = 14;

    /// <summary>The subscription acknowledged timestamp column index.</summary>
    private const int SubscriptionAcknowledgedAtColumn = 15;

    /// <summary>The subscription updated timestamp column index.</summary>
    private const int SubscriptionUpdatedAtColumn = 16;

    /// <summary>The subscription retention timestamp column index.</summary>
    private const int SubscriptionLastTouchedColumn = 17;

    /// <summary>The subscription logical byte count column index.</summary>
    private const int SubscriptionLogicalBytesColumn = 18;

    /// <summary>The offer cursor column index.</summary>
    private const int OfferCursorColumn = 0;

    /// <summary>The offer group sequence column index.</summary>
    private const int OfferGroupSequenceColumn = 1;

    /// <summary>The offer timestamp column index.</summary>
    private const int OfferOfferedAtColumn = 2;

    /// <summary>The offer logical byte count column index.</summary>
    private const int OfferLogicalBytesColumn = 3;

    /// <summary>The repeated SQLite cursor parameter name.</summary>
    private const string CursorParameterName = "$cursor";

    /// <summary>The repeated SQLite group sequence parameter name.</summary>
    private const string GroupSequenceParameterName = "$groupSequence";

    /// <summary>The repeated SQLite updated-at parameter name.</summary>
    private const string UpdatedAtUtcParameterName = "$updatedAtUtc";

    /// <summary>The repeated SQLite logical-bytes-delta parameter name.</summary>
    private const string LogicalBytesDeltaParameterName = "$logicalBytesDelta";

    /// <summary>The missing subscription row message.</summary>
    private const string MissingSubscriptionMessage = "The SQLite server subscription row is missing.";

    /// <summary>The missing subscription offer row message.</summary>
    private const string MissingOfferMessage = "The SQLite server subscription offer row is missing.";

    /// <summary>Registers a subscription inside an open transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The registration request.</param>
    /// <param name="updatedUtc">The update timestamp.</param>
    /// <param name="options">The journal options.</param>
    /// <returns>The subscription state.</returns>
    /// <exception cref="InvalidOperationException">The subscription identity conflicts with retained state.</exception>
    /// <exception cref="QueueCapacityExceededException">The subscription storage is full.</exception>
    private static ServerSubscriptionState RegisterSubscription(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRegistrationRequest request,
        DateTimeOffset updatedUtc,
        ServerCommitJournalOptions options)
    {
        var existing = ReadSubscriptionRecord(connection, transaction, request.Identity.SubscriptionId);
        if (existing is not null)
        {
            ThrowIfRegistrationMismatch(request, existing);
            UpdateSubscriptionUpdatedAt(connection, transaction, request.Identity.SubscriptionId, updatedUtc);
            existing.UpdatedAtUtc = updatedUtc;
            return ServerSubscriptionJournalOperations.CreateState(existing);
        }

        var stream = ReadStreamRecord(connection, transaction, request.Identity.StreamKey);
        var anchor = CaptureInitialAnchor(connection, transaction, request, stream);
        var logicalBytes = ServerSubscriptionJournalOperations.GetSubscriptionBytes(request.Identity, request.StartPosition, anchor.Cursor);
        if (!HasSubscriptionCapacity(connection, transaction, 1, 0, logicalBytes, options))
        {
            DeleteExpiredSubscriptions(connection, transaction, updatedUtc, options);
            if (!HasSubscriptionCapacity(connection, transaction, 1, 0, logicalBytes, options))
            {
                throw new QueueCapacityExceededException("The server subscription acknowledgement journal is full.", canFitWhenEmpty: false);
            }
        }

        InsertSubscription(connection, transaction, request, anchor, updatedUtc, logicalBytes);
        return new(request.Identity, null, 0, null, 0, 0);
    }

    /// <summary>Reads a registered subscription and validates its trusted binding.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="identity">The identity.</param>
    /// <returns>The subscription record.</returns>
    /// <exception cref="InvalidOperationException">The subscription is missing or bound to another identity.</exception>
    private static ServerSubscriptionRecord ReadRegisteredSubscription(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionIdentity identity)
    {
        var record = ReadSubscriptionRecord(connection, transaction, identity.SubscriptionId)
            ?? throw new InvalidOperationException("The subscription is not registered.");

        ThrowIfIdentityMismatch(identity, record);
        return record;
    }

    /// <summary>Reads a subscription record by identifier.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The subscription record or null.</returns>
    private static ServerSubscriptionRecord? ReadSubscriptionRecord(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT subscription_id, tenant_id, stream_id, client_id, initial_position_kind, initial_sequence,
                   initial_timestamp_utc, initial_cursor, initial_anchor_cursor, initial_anchor_group_sequence,
                   initial_anchor_resolved, acknowledged_cursor, acknowledged_group_sequence, latest_offered_cursor,
                   latest_offered_group_sequence, acknowledged_at_utc, updated_at_utc, last_touched_utc, logical_bytes
            FROM oc_server_journal_subscriptions
            WHERE subscription_id = $subscriptionId;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var identity = new ServerSubscriptionIdentity(
            new(
                ReadValidatedText(reader, SubscriptionTenantColumn, "The SQLite server subscription tenant is invalid."),
                ReadStreamId(reader, SubscriptionStreamColumn, "The SQLite server subscription stream is invalid.")),
            ReadValidatedText(reader, SubscriptionClientColumn, "The SQLite server subscription client is invalid."),
            new(ReadGuid(reader, SubscriptionIdColumn, "The SQLite server subscription id is invalid.")));
        var record = new ServerSubscriptionRecord(
            identity,
            ReadDateTimeOffset(reader, SubscriptionUpdatedAtColumn, "The SQLite server subscription timestamp is invalid."),
            ReadNonNegativeLong(reader, SubscriptionLogicalBytesColumn, "The SQLite server subscription logical bytes are invalid."))
        {
            InitialStartPosition = ReadStartPosition(reader),
            InitialAnchorCursor = ReadNullableCursor(reader, SubscriptionInitialAnchorCursorColumn, "The SQLite server subscription initial anchor cursor is invalid."),
            InitialAnchorGroupSequence = ReadNonNegativeLong(reader, SubscriptionInitialAnchorSequenceColumn, "The SQLite server subscription initial anchor sequence is invalid."),
            InitialAnchorResolved = ReadBoolean(reader, SubscriptionInitialAnchorResolvedColumn, "The SQLite server subscription initial anchor marker is invalid."),
            AcknowledgedCursor = ReadNullableCursor(reader, SubscriptionAcknowledgedCursorColumn, "The SQLite server subscription acknowledged cursor is invalid."),
            AcknowledgedGroupSequence = ReadNonNegativeLong(reader, SubscriptionAcknowledgedSequenceColumn, "The SQLite server subscription acknowledged sequence is invalid."),
            LatestOfferedCursor = ReadNullableCursor(reader, SubscriptionLatestOfferedCursorColumn, "The SQLite server subscription offered cursor is invalid."),
            LatestOfferedGroupSequence = ReadNonNegativeLong(reader, SubscriptionLatestOfferedSequenceColumn, "The SQLite server subscription offered sequence is invalid."),
            AcknowledgedAtUtc = ReadNullableDateTimeOffset(reader, SubscriptionAcknowledgedAtColumn, "The SQLite server subscription acknowledgement timestamp is invalid."),
            LastTouchedUtc = ReadDateTimeOffset(reader, SubscriptionLastTouchedColumn, "The SQLite server subscription touch timestamp is invalid."),
        };
        ReadOffers(connection, transaction, record);
        return record;
    }

    /// <summary>Reads offer rows for one subscription.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    private static void ReadOffers(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT cursor, group_sequence, offered_at_utc, logical_bytes
            FROM oc_server_journal_subscription_offers
            WHERE subscription_id = $subscriptionId
            ORDER BY group_sequence ASC, cursor ASC;
            """;
        AddSubscriptionIdParameter(command, record.Identity.SubscriptionId);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var cursor = ReadCursor(reader, OfferCursorColumn, "The SQLite server subscription offer cursor is invalid.");
            record.Offers.Add(
                cursor,
                new(
                    cursor,
                    ReadNonNegativeLong(reader, OfferGroupSequenceColumn, "The SQLite server subscription offer sequence is invalid."),
                    ReadDateTimeOffset(reader, OfferOfferedAtColumn, "The SQLite server subscription offer timestamp is invalid."),
                    ReadNonNegativeLong(reader, OfferLogicalBytesColumn, "The SQLite server subscription offer logical bytes are invalid.")));
        }
    }

    /// <summary>Inserts a subscription row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The registration request.</param>
    /// <param name="anchor">The initial anchor.</param>
    /// <param name="updatedUtc">The update timestamp.</param>
    /// <param name="logicalBytes">The logical bytes.</param>
    private static void InsertSubscription(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRegistrationRequest request,
        ServerSubscriptionInitialAnchor anchor,
        DateTimeOffset updatedUtc,
        long logicalBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_subscriptions
                (subscription_id, tenant_id, stream_id, client_id, initial_position_kind, initial_sequence, initial_timestamp_utc,
                 initial_cursor, initial_anchor_cursor, initial_anchor_group_sequence, initial_anchor_resolved,
                 acknowledged_cursor, acknowledged_group_sequence, latest_offered_cursor, latest_offered_group_sequence,
                 acknowledged_at_utc, updated_at_utc, last_touched_utc, logical_bytes)
            VALUES
                ($subscriptionId, $tenantId, $streamId, $clientId, $initialPositionKind, $initialSequence, $initialTimestampUtc,
                 $initialCursor, $initialAnchorCursor, $initialAnchorGroupSequence, $initialAnchorResolved,
                 NULL, 0, NULL, 0, NULL, $updatedAtUtc, $updatedAtUtc, $logicalBytes);
            """;
        AddSubscriptionIdParameter(command, request.Identity.SubscriptionId);
        AddStreamParameters(command, request.Identity.StreamKey);
        _ = command.Parameters.AddWithValue("$clientId", request.Identity.ClientId);
        AddStartPositionParameters(command, request.StartPosition);
        _ = command.Parameters.AddWithValue("$initialAnchorCursor", (object?)anchor.Cursor ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$initialAnchorGroupSequence", anchor.GroupSequence);
        _ = command.Parameters.AddWithValue("$initialAnchorResolved", anchor.IsResolved ? 1 : 0);
        _ = command.Parameters.AddWithValue(UpdatedAtUtcParameterName, FormatDateTimeOffset(updatedUtc));
        _ = command.Parameters.AddWithValue("$logicalBytes", logicalBytes);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Captures an initial anchor using durable SQLite sequence rows when needed.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The registration request.</param>
    /// <param name="stream">The retained stream.</param>
    /// <returns>The captured anchor.</returns>
    private static ServerSubscriptionInitialAnchor CaptureInitialAnchor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRegistrationRequest request,
        ServerCommitStreamRecord? stream)
    {
        if (request.StartPosition.Kind != StartPositionKind.FromSequence)
        {
            return ServerSubscriptionStartPositionOperations.CaptureInitialAnchor(request.Identity.StreamKey, request.StartPosition, stream);
        }

        return TryResolveSequenceAnchor(connection, transaction, request.Identity.StreamKey, request.StartPosition.Sequence.GetValueOrDefault(), stream, out var anchor)
            == ServerSubscriptionAnchorResolution.Resolved
            ? anchor
            : new(null, 0, false);
    }

    /// <summary>Resolves an initial anchor using durable SQLite sequence rows when needed.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The position kind is invalid.</exception>
    /// <exception cref="ArgumentException">The cursor does not identify a complete group.</exception>
    private static ServerSubscriptionAnchorResolution TryResolveInitialAnchor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor) =>
        record.InitialStartPosition.Kind == StartPositionKind.FromSequence
            ? TryResolveSequenceAnchor(
                connection,
                transaction,
                record.Identity.StreamKey,
                record.InitialStartPosition.Sequence.GetValueOrDefault(),
                stream,
                out anchor)
            : ServerSubscriptionStartPositionOperations.TryResolveAnchor(
                record.Identity.StreamKey,
                record.InitialStartPosition,
                stream,
                out anchor);

    /// <summary>Resolves a sequence anchor from durable event-sequence rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="sequence">The requested event sequence.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    private static ServerSubscriptionAnchorResolution TryResolveSequenceAnchor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        long sequence,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        anchor = default;
        if (sequence == 0)
        {
            anchor = new(null, 0, true);
            return ServerSubscriptionAnchorResolution.Resolved;
        }

        if (stream is null || sequence > stream.LastEventSequence)
        {
            return ServerSubscriptionAnchorResolution.Pending;
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT e.event_sequence, l.group_sequence
            FROM oc_server_journal_events e
            INNER JOIN oc_server_journal_ledger l
                ON l.tenant_id = e.tenant_id AND l.stream_id = e.stream_id
                AND l.client_id = e.client_id AND l.operation_id = e.operation_id
            WHERE e.tenant_id = $tenantId AND e.stream_id = $streamId
                AND e.event_sequence >= $eventSequence AND l.group_sequence IS NOT NULL
            ORDER BY e.event_sequence ASC
            LIMIT 1;
            """;
        AddStreamParameters(command, streamKey);
        _ = command.Parameters.AddWithValue(EventSequenceParameterName, sequence);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return ServerSubscriptionAnchorResolution.RetentionGap;
        }

        var firstSequence = ReadNonNegativeLong(reader, 0, InvalidEventSequenceMessage);
        var groupSequence = ReadNonNegativeLong(reader, 1, InvalidGroupSequenceMessage);
        anchor = CreateBeforeGroupAnchor(streamKey, groupSequence);
        if (firstSequence == sequence)
        {
            return ServerSubscriptionAnchorResolution.Resolved;
        }

        return stream.HasReceiveHistoryGap
            ? ServerSubscriptionAnchorResolution.RetentionGap
            : ServerSubscriptionAnchorResolution.Resolved;
    }

    /// <summary>Creates an anchor immediately before a selected group.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="groupSequence">The selected group sequence.</param>
    /// <returns>The anchor.</returns>
    private static ServerSubscriptionInitialAnchor CreateBeforeGroupAnchor(ServerStreamKey streamKey, long groupSequence)
    {
        var previousGroupSequence = checked(groupSequence - 1);
        return previousGroupSequence == 0
            ? new(null, 0, true)
            : new(ServerReceiveGroupCursor.Create(streamKey, previousGroupSequence), previousGroupSequence, true);
    }

    /// <summary>Reads the immutable initial start position from a subscription row.</summary>
    /// <param name="reader">The row reader.</param>
    /// <returns>The start position.</returns>
    /// <exception cref="InvalidOperationException">The position kind is invalid.</exception>
    private static StartPosition ReadStartPosition(SqliteDataReader reader)
    {
        var kind = (StartPositionKind)ReadNonNegativeLong(reader, SubscriptionInitialPositionKindColumn, "The SQLite server subscription initial position kind is invalid.");
        return kind switch
        {
            StartPositionKind.Latest => StartPosition.Latest,
            StartPositionKind.FromSequence => StartPosition.FromSequence(
                ReadNonNegativeLong(reader, SubscriptionInitialSequenceColumn, "The SQLite server subscription initial sequence is invalid.")),
            StartPositionKind.FromTimestamp => StartPosition.FromTimestamp(
                ReadDateTimeOffset(reader, SubscriptionInitialTimestampColumn, "The SQLite server subscription initial timestamp is invalid.")),
            StartPositionKind.FromCursor => StartPosition.FromCursor(
                ReadCursor(reader, SubscriptionInitialCursorColumn, "The SQLite server subscription initial cursor is invalid.")),
            _ => throw new InvalidOperationException("The SQLite server subscription initial position kind is invalid."),
        };
    }

    /// <summary>Adds initial start position parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="startPosition">The start position.</param>
    private static void AddStartPositionParameters(SqliteCommand command, StartPosition startPosition)
    {
        _ = command.Parameters.AddWithValue("$initialPositionKind", (int)startPosition.Kind);
        _ = command.Parameters.AddWithValue("$initialSequence", startPosition.Sequence.HasValue ? (object)startPosition.Sequence.Value : DBNull.Value);
        _ = command.Parameters.AddWithValue("$initialTimestampUtc", startPosition.Timestamp.HasValue ? FormatDateTimeOffset(startPosition.Timestamp.Value) : DBNull.Value);
        _ = command.Parameters.AddWithValue("$initialCursor", (object?)startPosition.Cursor ?? DBNull.Value);
    }

    /// <summary>Updates a deferred initial anchor on the subscription row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <param name="updatedUtc">The update timestamp.</param>
    /// <param name="logicalBytesDelta">The logical bytes delta.</param>
    /// <exception cref="InvalidOperationException">The subscription row is missing.</exception>
    private static void UpdateInitialAnchor(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        ServerSubscriptionInitialAnchor anchor,
        DateTimeOffset updatedUtc,
        long logicalBytesDelta)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_subscriptions
            SET initial_anchor_cursor = $cursor,
                initial_anchor_group_sequence = $groupSequence,
                initial_anchor_resolved = 1,
                updated_at_utc = $updatedAtUtc,
                last_touched_utc = $updatedAtUtc,
                logical_bytes = logical_bytes + $logicalBytesDelta
            WHERE subscription_id = $subscriptionId;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, (object?)anchor.Cursor ?? DBNull.Value);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, anchor.GroupSequence);
        _ = command.Parameters.AddWithValue(UpdatedAtUtcParameterName, FormatDateTimeOffset(updatedUtc));
        _ = command.Parameters.AddWithValue(LogicalBytesDeltaParameterName, logicalBytesDelta);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingSubscriptionMessage);
    }

    /// <summary>Adds or refreshes an offered cursor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="cursor">The offered cursor.</param>
    /// <param name="groupSequence">The offered group sequence.</param>
    /// <param name="offeredUtc">The offered timestamp.</param>
    /// <param name="options">The journal options.</param>
    /// <exception cref="InvalidOperationException">An offer row is missing.</exception>
    /// <exception cref="QueueCapacityExceededException">The offer storage is full.</exception>
    private static void AddOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        string cursor,
        long groupSequence,
        DateTimeOffset offeredUtc,
        ServerCommitJournalOptions options)
    {
        var latestDelta = groupSequence > record.LatestOfferedGroupSequence
            ? ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.LatestOfferedCursor, cursor)
            : 0;

        if (!record.Offers.ContainsKey(cursor))
        {
            var logicalBytes = ServerSubscriptionJournalOperations.GetOfferBytes(cursor);
            var addedLogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, latestDelta);
            if (!HasSubscriptionCapacity(connection, transaction, 0, 1, addedLogicalBytes, options))
            {
                DeleteExpiredOffers(connection, transaction, offeredUtc, options);
                if (!HasSubscriptionCapacity(connection, transaction, 0, 1, addedLogicalBytes, options))
                {
                    throw new QueueCapacityExceededException("The server subscription acknowledgement offer journal is full.", canFitWhenEmpty: false);
                }
            }

            InsertOffer(connection, transaction, record.Identity.SubscriptionId, cursor, groupSequence, offeredUtc, logicalBytes);
        }
        else
        {
            UpdateOffer(connection, transaction, record.Identity.SubscriptionId, cursor, offeredUtc);
        }

        if (groupSequence > record.LatestOfferedGroupSequence)
        {
            UpdateLatestOffer(connection, transaction, record.Identity.SubscriptionId, cursor, groupSequence, offeredUtc, latestDelta);
            return;
        }

        UpdateSubscriptionUpdatedAt(connection, transaction, record.Identity.SubscriptionId, offeredUtc);
    }

    /// <summary>Inserts an offered cursor row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="groupSequence">The group sequence.</param>
    /// <param name="offeredUtc">The offered timestamp.</param>
    /// <param name="logicalBytes">The logical bytes.</param>
    private static void InsertOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        string cursor,
        long groupSequence,
        DateTimeOffset offeredUtc,
        long logicalBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_subscription_offers
                (subscription_id, cursor, group_sequence, offered_at_utc, logical_bytes)
            VALUES
                ($subscriptionId, $cursor, $groupSequence, $offeredAtUtc, $logicalBytes);
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, cursor);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, groupSequence);
        _ = command.Parameters.AddWithValue("$offeredAtUtc", FormatDateTimeOffset(offeredUtc));
        _ = command.Parameters.AddWithValue("$logicalBytes", logicalBytes);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Refreshes an offered cursor timestamp.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="offeredUtc">The offered timestamp.</param>
    /// <exception cref="InvalidOperationException">The offer row is missing.</exception>
    private static void UpdateOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        string cursor,
        DateTimeOffset offeredUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_subscription_offers
            SET offered_at_utc = $offeredAtUtc
            WHERE subscription_id = $subscriptionId AND cursor = $cursor;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, cursor);
        _ = command.Parameters.AddWithValue("$offeredAtUtc", FormatDateTimeOffset(offeredUtc));
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingOfferMessage);
    }

    /// <summary>Updates the latest offered cursor on the subscription row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="cursor">The latest cursor.</param>
    /// <param name="groupSequence">The group sequence.</param>
    /// <param name="updatedUtc">The update timestamp.</param>
    /// <param name="logicalBytesDelta">The subscription logical byte delta.</param>
    /// <exception cref="InvalidOperationException">The subscription row is missing.</exception>
    private static void UpdateLatestOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        string cursor,
        long groupSequence,
        DateTimeOffset updatedUtc,
        long logicalBytesDelta)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_subscriptions
            SET latest_offered_cursor = $cursor,
                latest_offered_group_sequence = $groupSequence,
                updated_at_utc = $updatedAtUtc,
                last_touched_utc = $updatedAtUtc,
                logical_bytes = logical_bytes + $logicalBytesDelta
            WHERE subscription_id = $subscriptionId;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, cursor);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, groupSequence);
        _ = command.Parameters.AddWithValue(UpdatedAtUtcParameterName, FormatDateTimeOffset(updatedUtc));
        _ = command.Parameters.AddWithValue(LogicalBytesDeltaParameterName, logicalBytesDelta);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingSubscriptionMessage);
    }

    /// <summary>Acknowledges an offered cursor inside an open transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The subscription record.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the transaction.</param>
    /// <returns>The subscription state.</returns>
    /// <exception cref="InvalidOperationException">The acknowledgement is not valid for the subscription.</exception>
    private static ServerSubscriptionState Acknowledge(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        string cursor,
        DateTimeOffset observedUtc)
    {
        if (string.Equals(record.AcknowledgedCursor, cursor, StringComparison.Ordinal))
        {
            var duplicateUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
            UpdateSubscriptionUpdatedAt(connection, transaction, record.Identity.SubscriptionId, duplicateUtc);
            WriteLatestUtc(connection, transaction, duplicateUtc);
            record.UpdatedAtUtc = duplicateUtc;
            record.LastTouchedUtc = duplicateUtc;
            return ServerSubscriptionJournalOperations.CreateState(record);
        }

        if (!record.Offers.TryGetValue(cursor, out var offer))
        {
            throw new InvalidOperationException("The acknowledgement cursor was not offered to this subscription.");
        }

        var acknowledgedUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
        var acknowledgementDelta = ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.AcknowledgedCursor, offer.Cursor);
        UpdateAcknowledgement(connection, transaction, record.Identity.SubscriptionId, offer, acknowledgedUtc, acknowledgementDelta);
        DeleteAcknowledgedOffers(connection, transaction, record.Identity.SubscriptionId, offer.GroupSequence);
        WriteLatestUtc(connection, transaction, acknowledgedUtc);
        var remainingOffers = GetRemainingOfferCount(record, offer.GroupSequence);
        return new(
            record.Identity,
            record.LatestOfferedCursor,
            record.LatestOfferedGroupSequence,
            offer.Cursor,
            offer.GroupSequence,
            remainingOffers);
    }

    /// <summary>Counts offers retained after acknowledging a group sequence.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="acknowledgedGroupSequence">The acknowledged group sequence.</param>
    /// <returns>The remaining offer count.</returns>
    private static int GetRemainingOfferCount(ServerSubscriptionRecord record, long acknowledgedGroupSequence)
    {
        var count = 0;
        foreach (var offer in record.Offers.Values)
        {
            if (offer.GroupSequence > acknowledgedGroupSequence)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Updates the acknowledged cursor on the subscription row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="offer">The acknowledged offer.</param>
    /// <param name="acknowledgedUtc">The acknowledgement timestamp.</param>
    /// <param name="logicalBytesDelta">The subscription logical byte delta.</param>
    /// <exception cref="InvalidOperationException">The subscription row is missing.</exception>
    private static void UpdateAcknowledgement(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        ServerSubscriptionOffer offer,
        DateTimeOffset acknowledgedUtc,
        long logicalBytesDelta)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_subscriptions
            SET acknowledged_cursor = $cursor,
                acknowledged_group_sequence = $groupSequence,
                acknowledged_at_utc = $acknowledgedAtUtc,
                updated_at_utc = $acknowledgedAtUtc,
                last_touched_utc = $acknowledgedAtUtc,
                logical_bytes = logical_bytes + $logicalBytesDelta
            WHERE subscription_id = $subscriptionId;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, offer.Cursor);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, offer.GroupSequence);
        _ = command.Parameters.AddWithValue("$acknowledgedAtUtc", FormatDateTimeOffset(acknowledgedUtc));
        _ = command.Parameters.AddWithValue(LogicalBytesDeltaParameterName, logicalBytesDelta);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingSubscriptionMessage);
    }

    /// <summary>Deletes offers already covered by an acknowledged group sequence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="acknowledgedGroupSequence">The acknowledged sequence.</param>
    private static void DeleteAcknowledgedOffers(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        long acknowledgedGroupSequence)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_server_journal_subscription_offers
            WHERE subscription_id = $subscriptionId AND group_sequence <= $groupSequence;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, acknowledgedGroupSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes expired offers and offers already covered by durable acknowledgements.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="options">The journal options.</param>
    private static void DeleteExpiredOffers(
        SqliteConnection connection,
        SqliteTransaction transaction,
        DateTimeOffset utcNow,
        ServerCommitJournalOptions options)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_server_journal_subscription_offers
            WHERE rowid IN (
                SELECT offer.rowid
                FROM oc_server_journal_subscription_offers AS offer
                INNER JOIN oc_server_journal_subscriptions AS subscription
                    ON subscription.subscription_id = offer.subscription_id
                WHERE offer.group_sequence <= subscription.acknowledged_group_sequence
                   OR offer.offered_at_utc < $expiredBeforeUtc);
            """;
        _ = command.Parameters.AddWithValue("$expiredBeforeUtc", FormatDateTimeOffset(GetExpiryBoundary(utcNow, options)));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes subscription bindings after their binding retention horizon.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="options">The journal options.</param>
    private static void DeleteExpiredSubscriptions(
        SqliteConnection connection,
        SqliteTransaction transaction,
        DateTimeOffset utcNow,
        ServerCommitJournalOptions options)
    {
        DeleteExpiredOffers(connection, transaction, utcNow, options);
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_server_journal_subscriptions
            WHERE last_touched_utc < $expiredBeforeUtc;
            """;
        _ = command.Parameters.AddWithValue("$expiredBeforeUtc", FormatDateTimeOffset(GetSubscriptionExpiryBoundary(utcNow, options)));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Updates the retained subscription row timestamp.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="updatedUtc">The update timestamp.</param>
    /// <exception cref="InvalidOperationException">The subscription row is missing.</exception>
    private static void UpdateSubscriptionUpdatedAt(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        DateTimeOffset updatedUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_subscriptions
            SET updated_at_utc = $updatedAtUtc,
                last_touched_utc = $updatedAtUtc
            WHERE subscription_id = $subscriptionId;
            """;
        AddSubscriptionIdParameter(command, subscriptionId);
        _ = command.Parameters.AddWithValue(UpdatedAtUtcParameterName, FormatDateTimeOffset(updatedUtc));
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingSubscriptionMessage);
    }

    /// <summary>Checks whether subscription acknowledgement storage has capacity.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="addedSubscriptions">The subscriptions to add.</param>
    /// <param name="addedOffers">The offers to add.</param>
    /// <param name="addedLogicalBytes">The logical bytes to add.</param>
    /// <param name="options">The journal options.</param>
    /// <returns>Whether capacity remains.</returns>
    private static bool HasSubscriptionCapacity(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int addedSubscriptions,
        int addedOffers,
        long addedLogicalBytes,
        ServerCommitJournalOptions options)
    {
        var metrics = ReadMetrics(connection, transaction);
        var subscriptionCount = checked((long)metrics.SubscriptionCount + addedSubscriptions);
        var offerCount = checked((long)metrics.SubscriptionOfferCount + addedOffers);
        var logicalBytes = checked(metrics.LogicalBytes + addedLogicalBytes);
        return subscriptionCount <= options.MaximumSubscriptions
            && offerCount <= options.MaximumSubscriptionOffers
            && logicalBytes <= options.MaximumLogicalBytes;
    }

    /// <summary>Gets the oldest subscription binding timestamp allowed at a compaction instant.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="options">The journal options.</param>
    /// <returns>The timestamp before which subscription bindings expire.</returns>
    private static DateTimeOffset GetSubscriptionExpiryBoundary(DateTimeOffset utcNow, ServerCommitJournalOptions options)
    {
        try
        {
            return utcNow.Subtract(options.SubscriptionRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MinValue;
        }
    }

    /// <summary>Rejects an identity that attempts to reuse another binding's subscription id.</summary>
    /// <param name="identity">The supplied identity.</param>
    /// <param name="record">The retained record.</param>
    /// <exception cref="InvalidOperationException">The subscription belongs to another identity.</exception>
    private static void ThrowIfIdentityMismatch(ServerSubscriptionIdentity identity, ServerSubscriptionRecord record)
    {
        if (ServerSubscriptionJournalOperations.IdentityMatches(identity, record))
        {
            return;
        }

        throw new InvalidOperationException("The subscription identifier is already bound to another trusted identity.");
    }

    /// <summary>Rejects an identity or start position that conflicts with retained state.</summary>
    /// <param name="request">The supplied request.</param>
    /// <param name="record">The retained record.</param>
    /// <exception cref="InvalidOperationException">The registration is incompatible.</exception>
    private static void ThrowIfRegistrationMismatch(ServerSubscriptionRegistrationRequest request, ServerSubscriptionRecord record)
    {
        if (ServerSubscriptionJournalOperations.RegistrationMatches(request, record))
        {
            return;
        }

        throw new InvalidOperationException("The subscription registration is incompatible with retained state.");
    }

    /// <summary>Rejects a page that would move a subscription behind its durable acknowledgement.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="nextGroupSequence">The offered page sequence.</param>
    /// <exception cref="InvalidOperationException">The offered page would rewind the subscription.</exception>
    private static void ThrowIfPageRewindsAcknowledgement(ServerSubscriptionRecord record, long nextGroupSequence)
    {
        if (nextGroupSequence > record.AcknowledgedGroupSequence)
        {
            return;
        }

        throw new InvalidOperationException("The offered receive page would rewind the subscription acknowledgement.");
    }

    /// <summary>Reads an optional date-time offset column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The timestamp or null.</returns>
    private static DateTimeOffset? ReadNullableDateTimeOffset(SqliteDataReader reader, int index, string message) =>
        reader.IsDBNull(index) ? null : ReadDateTimeOffset(reader, index, message);

    /// <summary>Adds a subscription id parameter.</summary>
    /// <param name="command">The command.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    private static void AddSubscriptionIdParameter(SqliteCommand command, SubscriptionId subscriptionId) =>
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));

    /// <summary>Gets the oldest retained offer timestamp allowed at a compaction instant.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="options">The journal options.</param>
    /// <returns>The timestamp before which offers expire.</returns>
    private static DateTimeOffset GetExpiryBoundary(DateTimeOffset utcNow, ServerCommitJournalOptions options)
    {
        try
        {
            return utcNow.Subtract(options.OperationRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MinValue;
        }
    }
}
