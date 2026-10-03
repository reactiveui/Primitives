// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides static SQLite creation and write helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Computes the retained cursor byte delta for a commit.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The retained cursor byte delta.</returns>
    private static long GetLastCursorDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.LastCursor is null ? 0 : commit.LastCursorBytes - stream.LastCursorBytes;

    /// <summary>Reads the durable subscription generation high-water value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The current high-water value.</returns>
    /// <exception cref="InvalidOperationException">The stored generation value is invalid.</exception>
    private static long ReadSubscriptionGenerationHighWater(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var value = SelectMetadata(connection, transaction, SubscriptionGenerationHighWaterKey);
        return long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var generation) && generation >= 0
            ? generation
            : throw new InvalidOperationException("The SQLite server subscription generation high-water value is invalid.");
    }

    /// <summary>Writes the durable subscription generation high-water value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="generation">The high-water value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteSubscriptionGenerationHighWater(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        long generation) =>
        WriteMetadataValue(connection, transaction, SubscriptionGenerationHighWaterKey, generation.ToString(CultureInfo.InvariantCulture));

    /// <summary>Creates the SQLite schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSchema(SqliteDatabase connection, SqliteTransaction transaction)
    {
        SetUserVersion(connection, transaction);
        CreateMetadataTable(connection, transaction);
        CreateStreamsTable(connection, transaction);
        CreateLedgerTable(connection, transaction);
        CreateConflictsTable(connection, transaction);
        CreateEventsTable(connection, transaction);
        CreateEventMetadataTable(connection, transaction);
        CreateSubscriptionsTable(connection, transaction);
        CreateSubscriptionOffersTable(connection, transaction);
        InsertMetadata(connection, transaction, SchemaVersionKey, CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture));
        InsertMetadata(connection, transaction, LatestUtcKey, FormatDateTimeOffset(DateTimeOffset.MinValue));
        InsertMetadata(connection, transaction, SubscriptionGenerationHighWaterKey, "0");
    }

    /// <summary>Validates the current durable schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateExistingSchema(SqliteDatabase connection, SqliteTransaction transaction) =>
        ValidateExistingSchema(connection, transaction, GetUserVersion(connection, transaction));

    /// <summary>Validates the current durable schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="userVersion">The SQLite user version.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateExistingSchema(SqliteDatabase connection, SqliteTransaction transaction, long userVersion)
    {
        if (userVersion != CurrentSchemaVersion)
        {
            throw new InvalidOperationException("The SQLite server journal schema version is not supported.");
        }

        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
                SubscriptionOffersTableName,
                SubscriptionsTableName,
            ]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        ValidateTableDefinition(connection, transaction, StreamsTableName, StreamsTableSql);
        ValidateTableDefinition(connection, transaction, LedgerTableName, LedgerTableSql);
        ValidateTableDefinition(connection, transaction, ConflictsTableName, ConflictsTableSql);
        ValidateTableDefinition(connection, transaction, EventsTableName, EventsTableSql);
        ValidateTableDefinition(connection, transaction, EventMetadataTableName, EventMetadataTableSql);
        ValidateTableDefinition(connection, transaction, SubscriptionsTableName, SubscriptionsTableSql);
        ValidateTableDefinition(connection, transaction, SubscriptionOffersTableName, SubscriptionOffersTableSql);
        var metadataSchemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (metadataSchemaVersion == CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            _ = ReadSubscriptionGenerationHighWater(connection, transaction);
            return;
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Upserts one stream after admission.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The current stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void UpsertStream(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream,
        ServerCommitValidationResult commit)
    {
        ServerCommitJournalOperations.ApplyState(stream, commit);
        var lastCursor = commit.LastCursor ?? stream.LastCursor;
        var lastCursorBytes = commit.LastCursor is null ? stream.LastCursorBytes : commit.LastCursorBytes;
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            UPDATE oc_server_journal_streams
            SET revision = $revision,
                state_version = $stateVersion,
                state_payload_contract_id = $statePayloadContractId,
                state_payload_schema_version = $statePayloadSchemaVersion,
                state_payload_content_type = $statePayloadContentType,
                state_payload = $statePayload,
                state_payload_hash = $statePayloadHash,
                write_stamp_committed_at_utc = $writeStampCommittedAtUtc,
                write_stamp_client_id = $writeStampClientId,
                write_stamp_operation_id = $writeStampOperationId,
                last_cursor = $lastCursor,
                last_event_sequence = $lastEventSequence,
                state_bytes = $stateBytes,
                last_cursor_bytes = $lastCursorBytes,
                last_group_sequence = $lastGroupSequence,
                receive_history_incomplete = $receiveHistoryIncomplete
            WHERE tenant_id = $tenantId AND stream_id = $streamId;
            """);
        AddStreamParameters(command, streamKey);
        AddNullableStateParameters(command, stream.State, stream.StateBytes);
        AddNullableWriteStampParameters(command, stream.LastWriteStamp);
        _ = command.Bind("$revision", checked(stream.Revision + 1));
        _ = command.Bind("$lastCursor", (object?)lastCursor ?? DBNull.Value);
        _ = command.Bind("$lastEventSequence", checked(stream.LastEventSequence + commit.EventCount));
        _ = command.Bind("$lastCursorBytes", lastCursorBytes);
        _ = command.Bind("$lastGroupSequence", checked(stream.LastGroupSequence + commit.Entries.Length));
        _ = command.Bind("$receiveHistoryIncomplete", Convert.ToInt32(stream.HasReceiveHistoryGap));
        if (command.Execute() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite server journal stream row is missing.");
    }

    /// <summary>Inserts a stream shell.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    private static void InsertStream(SqliteDatabase connection, SqliteTransaction transaction, ServerStreamKey streamKey)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_server_journal_streams
                (tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
                 state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc, write_stamp_client_id,
                 write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes, last_cursor_bytes,
                 last_group_sequence, receive_history_incomplete)
            VALUES
                ($tenantId, $streamId, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0);
            """);
        AddStreamParameters(command, streamKey);
        _ = command.Execute();
    }

    /// <summary>Inserts committed ledger rows and sidecars.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entries">The committed entries.</param>
    /// <param name="entryBytes">The logical bytes per entry.</param>
    private static void InsertLedger(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry[] entries,
        long[] entryBytes)
    {
        var nextEventSequence = ReadLastEventSequence(connection, transaction, streamKey);
        var nextGroupSequence = ReadLastGroupSequence(connection, transaction, streamKey);
        for (var index = 0; index < entries.Length; index++)
        {
            nextGroupSequence = checked(nextGroupSequence + 1);
            InsertLedgerEntry(connection, transaction, streamKey, entries[index], entryBytes[index], nextGroupSequence);
            InsertConflicts(connection, transaction, streamKey, entries[index]);
            nextEventSequence = InsertEvents(connection, transaction, streamKey, entries[index], nextEventSequence);
        }
    }

    /// <summary>Inserts one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="logicalBytes">The retained logical bytes.</param>
    /// <param name="groupSequence">The receive group sequence.</param>
    private static void InsertLedgerEntry(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long logicalBytes,
        long groupSequence)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_server_journal_ledger
                (tenant_id, stream_id, client_id, operation_id, fingerprint, result_kind, result_reason_code,
                 result_server_version, committed_at_utc, expires_at_utc, logical_bytes, group_sequence)
            VALUES
                ($tenantId, $streamId, $clientId, $operationId, $fingerprint, $resultKind, $resultReasonCode,
                 $resultServerVersion, $committedAtUtc, $expiresAtUtc, $logicalBytes, $groupSequence);
            """);
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, entry.OperationKey);
        AddFingerprintParameter(command, entry.Fingerprint);
        _ = command.Bind("$resultKind", (int)entry.Result.Kind);
        _ = command.Bind("$resultReasonCode", (object?)entry.Result.ReasonCode ?? DBNull.Value);
        _ = command.Bind("$resultServerVersion", (object?)entry.Result.ServerVersion ?? DBNull.Value);
        _ = command.Bind("$committedAtUtc", FormatDateTimeOffset(entry.CommittedAtUtc));
        _ = command.Bind("$expiresAtUtc", FormatDateTimeOffset(entry.ExpiresAtUtc));
        _ = command.Bind("$logicalBytes", logicalBytes);
        _ = command.Bind("$groupSequence", groupSequence);
        _ = command.Execute();
    }

    /// <summary>Inserts conflict sidecars for one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    private static void InsertConflicts(SqliteDatabase connection, SqliteTransaction transaction, ServerStreamKey streamKey, ServerLedgerEntry entry)
    {
        for (var index = 0; index < entry.Conflicts.Count; index++)
        {
            var conflict = entry.Conflicts[index];
            using var command = connection.CreateStatement();
            command.UseTransaction(transaction);
            command.SetSql("""
                INSERT INTO oc_server_journal_conflicts
                    (tenant_id, stream_id, client_id, operation_id, conflict_index, resolution_code,
                     resolved_payload_contract_id, resolved_payload_schema_version, resolved_payload_content_type,
                     resolved_payload, resolved_payload_hash)
                VALUES
                    ($tenantId, $streamId, $clientId, $operationId, $conflictIndex, $resolutionCode,
                     $resolvedPayloadContractId, $resolvedPayloadSchemaVersion, $resolvedPayloadContentType,
                     $resolvedPayload, $resolvedPayloadHash);
                """);
            AddStreamParameters(command, streamKey);
            AddOperationParameters(command, entry.OperationKey);
            _ = command.Bind("$conflictIndex", index);
            _ = command.Bind("$resolutionCode", conflict.ResolutionCode);
            AddNullablePayloadParameters(command, "resolved", conflict.ResolvedPayload);
            _ = command.Execute();
        }
    }

    /// <summary>Inserts event sidecars for one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="nextEventSequence">The last event sequence.</param>
    /// <returns>The new last event sequence.</returns>
    private static long InsertEvents(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long nextEventSequence)
    {
        for (var index = 0; index < entry.Events.Count; index++)
        {
            nextEventSequence = checked(nextEventSequence + 1);
            var remoteEvent = entry.Events[index];
            InsertEvent(connection, transaction, streamKey, entry.OperationKey, remoteEvent, index, nextEventSequence);
            InsertEventMetadata(connection, transaction, streamKey, nextEventSequence, remoteEvent);
        }

        return nextEventSequence;
    }

    /// <summary>Inserts one event row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="operationKey">The operation key.</param>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="eventIndex">The event index inside the entry.</param>
    /// <param name="eventSequence">The stream event sequence.</param>
    private static void InsertEvent(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerOperationKey operationKey,
        RemoteEvent remoteEvent,
        int eventIndex,
        long eventSequence)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_server_journal_events
                (tenant_id, stream_id, event_sequence, client_id, operation_id, event_index, event_id, server_cursor,
                 committed_at_utc, caused_by_operation_id, origin_client_id, origin_operation_id, payload_contract_id,
                 payload_schema_version, payload_content_type, payload, payload_hash)
            VALUES
                ($tenantId, $streamId, $eventSequence, $clientId, $operationId, $eventIndex, $eventId, $serverCursor,
                 $committedAtUtc, $causedByOperationId, $originClientId, $originOperationId, $payloadContractId,
                 $payloadSchemaVersion, $payloadContentType, $payload, $payloadHash);
            """);
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, operationKey);
        _ = command.Bind(EventSequenceParameterName, eventSequence);
        _ = command.Bind("$eventIndex", eventIndex);
        _ = command.Bind("$eventId", remoteEvent.EventId.ToString("D"));
        _ = command.Bind("$serverCursor", remoteEvent.ServerCursor);
        _ = command.Bind("$committedAtUtc", FormatDateTimeOffset(remoteEvent.CommittedAtUtc));
        _ = command.Bind("$causedByOperationId", remoteEvent.CausedByOperationId.HasValue ? remoteEvent.CausedByOperationId.Value.Value.ToString("D") : DBNull.Value);
        _ = command.Bind("$originClientId", (object?)remoteEvent.Origin?.ClientId ?? DBNull.Value);
        _ = command.Bind("$originOperationId", remoteEvent.Origin is null ? DBNull.Value : remoteEvent.Origin.OperationId.Value.ToString("D"));
        AddPayloadParameters(command, remoteEvent.Payload);
        _ = command.Execute();
    }

    /// <summary>Inserts event metadata rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="eventSequence">The event sequence.</param>
    /// <param name="remoteEvent">The event.</param>
    private static void InsertEventMetadata(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        long eventSequence,
        RemoteEvent remoteEvent)
    {
        foreach (var pair in remoteEvent.Metadata)
        {
            using var command = connection.CreateStatement();
            command.UseTransaction(transaction);
            command.SetSql("""
                INSERT INTO oc_server_journal_event_metadata
                    (tenant_id, stream_id, event_sequence, key, value)
                VALUES
                    ($tenantId, $streamId, $eventSequence, $key, $value);
                """);
            AddStreamParameters(command, streamKey);
            _ = command.Bind(EventSequenceParameterName, eventSequence);
            _ = command.Bind("$key", pair.Key);
            _ = command.Bind(ValueParameterName, pair.Value);
            _ = command.Execute();
        }
    }
}
