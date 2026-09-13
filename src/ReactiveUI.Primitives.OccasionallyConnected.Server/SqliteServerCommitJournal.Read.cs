// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides SQLite read and reconstruction helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>The stream revision column index.</summary>
    private const int StreamRevisionColumn = 0;

    /// <summary>The stream state version column index.</summary>
    private const int StreamStateVersionColumn = 1;

    /// <summary>The stream state payload contract column index.</summary>
    private const int StreamStatePayloadContractColumn = 2;

    /// <summary>The stream state payload schema column index.</summary>
    private const int StreamStatePayloadSchemaColumn = 3;

    /// <summary>The stream state payload content type column index.</summary>
    private const int StreamStatePayloadContentTypeColumn = 4;

    /// <summary>The stream state payload bytes column index.</summary>
    private const int StreamStatePayloadColumn = 5;

    /// <summary>The stream state payload hash column index.</summary>
    private const int StreamStatePayloadHashColumn = 6;

    /// <summary>The stream write stamp committed column index.</summary>
    private const int StreamWriteStampCommittedColumn = 7;

    /// <summary>The stream write stamp client column index.</summary>
    private const int StreamWriteStampClientColumn = 8;

    /// <summary>The stream write stamp operation column index.</summary>
    private const int StreamWriteStampOperationColumn = 9;

    /// <summary>The stream cursor column index.</summary>
    private const int StreamLastCursorColumn = 10;

    /// <summary>The stream event sequence column index.</summary>
    private const int StreamLastEventSequenceColumn = 11;

    /// <summary>The stream state byte count column index.</summary>
    private const int StreamStateBytesColumn = 12;

    /// <summary>The stream cursor byte count column index.</summary>
    private const int StreamLastCursorBytesColumn = 13;

    /// <summary>The stream last receive group sequence column index.</summary>
    private const int StreamLastGroupSequenceColumn = 14;

    /// <summary>The stream receive history gap marker column index.</summary>
    private const int StreamReceiveHistoryGapColumn = 15;

    /// <summary>The ledger client column index.</summary>
    private const int LedgerClientColumn = 0;

    /// <summary>The ledger operation column index.</summary>
    private const int LedgerOperationColumn = 1;

    /// <summary>The ledger fingerprint column index.</summary>
    private const int LedgerFingerprintColumn = 2;

    /// <summary>The ledger result kind column index.</summary>
    private const int LedgerResultKindColumn = 3;

    /// <summary>The ledger reason code column index.</summary>
    private const int LedgerReasonCodeColumn = 4;

    /// <summary>The ledger server version column index.</summary>
    private const int LedgerServerVersionColumn = 5;

    /// <summary>The ledger committed timestamp column index.</summary>
    private const int LedgerCommittedAtColumn = 6;

    /// <summary>The ledger expiry timestamp column index.</summary>
    private const int LedgerExpiresAtColumn = 7;

    /// <summary>The ledger logical byte count column index.</summary>
    private const int LedgerLogicalBytesColumn = 8;

    /// <summary>The ledger receive group sequence column index.</summary>
    private const int LedgerGroupSequenceColumn = 9;

    /// <summary>The invalid group sequence message.</summary>
    private const string InvalidGroupSequenceMessage = "The SQLite server journal group sequence is invalid.";

    /// <summary>The conflict resolution code column index.</summary>
    private const int ConflictResolutionCodeColumn = 0;

    /// <summary>The conflict payload contract column index.</summary>
    private const int ConflictPayloadContractColumn = 1;

    /// <summary>The conflict payload schema column index.</summary>
    private const int ConflictPayloadSchemaColumn = 2;

    /// <summary>The conflict payload content type column index.</summary>
    private const int ConflictPayloadContentTypeColumn = 3;

    /// <summary>The conflict payload bytes column index.</summary>
    private const int ConflictPayloadColumn = 4;

    /// <summary>The conflict payload hash column index.</summary>
    private const int ConflictPayloadHashColumn = 5;

    /// <summary>The event sequence column index.</summary>
    private const int EventSequenceColumn = 0;

    /// <summary>The event id column index.</summary>
    private const int EventIdColumn = 1;

    /// <summary>The event cursor column index.</summary>
    private const int EventCursorColumn = 2;

    /// <summary>The event timestamp column index.</summary>
    private const int EventCommittedAtColumn = 3;

    /// <summary>The caused-by operation column index.</summary>
    private const int EventCausedByOperationColumn = 4;

    /// <summary>The event origin client column index.</summary>
    private const int EventOriginClientColumn = 5;

    /// <summary>The event origin operation column index.</summary>
    private const int EventOriginOperationColumn = 6;

    /// <summary>The event payload contract column index.</summary>
    private const int EventPayloadContractColumn = 7;

    /// <summary>The event payload schema column index.</summary>
    private const int EventPayloadSchemaColumn = 8;

    /// <summary>The event payload content type column index.</summary>
    private const int EventPayloadContentTypeColumn = 9;

    /// <summary>The event payload bytes column index.</summary>
    private const int EventPayloadColumn = 10;

    /// <summary>The event payload hash column index.</summary>
    private const int EventPayloadHashColumn = 11;

    /// <summary>The metrics state byte count column index.</summary>
    private const int MetricsStateBytesColumn = 2;

    /// <summary>The metrics cursor byte count column index.</summary>
    private const int MetricsCursorBytesColumn = 3;

    /// <summary>Attempts to read a stream record.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The stream record.</param>
    /// <returns>Whether the stream exists.</returns>
    private static bool TryReadStreamRecord(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        out ServerCommitStreamRecord? stream)
    {
        stream = ReadStreamHeader(connection, transaction, streamKey);
        if (stream is null)
        {
            return false;
        }

        ReadLedger(connection, transaction, streamKey, stream);
        return true;
    }

    /// <summary>Reads a stream record if present.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The stream record or null.</returns>
    private static ServerCommitStreamRecord? ReadStreamRecord(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey)
    {
        _ = TryReadStreamRecord(connection, transaction, streamKey, out var stream);
        return stream;
    }

    /// <summary>Reads one stream header.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The stream record or null.</returns>
    private static ServerCommitStreamRecord? ReadStreamHeader(SqliteConnection connection, SqliteTransaction transaction, ServerStreamKey streamKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT revision, state_version, state_payload_contract_id, state_payload_schema_version,
                   state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc,
                   write_stamp_client_id, write_stamp_operation_id, last_cursor, last_event_sequence,
                   state_bytes, last_cursor_bytes, last_group_sequence, receive_history_incomplete
            FROM oc_server_journal_streams
            WHERE tenant_id = $tenantId AND stream_id = $streamId;
            """;
        AddStreamParameters(command, streamKey);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var stream = new ServerCommitStreamRecord
        {
            Revision = ReadNonNegativeLong(reader, StreamRevisionColumn, "The SQLite server journal revision is invalid."),
            State = ReadNullableState(
                reader,
                streamKey.StreamId,
                new(
                    StreamStateVersionColumn,
                    new(
                        StreamStatePayloadContractColumn,
                        StreamStatePayloadSchemaColumn,
                        StreamStatePayloadContentTypeColumn,
                        StreamStatePayloadColumn,
                        StreamStatePayloadHashColumn))),
            LastWriteStamp = ReadNullableWriteStamp(reader, StreamWriteStampCommittedColumn, StreamWriteStampClientColumn, StreamWriteStampOperationColumn),
            LastCursor = ReadNullableCursor(reader, StreamLastCursorColumn, "The SQLite server journal cursor is invalid."),
            LastEventSequence = ReadNonNegativeLong(reader, StreamLastEventSequenceColumn, InvalidEventSequenceMessage),
            StateBytes = ReadNonNegativeLong(reader, StreamStateBytesColumn, "The SQLite server journal state bytes are invalid."),
            LastCursorBytes = ReadNonNegativeLong(reader, StreamLastCursorBytesColumn, "The SQLite server journal cursor bytes are invalid."),
            LastGroupSequence = ReadNonNegativeLong(reader, StreamLastGroupSequenceColumn, InvalidGroupSequenceMessage),
            HasReceiveHistoryGap = ReadBoolean(reader, StreamReceiveHistoryGapColumn, "The SQLite server journal receive gap marker is invalid."),
        };
        return stream;
    }

    /// <summary>Reads retained ledger rows for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The stream record.</param>
    private static void ReadLedger(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT client_id, operation_id, fingerprint, result_kind, result_reason_code, result_server_version,
                   committed_at_utc, expires_at_utc, logical_bytes, group_sequence
            FROM oc_server_journal_ledger
            WHERE tenant_id = $tenantId AND stream_id = $streamId
            ORDER BY group_sequence IS NULL ASC, group_sequence ASC, rowid ASC;
            """;
        AddStreamParameters(command, streamKey);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var operationKey = ReadOperationKey(reader, LedgerClientColumn, LedgerOperationColumn);
            var entry = new ServerLedgerEntry(
                    operationKey,
                    new(ReadFingerprint(reader, LedgerFingerprintColumn)),
                    new(
                        operationKey.OperationId,
                        ReadResultKind(reader, LedgerResultKindColumn),
                        ReadNullableString(reader, LedgerReasonCodeColumn, "The SQLite server journal result reason code is invalid."),
                        ReadNullableString(reader, LedgerServerVersionColumn, "The SQLite server journal result server version is invalid.")),
                    ReadConflicts(connection, transaction, streamKey, operationKey),
                    ReadEvents(connection, transaction, streamKey, operationKey))
                .Commit(
                    ReadDateTimeOffset(reader, LedgerCommittedAtColumn, "The SQLite server journal commit timestamp is invalid."),
                    ReadDateTimeOffset(reader, LedgerExpiresAtColumn, "The SQLite server journal expiry timestamp is invalid."));
            var logicalBytes = ReadNonNegativeLong(reader, LedgerLogicalBytesColumn, InvalidLogicalBytesMessage);
            var groupSequence = ReadNullableNonNegativeLong(reader, LedgerGroupSequenceColumn, InvalidGroupSequenceMessage);
            if (groupSequence.HasValue)
            {
                ServerCommitJournalOperations.AddLedgerRow(stream, streamKey, entry, logicalBytes, groupSequence.Value);
            }
            else
            {
                ServerCommitJournalOperations.AddUnsequencedLedgerRow(stream, streamKey, entry, logicalBytes);
            }
        }

        stream.LastEventSequence = ReadLastEventSequence(connection, transaction, streamKey);
        stream.LastGroupSequence = ReadLastGroupSequence(connection, transaction, streamKey);
    }

    /// <summary>Reads conflict rows for one ledger entry.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="operationKey">The operation key.</param>
    /// <returns>The conflict rows.</returns>
    private static List<ResolvedConflict> ReadConflicts(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerOperationKey operationKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT resolution_code, resolved_payload_contract_id, resolved_payload_schema_version,
                   resolved_payload_content_type, resolved_payload, resolved_payload_hash
            FROM oc_server_journal_conflicts
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND client_id = $clientId AND operation_id = $operationId
            ORDER BY conflict_index ASC;
            """;
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, operationKey);
        using var reader = command.ExecuteReader();
        var conflicts = new List<ResolvedConflict>();
        while (reader.Read())
        {
            conflicts.Add(new(
                operationKey.OperationId,
                ReadValidatedText(reader, ConflictResolutionCodeColumn, "The SQLite server journal conflict resolution is invalid."),
                ReadNullablePayload(reader, new(ConflictPayloadContractColumn, ConflictPayloadSchemaColumn, ConflictPayloadContentTypeColumn, ConflictPayloadColumn, ConflictPayloadHashColumn))));
        }

        return conflicts;
    }

    /// <summary>Reads event rows for one ledger entry.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="operationKey">The operation key.</param>
    /// <returns>The event rows.</returns>
    private static List<RemoteEvent> ReadEvents(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerOperationKey operationKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT event_sequence, event_id, server_cursor, committed_at_utc, caused_by_operation_id,
                   origin_client_id, origin_operation_id, payload_contract_id, payload_schema_version,
                   payload_content_type, payload, payload_hash
            FROM oc_server_journal_events
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND client_id = $clientId AND operation_id = $operationId
            ORDER BY event_index ASC;
            """;
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, operationKey);
        using var reader = command.ExecuteReader();
        var events = new List<RemoteEvent>();
        while (reader.Read())
        {
            var sequence = ReadNonNegativeLong(reader, EventSequenceColumn, InvalidEventSequenceMessage);
            var origin = ReadNullableOrigin(reader, EventOriginClientColumn, EventOriginOperationColumn);
            var remoteEvent = new RemoteEvent(
                ReadGuid(reader, EventIdColumn, "The SQLite server journal event id is invalid."),
                streamKey.StreamId,
                ReadCursor(reader, EventCursorColumn, "The SQLite server journal event cursor is invalid."),
                ReadDateTimeOffset(reader, EventCommittedAtColumn, "The SQLite server journal event timestamp is invalid."),
                ReadNullableOperationId(reader, EventCausedByOperationColumn),
                ReadPayload(reader, new(EventPayloadContractColumn, EventPayloadSchemaColumn, EventPayloadContentTypeColumn, EventPayloadColumn, EventPayloadHashColumn)),
                ReadEventMetadata(connection, transaction, streamKey, sequence))
            { Origin = origin };
            events.Add(remoteEvent);
        }

        return events;
    }

    /// <summary>Reads event metadata rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="eventSequence">The event sequence.</param>
    /// <returns>The metadata.</returns>
    private static Dictionary<string, string> ReadEventMetadata(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        long eventSequence)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT key, value
            FROM oc_server_journal_event_metadata
            WHERE tenant_id = $tenantId AND stream_id = $streamId AND event_sequence = $eventSequence
            ORDER BY key ASC;
            """;
        AddStreamParameters(command, streamKey);
        _ = command.Parameters.AddWithValue(EventSequenceParameterName, eventSequence);
        using var reader = command.ExecuteReader();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        while (reader.Read())
        {
            metadata.Add(
                    ReadString(reader, 0, "The SQLite server journal metadata key is invalid."),
                ReadString(reader, 1, "The SQLite server journal metadata value is invalid."));
        }

        return metadata;
    }

    /// <summary>Reads retained metrics from an open transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The retained metrics.</returns>
    private static RetainedMetrics ReadMetrics(SqliteConnection connection, SqliteTransaction transaction)
    {
        var metrics = new RetainedMetrics();
        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "SELECT tenant_id, stream_id, state_bytes, last_cursor_bytes FROM oc_server_journal_streams;";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var streamKey = new ServerStreamKey(
                    ReadValidatedText(reader, 0, "The SQLite server journal tenant is invalid."),
                    ReadStreamId(reader, 1, "The SQLite server journal stream is invalid."));
                metrics.StreamCount++;
                metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(metrics.LogicalBytes, ServerCommitJournalSizer.GetStreamKeyBytes(streamKey));
                metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                    metrics.LogicalBytes,
                    ReadNonNegativeLong(reader, MetricsStateBytesColumn, "The SQLite server journal state bytes are invalid."));
                metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                    metrics.LogicalBytes,
                    ReadNonNegativeLong(reader, MetricsCursorBytesColumn, "The SQLite server journal cursor bytes are invalid."));
            }
        }

        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "SELECT logical_bytes FROM oc_server_journal_ledger;";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                metrics.LedgerEntryCount++;
                metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                    metrics.LogicalBytes,
                    ReadNonNegativeLong(reader, 0, InvalidLogicalBytesMessage));
            }
        }

        AddSubscriptionMetrics(connection, transaction, metrics);
        AddSubscriptionOfferMetrics(connection, transaction, metrics);

        metrics.EventCount = ReadEventCount(connection, transaction);
        return metrics;
    }

    /// <summary>Adds subscription row accounting to retained metrics.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="metrics">The metrics to update.</param>
    private static void AddSubscriptionMetrics(
        SqliteConnection connection,
        SqliteTransaction transaction,
        RetainedMetrics metrics)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT logical_bytes FROM oc_server_journal_subscriptions;";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            metrics.SubscriptionCount++;
            metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                metrics.LogicalBytes,
                ReadNonNegativeLong(reader, 0, "The SQLite server subscription logical bytes are invalid."));
        }
    }

    /// <summary>Adds offered cursor accounting to retained metrics.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="metrics">The metrics to update.</param>
    private static void AddSubscriptionOfferMetrics(
        SqliteConnection connection,
        SqliteTransaction transaction,
        RetainedMetrics metrics)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT logical_bytes FROM oc_server_journal_subscription_offers;";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            metrics.SubscriptionOfferCount++;
            metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                metrics.LogicalBytes,
                ReadNonNegativeLong(reader, 0, "The SQLite server subscription offer logical bytes are invalid."));
        }
    }

    /// <summary>Reads projected expired metrics.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The projected metrics.</returns>
    private static RetainedMetrics ReadExpiredMetrics(SqliteConnection connection, SqliteTransaction transaction, DateTimeOffset utcNow)
    {
        var metrics = new RetainedMetrics();
        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "SELECT logical_bytes FROM oc_server_journal_ledger WHERE expires_at_utc < $utcNow;";
            _ = command.Parameters.AddWithValue(UtcNowParameterName, FormatDateTimeOffset(utcNow));
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                metrics.LedgerEntryCount++;
                metrics.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(
                    metrics.LogicalBytes,
                    ReadNonNegativeLong(reader, 0, InvalidLogicalBytesMessage));
            }
        }

        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                SELECT COUNT(*)
                FROM oc_server_journal_events AS event
                INNER JOIN oc_server_journal_ledger AS ledger
                    ON ledger.tenant_id = event.tenant_id
                    AND ledger.stream_id = event.stream_id
                    AND ledger.client_id = event.client_id
                    AND ledger.operation_id = event.operation_id
                WHERE ledger.expires_at_utc < $utcNow;
                """;
            _ = command.Parameters.AddWithValue(UtcNowParameterName, FormatDateTimeOffset(utcNow));
            metrics.EventCount = ReadCount(command.ExecuteScalar(), "The SQLite server journal event count is invalid.");
        }

        return metrics;
    }

    /// <summary>Deletes expired ledger rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The deleted ledger count.</returns>
    private static int DeleteExpired(SqliteConnection connection, SqliteTransaction transaction, DateTimeOffset utcNow)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM oc_server_journal_ledger WHERE expires_at_utc < $utcNow;";
        _ = command.Parameters.AddWithValue(UtcNowParameterName, FormatDateTimeOffset(utcNow));
        return command.ExecuteNonQuery();
    }

    /// <summary>Reads the last event sequence for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The last event sequence.</returns>
    private static long ReadLastEventSequence(SqliteConnection connection, SqliteTransaction transaction, ServerStreamKey streamKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT last_event_sequence
            FROM oc_server_journal_streams
            WHERE tenant_id = $tenantId AND stream_id = $streamId;
            """;
        AddStreamParameters(command, streamKey);
        return ReadNonNegativeLong(command.ExecuteScalar(), InvalidEventSequenceMessage);
    }

    /// <summary>Reads the last receive group sequence for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The last group sequence.</returns>
    private static long ReadLastGroupSequence(SqliteConnection connection, SqliteTransaction transaction, ServerStreamKey streamKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT last_group_sequence
            FROM oc_server_journal_streams
            WHERE tenant_id = $tenantId AND stream_id = $streamId;
            """;
        AddStreamParameters(command, streamKey);
        return ReadNonNegativeLong(command.ExecuteScalar(), InvalidGroupSequenceMessage);
    }

    /// <summary>Reads the latest UTC high-water timestamp.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The latest timestamp.</returns>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset ReadLatestUtc(SqliteConnection connection, SqliteTransaction transaction) =>
        ParseDateTimeOffset(SelectMetadata(connection, transaction, LatestUtcKey), "The SQLite server journal timestamp is invalid.");

    /// <summary>Writes the latest UTC high-water timestamp.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="utc">The timestamp.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteLatestUtc(SqliteConnection connection, SqliteTransaction transaction, DateTimeOffset utc) =>
        WriteMetadataValue(connection, transaction, LatestUtcKey, FormatDateTimeOffset(utc));

    /// <summary>Writes a metadata value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void WriteMetadataValue(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string key,
        string value)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE oc_server_journal_metadata SET value = $value WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue(ValueParameterName, value);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite server journal metadata is incomplete.");
    }

    /// <summary>Reads a metadata value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The metadata value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static string SelectMetadata(SqliteConnection connection, SqliteTransaction transaction, string key)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT value FROM oc_server_journal_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        return ReadStorage<string>(command.ExecuteScalar(), "The SQLite server journal metadata is incomplete.");
    }

    /// <summary>Inserts a metadata value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    private static void InsertMetadata(SqliteConnection connection, SqliteTransaction transaction, string key, string value)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO oc_server_journal_metadata (key, value) VALUES ($key, $value);";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue(ValueParameterName, value);
        _ = command.ExecuteNonQuery();
    }
}
