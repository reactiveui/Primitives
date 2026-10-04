// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Lists every protected column for the encryption transition and key rotation.</summary>
/// <remarks>
/// Protected columns: outbox payload, payload hash, base version and commit fingerprint; original authoritative mutation
/// payload and hash; outbox metadata values; snapshot payload, hash and cursor; current authoritative snapshot payload and
/// hash; stream cursors; inbox cursors; quarantine reason code, cursor and evidence; and dead-letter reason codes.
/// Identifiers, sequences, revisions, states, timestamps, payload contract identifiers, schema versions and content types
/// stay in plaintext for queries and are bound into the associated data of the protected values instead.
/// </remarks>
internal static class SqliteRecordProtectionTables
{
    /// <summary>The outbox batch query.</summary>
    internal const string OutboxSelectSql = """
        SELECT rowid, store_identity, operation_id, stream_id, client_sequence, operation_type,
               payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash,
               base_version, commit_fingerprint
        FROM oc_outbox WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The outbox update statement.</summary>
    internal const string OutboxUpdateSql = """
        UPDATE oc_outbox
        SET payload = $payload, payload_hash = $payload_hash, base_version = $base_version,
            commit_fingerprint = $commit_fingerprint
        WHERE rowid = $rowId;
        """;

    /// <summary>The original authoritative mutation batch query.</summary>
    internal const string OutboxAuthoritativeMutationsSelectSql = """
        SELECT rowid, store_identity, operation_id, payload_contract_id, payload_schema_version, payload_content_type,
               payload, payload_hash
        FROM oc_outbox_authoritative_mutations WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The original authoritative mutation update statement.</summary>
    internal const string OutboxAuthoritativeMutationsUpdateSql =
        "UPDATE oc_outbox_authoritative_mutations SET payload = $payload, payload_hash = $payload_hash WHERE rowid = $rowId;";

    /// <summary>The outbox metadata batch query.</summary>
    internal const string OutboxMetadataSelectSql = """
        SELECT rowid, store_identity, operation_id, key, value
        FROM oc_outbox_metadata WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The outbox metadata update statement.</summary>
    internal const string OutboxMetadataUpdateSql = "UPDATE oc_outbox_metadata SET value = $value WHERE rowid = $rowId;";

    /// <summary>The snapshot batch query.</summary>
    internal const string SnapshotsSelectSql = """
        SELECT rowid, store_identity, stream_id, format_version, revision, payload_contract_id, payload_schema_version,
               payload_content_type, payload, payload_hash, server_cursor
        FROM oc_snapshots WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The snapshot update statement.</summary>
    internal const string SnapshotsUpdateSql = """
        UPDATE oc_snapshots SET payload = $payload, payload_hash = $payload_hash, server_cursor = $server_cursor
        WHERE rowid = $rowId;
        """;

    /// <summary>The current authoritative snapshot batch query.</summary>
    internal const string SnapshotAuthoritativeStatesSelectSql = """
        SELECT rowid, store_identity, stream_id, payload_contract_id, payload_schema_version, payload_content_type,
               payload, payload_hash
        FROM oc_snapshot_authoritative_states WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The current authoritative snapshot update statement.</summary>
    internal const string SnapshotAuthoritativeStatesUpdateSql =
        "UPDATE oc_snapshot_authoritative_states SET payload = $payload, payload_hash = $payload_hash WHERE rowid = $rowId;";

    /// <summary>The stream batch query.</summary>
    internal const string StreamsSelectSql = """
        SELECT rowid, store_identity, stream_id, server_cursor
        FROM oc_streams WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The stream update statement.</summary>
    internal const string StreamsUpdateSql = "UPDATE oc_streams SET server_cursor = $server_cursor WHERE rowid = $rowId;";

    /// <summary>The inbox batch query.</summary>
    internal const string InboxSelectSql = """
        SELECT rowid, store_identity, stream_id, event_id, server_cursor
        FROM oc_inbox WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The inbox update statement.</summary>
    internal const string InboxUpdateSql = "UPDATE oc_inbox SET server_cursor = $server_cursor WHERE rowid = $rowId;";

    /// <summary>The payload quarantine batch query.</summary>
    internal const string PayloadQuarantineSelectSql = """
        SELECT rowid, store_identity, stream_id, quarantine_id, reason_code, cursor, evidence_contract_id,
               evidence_content_type, evidence_payload_hash, evidence_payload_prefix
        FROM oc_payload_quarantine WHERE rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The payload quarantine update statement.</summary>
    internal const string PayloadQuarantineUpdateSql = """
        UPDATE oc_payload_quarantine
        SET reason_code = $reason_code, cursor = $cursor, evidence_contract_id = $evidence_contract_id,
            evidence_content_type = $evidence_content_type, evidence_payload_hash = $evidence_payload_hash,
            evidence_payload_prefix = $evidence_payload_prefix
        WHERE rowid = $rowId;
        """;

    /// <summary>The dead-letter batch query.</summary>
    internal const string DeadLettersSelectSql = """
        SELECT rowid, store_identity, operation_id, attempt_count, changed_at_utc, reason_code
        FROM oc_outbox_operation_states
        WHERE operation_state = 6 AND rowid > $afterRowId ORDER BY rowid LIMIT $batchSize;
        """;

    /// <summary>The dead-letter update statement.</summary>
    internal const string DeadLettersUpdateSql = "UPDATE oc_outbox_operation_states SET reason_code = $reason_code WHERE rowid = $rowId;";

    /// <summary>The operation identifier column.</summary>
    private const string OperationIdColumn = "operation_id";

    /// <summary>The stream identifier column.</summary>
    private const string StreamIdColumn = "stream_id";

    /// <summary>The payload contract column.</summary>
    private const string PayloadContractColumn = "payload_contract_id";

    /// <summary>The payload schema version column.</summary>
    private const string PayloadSchemaColumn = "payload_schema_version";

    /// <summary>The payload content type column.</summary>
    private const string PayloadContentTypeColumn = "payload_content_type";

    /// <summary>Gets every protected table.</summary>
    internal static IReadOnlyList<SqliteProtectedTable> All { get; } =
    [
        new(SqliteProtectedTableKind.Outbox, DescribeOutbox),
        new(SqliteProtectedTableKind.OutboxAuthoritativeMutations, DescribeOutboxAuthoritativeMutation),
        new(SqliteProtectedTableKind.OutboxMetadata, DescribeOutboxMetadata),
        new(SqliteProtectedTableKind.Snapshots, DescribeSnapshot),
        new(SqliteProtectedTableKind.SnapshotAuthoritativeStates, DescribeSnapshotAuthoritativeState),
        new(SqliteProtectedTableKind.Streams, DescribeStream),
        new(SqliteProtectedTableKind.Inbox, DescribeInbox),
        new(SqliteProtectedTableKind.PayloadQuarantine, DescribeQuarantine),
        new(SqliteProtectedTableKind.DeadLetters, DescribeDeadLetter),
    ];

    /// <summary>Gets a column value by name.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The stored value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static object GetValue(SqliteRows reader, string column) => reader.GetValue(reader.GetOrdinal(column));

    /// <summary>Sets the constant batch query of a table.</summary>
    /// <param name="command">The command.</param>
    /// <param name="kind">The table.</param>
    internal static void SetSelectSql(SqliteStatement command, SqliteProtectedTableKind kind)
    {
        switch (kind)
        {
            case SqliteProtectedTableKind.Outbox:
            {
                command.SetSql(OutboxSelectSql);
                break;
            }

            case SqliteProtectedTableKind.OutboxAuthoritativeMutations:
            {
                command.SetSql(OutboxAuthoritativeMutationsSelectSql);
                break;
            }

            case SqliteProtectedTableKind.OutboxMetadata:
            {
                command.SetSql(OutboxMetadataSelectSql);
                break;
            }

            case SqliteProtectedTableKind.Snapshots:
            {
                command.SetSql(SnapshotsSelectSql);
                break;
            }

            case SqliteProtectedTableKind.SnapshotAuthoritativeStates:
            {
                command.SetSql(SnapshotAuthoritativeStatesSelectSql);
                break;
            }

            case SqliteProtectedTableKind.Streams:
            {
                command.SetSql(StreamsSelectSql);
                break;
            }

            case SqliteProtectedTableKind.Inbox:
            {
                command.SetSql(InboxSelectSql);
                break;
            }

            case SqliteProtectedTableKind.PayloadQuarantine:
            {
                command.SetSql(PayloadQuarantineSelectSql);
                break;
            }

            default:
            {
                command.SetSql(DeadLettersSelectSql);
                break;
            }
        }
    }

    /// <summary>Sets the constant update statement of a table.</summary>
    /// <param name="command">The command.</param>
    /// <param name="kind">The table.</param>
    internal static void SetUpdateSql(SqliteStatement command, SqliteProtectedTableKind kind)
    {
        switch (kind)
        {
            case SqliteProtectedTableKind.Outbox:
            {
                command.SetSql(OutboxUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.OutboxAuthoritativeMutations:
            {
                command.SetSql(OutboxAuthoritativeMutationsUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.OutboxMetadata:
            {
                command.SetSql(OutboxMetadataUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.Snapshots:
            {
                command.SetSql(SnapshotsUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.SnapshotAuthoritativeStates:
            {
                command.SetSql(SnapshotAuthoritativeStatesUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.Streams:
            {
                command.SetSql(StreamsUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.Inbox:
            {
                command.SetSql(InboxUpdateSql);
                break;
            }

            case SqliteProtectedTableKind.PayloadQuarantine:
            {
                command.SetSql(PayloadQuarantineUpdateSql);
                break;
            }

            default:
            {
                command.SetSql(DeadLettersUpdateSql);
                break;
            }
        }
    }

    /// <summary>Describes an outbox row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeOutbox(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadOperationId(reader, OperationIdColumn, out var operationId)
            || !TryReadStreamId(reader, StreamIdColumn, out var streamId)
            || GetValue(reader, "client_sequence") is not long clientSequence
            || !TryReadInt(reader, "operation_type", out var operationType))
        {
            return false;
        }

        var context = SqliteRecordContext.Outbox(operationId, streamId, clientSequence, (SyncOperationType)operationType);
        if (!TryReadPayloadContext(reader, context, out var payloadContext))
        {
            return false;
        }

        AddPayloadValues(values, payloadContext);
        values.Add(new(SqliteRecordContext.BaseVersionColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.CommitFingerprintColumn, IsBlob: true, context));
        return true;
    }

    /// <summary>Describes an original authoritative outbox mutation row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeOutboxAuthoritativeMutation(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadOperationId(reader, OperationIdColumn, out var operationId)
            || !TryReadPayloadContext(reader, SqliteRecordContext.OutboxAuthoritativeMutation(operationId), out var payloadContext))
        {
            return false;
        }

        AddPayloadValues(values, payloadContext);
        return true;
    }

    /// <summary>Describes an outbox metadata row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeOutboxMetadata(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadOperationId(reader, OperationIdColumn, out var operationId) || GetValue(reader, "key") is not string key)
        {
            return false;
        }

        values.Add(new(SqliteRecordContext.ValueColumn, IsBlob: false, SqliteRecordContext.OutboxMetadata(operationId, key)));
        return true;
    }

    /// <summary>Describes a snapshot row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeSnapshot(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadStreamId(reader, StreamIdColumn, out var streamId)
            || !TryReadInt(reader, "format_version", out var formatVersion)
            || GetValue(reader, "revision") is not long revision)
        {
            return false;
        }

        var context = SqliteRecordContext.Snapshot(streamId, formatVersion, revision);
        if (!TryReadPayloadContext(reader, context, out var payloadContext))
        {
            return false;
        }

        AddPayloadValues(values, payloadContext);
        values.Add(new(SqliteRecordContext.ServerCursorColumn, IsBlob: false, context));
        return true;
    }

    /// <summary>Describes a current authoritative snapshot row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeSnapshotAuthoritativeState(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadStreamId(reader, StreamIdColumn, out var streamId)
            || !TryReadPayloadContext(reader, SqliteRecordContext.SnapshotAuthoritativeState(streamId), out var payloadContext))
        {
            return false;
        }

        AddPayloadValues(values, payloadContext);
        return true;
    }

    /// <summary>Describes a stream row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeStream(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadStreamId(reader, StreamIdColumn, out var streamId))
        {
            return false;
        }

        values.Add(new(SqliteRecordContext.ServerCursorColumn, IsBlob: false, SqliteRecordContext.Stream(streamId)));
        return true;
    }

    /// <summary>Describes an inbox row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeInbox(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadStreamId(reader, StreamIdColumn, out var streamId) || !TryReadGuid(reader, "event_id", out var eventId))
        {
            return false;
        }

        values.Add(new(SqliteRecordContext.ServerCursorColumn, IsBlob: false, SqliteRecordContext.Inbox(streamId, eventId)));
        return true;
    }

    /// <summary>Describes a payload quarantine row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeQuarantine(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadStreamId(reader, StreamIdColumn, out var streamId) || !TryReadGuid(reader, "quarantine_id", out var quarantineId))
        {
            return false;
        }

        var context = SqliteRecordContext.Quarantine(streamId, quarantineId);
        values.Add(new(SqliteRecordContext.ReasonCodeColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.CursorColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.EvidenceContractColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.EvidenceContentTypeColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.EvidencePayloadHashColumn, IsBlob: false, context));
        values.Add(new(SqliteRecordContext.EvidencePayloadPrefixColumn, IsBlob: true, context));
        return true;
    }

    /// <summary>Describes a dead-lettered operation state row.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="values">The protected values.</param>
    /// <returns>Whether the row can be bound.</returns>
    private static bool DescribeDeadLetter(SqliteRows reader, List<SqliteProtectedValue> values)
    {
        if (!TryReadOperationId(reader, OperationIdColumn, out var operationId)
            || !TryReadInt(reader, "attempt_count", out var attemptCount)
            || GetValue(reader, "changed_at_utc") is not string changedAtUtc)
        {
            return false;
        }

        values.Add(new(
            SqliteRecordContext.ReasonCodeColumn,
            IsBlob: false,
            SqliteRecordContext.DeadLetter(operationId, attemptCount, changedAtUtc)));
        return true;
    }

    /// <summary>Adds the payload and payload hash values.</summary>
    /// <param name="values">The protected values.</param>
    /// <param name="payloadContext">The payload context.</param>
    private static void AddPayloadValues(List<SqliteProtectedValue> values, SqliteRecordContext payloadContext)
    {
        values.Add(new(SqliteRecordContext.PayloadColumn, IsBlob: true, payloadContext));
        values.Add(new(SqliteRecordContext.PayloadHashColumn, IsBlob: false, payloadContext));
    }

    /// <summary>Reads the payload metadata that the payload context binds.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="context">The row context.</param>
    /// <param name="payloadContext">The payload context.</param>
    /// <returns>Whether the metadata is well formed.</returns>
    private static bool TryReadPayloadContext(SqliteRows reader, SqliteRecordContext context, out SqliteRecordContext payloadContext)
    {
        if (GetValue(reader, PayloadContractColumn) is string contractId
            && TryReadInt(reader, PayloadSchemaColumn, out var schemaVersion)
            && GetValue(reader, PayloadContentTypeColumn) is string contentType)
        {
            payloadContext = context.WithPayloadMetadata(contractId, schemaVersion, contentType);
            return true;
        }

        payloadContext = context;
        return false;
    }

    /// <summary>Reads an operation identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The column name.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>Whether the value is a non-empty GUID.</returns>
    private static bool TryReadOperationId(SqliteRows reader, string column, out OperationId operationId)
    {
        var parsed = TryReadGuid(reader, column, out var value);
        operationId = new(value);
        return parsed;
    }

    /// <summary>Reads a non-empty GUID.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The column name.</param>
    /// <param name="value">The GUID.</param>
    /// <returns>Whether the value is a non-empty GUID.</returns>
    private static bool TryReadGuid(SqliteRows reader, string column, out Guid value) =>
        Guid.TryParse(GetValue(reader, column) as string, out value) && value != Guid.Empty;

    /// <summary>Reads a stream identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The column name.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>Whether the value is a valid stream identifier.</returns>
    private static bool TryReadStreamId(SqliteRows reader, string column, out StreamId streamId)
    {
        streamId = default;
        if (GetValue(reader, column) is not string value)
        {
            return false;
        }

        try
        {
            streamId = new(value);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>Reads a 32-bit integer stored as a SQLite INTEGER.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The column name.</param>
    /// <param name="value">The integer.</param>
    /// <returns>Whether the value is an INTEGER in the 32-bit range.</returns>
    private static bool TryReadInt(SqliteRows reader, string column, out int value)
    {
        if (GetValue(reader, column) is long number && number is >= int.MinValue and <= int.MaxValue)
        {
            value = (int)number;
            return true;
        }

        value = 0;
        return false;
    }
}
