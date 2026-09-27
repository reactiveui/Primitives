// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies one protected SQLite record so its ciphertext cannot move to another row, column, or store.</summary>
/// <remarks>
/// The associated data binds each value to the store identity, a logical record kind, the column name, and the plaintext
/// key columns that locate the row. Those key columns stay in plaintext so SQLite can index and query them, and any change
/// to them fails authentication when the protected value is read.
/// </remarks>
internal sealed class SqliteRecordContext
{
    /// <summary>The payload column.</summary>
    internal const string PayloadColumn = "payload";

    /// <summary>The payload hash column.</summary>
    internal const string PayloadHashColumn = "payload_hash";

    /// <summary>The optimistic base version column.</summary>
    internal const string BaseVersionColumn = "base_version";

    /// <summary>The commit fingerprint column.</summary>
    internal const string CommitFingerprintColumn = "commit_fingerprint";

    /// <summary>The metadata value column.</summary>
    internal const string ValueColumn = "value";

    /// <summary>The server cursor column.</summary>
    internal const string ServerCursorColumn = "server_cursor";

    /// <summary>The reason code column.</summary>
    internal const string ReasonCodeColumn = "reason_code";

    /// <summary>The quarantine cursor column.</summary>
    internal const string CursorColumn = "cursor";

    /// <summary>The quarantine evidence contract column.</summary>
    internal const string EvidenceContractColumn = "evidence_contract_id";

    /// <summary>The quarantine evidence content type column.</summary>
    internal const string EvidenceContentTypeColumn = "evidence_content_type";

    /// <summary>The quarantine evidence hash column.</summary>
    internal const string EvidencePayloadHashColumn = "evidence_payload_hash";

    /// <summary>The quarantine evidence prefix column.</summary>
    internal const string EvidencePayloadPrefixColumn = "evidence_payload_prefix";

    /// <summary>The domain separator at the start of all associated data.</summary>
    private static readonly byte[] Domain = "ReactiveUI.OccasionallyConnected.Sqlite.Record.v1"u8.ToArray();

    /// <summary>The logical record kind.</summary>
    private readonly string _kind;

    /// <summary>The plaintext fields that locate and describe the row.</summary>
    private readonly string[] _fields;

    /// <summary>Initializes a new instance of the <see cref="SqliteRecordContext"/> class.</summary>
    /// <param name="kind">The logical record kind.</param>
    /// <param name="fields">The bound plaintext fields.</param>
    private SqliteRecordContext(string kind, string[] fields)
    {
        _kind = kind;
        _fields = fields;
    }

    /// <summary>Creates the context for an outbox operation row.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="operationType">The operation type.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext Outbox(OperationId operationId, StreamId streamId, long clientSequence, SyncOperationType operationType) =>
        new(
            "outbox",
            [
                FormatGuid(operationId.Value),
                streamId.Value,
                clientSequence.ToString(CultureInfo.InvariantCulture),
                ((int)operationType).ToString(CultureInfo.InvariantCulture),
            ]);

    /// <summary>Creates the context for an outbox operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The context.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static SqliteRecordContext Outbox(SyncOperation operation) =>
        Outbox(operation.OperationId, operation.StreamId, operation.ClientSequence, operation.Type);

    /// <summary>Creates the context for an original authoritative outbox mutation row.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext OutboxAuthoritativeMutation(OperationId operationId) =>
        new("outbox-authoritative-mutation", [FormatGuid(operationId.Value)]);

    /// <summary>Creates the context for an outbox metadata row.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext OutboxMetadata(OperationId operationId, string key) =>
        new("outbox-metadata", [FormatGuid(operationId.Value), key]);

    /// <summary>Creates the context for a snapshot row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="formatVersion">The snapshot format version.</param>
    /// <param name="revision">The snapshot revision.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext Snapshot(StreamId streamId, int formatVersion, long revision) =>
        new(
            "snapshot",
            [
                streamId.Value,
                formatVersion.ToString(CultureInfo.InvariantCulture),
                revision.ToString(CultureInfo.InvariantCulture),
            ]);

    /// <summary>Creates the context for the current authoritative snapshot row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext SnapshotAuthoritativeState(StreamId streamId) =>
        new("snapshot-authoritative-state", [streamId.Value]);

    /// <summary>Creates the context for a stream row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext Stream(StreamId streamId) => new("stream", [streamId.Value]);

    /// <summary>Creates the context for an inbox row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventId">The remote event identifier.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext Inbox(StreamId streamId, Guid eventId) =>
        new("inbox", [streamId.Value, FormatGuid(eventId)]);

    /// <summary>Creates the context for a payload quarantine row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="quarantineId">The quarantine identifier.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext Quarantine(StreamId streamId, Guid quarantineId) =>
        new("payload-quarantine", [streamId.Value, FormatGuid(quarantineId)]);

    /// <summary>Creates the context for the terminal dead-letter state of an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="attemptCount">The preserved attempt count.</param>
    /// <param name="changedAtUtc">The stored state change timestamp text.</param>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext DeadLetter(OperationId operationId, int attemptCount, string changedAtUtc) =>
        new(
            "dead-letter",
            [
                FormatGuid(operationId.Value),
                attemptCount.ToString(CultureInfo.InvariantCulture),
                changedAtUtc,
            ]);

    /// <summary>Creates the context for the store key check value.</summary>
    /// <returns>The context.</returns>
    internal static SqliteRecordContext KeyCheck() => new("store-key-check", []);

    /// <summary>Adds the plaintext payload envelope metadata to this context.</summary>
    /// <param name="contractId">The payload contract identifier.</param>
    /// <param name="schemaVersion">The payload schema version.</param>
    /// <param name="contentType">The payload content type.</param>
    /// <returns>The payload context.</returns>
    internal SqliteRecordContext WithPayloadMetadata(string contractId, int schemaVersion, string contentType) =>
        new(_kind, [.. _fields, contractId, schemaVersion.ToString(CultureInfo.InvariantCulture), contentType]);

    /// <summary>Creates the associated data for one protected column value.</summary>
    /// <param name="storeIdentity">The store identity partition.</param>
    /// <param name="column">The column name.</param>
    /// <param name="header">The envelope header.</param>
    /// <returns>The associated data bytes.</returns>
    internal byte[] CreateAssociatedData(string storeIdentity, string column, byte[] header)
    {
        using var buffer = new MemoryStream();
        WriteField(buffer, Domain);
        WriteField(buffer, header);
        WriteField(buffer, storeIdentity);
        WriteField(buffer, _kind);
        WriteField(buffer, column);
        WriteLength(buffer, _fields.Length);
        for (var index = 0; index < _fields.Length; index++)
        {
            WriteField(buffer, _fields[index]);
        }

        return buffer.ToArray();
    }

    /// <summary>Formats a GUID the way the SQLite schema stores it.</summary>
    /// <param name="value">The GUID.</param>
    /// <returns>The canonical text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatGuid(Guid value) => value.ToString("D");

    /// <summary>Writes one length-prefixed UTF-8 field.</summary>
    /// <param name="buffer">The target buffer.</param>
    /// <param name="value">The field value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteField(MemoryStream buffer, string value) => WriteField(buffer, Encoding.UTF8.GetBytes(value));

    /// <summary>Writes one length-prefixed byte field.</summary>
    /// <param name="buffer">The target buffer.</param>
    /// <param name="value">The field bytes.</param>
    private static void WriteField(MemoryStream buffer, byte[] value)
    {
        WriteLength(buffer, value.Length);
        buffer.Write(value, 0, value.Length);
    }

    /// <summary>Writes a big-endian 32-bit length.</summary>
    /// <param name="buffer">The target buffer.</param>
    /// <param name="length">The length.</param>
    private static void WriteLength(MemoryStream buffer, int length)
    {
        buffer.WriteByte((byte)(length >> 24));
        buffer.WriteByte((byte)(length >> 16));
        buffer.WriteByte((byte)(length >> 8));
        buffer.WriteByte((byte)length);
    }
}
