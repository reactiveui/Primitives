// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local payload quarantine markers.</summary>
/// <content>Contains side-table queries for quarantine markers.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The stable message used when a stream is quarantined.</summary>
    private const string StreamQuarantinedMessage = "The local stream is quarantined.";

    /// <summary>The quarantine identifier column index.</summary>
    private const int QuarantineIdIndex = 0;

    /// <summary>The quarantine stream column index.</summary>
    private const int QuarantineStreamIndex = 1;

    /// <summary>The quarantine subscription column index.</summary>
    private const int QuarantineSubscriptionIndex = 2;

    /// <summary>The quarantine operation column index.</summary>
    private const int QuarantineOperationIndex = 3;

    /// <summary>The quarantine event column index.</summary>
    private const int QuarantineEventIndex = 4;

    /// <summary>The quarantine source column index.</summary>
    private const int QuarantineSourceIndex = 5;

    /// <summary>The quarantine reason column index.</summary>
    private const int QuarantineReasonIndex = 6;

    /// <summary>The quarantine reason code column index.</summary>
    private const int QuarantineReasonCodeIndex = 7;

    /// <summary>The quarantine cursor column index.</summary>
    private const int QuarantineCursorIndex = 8;

    /// <summary>The quarantine evidence contract column index.</summary>
    private const int QuarantineEvidenceContractIndex = 9;

    /// <summary>The quarantine evidence schema column index.</summary>
    private const int QuarantineEvidenceSchemaIndex = 10;

    /// <summary>The quarantine evidence content type column index.</summary>
    private const int QuarantineEvidenceContentTypeIndex = 11;

    /// <summary>The quarantine evidence payload length column index.</summary>
    private const int QuarantineEvidencePayloadLengthIndex = 12;

    /// <summary>The quarantine evidence payload hash column index.</summary>
    private const int QuarantineEvidencePayloadHashIndex = 13;

    /// <summary>The quarantine evidence prefix column index.</summary>
    private const int QuarantineEvidencePrefixIndex = 14;

    /// <summary>The quarantine observed timestamp column index.</summary>
    private const int QuarantineObservedAtIndex = 15;

    /// <summary>The SQL storage class name for BLOB values.</summary>
    private const string BlobStorageType = "blob";

    /// <summary>The SQL storage class name for INTEGER values.</summary>
    private const string IntegerStorageType = "integer";

    /// <summary>The SQL storage class name for TEXT values.</summary>
    private const string TextStorageType = "text";

    /// <summary>The UTF-8 byte count for a two-byte scalar.</summary>
    private const int TwoByteScalarUtf8ByteCount = 2;

    /// <summary>The UTF-8 byte count for a three-byte scalar.</summary>
    private const int ThreeByteScalarUtf8ByteCount = 3;

    /// <summary>The UTF-8 byte count for a four-byte scalar.</summary>
    private const int FourByteScalarUtf8ByteCount = 4;

    /// <summary>The empty payload prefix used when the raw SQLite value cannot be read as bytes.</summary>
    private static readonly byte[] EmptyPayloadPrefix = [];

    /// <summary>The strict UTF-8 encoding used for raw evidence metadata projection.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Inserts or returns the existing payload quarantine marker for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The normalized quarantine request.</param>
    /// <param name="quarantineId">The new marker identifier.</param>
    /// <returns>The durable quarantine result.</returns>
    /// <exception cref="InvalidOperationException">The quarantine marker cannot be read after insertion.</exception>
    internal static LocalPayloadQuarantineResult InsertPayloadQuarantine(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SqliteNormalizedPayloadQuarantineRequest request,
        Guid quarantineId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_payload_quarantine
                (store_identity, stream_id, quarantine_id, subscription_id, operation_id, event_id, source, reason,
                 reason_code, cursor, evidence_contract_id, evidence_schema_version, evidence_content_type,
                 evidence_payload_length, evidence_payload_hash, evidence_payload_prefix, observed_at_utc)
            VALUES
                ($storeIdentity, $streamId, $quarantineId, $subscriptionId, $operationId, $eventId, $source, $reason,
                 $reasonCode, $cursor, $evidenceContractId, $evidenceSchemaVersion, $evidenceContentType,
                 $evidencePayloadLength, $evidencePayloadHash, $evidencePayloadPrefix, $observedAtUtc)
            ON CONFLICT (store_identity, stream_id) DO NOTHING;
            """;
        AddQuarantineParameters(command, storeIdentity, request, quarantineId);
        var created = command.ExecuteNonQuery() > 0;
        var record = ReadPayloadQuarantine(connection, transaction, storeIdentity, request.Request.StreamId)
            ?? throw new InvalidOperationException("The SQLite payload quarantine marker was not persisted.");
        return new(record, created);
    }

    /// <summary>Reads the payload quarantine marker for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The quarantine record, if present.</returns>
    internal static LocalPayloadQuarantineRecord? ReadPayloadQuarantine(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT quarantine_id, stream_id, subscription_id, operation_id, event_id, source, reason, reason_code,
                   cursor, evidence_contract_id, evidence_schema_version, evidence_content_type,
                   evidence_payload_length, evidence_payload_hash, evidence_payload_prefix, observed_at_utc
            FROM oc_payload_quarantine
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadQuarantineRecord(reader) : null;
    }

    /// <summary>Throws when a stream is quarantined.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <exception cref="InvalidOperationException">The stream is quarantined.</exception>
    internal static void ThrowIfStreamQuarantined(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        if (!IsStreamQuarantined(connection, transaction, storeIdentity, streamId))
        {
            return;
        }

        throw new InvalidOperationException(StreamQuarantinedMessage);
    }

    /// <summary>Throws when an operation belongs to a quarantined stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="InvalidOperationException">The stream is quarantined.</exception>
    internal static void ThrowIfOperationStreamQuarantined(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT 1
            FROM oc_outbox AS outbox
            INNER JOIN oc_payload_quarantine AS quarantine
                ON quarantine.store_identity = outbox.store_identity
                AND quarantine.stream_id = outbox.stream_id
            WHERE outbox.store_identity = $storeIdentity AND outbox.operation_id = $operationId
            LIMIT 1;
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        if (command.ExecuteScalar() is null)
        {
            return;
        }

        throw new InvalidOperationException(StreamQuarantinedMessage);
    }

    /// <summary>Throws when any lease member belongs to a quarantined stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <exception cref="InvalidOperationException">The stream is quarantined.</exception>
    internal static void ThrowIfLeaseQuarantined(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT 1
            FROM oc_outbox_leases AS lease
            INNER JOIN oc_payload_quarantine AS quarantine
                ON quarantine.store_identity = lease.store_identity
                AND quarantine.stream_id = lease.stream_id
            WHERE lease.store_identity = $storeIdentity AND lease.lease_id = $leaseId
            LIMIT 1;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        if (command.ExecuteScalar() is null)
        {
            return;
        }

        throw new InvalidOperationException(StreamQuarantinedMessage);
    }

    /// <summary>Determines whether a stream is quarantined.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>Whether a quarantine marker exists.</returns>
    internal static bool IsStreamQuarantined(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT 1
            FROM oc_payload_quarantine
            WHERE store_identity = $storeIdentity AND stream_id = $streamId
            LIMIT 1;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        return command.ExecuteScalar() is not null;
    }

    /// <summary>Captures bounded raw evidence from the current payload row without requiring a valid envelope.</summary>
    /// <param name="reader">The reader positioned on the row.</param>
    /// <param name="columns">The bounded evidence projection columns.</param>
    /// <returns>The bounded raw evidence.</returns>
    internal static LocalPayloadQuarantineEvidence CapturePayloadEvidence(
        SqliteDataReader reader,
        SqlitePayloadEvidenceColumns columns)
    {
        var (payloadLength, payloadPrefix) = TryReadPayloadEvidenceBytes(reader, columns);
        return new(
            TryReadTextEvidence(reader, columns.ContractStorageTypeIndex, columns.ContractBytesIndex),
            TryReadIntEvidence(reader, columns.SchemaStorageTypeIndex, columns.SchemaValueIndex),
            TryReadTextEvidence(reader, columns.ContentTypeStorageTypeIndex, columns.ContentTypeBytesIndex),
            payloadLength,
            TryReadTextEvidence(reader, columns.HashStorageTypeIndex, columns.HashBytesIndex),
            payloadPrefix);
    }

    /// <summary>Validates payload storage classes from bounded SQL projections before full managed reads.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The evidence projection columns.</param>
    /// <exception cref="InvalidOperationException">The stored payload has an unexpected storage class.</exception>
    internal static void ValidatePayloadStorageTypes(SqliteDataReader reader, SqlitePayloadEvidenceColumns columns)
    {
        ValidateStorageType(reader, columns.ContractStorageTypeIndex, TextStorageType, "The SQLite payload contract is invalid.");
        ValidateStorageType(reader, columns.SchemaStorageTypeIndex, IntegerStorageType, "The SQLite payload schema version is invalid.");
        ValidateStorageType(reader, columns.ContentTypeStorageTypeIndex, TextStorageType, "The SQLite payload content type is invalid.");
        ValidateStorageType(reader, columns.PayloadStorageTypeIndex, BlobStorageType, "The SQLite payload bytes are invalid.");
        ValidateStorageType(reader, columns.HashStorageTypeIndex, TextStorageType, "The SQLite payload hash is invalid.");
    }

    /// <summary>Adds quarantine insert parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The normalized request.</param>
    /// <param name="quarantineId">The quarantine identifier.</param>
    private static void AddQuarantineParameters(
        SqliteCommand command,
        string storeIdentity,
        SqliteNormalizedPayloadQuarantineRequest request,
        Guid quarantineId)
    {
        AddStreamParameters(command, storeIdentity, request.Request.StreamId);
        _ = command.Parameters.AddWithValue("$quarantineId", quarantineId.ToString("D"));
        AddNullableGuidParameter(command, "$subscriptionId", request.Request.SubscriptionId?.Value);
        AddNullableGuidParameter(command, "$operationId", request.Request.OperationId?.Value);
        AddNullableGuidParameter(command, "$eventId", request.Request.EventId);
        _ = command.Parameters.AddWithValue("$source", (int)request.Request.Source);
        _ = command.Parameters.AddWithValue("$reason", (int)request.Request.Reason);
        _ = command.Parameters.AddWithValue("$reasonCode", (object?)request.Request.ReasonCode ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$cursor", (object?)request.Request.Cursor ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$evidenceContractId", (object?)request.Evidence.ContractId ?? DBNull.Value);
        AddNullableIntParameter(command, "$evidenceSchemaVersion", request.Evidence.SchemaVersion);
        _ = command.Parameters.AddWithValue("$evidenceContentType", (object?)request.Evidence.ContentType ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$evidencePayloadLength", request.Evidence.PayloadLength);
        _ = command.Parameters.AddWithValue("$evidencePayloadHash", (object?)request.Evidence.PayloadHash ?? DBNull.Value);
        _ = command.Parameters.Add("$evidencePayloadPrefix", SqliteType.Blob);
        command.Parameters["$evidencePayloadPrefix"].Value = request.Evidence.PayloadPrefix.ToArray();
        _ = command.Parameters.AddWithValue("$observedAtUtc", FormatDateTimeOffset(request.Request.ObservedAtUtc));
    }

    /// <summary>Reads a bounded projected text evidence value.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="storageTypeIndex">The projected storage type column index.</param>
    /// <param name="bytesIndex">The projected bounded bytes column index.</param>
    /// <returns>The string value, or null when the column cannot be read as a string.</returns>
    private static string? TryReadTextEvidence(SqliteDataReader reader, int storageTypeIndex, int bytesIndex)
    {
        if (!IsStorageType(reader, storageTypeIndex, TextStorageType) || reader.IsDBNull(bytesIndex))
        {
            return null;
        }

        var bytes = reader.GetFieldValue<byte[]>(bytesIndex);
        var prefixLength = GetValidUtf8PrefixLength(bytes);
        return prefixLength < 0 ? null : StrictUtf8.GetString(bytes, 0, prefixLength);
    }

    /// <summary>Gets the valid UTF-8 prefix length bounded at the quarantine evidence limit.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <returns>The prefix length, or -1 when the bytes contain malformed UTF-8.</returns>
    private static int GetValidUtf8PrefixLength(byte[] bytes)
    {
        var maximum = Math.Min(bytes.Length, LocalPayloadQuarantineEvidenceFactory.DefaultMaximumEvidenceBytes);
        var endIndex = 0;
        for (var index = 0; index < maximum;)
        {
            var scalarBytes = GetUtf8ScalarByteCount(bytes, index, maximum);
            if (scalarBytes == 0)
            {
                break;
            }

            if (scalarBytes < 0)
            {
                return -1;
            }

            index += scalarBytes;
            endIndex = index;
        }

        return endIndex;
    }

    /// <summary>Gets the UTF-8 byte count for one scalar in a bounded byte buffer.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="maximum">The exclusive scan limit.</param>
    /// <returns>The scalar byte count, zero for incomplete trailing scalar, or -1 for malformed UTF-8.</returns>
    private static int GetUtf8ScalarByteCount(byte[] bytes, int index, int maximum)
    {
        var first = bytes[index];
        if (first <= 0x7F)
        {
            return 1;
        }

        if (first is >= 0xC2 and <= 0xDF)
        {
            return GetMultiByteUtf8ScalarByteCount(bytes, index, maximum, TwoByteScalarUtf8ByteCount);
        }

        if (first is >= 0xE0 and <= 0xEF)
        {
            return GetThreeByteUtf8ScalarByteCount(bytes, index, maximum, first);
        }

        return first is >= 0xF0 and <= 0xF4 ? GetFourByteUtf8ScalarByteCount(bytes, index, maximum, first) : -1;
    }

    /// <summary>Gets a multibyte UTF-8 scalar count when all continuation bytes are present.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="maximum">The exclusive scan limit.</param>
    /// <param name="byteCount">The expected scalar byte count.</param>
    /// <returns>The byte count, zero when the scalar is incomplete at the boundary, or -1 when malformed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetMultiByteUtf8ScalarByteCount(byte[] bytes, int index, int maximum, int byteCount) =>
        GetUtf8ContinuationByteCount(bytes, index, maximum, byteCount);

    /// <summary>Gets the UTF-8 byte count for one three-byte scalar.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="maximum">The exclusive scan limit.</param>
    /// <param name="first">The first scalar byte.</param>
    /// <returns>The scalar byte count, zero for incomplete trailing scalar, or -1 for malformed UTF-8.</returns>
    private static int GetThreeByteUtf8ScalarByteCount(byte[] bytes, int index, int maximum, byte first)
    {
        var continuationBytes = GetUtf8ContinuationByteCount(bytes, index, maximum, ThreeByteScalarUtf8ByteCount);
        return continuationBytes == ThreeByteScalarUtf8ByteCount
            ? GetThreeByteUtf8ScalarByteCount(bytes, index, first)
            : continuationBytes;
    }

    /// <summary>Gets the UTF-8 byte count for one complete three-byte scalar.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="first">The first scalar byte.</param>
    /// <returns>The scalar byte count, or -1 for malformed UTF-8.</returns>
    private static int GetThreeByteUtf8ScalarByteCount(byte[] bytes, int index, byte first)
    {
        var second = bytes[index + 1];
        return first switch
        {
            0xE0 when second < 0xA0 => -1,
            0xED when second >= 0xA0 => -1,
            _ => ThreeByteScalarUtf8ByteCount,
        };
    }

    /// <summary>Gets the UTF-8 byte count for one four-byte scalar.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="maximum">The exclusive scan limit.</param>
    /// <param name="first">The first scalar byte.</param>
    /// <returns>The scalar byte count, zero for incomplete trailing scalar, or -1 for malformed UTF-8.</returns>
    private static int GetFourByteUtf8ScalarByteCount(byte[] bytes, int index, int maximum, byte first)
    {
        var continuationBytes = GetUtf8ContinuationByteCount(bytes, index, maximum, FourByteScalarUtf8ByteCount);
        return continuationBytes == FourByteScalarUtf8ByteCount
            ? GetFourByteUtf8ScalarByteCount(bytes, index, first)
            : continuationBytes;
    }

    /// <summary>Gets the UTF-8 byte count for one complete four-byte scalar.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="first">The first scalar byte.</param>
    /// <returns>The scalar byte count, or -1 for malformed UTF-8.</returns>
    private static int GetFourByteUtf8ScalarByteCount(byte[] bytes, int index, byte first)
    {
        var second = bytes[index + 1];
        return first switch
        {
            0xF0 when second < 0x90 => -1,
            0xF4 when second >= 0x90 => -1,
            _ => FourByteScalarUtf8ByteCount,
        };
    }

    /// <summary>Gets whether a UTF-8 scalar has the requested continuation bytes inside the scan limit.</summary>
    /// <param name="bytes">The projected bytes.</param>
    /// <param name="index">The scalar start index.</param>
    /// <param name="maximum">The exclusive scan limit.</param>
    /// <param name="byteCount">The expected scalar byte count.</param>
    /// <returns>The byte count, zero when incomplete at the boundary, or -1 when malformed.</returns>
    private static int GetUtf8ContinuationByteCount(byte[] bytes, int index, int maximum, int byteCount)
    {
        if (index + byteCount > maximum)
        {
            return 0;
        }

        for (var offset = 1; offset < byteCount; offset++)
        {
            if (bytes[index + offset] is < 0x80 or > 0xBF)
            {
                return -1;
            }
        }

        return byteCount;
    }

    /// <summary>Reads an integer column when possible.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="storageTypeIndex">The projected storage type column index.</param>
    /// <param name="valueIndex">The value column index.</param>
    /// <returns>The integer value, or null when the column cannot be read as an integer.</returns>
    private static int? TryReadIntEvidence(SqliteDataReader reader, int storageTypeIndex, int valueIndex)
    {
        if (!IsStorageType(reader, storageTypeIndex, IntegerStorageType))
        {
            return null;
        }

        try
        {
            return reader.GetInt32(valueIndex);
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    /// <summary>Reads the payload byte length and bounded prefix when possible.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The bounded evidence projection columns.</param>
    /// <returns>The original payload length and bounded prefix.</returns>
    private static (int PayloadLength, byte[] PayloadPrefix) TryReadPayloadEvidenceBytes(
        SqliteDataReader reader,
        SqlitePayloadEvidenceColumns columns) =>
        reader.IsDBNull(columns.PayloadLengthIndex) || reader.IsDBNull(columns.PayloadPrefixIndex)
            ? (0, EmptyPayloadPrefix)
            : (checked((int)reader.GetInt64(columns.PayloadLengthIndex)), reader.GetFieldValue<byte[]>(columns.PayloadPrefixIndex));

    /// <summary>Validates one projected storage class.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="storageTypeIndex">The storage type column index.</param>
    /// <param name="expected">The expected storage type.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The storage class does not match.</exception>
    private static void ValidateStorageType(SqliteDataReader reader, int storageTypeIndex, string expected, string message) =>
        _ = IsStorageType(reader, storageTypeIndex, expected) ? true : throw new InvalidOperationException(message);

    /// <summary>Determines whether a projected storage class matches an expected value.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="storageTypeIndex">The storage type column index.</param>
    /// <param name="expected">The expected storage type.</param>
    /// <returns>Whether the storage type matches.</returns>
    private static bool IsStorageType(SqliteDataReader reader, int storageTypeIndex, string expected) =>
        !reader.IsDBNull(storageTypeIndex)
        && string.Equals(reader.GetString(storageTypeIndex), expected, StringComparison.OrdinalIgnoreCase);

    /// <summary>Reads a quarantine record.</summary>
    /// <param name="reader">The reader.</param>
    /// <returns>The quarantine record.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static LocalPayloadQuarantineRecord ReadQuarantineRecord(SqliteDataReader reader)
    {
        var evidence = new LocalPayloadQuarantineEvidence(
            ReadNullableString(reader, QuarantineEvidenceContractIndex),
            ReadNullableInt(reader, QuarantineEvidenceSchemaIndex),
            ReadNullableString(reader, QuarantineEvidenceContentTypeIndex),
            ReadNonNegativeInt(reader, QuarantineEvidencePayloadLengthIndex, "The SQLite quarantine evidence length is invalid."),
            ReadNullableString(reader, QuarantineEvidencePayloadHashIndex),
            ReadBytes(reader, QuarantineEvidencePrefixIndex, "The SQLite quarantine evidence prefix is invalid."));
        return new(
            ReadGuid(reader, QuarantineIdIndex, "The SQLite quarantine id is invalid."),
            new(ReadString(reader, QuarantineStreamIndex, "The SQLite quarantine stream is invalid.")),
            ReadNullableSubscriptionId(reader, QuarantineSubscriptionIndex),
            ReadNullableOperationId(reader, QuarantineOperationIndex),
            ReadNullableGuid(reader, QuarantineEventIndex),
            (LocalPayloadQuarantineSource)ReadInt(reader, QuarantineSourceIndex, "The SQLite quarantine source is invalid."),
            (LocalPayloadQuarantineReason)ReadInt(reader, QuarantineReasonIndex, "The SQLite quarantine reason is invalid."),
            ReadNullableString(reader, QuarantineReasonCodeIndex),
            ReadNullableString(reader, QuarantineCursorIndex),
            evidence,
            ReadDateTimeOffset(reader, QuarantineObservedAtIndex, "The SQLite quarantine timestamp is invalid."));
    }

    /// <summary>Adds a nullable GUID parameter.</summary>
    /// <param name="command">The command.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="value">The value.</param>
    private static void AddNullableGuidParameter(SqliteCommand command, string parameterName, Guid? value) =>
        _ = command.Parameters.AddWithValue(parameterName, value.HasValue ? value.Value.ToString("D") : DBNull.Value);

    /// <summary>Adds a nullable integer parameter.</summary>
    /// <param name="command">The command.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="value">The value.</param>
    private static void AddNullableIntParameter(SqliteCommand command, string parameterName, int? value) =>
        _ = command.Parameters.AddWithValue(parameterName, value.HasValue ? value.Value : DBNull.Value);

    /// <summary>Reads a nullable integer column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The nullable integer.</returns>
    private static int? ReadNullableInt(SqliteDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : reader.GetInt32(index);

    /// <summary>Reads a nullable operation identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The nullable operation identifier.</returns>
    private static OperationId? ReadNullableOperationId(SqliteDataReader reader, int index)
    {
        var value = ReadNullableGuid(reader, index);
        return value.HasValue ? new(value.Value) : null;
    }

    /// <summary>Reads a nullable subscription identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The nullable subscription identifier.</returns>
    private static SubscriptionId? ReadNullableSubscriptionId(SqliteDataReader reader, int index) =>
        ReadNullableGuid(reader, index) is { } value ? new(value) : null;

    /// <summary>Reads a nullable GUID column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The nullable GUID.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static Guid? ReadNullableGuid(SqliteDataReader reader, int index)
    {
        if (reader.IsDBNull(index))
        {
            return null;
        }

        var text = reader.GetString(index);
        if (Guid.TryParseExact(text, "D", out var value) && value != Guid.Empty)
        {
            return value;
        }

        throw new InvalidOperationException("The SQLite quarantine identifier is invalid.");
    }
}
