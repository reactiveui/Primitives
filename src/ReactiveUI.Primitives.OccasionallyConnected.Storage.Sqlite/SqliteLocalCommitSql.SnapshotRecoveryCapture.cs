// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes bounded snapshot recovery capture preflight statements.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The canonical subscription id text length.</summary>
    private const int SnapshotRecoverySubscriptionIdTextLength = 36;

    /// <summary>The logical byte width counted for Int32 and enum values.</summary>
    private const long SnapshotRecoveryCaptureInt32Bytes = 4;

    /// <summary>The logical byte width counted for Int64 values.</summary>
    private const long SnapshotRecoveryCaptureInt64Bytes = 8;

    /// <summary>The logical byte width counted for Guid values.</summary>
    private const long SnapshotRecoveryCaptureGuidBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset values.</summary>
    private const long SnapshotRecoveryCaptureDateTimeOffsetBytes = 16;

    /// <summary>The invalid subscription identity message.</summary>
    private const string InvalidSnapshotRecoverySubscriptionIdentityMessage = "The SQLite subscription identity is invalid.";

    /// <summary>The maximum timestamp text length admitted before decoder validation.</summary>
    private const int SnapshotRecoveryCaptureTimestampTextLength = 64;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SubscriptionTypeIndex = 0;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SubscriptionLengthIndex = 1;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SubscriptionValueIndex = 2;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamSequenceIndex = 0;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamCursorTypeIndex = 1;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamCursorLengthIndex = 2;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamCursorValueIndex = 3;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamSubscriptionTypeIndex = 4;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamSubscriptionLengthIndex = 5;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamSubscriptionValueIndex = 6;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamIdentitySubscriptionTypeIndex = 7;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamIdentitySubscriptionLengthIndex = 8;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int StreamIdentitySubscriptionValueIndex = 9;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotCursorLengthIndex = 1;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotPayloadContractLengthIndex = 2;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotPayloadContentTypeLengthIndex = 3;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotPayloadHashLengthIndex = 4;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotPayloadLengthIndex = 5;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotFormatTypeIndex = 6;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotFormatValueIndex = 7;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotPayloadSchemaTypeIndex = 8;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotRevisionTypeIndex = 9;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotRevisionValueIndex = 10;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotSavedTypeIndex = 11;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int SnapshotSavedLengthIndex = 12;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int AuthoritativePayloadContractLengthIndex = 0;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int AuthoritativePayloadContentTypeLengthIndex = 1;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int AuthoritativePayloadHashLengthIndex = 2;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int AuthoritativePayloadLengthIndex = 3;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int AuthoritativePayloadSchemaTypeIndex = 4;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPayloadSchemaTypeIndex = 0;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPayloadContractLengthIndex = 1;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPayloadContentTypeLengthIndex = 2;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPayloadHashLengthIndex = 3;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPayloadLengthIndex = 4;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationBaseVersionLengthIndex = 5;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationMetadataCountIndex = 6;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationMetadataBytesIndex = 7;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int CaptureOperationIdTypeIndex = 8;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int CaptureOperationIdLengthIndex = 9;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationSequenceTypeIndex = 10;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationSequenceValueIndex = 11;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationTimestampTypeIndex = 12;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationTimestampLengthIndex = 13;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationBaseVersionTypeIndex = 14;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationTypeTypeIndex = 15;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationTypeValueIndex = 16;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationDeliveryTypeIndex = 17;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationDeliveryValueIndex = 18;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationDurabilityTypeIndex = 19;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationDurabilityValueIndex = 20;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPriorityTypeIndex = 21;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationPriorityValueIndex = 22;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationConflictTypeIndex = 23;

    /// <summary>Column index used by snapshot recovery capture preflight.</summary>
    private const int OperationConflictValueIndex = 24;

    /// <summary>SQL statement used by snapshot recovery capture preflight.</summary>
    private const string SnapshotRecoveryCaptureOperationBytesSql = """
        SELECT typeof(outbox.payload_schema_version),
               length(CAST(outbox.payload_contract_id AS BLOB)),
               length(CAST(outbox.payload_content_type AS BLOB)),
               length(CAST(outbox.payload_hash AS BLOB)),
               length(CAST(outbox.payload AS BLOB)),
               CASE WHEN outbox.base_version IS NULL THEN 0 ELSE length(CAST(outbox.base_version AS BLOB)) END,
               COUNT(metadata.key) AS metadata_count,
               4 + COALESCE(SUM(length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB))), 0) AS metadata_bytes,
               typeof(outbox.operation_id),
               length(CAST(outbox.operation_id AS BLOB)),
               typeof(outbox.client_sequence),
               CASE WHEN typeof(outbox.client_sequence) = 'integer' THEN outbox.client_sequence ELSE NULL END,
               typeof(outbox.timestamp_utc),
               length(CAST(outbox.timestamp_utc AS BLOB)),
               typeof(outbox.base_version),
               typeof(outbox.operation_type),
               CASE WHEN typeof(outbox.operation_type) = 'integer' THEN outbox.operation_type ELSE NULL END,
               typeof(outbox.policy_delivery_guarantee),
               CASE WHEN typeof(outbox.policy_delivery_guarantee) = 'integer' THEN outbox.policy_delivery_guarantee ELSE NULL END,
               typeof(outbox.policy_durability),
               CASE WHEN typeof(outbox.policy_durability) = 'integer' THEN outbox.policy_durability ELSE NULL END,
               typeof(outbox.policy_priority),
               CASE WHEN typeof(outbox.policy_priority) = 'integer' THEN outbox.policy_priority ELSE NULL END,
               typeof(outbox.policy_conflict),
               CASE WHEN typeof(outbox.policy_conflict) = 'integer' THEN outbox.policy_conflict ELSE NULL END
        FROM oc_outbox AS outbox
        LEFT JOIN oc_outbox_operation_states AS state
            ON state.store_identity = outbox.store_identity
            AND state.operation_id = outbox.operation_id
        LEFT JOIN oc_outbox_receive_inclusions AS inclusion
            ON inclusion.store_identity = outbox.store_identity
            AND inclusion.operation_id = outbox.operation_id
        LEFT JOIN oc_outbox_metadata AS metadata
            ON metadata.store_identity = outbox.store_identity
            AND metadata.operation_id = outbox.operation_id
        WHERE outbox.store_identity = $storeIdentity
            AND outbox.stream_id = $streamId
            AND (
                state.operation_state IS NULL
                OR state.operation_state NOT IN (4, 5, 6)
                OR (inclusion.operation_id IS NULL AND state.operation_state NOT IN (5, 6))
            )
        GROUP BY outbox.operation_id
        LIMIT $maximumRows;
        """;

    /// <summary>Validates capture-visible scalar state and returns bounded stream state for later payload reads.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The capture request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stream state, or null when no durable stream row exists.</returns>
    /// <exception cref="InvalidOperationException">The stored stream state is invalid.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    internal static SqliteLocalStreamState? PreflightSnapshotRecoveryCapture(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken)
    {
        var subscriptionId = ReadSnapshotRecoveryCaptureSubscriptionId(connection, transaction, storeIdentity, request.StreamId);
        if (subscriptionId != request.SubscriptionId)
        {
            throw new InvalidOperationException("The recovered subscription identity does not match the requested identity.");
        }

        if (SnapshotRecoveryCaptureQuarantineExists(connection, transaction, storeIdentity, request.StreamId))
        {
            throw new InvalidOperationException("The local stream is quarantined.");
        }

        var stream = ReadSnapshotRecoveryCaptureStream(connection, transaction, storeIdentity, request);
        var streamCursorBytes = stream.HasValue && stream.Value.ServerCursor is not null
            ? Encoding.UTF8.GetByteCount(stream.Value.ServerCursor)
            : 0;
        var logicalBytes = checked(
            Encoding.UTF8.GetByteCount(request.StreamId.Value)
            + SnapshotRecoveryCaptureGuidBytes
            + streamCursorBytes
            + SnapshotRecoveryCaptureInt64Bytes
            + SnapshotRecoveryCaptureInt32Bytes
            + SnapshotRecoveryCaptureInt32Bytes);
        ThrowIfSnapshotRecoveryCapacityExceeded(logicalBytes, request.Limits.MaximumLogicalBytes, nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));

        logicalBytes = AddSnapshotRecoveryCaptureSnapshotBytes(connection, transaction, storeIdentity, request, logicalBytes);
        _ = AddSnapshotRecoveryCaptureOperationBytes(connection, transaction, storeIdentity, request, logicalBytes, cancellationToken);
        return stream;
    }

    /// <summary>Reads a bounded subscription identity.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The subscription identifier.</returns>
    /// <exception cref="InvalidOperationException">The stored subscription identity is invalid.</exception>
    private static SubscriptionId ReadSnapshotRecoveryCaptureSubscriptionId(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT typeof(subscription_id), length(CAST(subscription_id AS BLOB)), substr(subscription_id, 1, $subscriptionIdTextLength)
            FROM oc_subscription_identities
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Parameters.AddWithValue("$subscriptionIdTextLength", SnapshotRecoverySubscriptionIdTextLength);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException("The SQLite subscription identity is missing.");
        }

        var storageType = ReadString(reader, SubscriptionTypeIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        var length = ReadNonNegativeLong(reader, SubscriptionLengthIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        if (!string.Equals(storageType, "text", StringComparison.Ordinal) || length != SnapshotRecoverySubscriptionIdTextLength)
        {
            throw new InvalidOperationException(InvalidSnapshotRecoverySubscriptionIdentityMessage);
        }

        var text = ReadString(reader, SubscriptionValueIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        return Guid.TryParse(text, out var value) && value != Guid.Empty
            ? new(value)
            : throw new InvalidOperationException(InvalidSnapshotRecoverySubscriptionIdentityMessage);
    }

    /// <summary>Reads stream state using bounded cursor evidence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The capture request.</param>
    /// <returns>The stream state, or null when absent.</returns>
    /// <exception cref="InvalidOperationException">The stored stream state is invalid.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static SqliteLocalStreamState? ReadSnapshotRecoveryCaptureStream(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryCaptureRequest request)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT CASE WHEN typeof(stream.next_client_sequence) = 'integer' THEN stream.next_client_sequence ELSE NULL END,
                   typeof(stream.server_cursor),
                   CASE WHEN stream.server_cursor IS NULL THEN 0 ELSE length(CAST(stream.server_cursor AS BLOB)) END,
                   CASE WHEN stream.server_cursor IS NULL THEN NULL ELSE substr(stream.server_cursor, 1, $cursorLimit) END,
                   typeof(stream.subscription_id),
                   length(CAST(stream.subscription_id AS BLOB)),
                   substr(stream.subscription_id, 1, $subscriptionIdTextLength),
                   typeof(identity.subscription_id),
                   length(CAST(identity.subscription_id AS BLOB)),
                   substr(identity.subscription_id, 1, $subscriptionIdTextLength)
            FROM oc_streams AS stream
            INNER JOIN oc_subscription_identities AS identity
                ON identity.store_identity = stream.store_identity AND identity.stream_id = stream.stream_id
            WHERE stream.store_identity = $storeIdentity AND stream.stream_id = $streamId;
        """;
        AddStreamParameters(command, storeIdentity, request.StreamId);
        _ = command.Parameters.AddWithValue("$cursorLimit", request.Limits.MaximumCursorUtf8Bytes);
        _ = command.Parameters.AddWithValue("$subscriptionIdTextLength", SnapshotRecoverySubscriptionIdTextLength);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var streamSubscriptionId = ReadSnapshotRecoveryCaptureProjectedSubscriptionId(
            reader,
            StreamSubscriptionTypeIndex,
            StreamSubscriptionLengthIndex,
            StreamSubscriptionValueIndex);
        var identitySubscriptionId = ReadSnapshotRecoveryCaptureProjectedSubscriptionId(
            reader,
            StreamIdentitySubscriptionTypeIndex,
            StreamIdentitySubscriptionLengthIndex,
            StreamIdentitySubscriptionValueIndex);
        if (streamSubscriptionId != identitySubscriptionId || streamSubscriptionId != request.SubscriptionId)
        {
            throw new InvalidOperationException("The SQLite stream subscription identity is inconsistent.");
        }

        var cursorBytes = ReadNonNegativeLong(reader, StreamCursorLengthIndex, "The SQLite stream cursor is invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(cursorBytes, request.Limits.MaximumCursorUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        var cursor = ReadSnapshotRecoveryCaptureNullableText(reader, StreamCursorTypeIndex, StreamCursorValueIndex, "The SQLite stream cursor is invalid.");
        return new(ReadPositiveLong(reader, StreamSequenceIndex, "The SQLite stream sequence is invalid."), cursor);
    }

    /// <summary>Determines whether a stream already has a quarantine marker without materializing it.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>Whether a quarantine marker exists.</returns>
    private static bool SnapshotRecoveryCaptureQuarantineExists(
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

    /// <summary>Adds snapshot and authoritative snapshot logical bytes from bounded scalar evidence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The capture request.</param>
    /// <param name="logicalBytes">The current logical byte count.</param>
    /// <returns>The updated logical byte count.</returns>
    private static long AddSnapshotRecoveryCaptureSnapshotBytes(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryCaptureRequest request,
        long logicalBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT typeof(server_cursor),
                   CASE WHEN server_cursor IS NULL THEN 0 ELSE length(CAST(server_cursor AS BLOB)) END,
                   length(CAST(payload_contract_id AS BLOB)),
                   length(CAST(payload_content_type AS BLOB)),
                   length(CAST(payload_hash AS BLOB)),
                   length(CAST(payload AS BLOB)),
                   typeof(format_version),
                   CASE WHEN typeof(format_version) = 'integer' THEN format_version ELSE NULL END,
                   typeof(payload_schema_version),
                   typeof(revision),
                   CASE WHEN typeof(revision) = 'integer' THEN revision ELSE NULL END,
                   typeof(saved_at_utc),
                   length(CAST(saved_at_utc AS BLOB))
            FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, request.StreamId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return logicalBytes;
        }

        var snapshotCursorLength = ReadNonNegativeLong(reader, SnapshotCursorLengthIndex, "The SQLite snapshot cursor is invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(snapshotCursorLength, request.Limits.MaximumCursorUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        logicalBytes = checked(logicalBytes
            + Encoding.UTF8.GetByteCount(request.StreamId.Value)
            + SnapshotRecoveryCaptureInt32Bytes
            + snapshotCursorLength
            + GetSnapshotRecoveryCapturePayloadLogicalBytes(
                reader,
                SnapshotPayloadContractLengthIndex,
                SnapshotPayloadContentTypeLengthIndex,
                SnapshotPayloadHashLengthIndex,
                SnapshotPayloadLengthIndex,
                request.Limits)
            + SnapshotRecoveryCaptureInt64Bytes
            + SnapshotRecoveryCaptureDateTimeOffsetBytes);
        ThrowIfSnapshotRecoveryCapacityExceeded(logicalBytes, request.Limits.MaximumLogicalBytes, nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        ValidateSnapshotRecoveryCaptureSnapshotScalars(reader);
        logicalBytes = AddSnapshotRecoveryCaptureAuthoritativeSnapshotBytes(connection, transaction, storeIdentity, request, logicalBytes);
        return logicalBytes;
    }

    /// <summary>Adds authoritative snapshot payload logical bytes from bounded scalar evidence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The capture request.</param>
    /// <param name="logicalBytes">The current logical byte count.</param>
    /// <returns>The updated logical byte count.</returns>
    private static long AddSnapshotRecoveryCaptureAuthoritativeSnapshotBytes(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryCaptureRequest request,
        long logicalBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT length(CAST(payload_contract_id AS BLOB)),
                   length(CAST(payload_content_type AS BLOB)),
                   length(CAST(payload_hash AS BLOB)),
                   length(CAST(payload AS BLOB)),
                   typeof(payload_schema_version)
            FROM oc_snapshot_authoritative_states
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, request.StreamId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return logicalBytes;
        }

        logicalBytes = checked(logicalBytes + GetSnapshotRecoveryCapturePayloadLogicalBytes(
            reader,
            AuthoritativePayloadContractLengthIndex,
            AuthoritativePayloadContentTypeLengthIndex,
            AuthoritativePayloadHashLengthIndex,
            AuthoritativePayloadLengthIndex,
            request.Limits));
        ThrowIfSnapshotRecoveryCapacityExceeded(logicalBytes, request.Limits.MaximumLogicalBytes, nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        ValidateSnapshotRecoveryCapturePayloadSchemaType(
            reader,
            AuthoritativePayloadSchemaTypeIndex,
            "The SQLite authoritative snapshot payload schema version is invalid.");
        return logicalBytes;
    }

    /// <summary>Adds capture-visible operation logical bytes from bounded scalar evidence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The capture request.</param>
    /// <param name="logicalBytes">The current logical byte count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The updated logical byte count.</returns>
    private static long AddSnapshotRecoveryCaptureOperationBytes(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryCaptureRequest request,
        long logicalBytes,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SnapshotRecoveryCaptureOperationBytesSql;
        AddStreamParameters(command, storeIdentity, request.StreamId);
        _ = command.Parameters.AddWithValue("$maximumRows", (long)request.Limits.MaximumPendingOperations + 1L);
        using var reader = command.ExecuteReader();
        long count = 0;
        var streamIdBytes = Encoding.UTF8.GetByteCount(request.StreamId.Value);
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            count++;
            ThrowIfSnapshotRecoveryCapacityExceeded(count, request.Limits.MaximumPendingOperations, nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
            var metadataCount = ReadNonNegativeLong(reader, OperationMetadataCountIndex, "The SQLite metadata count is invalid.");
            ThrowIfSnapshotRecoveryCapacityExceeded(metadataCount, request.Limits.MaximumMetadataEntries, nameof(SnapshotRecoveryLimits.MaximumMetadataEntries));
            var metadataBytes = ReadNonNegativeLong(reader, OperationMetadataBytesIndex, "The SQLite metadata bytes are invalid.");
            ThrowIfSnapshotRecoveryCapacityExceeded(metadataBytes, request.Limits.MaximumMetadataBytes, nameof(SnapshotRecoveryLimits.MaximumMetadataBytes));
            var baseVersionBytes = ReadNonNegativeLong(reader, OperationBaseVersionLengthIndex, "The SQLite operation base version is invalid.");
            ThrowIfSnapshotRecoveryCapacityExceeded(baseVersionBytes, request.Limits.MaximumContractUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumContractUtf8Bytes));
            logicalBytes = checked(logicalBytes
                + SnapshotRecoveryCaptureGuidBytes
                + streamIdBytes
                + SnapshotRecoveryCaptureInt64Bytes
                + SnapshotRecoveryCaptureDateTimeOffsetBytes
                + SnapshotRecoveryCaptureInt32Bytes
                + GetSnapshotRecoveryCapturePayloadLogicalBytes(
                    reader,
                    OperationPayloadContractLengthIndex,
                    OperationPayloadContentTypeLengthIndex,
                    OperationPayloadHashLengthIndex,
                    OperationPayloadLengthIndex,
                    request.Limits)
                + baseVersionBytes
                + metadataBytes
                + SnapshotRecoveryCaptureInt32Bytes
                + SnapshotRecoveryCaptureInt32Bytes
                + SnapshotRecoveryCaptureInt32Bytes
                + SnapshotRecoveryCaptureInt32Bytes);
            ThrowIfSnapshotRecoveryCapacityExceeded(logicalBytes, request.Limits.MaximumLogicalBytes, nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
            ValidateSnapshotRecoveryCaptureOperationScalars(reader);
        }

        return logicalBytes;
    }

    /// <summary>Gets one payload's logical bytes from scalar evidence.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="contractLengthIndex">The contract length column index.</param>
    /// <param name="contentTypeLengthIndex">The content type length column index.</param>
    /// <param name="hashLengthIndex">The hash length column index.</param>
    /// <param name="payloadLengthIndex">The payload length column index.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The payload logical byte count.</returns>
    private static long GetSnapshotRecoveryCapturePayloadLogicalBytes(
        SqliteDataReader reader,
        int contractLengthIndex,
        int contentTypeLengthIndex,
        int hashLengthIndex,
        int payloadLengthIndex,
        SnapshotRecoveryLimits limits)
    {
        var contractLength = ReadNonNegativeLong(reader, contractLengthIndex, "The SQLite payload contract is invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(contractLength, limits.MaximumContractUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumContractUtf8Bytes));
        var contentTypeLength = ReadNonNegativeLong(reader, contentTypeLengthIndex, "The SQLite payload content type is invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(contentTypeLength, limits.MaximumContractUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumContractUtf8Bytes));
        var hashLength = ReadNonNegativeLong(reader, hashLengthIndex, "The SQLite payload hash is invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(hashLength, limits.MaximumContractUtf8Bytes, nameof(SnapshotRecoveryLimits.MaximumContractUtf8Bytes));
        var payloadLength = ReadNonNegativeLong(reader, payloadLengthIndex, "The SQLite payload bytes are invalid.");
        ThrowIfSnapshotRecoveryCapacityExceeded(payloadLength, limits.MaximumPayloadBytes, nameof(SnapshotRecoveryLimits.MaximumPayloadBytes));
        return SnapshotRecoveryCaptureInt32Bytes
            + SnapshotRecoveryCaptureInt64Bytes
            + contractLength
            + contentTypeLength
            + hashLength
            + payloadLength;
    }

    /// <summary>Reads a nullable bounded text projection.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="valueIndex">The bounded value column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The nullable text.</returns>
    /// <exception cref="InvalidOperationException">The stored text projection is invalid.</exception>
    private static string? ReadSnapshotRecoveryCaptureNullableText(SqliteDataReader reader, int typeIndex, int valueIndex, string message)
    {
        var storageType = ReadString(reader, typeIndex, message);
        if (string.Equals(storageType, "null", StringComparison.Ordinal))
        {
            return null;
        }

        return string.Equals(storageType, "text", StringComparison.Ordinal)
            ? ReadString(reader, valueIndex, message)
            : throw new InvalidOperationException(message);
    }

    /// <summary>Reads a bounded subscription identifier from projected storage evidence.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="lengthIndex">The byte length column index.</param>
    /// <param name="valueIndex">The bounded value column index.</param>
    /// <returns>The subscription identifier.</returns>
    /// <exception cref="InvalidOperationException">The stored subscription identity is invalid.</exception>
    private static SubscriptionId ReadSnapshotRecoveryCaptureProjectedSubscriptionId(
        SqliteDataReader reader,
        int typeIndex,
        int lengthIndex,
        int valueIndex)
    {
        var storageType = ReadString(reader, typeIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        var length = ReadNonNegativeLong(reader, lengthIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        if (!string.Equals(storageType, "text", StringComparison.Ordinal) || length != SnapshotRecoverySubscriptionIdTextLength)
        {
            throw new InvalidOperationException(InvalidSnapshotRecoverySubscriptionIdentityMessage);
        }

        var text = ReadString(reader, valueIndex, InvalidSnapshotRecoverySubscriptionIdentityMessage);
        return Guid.TryParse(text, out var value) && value != Guid.Empty
            ? new(value)
            : throw new InvalidOperationException(InvalidSnapshotRecoverySubscriptionIdentityMessage);
    }

    /// <summary>Validates operation scalar columns that the later decoder will read.</summary>
    /// <param name="reader">The reader.</param>
    /// <exception cref="InvalidOperationException">The stored operation scalar is invalid.</exception>
    private static void ValidateSnapshotRecoveryCaptureOperationScalars(SqliteDataReader reader)
    {
        ValidateSnapshotRecoveryCapturePayloadSchemaType(reader, OperationPayloadSchemaTypeIndex, "The SQLite operation payload schema version is invalid.");
        ValidateSnapshotRecoveryCaptureTextLength(
            reader,
            CaptureOperationIdTypeIndex,
            CaptureOperationIdLengthIndex,
            SnapshotRecoveryOperationIdTextLength,
            "The SQLite operation id is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationSequenceTypeIndex, "The SQLite operation sequence is invalid.");
        _ = ReadPositiveLong(reader, OperationSequenceValueIndex, "The SQLite operation sequence is invalid.");
        ValidateSnapshotRecoveryCaptureTextLength(
            reader,
            OperationTimestampTypeIndex,
            OperationTimestampLengthIndex,
            SnapshotRecoveryCaptureTimestampTextLength,
            "The SQLite operation timestamp is invalid.");
        ValidateSnapshotRecoveryCaptureNullableTextType(reader, OperationBaseVersionTypeIndex, "The SQLite operation base version is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationTypeTypeIndex, "The SQLite operation type is invalid.");
        _ = ReadOperationType(reader, OperationTypeValueIndex);
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationDeliveryTypeIndex, "The SQLite delivery guarantee is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationDurabilityTypeIndex, "The SQLite durability is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationPriorityTypeIndex, "The SQLite priority is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, OperationConflictTypeIndex, "The SQLite conflict policy is invalid.");
        _ = ReadPolicy(reader, OperationDeliveryValueIndex, OperationDurabilityValueIndex, OperationPriorityValueIndex, OperationConflictValueIndex);
    }

    /// <summary>Validates snapshot scalar columns that the later decoder will read.</summary>
    /// <param name="reader">The reader.</param>
    /// <exception cref="InvalidOperationException">The stored snapshot scalar is invalid.</exception>
    private static void ValidateSnapshotRecoveryCaptureSnapshotScalars(SqliteDataReader reader)
    {
        ValidateSnapshotRecoveryCaptureIntegerType(reader, SnapshotFormatTypeIndex, "The SQLite snapshot format version is invalid.");
        _ = ReadPositiveInt(reader, SnapshotFormatValueIndex, "The SQLite snapshot format version is invalid.");
        ValidateSnapshotRecoveryCapturePayloadSchemaType(reader, SnapshotPayloadSchemaTypeIndex, "The SQLite snapshot payload schema version is invalid.");
        ValidateSnapshotRecoveryCaptureIntegerType(reader, SnapshotRevisionTypeIndex, InvalidSnapshotRevisionMessage);
        _ = ReadNonNegativeLong(reader, SnapshotRevisionValueIndex, InvalidSnapshotRevisionMessage);
        ValidateSnapshotRecoveryCaptureTextLength(
            reader,
            SnapshotSavedTypeIndex,
            SnapshotSavedLengthIndex,
            SnapshotRecoveryCaptureTimestampTextLength,
            "The SQLite snapshot timestamp is invalid.");
    }

    /// <summary>Validates payload schema storage type without applying schema semantics.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The storage type is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateSnapshotRecoveryCapturePayloadSchemaType(SqliteDataReader reader, int typeIndex, string message) =>
        ValidateSnapshotRecoveryCaptureIntegerType(reader, typeIndex, message);

    /// <summary>Validates integer storage type without materializing the stored value.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The storage type is invalid.</exception>
    private static void ValidateSnapshotRecoveryCaptureIntegerType(SqliteDataReader reader, int typeIndex, string message)
    {
        var storageType = ReadString(reader, typeIndex, message);
        if (!string.Equals(storageType, "integer", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(message);
        }
    }

    /// <summary>Validates a bounded text storage projection.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="lengthIndex">The length column index.</param>
    /// <param name="maximumLength">The maximum accepted length.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The storage type or length is invalid.</exception>
    private static void ValidateSnapshotRecoveryCaptureTextLength(
        SqliteDataReader reader,
        int typeIndex,
        int lengthIndex,
        long maximumLength,
        string message)
    {
        var storageType = ReadString(reader, typeIndex, message);
        var length = ReadNonNegativeLong(reader, lengthIndex, message);
        if (!string.Equals(storageType, "text", StringComparison.Ordinal) || length > maximumLength)
        {
            throw new InvalidOperationException(message);
        }
    }

    /// <summary>Validates nullable text storage type without materializing the value.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="typeIndex">The storage type column index.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">The storage type is invalid.</exception>
    private static void ValidateSnapshotRecoveryCaptureNullableTextType(SqliteDataReader reader, int typeIndex, string message)
    {
        var storageType = ReadString(reader, typeIndex, message);
        if (!string.Equals(storageType, "null", StringComparison.Ordinal) && !string.Equals(storageType, "text", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(message);
        }
    }

    /// <summary>Throws when a capacity is exceeded.</summary>
    /// <param name="observed">The observed value.</param>
    /// <param name="maximum">The configured maximum.</param>
    /// <param name="limitName">The limit name.</param>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The observed value exceeds the configured maximum.</exception>
    private static void ThrowIfSnapshotRecoveryCapacityExceeded(long observed, long maximum, string limitName)
    {
        if (observed <= maximum)
        {
            return;
        }

        throw new SnapshotRecoveryCapacityExceededException(limitName, maximum, observed);
    }
}
