// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes authoritative snapshot state statements.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Upserts the current authoritative snapshot payload when one is supplied.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    private static void UpsertSnapshotAuthoritativeState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SnapshotMutation snapshotMutation)
    {
        if (snapshotMutation.AuthoritativeState is null)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_snapshot_authoritative_states
                (store_identity, stream_id, payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash)
            VALUES
                ($storeIdentity, $streamId, $payloadContractId, $payloadSchemaVersion, $payloadContentType, $payload, $payloadHash)
            ON CONFLICT (store_identity, stream_id) DO UPDATE SET
                payload_contract_id = excluded.payload_contract_id,
                payload_schema_version = excluded.payload_schema_version,
                payload_content_type = excluded.payload_content_type,
                payload = excluded.payload,
                payload_hash = excluded.payload_hash;
            """;
        AddStreamParameters(command, storeIdentity, snapshotMutation.StreamId);
        AddPayloadParameters(command, snapshotMutation.AuthoritativeState);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Determines whether a stored fingerprint matches the current or compatible legacy canonical intent.</summary>
    /// <param name="storedFingerprint">The stored fingerprint.</param>
    /// <param name="fingerprint">The current requested fingerprint.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>Whether the fingerprints match.</returns>
    private static bool HasSameCommitFingerprint(
        byte[] storedFingerprint,
        byte[] fingerprint,
        SyncOperation operation,
        SnapshotMutation snapshotMutation)
    {
        if (SqliteCommitFingerprint.Matches(storedFingerprint, fingerprint))
        {
            return true;
        }

        return snapshotMutation.AuthoritativeState is null
            && SqliteCommitFingerprint.Matches(storedFingerprint, SqliteCommitFingerprint.ComputeLegacy(operation, snapshotMutation));
    }

    /// <summary>Determines whether the repeated operation carries the same original authoritative mutation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="requestedAuthoritativeState">The requested authoritative mutation.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>Whether the original authoritative mutation matches.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasSameOriginalAuthoritativeMutation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId,
        PayloadEnvelope? requestedAuthoritativeState,
        long maximumPayloadBytes) =>
        OptionalPayloadEquals(
            ReadOutboxAuthoritativeMutation(connection, transaction, storeIdentity, operationId, maximumPayloadBytes),
            requestedAuthoritativeState);

    /// <summary>Reads the original authoritative mutation stored for an outbox operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The original authoritative mutation, or null when absent.</returns>
    private static PayloadEnvelope? ReadOutboxAuthoritativeMutation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId,
        long maximumPayloadBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash, rowid,
                   typeof(payload_contract_id), length(CAST(payload_contract_id AS BLOB)), IFNULL(substr(CAST(payload_contract_id AS BLOB), 1, 4100), x''),
                   typeof(payload_schema_version), payload_schema_version,
                   typeof(payload_content_type), length(CAST(payload_content_type AS BLOB)), IFNULL(substr(CAST(payload_content_type AS BLOB), 1, 4100), x''),
                   typeof(payload), length(CAST(payload AS BLOB)), IFNULL(substr(CAST(payload AS BLOB), 1, 4096), x''),
                   typeof(payload_hash), length(CAST(payload_hash AS BLOB)), IFNULL(substr(CAST(payload_hash AS BLOB), 1, 4100), x'')
            FROM oc_outbox_authoritative_mutations
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        const int RowIdIndex = 5;
        const int EvidenceIndex = 6;
        return reader.Read()
            ? ReadAuthoritativePayload(
                connection,
                reader,
                SqlitePayloadColumns.Create(
                    contractIndex: 0,
                    schemaIndex: 1,
                    contentTypeIndex: 2,
                    payloadIndex: 3,
                    hashIndex: 4,
                    source: new(RowIdIndex, SqliteStoreSchema.OutboxAuthoritativeMutationsTableName, PayloadColumnName),
                    evidenceStartIndex: EvidenceIndex),
                maximumPayloadBytes)
            : null;
    }

    /// <summary>Determines whether optional payload envelopes contain the same content.</summary>
    /// <param name="left">The first optional payload.</param>
    /// <param name="right">The second optional payload.</param>
    /// <returns>Whether the payloads match.</returns>
    private static bool OptionalPayloadEquals(PayloadEnvelope? left, PayloadEnvelope? right) =>
        left is null ? right is null : right is not null && PayloadEquals(left, right);

    /// <summary>Determines whether payload envelopes contain the same content.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The second payload.</param>
    /// <returns>Whether the payloads match.</returns>
    private static bool PayloadEquals(PayloadEnvelope left, PayloadEnvelope right) =>
        left.SchemaVersion == right.SchemaVersion
        && string.Equals(left.ContractId, right.ContractId, StringComparison.Ordinal)
        && string.Equals(left.ContentType, right.ContentType, StringComparison.Ordinal)
        && left.PayloadLength == right.PayloadLength
        && PayloadHashEquals(left.PayloadHash, right.PayloadHash)
        && left.Payload.Span.SequenceEqual(right.Payload.Span);

    /// <summary>Determines whether two payload hashes match.</summary>
    /// <param name="left">The first hash.</param>
    /// <param name="right">The second hash.</param>
    /// <returns>Whether the hashes match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool PayloadHashEquals(string left, string right) =>
#if NET5_0_OR_GREATER
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(left), Encoding.UTF8.GetBytes(right));
#else
        string.Equals(left, right, StringComparison.Ordinal);
#endif

    /// <summary>Reads an authoritative payload and validates canonical hash integrity when encoded.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload and evidence columns.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The validated payload.</returns>
    private static PayloadEnvelope ReadAuthoritativePayload(
        SqliteConnection connection,
        SqliteDataReader reader,
        SqlitePayloadColumns columns,
        long maximumPayloadBytes)
    {
        var payload = ReadPayload(connection, reader, columns, maximumPayloadBytes);
        SqliteLocalCommitValidation.ValidateAuthoritativePayload(payload, nameof(payload));
        return payload;
    }

    /// <summary>Reads the current authoritative snapshot payload.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The authoritative payload, or null when unknown.</returns>
    private static PayloadEnvelope? ReadSnapshotAuthoritativeState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        long maximumPayloadBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash, rowid,
                   typeof(payload_contract_id), length(CAST(payload_contract_id AS BLOB)), IFNULL(substr(CAST(payload_contract_id AS BLOB), 1, 4100), x''),
                   typeof(payload_schema_version), payload_schema_version,
                   typeof(payload_content_type), length(CAST(payload_content_type AS BLOB)), IFNULL(substr(CAST(payload_content_type AS BLOB), 1, 4100), x''),
                   typeof(payload), length(CAST(payload AS BLOB)), IFNULL(substr(CAST(payload AS BLOB), 1, 4096), x''),
                   typeof(payload_hash), length(CAST(payload_hash AS BLOB)), IFNULL(substr(CAST(payload_hash AS BLOB), 1, 4100), x'')
            FROM oc_snapshot_authoritative_states
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
        """;
        AddStreamParameters(command, storeIdentity, streamId);
        using var reader = command.ExecuteReader();
        const int RowIdIndex = 5;
        const int EvidenceIndex = 6;
        return reader.Read()
            ? ReadAuthoritativePayload(
                connection,
                reader,
                SqlitePayloadColumns.Create(
                    contractIndex: 0,
                    schemaIndex: 1,
                    contentTypeIndex: 2,
                    payloadIndex: 3,
                    hashIndex: 4,
                    source: new(RowIdIndex, SqliteStoreSchema.SnapshotAuthoritativeStatesTableName, PayloadColumnName),
                    evidenceStartIndex: EvidenceIndex),
                maximumPayloadBytes)
            : null;
    }
}
