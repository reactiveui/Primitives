// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
internal static class SqliteLocalCommitSql
{
    /// <summary>The operation identifier SQL parameter.</summary>
    private const string OperationIdParameter = "$operationId";

    /// <summary>The store identity SQL parameter.</summary>
    private const string StoreIdentityParameter = "$storeIdentity";

    /// <summary>The invalid snapshot revision message.</summary>
    private const string InvalidSnapshotRevisionMessage = "The SQLite snapshot revision is invalid.";

    /// <summary>Ensures a stream row exists for an identity mapping.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    internal static void EnsureStreamRow(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        SubscriptionId subscriptionId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_streams
                (store_identity, stream_id, subscription_id, next_client_sequence, server_cursor)
            VALUES
                ($storeIdentity, $streamId, $subscriptionId, 1, NULL)
            ON CONFLICT (store_identity, stream_id) DO NOTHING;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Selects the subscription identity for a stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The subscription id.</returns>
    internal static SubscriptionId SelectSubscriptionId(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT subscription_id FROM oc_subscription_identities
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        return SqliteIdentityStoreData.ReadSubscriptionId(command.ExecuteScalar());
    }

    /// <summary>Reads stream state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The stream state.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static SqliteLocalStreamState ReadStreamState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        if (TryReadStreamState(connection, transaction, storeIdentity, streamId, out var stream))
        {
            return stream;
        }

        throw new InvalidOperationException("The SQLite stream row is missing.");
    }

    /// <summary>Attempts to read stream state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="stream">The stream state.</param>
    /// <returns>Whether the row exists.</returns>
    /// <exception cref="InvalidOperationException">Stored stream data or its subscription identity is inconsistent.</exception>
    internal static bool TryReadStreamState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        out SqliteLocalStreamState stream)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT stream.next_client_sequence, stream.server_cursor, stream.subscription_id, identity.subscription_id
            FROM oc_streams AS stream
            INNER JOIN oc_subscription_identities AS identity
                ON identity.store_identity = stream.store_identity AND identity.stream_id = stream.stream_id
            WHERE stream.store_identity = $storeIdentity AND stream.stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            stream = default;
            return false;
        }

        const int StreamSubscriptionIndex = 2;
        const int IdentitySubscriptionIndex = 3;
        var nextSequence = ReadPositiveLong(reader, 0, "The SQLite stream sequence is invalid.");
        var streamSubscriptionId = SqliteIdentityStoreData.ReadSubscriptionId(reader.GetValue(StreamSubscriptionIndex));
        var identitySubscriptionId = SqliteIdentityStoreData.ReadSubscriptionId(reader.GetValue(IdentitySubscriptionIndex));
        if (streamSubscriptionId != identitySubscriptionId)
        {
            throw new InvalidOperationException("The SQLite stream subscription does not match its durable identity.");
        }

        stream = new(nextSequence, ReadNullableString(reader, 1));
        return true;
    }

    /// <summary>Reads the current snapshot revision.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The revision.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static long ReadSnapshotRevision(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT revision FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        var value = command.ExecuteScalar();
        return value is null ? 0 : ReadNonNegativeLong(value, "The SQLite snapshot revision is invalid.");
    }

    /// <summary>Inserts an outbox operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshotRevision">The snapshot revision produced by the operation.</param>
    /// <param name="fingerprint">The canonical commit intent fingerprint.</param>
    /// <param name="committedAtUtc">The commit time.</param>
    internal static void InsertOutboxOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        long snapshotRevision,
        byte[] fingerprint,
        DateTimeOffset committedAtUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_outbox
                (store_identity, operation_id, stream_id, client_sequence, timestamp_utc, base_version, operation_type,
                 payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash,
                 policy_delivery_guarantee, policy_durability, policy_priority, policy_conflict, snapshot_revision, committed_at_utc, commit_fingerprint)
            VALUES
                ($storeIdentity, $operationId, $streamId, $clientSequence, $timestampUtc, $baseVersion, $operationType,
                 $payloadContractId, $payloadSchemaVersion, $payloadContentType, $payload, $payloadHash,
                 $policyDeliveryGuarantee, $policyDurability, $policyPriority, $policyConflict, $snapshotRevision, $committedAtUtc, $commitFingerprint);
            """;
        AddOperationParameters(command, storeIdentity, operation, committedAtUtc);
        _ = command.Parameters.AddWithValue("$snapshotRevision", snapshotRevision);
        _ = command.Parameters.AddWithValue("$commitFingerprint", fingerprint);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts operation metadata rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The operation.</param>
    internal static void InsertOperationMetadata(SqliteConnection connection, SqliteTransaction transaction, string storeIdentity, SyncOperation operation)
    {
        foreach (var pair in operation.Metadata)
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO oc_outbox_metadata
                    (store_identity, operation_id, key, value)
                VALUES
                    ($storeIdentity, $operationId, $key, $value);
                """;
            _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
            _ = command.Parameters.AddWithValue(OperationIdParameter, operation.OperationId.Value.ToString("D"));
            _ = command.Parameters.AddWithValue("$key", pair.Key);
            _ = command.Parameters.AddWithValue("$value", pair.Value);
            _ = command.ExecuteNonQuery();
        }
    }

    /// <summary>Upserts a snapshot row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <param name="revision">The new revision.</param>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="savedAtUtc">The save time.</param>
    internal static void UpsertSnapshot(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SnapshotMutation snapshotMutation,
        long revision,
        string? serverCursor,
        DateTimeOffset savedAtUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_snapshots
                (store_identity, stream_id, format_version, server_cursor, payload_contract_id, payload_schema_version,
                 payload_content_type, payload, payload_hash, revision, saved_at_utc)
            VALUES
                ($storeIdentity, $streamId, $formatVersion, $serverCursor, $payloadContractId, $payloadSchemaVersion,
                 $payloadContentType, $payload, $payloadHash, $revision, $savedAtUtc)
            ON CONFLICT (store_identity, stream_id) DO UPDATE SET
                format_version = excluded.format_version,
                server_cursor = excluded.server_cursor,
                payload_contract_id = excluded.payload_contract_id,
                payload_schema_version = excluded.payload_schema_version,
                payload_content_type = excluded.payload_content_type,
                payload = excluded.payload,
                payload_hash = excluded.payload_hash,
                revision = excluded.revision,
                saved_at_utc = excluded.saved_at_utc;
            """;
        AddStreamParameters(command, storeIdentity, snapshotMutation.StreamId);
        AddPayloadParameters(command, snapshotMutation.State);
        _ = command.Parameters.AddWithValue("$formatVersion", snapshotMutation.FormatVersion);
        _ = command.Parameters.AddWithValue("$serverCursor", (object?)serverCursor ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$revision", revision);
        _ = command.Parameters.AddWithValue("$savedAtUtc", FormatDateTimeOffset(savedAtUtc));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Updates the stream next client sequence.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="nextClientSequence">The next client sequence.</param>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static void UpdateNextClientSequence(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        long nextClientSequence)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_streams
            SET next_client_sequence = $nextClientSequence
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Parameters.AddWithValue("$nextClientSequence", nextClientSequence);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite stream row is missing.");
    }

    /// <summary>Returns the original receipt when a repeated operation has identical commit intent.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store partition.</param>
    /// <param name="operation">The repeated operation.</param>
    /// <param name="snapshotMutation">The original snapshot mutation.</param>
    /// <param name="fingerprint">The canonical commit intent fingerprint.</param>
    /// <param name="result">The original receipt when present.</param>
    /// <returns>Whether the operation was previously committed.</returns>
    /// <exception cref="InvalidOperationException">Stored receipt data or repeated commit intent is inconsistent.</exception>
    internal static bool TryReadCommittedResult(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        byte[] fingerprint,
        out LocalCommitResult? result)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT client_sequence, snapshot_revision, committed_at_utc, commit_fingerprint
            FROM oc_outbox
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operation.OperationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            result = null;
            return false;
        }

        const int SequenceIndex = 0;
        const int RevisionIndex = 1;
        const int CommittedAtIndex = 2;
        const int FingerprintIndex = 3;
        var sequence = ReadPositiveLong(reader, SequenceIndex, "The SQLite operation sequence is invalid.");
        var revision = ReadNonNegativeLong(reader, RevisionIndex, InvalidSnapshotRevisionMessage);
        var storedFingerprint = ReadBytes(reader, FingerprintIndex, "The SQLite commit fingerprint is invalid.");
        if (sequence != operation.ClientSequence || revision != snapshotMutation.ExpectedRevision + 1
            || !SqliteCommitFingerprint.Matches(storedFingerprint, fingerprint))
        {
            throw new InvalidOperationException("The SQLite operation id has already been committed with different content.");
        }

        result = new(
            operation.OperationId,
            sequence,
            revision,
            ReadDateTimeOffset(reader, CommittedAtIndex, "The SQLite operation commit timestamp is invalid."));
        return true;
    }

    /// <summary>Reads a snapshot row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The snapshot or null.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static LocalSnapshot? ReadSnapshot(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT format_version, server_cursor, payload_contract_id, payload_schema_version, payload_content_type,
                   payload, payload_hash, revision, saved_at_utc
            FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        const int FormatVersionIndex = 0;
        const int ServerCursorIndex = 1;
        const int PayloadContractIndex = 2;
        const int PayloadSchemaIndex = 3;
        const int PayloadContentTypeIndex = 4;
        const int PayloadIndex = 5;
        const int PayloadHashIndex = 6;
        const int RevisionIndex = 7;
        const int SavedAtIndex = 8;
        var snapshot = new LocalSnapshot(
            streamId,
            ReadPositiveInt(reader, FormatVersionIndex, "The SQLite snapshot format version is invalid."),
            ReadNullableString(reader, ServerCursorIndex),
            ReadPayload(reader, PayloadContractIndex, PayloadSchemaIndex, PayloadContentTypeIndex, PayloadIndex, PayloadHashIndex),
            ReadNonNegativeLong(reader, RevisionIndex, InvalidSnapshotRevisionMessage),
            ReadDateTimeOffset(reader, SavedAtIndex, "The SQLite snapshot timestamp is invalid."));
        SqliteLocalCommitValidation.ValidatePayload(snapshot.State, nameof(snapshot));
        return snapshot;
    }

    /// <summary>Reads pending operations in client sequence order.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The pending operations.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static List<SyncOperation> ReadPendingOperations(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT operation_id, client_sequence, timestamp_utc, base_version, operation_type,
                   payload_contract_id, payload_schema_version, payload_content_type, payload, payload_hash,
                   policy_delivery_guarantee, policy_durability, policy_priority, policy_conflict
            FROM oc_outbox
            WHERE store_identity = $storeIdentity AND stream_id = $streamId
            ORDER BY client_sequence ASC;
            """;
        AddStreamParameters(command, storeIdentity, streamId);
        using var reader = command.ExecuteReader();
        var operations = new List<SyncOperation>();
        while (reader.Read())
        {
            const int OperationIdIndex = 0;
            const int ClientSequenceIndex = 1;
            const int TimestampIndex = 2;
            const int BaseVersionIndex = 3;
            const int TypeIndex = 4;
            const int PayloadContractIndex = 5;
            const int PayloadSchemaIndex = 6;
            const int PayloadContentTypeIndex = 7;
            const int PayloadIndex = 8;
            const int PayloadHashIndex = 9;
            const int DeliveryIndex = 10;
            const int DurabilityIndex = 11;
            const int PriorityIndex = 12;
            const int ConflictIndex = 13;
            var operationId = ReadOperationId(reader, OperationIdIndex);
            var operation = new SyncOperation
            {
                OperationId = operationId,
                StreamId = streamId,
                ClientSequence = ReadPositiveLong(reader, ClientSequenceIndex, "The SQLite operation sequence is invalid."),
                TimestampUtc = ReadDateTimeOffset(reader, TimestampIndex, "The SQLite operation timestamp is invalid."),
                BaseVersion = ReadNullableString(reader, BaseVersionIndex),
                Type = ReadOperationType(reader, TypeIndex),
                Payload = ReadPayload(reader, PayloadContractIndex, PayloadSchemaIndex, PayloadContentTypeIndex, PayloadIndex, PayloadHashIndex),
                Policy = ReadPolicy(reader, DeliveryIndex, DurabilityIndex, PriorityIndex, ConflictIndex),
                Metadata = ReadMetadata(connection, transaction, storeIdentity, operationId),
            };
            SqliteLocalCommitValidation.ValidateCommitInput(operation, new(streamId, operation.Payload, FormatVersion: 1, ExpectedRevision: 0));
            operations.Add(operation);
        }

        return operations;
    }

    /// <summary>Reads operation metadata.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The metadata.</returns>
    internal static Dictionary<string, string> ReadMetadata(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT key, value
            FROM oc_outbox_metadata
            WHERE store_identity = $storeIdentity AND operation_id = $operationId
            ORDER BY key ASC;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        while (reader.Read())
        {
            metadata.Add(ReadString(reader, 0, "The SQLite metadata key is invalid."), ReadString(reader, 1, "The SQLite metadata value is invalid."));
        }

        return metadata;
    }

    /// <summary>Reads a payload envelope.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="contractIndex">The contract index.</param>
    /// <param name="schemaIndex">The schema index.</param>
    /// <param name="contentTypeIndex">The content type index.</param>
    /// <param name="payloadIndex">The payload index.</param>
    /// <param name="hashIndex">The hash index.</param>
    /// <returns>The payload.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static PayloadEnvelope ReadPayload(
        SqliteDataReader reader,
        int contractIndex,
        int schemaIndex,
        int contentTypeIndex,
        int payloadIndex,
        int hashIndex)
    {
        var payload = new PayloadEnvelope(
            ReadString(reader, contractIndex, "The SQLite payload contract is invalid."),
            ReadPositiveInt(reader, schemaIndex, "The SQLite payload schema version is invalid."),
            ReadString(reader, contentTypeIndex, "The SQLite payload content type is invalid."),
            ReadBytes(reader, payloadIndex, "The SQLite payload bytes are invalid."),
            ReadString(reader, hashIndex, "The SQLite payload hash is invalid."));
        SqliteLocalCommitValidation.ValidatePayload(payload, nameof(payload));
        return payload;
    }

    /// <summary>Reads operation policy.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="deliveryIndex">The delivery index.</param>
    /// <param name="durabilityIndex">The durability index.</param>
    /// <param name="priorityIndex">The priority index.</param>
    /// <param name="conflictIndex">The conflict index.</param>
    /// <returns>The policy.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static OperationPolicy ReadPolicy(
        SqliteDataReader reader,
        int deliveryIndex,
        int durabilityIndex,
        int priorityIndex,
        int conflictIndex)
    {
        var policy = new OperationPolicy(
            (DeliveryGuarantee)ReadInt(reader, deliveryIndex, "The SQLite delivery guarantee is invalid."),
            (OperationDurability)ReadInt(reader, durabilityIndex, "The SQLite durability is invalid."),
            ReadInt(reader, priorityIndex, "The SQLite priority is invalid."),
            (ConflictPolicy)ReadInt(reader, conflictIndex, "The SQLite conflict policy is invalid."));
        policy.Validate();
        return policy;
    }

    /// <summary>Reads an operation type.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation type.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static SyncOperationType ReadOperationType(SqliteDataReader reader, int index)
    {
        var operationType = (SyncOperationType)ReadInt(reader, index, "The SQLite operation type is invalid.");
        SqliteLocalCommitValidation.ValidateOperationType(operationType);
        return operationType;
    }

    /// <summary>Reads an operation id.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation id.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static OperationId ReadOperationId(SqliteDataReader reader, int index)
    {
        var text = ReadString(reader, index, "The SQLite operation id is invalid.");
        if (Guid.TryParse(text, out var value) && value != Guid.Empty)
        {
            return new(value);
        }

        throw new InvalidOperationException("The SQLite operation id is invalid.");
    }

    /// <summary>Adds stream parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    internal static void AddStreamParameters(SqliteCommand command, string storeIdentity, StreamId streamId)
    {
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue("$streamId", streamId.Value);
    }

    /// <summary>Adds operation parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="committedAtUtc">The commit time.</param>
    internal static void AddOperationParameters(SqliteCommand command, string storeIdentity, SyncOperation operation, DateTimeOffset committedAtUtc)
    {
        _ = command.Parameters.AddWithValue(OperationIdParameter, operation.OperationId.Value.ToString("D"));
        AddStreamParameters(command, storeIdentity, operation.StreamId);
        _ = command.Parameters.AddWithValue("$clientSequence", operation.ClientSequence);
        _ = command.Parameters.AddWithValue("$timestampUtc", FormatDateTimeOffset(operation.TimestampUtc));
        _ = command.Parameters.AddWithValue("$baseVersion", (object?)operation.BaseVersion ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$operationType", (int)operation.Type);
        AddPayloadParameters(command, operation.Payload);
        _ = command.Parameters.AddWithValue("$policyDeliveryGuarantee", (int)operation.Policy.DeliveryGuarantee);
        _ = command.Parameters.AddWithValue("$policyDurability", (int)operation.Policy.Durability);
        _ = command.Parameters.AddWithValue("$policyPriority", operation.Policy.Priority);
        _ = command.Parameters.AddWithValue("$policyConflict", (int)operation.Policy.ConflictPolicy);
        _ = command.Parameters.AddWithValue("$committedAtUtc", FormatDateTimeOffset(committedAtUtc));
    }

    /// <summary>Adds payload parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="payload">The payload.</param>
    internal static void AddPayloadParameters(SqliteCommand command, PayloadEnvelope payload)
    {
        _ = command.Parameters.AddWithValue("$payloadContractId", payload.ContractId);
        _ = command.Parameters.AddWithValue("$payloadSchemaVersion", payload.SchemaVersion);
        _ = command.Parameters.AddWithValue("$payloadContentType", payload.ContentType);
        _ = command.Parameters.Add("$payload", SqliteType.Blob);
        command.Parameters["$payload"].Value = payload.Payload.ToArray();
        _ = command.Parameters.AddWithValue("$payloadHash", payload.PayloadHash);
    }

    /// <summary>Reads a string column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The string.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static string ReadString(SqliteDataReader reader, int index, string message)
    {
        if (!reader.IsDBNull(index))
        {
            return reader.GetString(index);
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a nullable string column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The string or null.</returns>
    internal static string? ReadNullableString(SqliteDataReader reader, int index) => reader.IsDBNull(index) ? null : reader.GetString(index);

    /// <summary>Reads a byte array column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The bytes.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static byte[] ReadBytes(SqliteDataReader reader, int index, string message)
    {
        if (!reader.IsDBNull(index) && reader.GetFieldValue<byte[]>(index) is { } bytes)
        {
            return bytes;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads an integer column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The integer.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static int ReadInt(SqliteDataReader reader, int index, string message)
    {
        if (!reader.IsDBNull(index))
        {
            return reader.GetInt32(index);
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a positive integer column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The integer.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static int ReadPositiveInt(SqliteDataReader reader, int index, string message)
    {
        var value = ReadInt(reader, index, message);
        if (value > 0)
        {
            return value;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a positive long column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static long ReadPositiveLong(SqliteDataReader reader, int index, string message)
    {
        if (!reader.IsDBNull(index))
        {
            var value = reader.GetInt64(index);
            if (value > 0)
            {
                return value;
            }
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a non-negative long column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static long ReadNonNegativeLong(SqliteDataReader reader, int index, string message)
    {
        if (!reader.IsDBNull(index))
        {
            return ReadNonNegativeLong(reader.GetInt64(index), message);
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a non-negative long scalar.</summary>
    /// <param name="value">The scalar.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static long ReadNonNegativeLong(object? value, string message)
    {
        if (value is long number && number >= 0)
        {
            return number;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Reads a date-time offset column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The date-time offset.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static DateTimeOffset ReadDateTimeOffset(SqliteDataReader reader, int index, string message)
    {
        var value = ReadString(reader, index, message);
        if (DateTimeOffset.TryParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var timestamp))
        {
            return timestamp;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Formats a date-time offset for storage.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The formatted value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string FormatDateTimeOffset(DateTimeOffset value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
}
