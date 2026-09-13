// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes SQLite statements for outbox lease rows.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The lease operation identifier column index.</summary>
    private const int LeaseOperationIdIndex = 0;

    /// <summary>The lease stream identifier column index.</summary>
    private const int LeaseStreamIdIndex = 1;

    /// <summary>The lease client sequence column index.</summary>
    private const int LeaseClientSequenceIndex = 2;

    /// <summary>The lease payload bytes column index.</summary>
    private const int LeasePayloadBytesIndex = 3;

    /// <summary>The lease identifier column index.</summary>
    private const int LeaseIdIndex = 4;

    /// <summary>The lease expiry column index.</summary>
    private const int LeaseExpiryIndex = 5;

    /// <summary>The lease operation state column index.</summary>
    private const int LeaseOperationStateIndex = 6;

    /// <summary>The lease operation attempt column index.</summary>
    private const int LeaseAttemptIndex = 7;

    /// <summary>The lease delivery guarantee column index.</summary>
    private const int LeaseDeliveryGuaranteeIndex = 8;

    /// <summary>The lease retry due UTC column index.</summary>
    private const int LeaseRetryDueUtcIndex = 9;

    /// <summary>The invalid lease expiry message.</summary>
    private const string InvalidLeaseExpiryMessage = "The SQLite outbox lease expiry is invalid.";

    /// <summary>The invalid lease identifier message.</summary>
    private const string InvalidLeaseIdMessage = "The SQLite outbox lease id is invalid.";

    /// <summary>The invalid operation stream message.</summary>
    private const string InvalidOperationStreamMessage = "The SQLite operation stream is invalid.";

    /// <summary>Selects a contiguous leaseable operation prefix without reading payload bytes.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The lease request.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The operation rows selected for lease membership.</returns>
    /// <exception cref="OperationCanceledException">The operation is canceled while traversing candidates.</exception>
    internal static IReadOnlyList<SqliteOutboxLeaseMember> SelectLeaseableOperationIds(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OutboxLeaseRequest request,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        if (request.StreamId.HasValue)
        {
            return SelectLeaseableOperationIdsForStream(
                connection,
                transaction,
                storeIdentity,
                request.StreamId.GetValueOrDefault(),
                request,
                nowUtc,
                cancellationToken);
        }

        var head = SelectFirstLeaseableStreamHead(
            connection,
            transaction,
            storeIdentity,
            request,
            nowUtc,
            cancellationToken);
        return head.HasValue
            ? SelectLeaseableOperationIdsForStream(
                connection,
                transaction,
                storeIdentity,
                head.GetValueOrDefault().StreamId,
                request,
                nowUtc,
                cancellationToken)
            : [];
    }

    /// <summary>Inserts lease membership rows for a selected batch.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="expiresAtUtc">The expiry timestamp.</param>
    /// <param name="operations">The selected operation rows.</param>
    internal static void InsertLeaseMembership(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        DateTimeOffset expiresAtUtc,
        IReadOnlyList<SqliteOutboxLeaseMember> operations)
    {
        for (var index = 0; index < operations.Count; index++)
        {
            var operation = operations[index];
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO oc_outbox_leases
                    (store_identity, lease_id, operation_id, stream_id, client_sequence, lease_expires_at_utc, lease_member_count)
                VALUES
                    ($storeIdentity, $leaseId, $operationId, $streamId, $clientSequence, $leaseExpiresAtUtc, $leaseMemberCount);
                """;
            AddLeaseParameters(command, storeIdentity, leaseId);
            _ = command.Parameters.AddWithValue(OperationIdParameter, operation.OperationId.Value.ToString("D"));
            _ = command.Parameters.AddWithValue(StreamIdParameter, operation.StreamId.Value);
            _ = command.Parameters.AddWithValue("$clientSequence", operation.ClientSequence);
            _ = command.Parameters.AddWithValue("$leaseExpiresAtUtc", FormatDateTimeOffset(expiresAtUtc));
            _ = command.Parameters.AddWithValue("$leaseMemberCount", operations.Count);
            _ = command.ExecuteNonQuery();
        }
    }

    /// <summary>Deletes existing lease rows only for the selected operation rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operations">The selected operations.</param>
    internal static void ReclaimSelectedLeaseRows(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        IReadOnlyList<SqliteOutboxLeaseMember> operations)
    {
        for (var index = 0; index < operations.Count; index++)
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                DELETE FROM oc_outbox_leases
                WHERE store_identity = $storeIdentity AND operation_id = $operationId;
                """;
            _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
            _ = command.Parameters.AddWithValue(OperationIdParameter, operations[index].OperationId.Value.ToString("D"));
            _ = command.ExecuteNonQuery();
        }
    }

    /// <summary>Reads only the operations currently owned by one lease.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The leased operations.</returns>
    /// <exception cref="SqlitePayloadQuarantineException">Stored SQLite payload data is invalid.</exception>
    internal static List<SyncOperation> ReadLeasedOperations(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        long maximumPayloadBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT outbox.operation_id, outbox.stream_id, outbox.client_sequence, outbox.timestamp_utc,
                   outbox.base_version, outbox.operation_type, outbox.payload_contract_id, outbox.payload_schema_version,
                   outbox.payload_content_type, outbox.payload, outbox.payload_hash, outbox.policy_delivery_guarantee,
                   outbox.policy_durability, outbox.policy_priority, outbox.policy_conflict, outbox.rowid,
                   typeof(outbox.payload_contract_id), length(CAST(outbox.payload_contract_id AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_contract_id AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload_schema_version), outbox.payload_schema_version,
                   typeof(outbox.payload_content_type), length(CAST(outbox.payload_content_type AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_content_type AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload), length(CAST(outbox.payload AS BLOB)), IFNULL(substr(CAST(outbox.payload AS BLOB), 1, 4096), x''),
                   typeof(outbox.payload_hash), length(CAST(outbox.payload_hash AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_hash AS BLOB), 1, 4100), x''), lease.stream_id
            FROM oc_outbox AS outbox
            INNER JOIN oc_outbox_leases AS lease
                ON lease.store_identity = outbox.store_identity
                AND lease.operation_id = outbox.operation_id
            WHERE lease.store_identity = $storeIdentity AND lease.lease_id = $leaseId
            ORDER BY lease.client_sequence ASC;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        using var reader = command.ExecuteReader();
        List<SyncOperation> operations = [];
        while (reader.Read())
        {
            operations.Add(ReadLeasedOperation(connection, transaction, storeIdentity, reader, maximumPayloadBytes));
        }

        return operations;
    }

    /// <summary>Reads one operation owned by the active lease.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The leased operation.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own the operation or stored data is invalid.</exception>
    internal static SyncOperation ReadLeasedOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId,
        long maximumPayloadBytes)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT outbox.operation_id, outbox.stream_id, outbox.client_sequence, outbox.timestamp_utc,
                   outbox.base_version, outbox.operation_type, outbox.payload_contract_id, outbox.payload_schema_version,
                   outbox.payload_content_type, outbox.payload, outbox.payload_hash, outbox.policy_delivery_guarantee,
                   outbox.policy_durability, outbox.policy_priority, outbox.policy_conflict, outbox.rowid,
                   typeof(outbox.payload_contract_id), length(CAST(outbox.payload_contract_id AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_contract_id AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload_schema_version), outbox.payload_schema_version,
                   typeof(outbox.payload_content_type), length(CAST(outbox.payload_content_type AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_content_type AS BLOB), 1, 4100), x''),
                   typeof(outbox.payload), length(CAST(outbox.payload AS BLOB)), IFNULL(substr(CAST(outbox.payload AS BLOB), 1, 4096), x''),
                   typeof(outbox.payload_hash), length(CAST(outbox.payload_hash AS BLOB)),
                   IFNULL(substr(CAST(outbox.payload_hash AS BLOB), 1, 4100), x''), lease.stream_id
            FROM oc_outbox AS outbox
            INNER JOIN oc_outbox_leases AS lease
                ON lease.store_identity = outbox.store_identity
                AND lease.operation_id = outbox.operation_id
            WHERE lease.store_identity = $storeIdentity
                AND lease.lease_id = $leaseId
                AND lease.operation_id = $operationId;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException("The SQLite outbox lease does not own the operation.");
        }

        return ReadLeasedOperation(connection, transaction, storeIdentity, reader, maximumPayloadBytes);
    }

    /// <summary>Validates that a lease still owns its complete original batch.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <returns>The lease expiry timestamp.</returns>
    /// <exception cref="InvalidOperationException">The lease is missing or incomplete.</exception>
    internal static DateTimeOffset ValidateLeaseMembership(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT COUNT(*), MIN(lease_member_count), MAX(lease_member_count),
                   MIN(lease_expires_at_utc), MAX(lease_expires_at_utc)
            FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        using var reader = command.ExecuteReader();
        _ = reader.Read();
        var count = ReadPositiveLong(reader, 0, MissingLeaseMessage);
        var minimumMemberCount = ReadPositiveLong(reader, LeaseMemberCountMinimumIndex, "The SQLite outbox lease member count is invalid.");
        var maximumMemberCount = ReadPositiveLong(reader, LeaseMemberCountMaximumIndex, "The SQLite outbox lease member count is invalid.");
        if (minimumMemberCount != maximumMemberCount || minimumMemberCount != count)
        {
            throw new InvalidOperationException("The SQLite outbox lease membership is incomplete.");
        }

        var minimumExpiry = ReadDateTimeOffset(reader, LeaseExpiryMinimumIndex, InvalidLeaseExpiryMessage);
        var maximumExpiry = ReadDateTimeOffset(reader, LeaseExpiryMaximumIndex, InvalidLeaseExpiryMessage);
        if (minimumExpiry != maximumExpiry)
        {
            throw new InvalidOperationException("The SQLite outbox lease expiry is inconsistent.");
        }

        return minimumExpiry;
    }

    /// <summary>Renews every row for a validated lease.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="expiresAtUtc">The new expiry timestamp.</param>
    /// <exception cref="InvalidOperationException">The lease is missing.</exception>
    internal static void RenewLease(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        DateTimeOffset expiresAtUtc)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_expires_at_utc = $leaseExpiresAtUtc
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        _ = command.Parameters.AddWithValue("$leaseExpiresAtUtc", FormatDateTimeOffset(expiresAtUtc));
        if (command.ExecuteNonQuery() > 0)
        {
            return;
        }

        throw new InvalidOperationException(MissingLeaseMessage);
    }

    /// <summary>Releases every row for a validated lease.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <exception cref="InvalidOperationException">The lease is missing.</exception>
    internal static void ReleaseLease(SqliteConnection connection, SqliteTransaction transaction, string storeIdentity, Guid leaseId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        if (command.ExecuteNonQuery() > 0)
        {
            return;
        }

        throw new InvalidOperationException(MissingLeaseMessage);
    }

    /// <summary>Releases one operation from a validated lease and keeps remaining members leased.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="InvalidOperationException">The lease does not own the operation.</exception>
    internal static void ReleaseLeaseOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId)
    {
        using (var delete = connection.CreateCommand())
        {
            delete.Transaction = transaction;
            delete.CommandText = """
                DELETE FROM oc_outbox_leases
                WHERE store_identity = $storeIdentity
                    AND lease_id = $leaseId
                    AND operation_id = $operationId;
                """;
            AddLeaseParameters(delete, storeIdentity, leaseId);
            _ = delete.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
            if (delete.ExecuteNonQuery() != 1)
            {
                throw new InvalidOperationException("The SQLite outbox lease does not own the operation.");
            }
        }

        using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_member_count = lease_member_count - 1
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        AddLeaseParameters(update, storeIdentity, leaseId);
        _ = update.ExecuteNonQuery();
    }

    /// <summary>Reads one operation from a leased batch row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="reader">The row reader.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The leased operation.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    /// <exception cref="SqlitePayloadQuarantineException">Stored SQLite payload data is invalid.</exception>
    private static SyncOperation ReadLeasedOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SqliteDataReader reader,
        long maximumPayloadBytes)
    {
        const int OperationIdIndex = 0;
        const int StreamIdIndex = 1;
        const int ClientSequenceIndex = 2;
        const int TimestampIndex = 3;
        const int BaseVersionIndex = 4;
        const int TypeIndex = 5;
        const int PayloadContractIndex = 6;
        const int PayloadSchemaIndex = 7;
        const int PayloadContentTypeIndex = 8;
        const int PayloadIndex = 9;
        const int PayloadHashIndex = 10;
        const int DeliveryIndex = 11;
        const int DurabilityIndex = 12;
        const int PriorityIndex = 13;
        const int ConflictIndex = 14;
        const int RowIdIndex = 15;
        const int EvidenceIndex = 16;
        const int LeaseRowStreamIdIndex = 30;
        var operationId = ReadOperationId(reader, OperationIdIndex);
        var streamId = new StreamId(ReadString(reader, StreamIdIndex, InvalidOperationStreamMessage));
        var leaseStreamId = new StreamId(ReadString(reader, LeaseRowStreamIdIndex, InvalidOperationStreamMessage));
        ValidateLeaseStream(streamId, leaseStreamId);
        var operation = new SyncOperation
        {
            OperationId = operationId,
            StreamId = streamId,
            ClientSequence = ReadPositiveLong(reader, ClientSequenceIndex, InvalidOperationSequenceMessage),
            TimestampUtc = ReadDateTimeOffset(reader, TimestampIndex, "The SQLite operation timestamp is invalid."),
            BaseVersion = ReadNullableString(reader, BaseVersionIndex),
            Type = ReadOperationType(reader, TypeIndex),
            Payload = ReadOperationPayload(
                connection,
                reader,
                SqlitePayloadColumns.Create(
                    PayloadContractIndex,
                    PayloadSchemaIndex,
                    PayloadContentTypeIndex,
                    PayloadIndex,
                    PayloadHashIndex,
                    new(RowIdIndex, SqliteStoreSchema.OutboxTableName, PayloadColumnName),
                    EvidenceIndex),
                operationId,
                maximumPayloadBytes),
            Policy = ReadPolicy(reader, DeliveryIndex, DurabilityIndex, PriorityIndex, ConflictIndex),
            Metadata = ReadMetadata(connection, transaction, storeIdentity, operationId),
        };
        SqliteLocalCommitValidation.ValidateCommitInput(operation, new(streamId, operation.Payload, FormatVersion: 1, ExpectedRevision: 0));
        return operation;
    }

    /// <summary>Validates that lease stream metadata matches the authoritative outbox stream.</summary>
    /// <param name="outboxStreamId">The stream stored on the outbox row.</param>
    /// <param name="leaseStreamId">The stream stored on the lease row.</param>
    /// <exception cref="InvalidOperationException">The lease stream does not match the operation stream.</exception>
    private static void ValidateLeaseStream(StreamId outboxStreamId, StreamId leaseStreamId)
    {
        if (outboxStreamId == leaseStreamId)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite outbox lease stream does not match the operation stream.");
    }

    /// <summary>Selects a contiguous leaseable operation prefix for one stream.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="request">The lease request.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The selected operation rows.</returns>
    /// <exception cref="OperationCanceledException">The operation is canceled while traversing candidates.</exception>
    private static List<SqliteOutboxLeaseMember> SelectLeaseableOperationIdsForStream(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        OutboxLeaseRequest request,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT outbox.operation_id, outbox.stream_id, outbox.client_sequence, length(outbox.payload),
                   lease.lease_id, lease.lease_expires_at_utc, state.operation_state,
                   state.attempt_count, outbox.policy_delivery_guarantee, state.retry_due_utc
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            LEFT JOIN oc_outbox_leases AS lease
                ON lease.store_identity = outbox.store_identity
                AND lease.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity AND outbox.stream_id = $streamId
                AND (state.operation_state IS NULL OR state.operation_state NOT IN (4, 5, 6))
                AND NOT EXISTS (
                    SELECT 1
                    FROM oc_payload_quarantine AS quarantine
                    WHERE quarantine.store_identity = outbox.store_identity
                        AND quarantine.stream_id = outbox.stream_id)
            ORDER BY outbox.client_sequence ASC
            LIMIT $maximumOperations;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        _ = command.Parameters.AddWithValue("$maximumOperations", request.MaximumOperations);
        using var reader = command.ExecuteReader();
        List<SqliteOutboxLeaseMember> selected = [];
        var payloadBytes = 0L;
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = ReadLeaseCandidateRow(reader, nowUtc);
            if (!row.IsEligibleNow || row.HasActiveLease || row.PayloadBytes > request.MaximumBytes - payloadBytes)
            {
                return selected;
            }

            selected.Add(new(row.OperationId, row.StreamId, row.ClientSequence));
            payloadBytes += row.PayloadBytes;
        }

        return selected;
    }

    /// <summary>Selects the first stream whose head operation can start a lease.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The lease request.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The first leaseable stream head, if any.</returns>
    /// <exception cref="OperationCanceledException">The operation is canceled while traversing stream heads.</exception>
    private static LeaseCandidateRow? SelectFirstLeaseableStreamHead(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OutboxLeaseRequest request,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT outbox.operation_id, outbox.stream_id, outbox.client_sequence, length(outbox.payload),
                   lease.lease_id, lease.lease_expires_at_utc, state.operation_state,
                   state.attempt_count, outbox.policy_delivery_guarantee, state.retry_due_utc
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            LEFT JOIN oc_outbox_leases AS lease
                ON lease.store_identity = outbox.store_identity
                AND lease.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity
                AND (state.operation_state IS NULL OR state.operation_state NOT IN (4, 5, 6))
                AND NOT EXISTS (
                    SELECT 1
                    FROM oc_payload_quarantine AS quarantine
                    WHERE quarantine.store_identity = outbox.store_identity
                        AND quarantine.stream_id = outbox.stream_id)
                AND outbox.client_sequence = (
                    SELECT MIN(head.client_sequence)
                    FROM oc_outbox AS head
                    LEFT JOIN oc_outbox_operation_states AS head_state
                        ON head_state.store_identity = head.store_identity
                        AND head_state.operation_id = head.operation_id
                    WHERE head.store_identity = outbox.store_identity
                        AND head.stream_id = outbox.stream_id
                        AND (head_state.operation_state IS NULL OR head_state.operation_state NOT IN (4, 5, 6))
                        AND NOT EXISTS (
                            SELECT 1
                            FROM oc_payload_quarantine AS head_quarantine
                            WHERE head_quarantine.store_identity = head.store_identity
                                AND head_quarantine.stream_id = head.stream_id))
            ORDER BY outbox.stream_id ASC;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = ReadLeaseCandidateRow(reader, nowUtc);
            if (row.IsEligibleNow && !row.HasActiveLease && row.PayloadBytes <= request.MaximumBytes)
            {
                return row;
            }
        }

        return null;
    }

    /// <summary>Reads one lease candidate row and validates any selected lease expiry.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <returns>The candidate row.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static LeaseCandidateRow ReadLeaseCandidateRow(SqliteDataReader reader, DateTimeOffset nowUtc)
    {
        var operationId = ReadOperationId(reader, LeaseOperationIdIndex);
        var streamId = new StreamId(ReadString(reader, LeaseStreamIdIndex, InvalidOperationStreamMessage));
        var clientSequence = ReadPositiveLong(reader, LeaseClientSequenceIndex, InvalidOperationSequenceMessage);
        var payloadBytes = ReadNonNegativeLong(reader, LeasePayloadBytesIndex, "The SQLite operation payload length is invalid.");
        var state = ReadOperationState(reader, LeaseOperationStateIndex);
        var attempt = ReadNonNegativeInt(reader, LeaseAttemptIndex, InvalidAttemptCountMessage);
        var deliveryGuarantee = ReadDeliveryGuarantee(reader, LeaseDeliveryGuaranteeIndex);
        var retryDueUtc = ReadNullableDateTimeOffset(reader, LeaseRetryDueUtcIndex, "The SQLite retry due timestamp is invalid.");
        var hasActiveLease = false;
        if (!reader.IsDBNull(LeaseIdIndex))
        {
            _ = ReadLeaseId(reader, LeaseIdIndex);
            hasActiveLease = ReadDateTimeOffset(reader, LeaseExpiryIndex, InvalidLeaseExpiryMessage) > nowUtc;
        }

        var isBlockedByAtMostOnceAmbiguity = state == SyncOperationState.Ambiguous
            && deliveryGuarantee == DeliveryGuarantee.AtMostOnce;
        var isBlockedByAtMostOnceAttempt = deliveryGuarantee == DeliveryGuarantee.AtMostOnce && attempt > 0;
        var isBlockedByUnresolvedState = state is SyncOperationState.Conflict or SyncOperationState.GuaranteeExpired;
        var isBlockedByRetryDue = retryDueUtc.HasValue && retryDueUtc.GetValueOrDefault() > nowUtc;
        return new(
            operationId,
            streamId,
            clientSequence,
            payloadBytes,
            hasActiveLease,
            !isBlockedByAtMostOnceAmbiguity && !isBlockedByAtMostOnceAttempt && !isBlockedByUnresolvedState && !isBlockedByRetryDue);
    }

    /// <summary>Reads a persisted lease identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The lease identifier.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static Guid ReadLeaseId(SqliteDataReader reader, int index)
    {
        var value = ReadString(reader, index, InvalidLeaseIdMessage);
        return Guid.TryParseExact(value, "D", out var leaseId)
            ? leaseId
            : throw new InvalidOperationException(InvalidLeaseIdMessage);
    }

    /// <summary>One leaseable operation row selected before payload materialization.</summary>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="StreamId">The stream identifier.</param>
    /// <param name="ClientSequence">The client sequence.</param>
    /// <param name="PayloadBytes">The payload byte count.</param>
    /// <param name="HasActiveLease">Whether an active lease currently owns the operation.</param>
    /// <param name="IsEligibleNow">Whether retry and delivery state permit leasing the operation now.</param>
    private readonly record struct LeaseCandidateRow(
        OperationId OperationId,
        StreamId StreamId,
        long ClientSequence,
        long PayloadBytes,
        bool HasActiveLease,
        bool IsEligibleNow);
}
