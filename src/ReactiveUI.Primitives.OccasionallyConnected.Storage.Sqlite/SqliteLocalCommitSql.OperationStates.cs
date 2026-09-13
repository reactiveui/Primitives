// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes SQLite statements for operation lifecycle state rows.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The operation state parameter name.</summary>
    private const string OperationStateParameter = "$operationState";

    /// <summary>The attempt count parameter name.</summary>
    private const string AttemptCountParameter = "$attemptCount";

    /// <summary>The changed-at parameter name.</summary>
    private const string ChangedAtUtcParameter = "$changedAtUtc";

    /// <summary>The reason code parameter name.</summary>
    private const string ReasonCodeParameter = "$reasonCode";

    /// <summary>The missing operation state message.</summary>
    private const string MissingOperationStateMessage = "The SQLite operation state is missing.";

    /// <summary>The invalid operation state message.</summary>
    private const string InvalidOperationStateMessage = "The SQLite operation state is invalid.";

    /// <summary>The invalid attempt count message.</summary>
    private const string InvalidAttemptCountMessage = "The SQLite operation attempt count is invalid.";

    /// <summary>The stream id column index for operation status reads.</summary>
    private const int StatusStreamIndex = 0;

    /// <summary>The operation state column index for operation status reads.</summary>
    private const int StatusStateIndex = 1;

    /// <summary>The attempt count column index for operation status reads.</summary>
    private const int StatusAttemptIndex = 2;

    /// <summary>The changed-at column index for operation status reads.</summary>
    private const int StatusChangedAtIndex = 3;

    /// <summary>The reason code column index for operation status reads.</summary>
    private const int StatusReasonCodeIndex = 4;

    /// <summary>The retry started column index.</summary>
    private const int RetryStartedIndex = 0;

    /// <summary>The retry due column index.</summary>
    private const int RetryDueIndex = 1;

    /// <summary>The retry previous delay ticks column index.</summary>
    private const int RetryPreviousDelayTicksIndex = 2;

    /// <summary>The retry transient attempt count column index.</summary>
    private const int RetryTransientAttemptCountIndex = 3;

    /// <summary>The retry authentication state column index.</summary>
    private const int RetryAuthenticationStateIndex = 4;

    /// <summary>The retry credentials version column index.</summary>
    private const int RetryCredentialsVersionIndex = 5;

    /// <summary>The retry state operation id column index.</summary>
    private const int RetryOperationIdIndex = 6;

    /// <summary>The operation state target state column index.</summary>
    private const int OperationTargetStateIndex = 0;

    /// <summary>The operation state target attempt column index.</summary>
    private const int OperationTargetAttemptIndex = 1;

    /// <summary>The operation state target delivery guarantee column index.</summary>
    private const int OperationTargetDeliveryGuaranteeIndex = 2;

    /// <summary>Inserts the initial durable operation state for a committed outbox row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="committedAtUtc">The commit timestamp.</param>
    /// <exception cref="InvalidOperationException">The operation state cannot be inserted.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void InsertInitialOperationState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SyncOperation operation,
        DateTimeOffset committedAtUtc) =>
        UpsertOperationState(
            connection,
            transaction,
            storeIdentity,
            new(operation.OperationId, SyncOperationState.QueuedForUpload, Attempt: 0, committedAtUtc, ReasonCode: null));

    /// <summary>Reads the operation status for the initialized store partition.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The status, or null when no operation exists.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static SyncOperationStatus? ReadOperationStatus(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT outbox.stream_id, state.operation_state, state.attempt_count, state.changed_at_utc, state.reason_code
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity AND outbox.operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var streamId = new StreamId(ReadString(reader, StatusStreamIndex, "The SQLite operation stream is invalid."));
        return new(
            operationId,
            streamId,
            ReadOperationState(reader, StatusStateIndex),
            ReadNonNegativeInt(reader, StatusAttemptIndex, InvalidAttemptCountMessage),
            ReadDateTimeOffset(reader, StatusChangedAtIndex, "The SQLite operation state timestamp is invalid."),
            ReadReasonCode(reader, StatusReasonCodeIndex));
    }

    /// <summary>Reads persisted retry state for an operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The retry state, if present.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static RetryState? ReadRetryState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT retry_started_utc, retry_due_utc, retry_previous_delay_ticks,
                   retry_transient_attempt_count, retry_authentication_state, retry_credentials_version,
                   state.operation_id
            FROM oc_outbox AS outbox
            LEFT JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity AND outbox.operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        if (reader.IsDBNull(RetryOperationIdIndex))
        {
            throw new InvalidOperationException(MissingOperationStateMessage);
        }

        if (reader.IsDBNull(RetryStartedIndex))
        {
            return null;
        }

        var previousDelayTicks = ReadNullableLong(reader, RetryPreviousDelayTicksIndex, "The SQLite retry previous delay is invalid.");
        return new(
            ReadDateTimeOffset(reader, RetryStartedIndex, "The SQLite retry start timestamp is invalid."),
            ReadNullableDateTimeOffset(reader, RetryDueIndex, "The SQLite retry due timestamp is invalid."),
            previousDelayTicks.HasValue ? TimeSpan.FromTicks(previousDelayTicks.GetValueOrDefault()) : null,
            ReadNonNegativeInt(reader, RetryTransientAttemptCountIndex, "The SQLite retry attempt count is invalid."),
            ReadRetryAuthenticationState(reader, RetryAuthenticationStateIndex),
            ReadNullableString(reader, RetryCredentialsVersionIndex));
    }

    /// <summary>Records a pre-send attempt barrier for a leased operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nextAttempt">The next attempt number.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The attempt barrier result.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own the operation or stored data is invalid.</exception>
    internal static AttemptBarrierResult TryBeginRemoteAttempt(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        DateTimeOffset nowUtc)
    {
        var ownership = ReadLeasedOperationState(connection, transaction, storeIdentity, leaseId, operationId);
        if (ownership.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce && ownership.Attempt > 0)
        {
            return new(operationId, nextAttempt, MaySend: false, "OC.AtMostOnceAttempted");
        }

        if (IsTerminal(ownership.State))
        {
            return new(operationId, nextAttempt, MaySend: false, "OC.OperationTerminal");
        }

        if (ownership.State == SyncOperationState.Ambiguous && ownership.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce)
        {
            return new(operationId, nextAttempt, MaySend: false, "OC.AtMostOnceAmbiguous");
        }

        if (nextAttempt <= ownership.Attempt)
        {
            return new(operationId, nextAttempt, MaySend: false, "OC.AttemptNotAdvanced");
        }

        var state = GetAttemptState(ownership.DeliveryGuarantee);
        var reasonCode = state == SyncOperationState.Ambiguous ? "OC.AttemptAmbiguous" : null;
        UpsertOperationState(
            connection,
            transaction,
            storeIdentity,
            new(operationId, state, nextAttempt, nowUtc, reasonCode));
        return new(operationId, nextAttempt, MaySend: true, null);
    }

    /// <summary>Applies a validated remote sync result to operation state rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="result">The result.</param>
    /// <param name="changedAtUtc">The state change timestamp.</param>
    /// <exception cref="InvalidOperationException">The operation result cannot be applied.</exception>
    internal static void ApplySyncResult(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        RemoteSyncResult result,
        DateTimeOffset changedAtUtc)
    {
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var operation = result.Operations[index];
            var nextState = operation.Kind switch
            {
                OperationResultKind.Accepted => SyncOperationState.Synchronized,
                OperationResultKind.Conflict => SyncOperationState.Conflict,
                OperationResultKind.Rejected => SyncOperationState.Rejected,
                OperationResultKind.Retryable => SyncOperationState.QueuedForUpload,
                _ => throw new InvalidOperationException(InvalidOperationStateMessage),
            };
            UpdateOperationState(connection, transaction, storeIdentity, operation.OperationId, nextState, changedAtUtc, operation.ReasonCode);
        }
    }

    /// <summary>Moves one operation to the dead-letter state while preserving its durable attempt count.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="changedAtUtc">The state change timestamp.</param>
    /// <exception cref="InvalidOperationException">The operation is already terminal or missing.</exception>
    internal static void DeadLetterOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId,
        string reasonCode,
        DateTimeOffset changedAtUtc)
    {
        var current = ReadOperationRetryTarget(connection, transaction, storeIdentity, operationId);
        if (IsTerminal(current.State))
        {
            throw new InvalidOperationException("The SQLite operation state is terminal.");
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $operationState,
                changed_at_utc = $changedAtUtc,
                reason_code = $reasonCode,
                retry_started_utc = NULL,
                retry_due_utc = NULL,
                retry_previous_delay_ticks = NULL,
                retry_transient_attempt_count = NULL,
                retry_authentication_state = NULL,
                retry_credentials_version = NULL
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        AddStatusParameters(command, storeIdentity, operationId, SyncOperationState.DeadLettered, changedAtUtc, reasonCode);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingOperationStateMessage);
    }

    /// <summary>Saves retry state for an operation and returns it to queued eligibility.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="retryState">The retry state.</param>
    /// <param name="changedAtUtc">The status change timestamp.</param>
    /// <exception cref="InvalidOperationException">The operation cannot accept retry state.</exception>
    internal static void SaveRetryState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId,
        RetryState retryState,
        DateTimeOffset changedAtUtc)
    {
        var current = ReadOperationRetryTarget(connection, transaction, storeIdentity, operationId);
        if (IsTerminal(current.State) || current.State == SyncOperationState.Conflict)
        {
            throw new InvalidOperationException("The SQLite operation state is terminal.");
        }

        if (current.State == SyncOperationState.Ambiguous && current.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce)
        {
            throw new InvalidOperationException("At-most-once operations cannot be retried after an ambiguous attempt.");
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $operationState,
                changed_at_utc = $changedAtUtc,
                retry_started_utc = $retryStartedUtc,
                retry_due_utc = $retryDueUtc,
                retry_previous_delay_ticks = $retryPreviousDelayTicks,
                retry_transient_attempt_count = $retryTransientAttemptCount,
                retry_authentication_state = $retryAuthenticationState,
                retry_credentials_version = $retryCredentialsVersion
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(OperationStateParameter, (int)SyncOperationState.QueuedForUpload);
        _ = command.Parameters.AddWithValue(ChangedAtUtcParameter, FormatDateTimeOffset(changedAtUtc));
        _ = command.Parameters.AddWithValue("$retryStartedUtc", FormatDateTimeOffset(retryState.StartedUtc));
        _ = command.Parameters.AddWithValue("$retryDueUtc", (object?)FormatNullableDateTimeOffset(retryState.DueUtc) ?? DBNull.Value);
        _ = command.Parameters.AddWithValue(
            "$retryPreviousDelayTicks",
            retryState.PreviousDelay.HasValue ? retryState.PreviousDelay.GetValueOrDefault().Ticks : DBNull.Value);
        _ = command.Parameters.AddWithValue("$retryTransientAttemptCount", retryState.TransientAttemptCount);
        _ = command.Parameters.AddWithValue("$retryAuthenticationState", (int)retryState.AuthenticationState);
        _ = command.Parameters.AddWithValue("$retryCredentialsVersion", (object?)retryState.CredentialsVersion ?? DBNull.Value);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingOperationStateMessage);
    }

    /// <summary>Gets the durable state recorded when an attempt starts.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <returns>The attempt state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperationState GetAttemptState(DeliveryGuarantee deliveryGuarantee) =>
        deliveryGuarantee == DeliveryGuarantee.AtMostOnce ? SyncOperationState.Ambiguous : SyncOperationState.Uploading;

    /// <summary>Updates one operation state while preserving its current attempt count.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The new state.</param>
    /// <param name="changedAtUtc">The change timestamp.</param>
    /// <param name="reasonCode">The optional reason code.</param>
    /// <exception cref="InvalidOperationException">The operation state cannot be updated.</exception>
    private static void UpdateOperationState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId,
        SyncOperationState state,
        DateTimeOffset changedAtUtc,
        string? reasonCode)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $operationState,
                changed_at_utc = $changedAtUtc,
                reason_code = $reasonCode
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        AddStatusParameters(command, storeIdentity, operationId, state, changedAtUtc, reasonCode);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException(MissingOperationStateMessage);
    }

    /// <summary>Upserts one operation state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="state">The operation state.</param>
    /// <exception cref="InvalidOperationException">The operation state cannot be inserted.</exception>
    private static void UpsertOperationState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        in OperationStateWrite state)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_outbox_operation_states
                (store_identity, operation_id, operation_state, attempt_count, changed_at_utc, reason_code)
            VALUES
                ($storeIdentity, $operationId, $operationState, $attemptCount, $changedAtUtc, $reasonCode)
            ON CONFLICT (store_identity, operation_id) DO UPDATE SET
                operation_state = excluded.operation_state,
                attempt_count = excluded.attempt_count,
                changed_at_utc = excluded.changed_at_utc,
                reason_code = excluded.reason_code;
            """;
        AddStatusParameters(command, storeIdentity, state.OperationId, state.State, state.ChangedAtUtc, state.ReasonCode);
        _ = command.Parameters.AddWithValue(AttemptCountParameter, state.Attempt);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite operation state was not persisted.");
    }

    /// <summary>Adds common status parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The operation state.</param>
    /// <param name="changedAtUtc">The changed-at timestamp.</param>
    /// <param name="reasonCode">The optional reason code.</param>
    private static void AddStatusParameters(
        SqliteCommand command,
        string storeIdentity,
        OperationId operationId,
        SyncOperationState state,
        DateTimeOffset changedAtUtc,
        string? reasonCode)
    {
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationStateParameter, (int)state);
        _ = command.Parameters.AddWithValue(ChangedAtUtcParameter, FormatDateTimeOffset(changedAtUtc));
        _ = command.Parameters.AddWithValue(ReasonCodeParameter, (object?)reasonCode ?? DBNull.Value);
    }

    /// <summary>Reads one leased operation state for barrier validation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The leased operation state.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own the operation or stored data is invalid.</exception>
    private static OperationStateTarget ReadLeasedOperationState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT state.operation_state, state.attempt_count, outbox.policy_delivery_guarantee
            FROM oc_outbox_operation_states AS state
            INNER JOIN oc_outbox AS outbox
                ON outbox.store_identity = state.store_identity
                AND outbox.operation_id = state.operation_id
            INNER JOIN oc_outbox_leases AS lease
                ON lease.store_identity = state.store_identity
                AND lease.operation_id = state.operation_id
            WHERE state.store_identity = $storeIdentity
                AND state.operation_id = $operationId
                AND lease.lease_id = $leaseId;
            """;
        AddLeaseParameters(command, storeIdentity, leaseId);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException("The SQLite outbox lease does not own the operation.");
        }

        return new(
            ReadOperationState(reader, OperationTargetStateIndex),
            ReadNonNegativeInt(reader, OperationTargetAttemptIndex, InvalidAttemptCountMessage),
            ReadDeliveryGuarantee(reader, OperationTargetDeliveryGuaranteeIndex));
    }

    /// <summary>Reads one operation state for retry validation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The operation state.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static OperationStateTarget ReadOperationRetryTarget(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT state.operation_state, state.attempt_count, outbox.policy_delivery_guarantee
            FROM oc_outbox_operation_states AS state
            INNER JOIN oc_outbox AS outbox
                ON outbox.store_identity = state.store_identity
                AND outbox.operation_id = state.operation_id
            WHERE state.store_identity = $storeIdentity AND state.operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException(MissingOperationStateMessage);
        }

        return new(
            ReadOperationState(reader, OperationTargetStateIndex),
            ReadNonNegativeInt(reader, OperationTargetAttemptIndex, InvalidAttemptCountMessage),
            ReadDeliveryGuarantee(reader, OperationTargetDeliveryGuaranteeIndex));
    }

    /// <summary>Reads an operation state enum.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation state.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static SyncOperationState ReadOperationState(SqliteDataReader reader, int index)
    {
        var state = (SyncOperationState)ReadInt(reader, index, InvalidOperationStateMessage);
        return IsDefined(state) ? state : throw new InvalidOperationException(InvalidOperationStateMessage);
    }

    /// <summary>Reads a delivery guarantee enum.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The delivery guarantee.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static DeliveryGuarantee ReadDeliveryGuarantee(SqliteDataReader reader, int index)
    {
        var guarantee = (DeliveryGuarantee)ReadInt(reader, index, "The SQLite delivery guarantee is invalid.");
        return guarantee is DeliveryGuarantee.AtMostOnce or DeliveryGuarantee.AtLeastOnce or DeliveryGuarantee.ExactlyOnce
            ? guarantee
            : throw new InvalidOperationException("The SQLite delivery guarantee is invalid.");
    }

    /// <summary>Reads a retry authentication state enum.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The retry authentication state.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static RetryAuthenticationState ReadRetryAuthenticationState(SqliteDataReader reader, int index)
    {
        var state = (RetryAuthenticationState)ReadInt(reader, index, "The SQLite retry authentication state is invalid.");
        return state is RetryAuthenticationState.None or RetryAuthenticationState.RenewalRetryUsed
            ? state
            : throw new InvalidOperationException("The SQLite retry authentication state is invalid.");
    }

    /// <summary>Reads a nullable timestamp.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The timestamp.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static DateTimeOffset? ReadNullableDateTimeOffset(SqliteDataReader reader, int index, string message) =>
        reader.IsDBNull(index) ? null : ReadDateTimeOffset(reader, index, message);

    /// <summary>Reads a nullable long.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static long? ReadNullableLong(SqliteDataReader reader, int index, string message)
    {
        if (reader.IsDBNull(index))
        {
            return null;
        }

        var value = reader.GetInt64(index);
        return value >= 0 ? value : throw new InvalidOperationException(message);
    }

    /// <summary>Reads a non-negative integer.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static int ReadNonNegativeInt(SqliteDataReader reader, int index, string message)
    {
        var value = ReadInt(reader, index, message);
        return value >= 0 ? value : throw new InvalidOperationException(message);
    }

    /// <summary>Reads an optional reason code.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The reason code.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static string? ReadReasonCode(SqliteDataReader reader, int index)
    {
        var reason = ReadNullableString(reader, index);
        return reason is null || reason.Length > 0
            ? reason
            : throw new InvalidOperationException("The SQLite operation reason code is invalid.");
    }

    /// <summary>Formats a nullable date-time offset.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The formatted value.</returns>
    private static string? FormatNullableDateTimeOffset(DateTimeOffset? value) =>
        value.HasValue ? FormatDateTimeOffset(value.GetValueOrDefault()) : null;

    /// <summary>Determines whether an operation state is a terminal upload state.</summary>
    /// <param name="state">The state.</param>
    /// <returns>Whether the state is terminal.</returns>
    private static bool IsTerminal(SyncOperationState state) =>
        state is SyncOperationState.Conflict
            or SyncOperationState.Synchronized
            or SyncOperationState.Rejected
            or SyncOperationState.DeadLettered
            or SyncOperationState.GuaranteeExpired;

    /// <summary>Determines whether the operation state enum value is defined.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the value is defined.</returns>
    private static bool IsDefined(SyncOperationState state) =>
        state is SyncOperationState.SavedLocally
            or SyncOperationState.QueuedForUpload
            or SyncOperationState.Uploading
            or SyncOperationState.Conflict
            or SyncOperationState.Synchronized
            or SyncOperationState.Rejected
            or SyncOperationState.DeadLettered
            or SyncOperationState.Ambiguous
            or SyncOperationState.GuaranteeExpired;

    /// <summary>One operation state with the persisted delivery guarantee.</summary>
    /// <param name="State">The operation state.</param>
    /// <param name="Attempt">The attempt count.</param>
    /// <param name="DeliveryGuarantee">The delivery guarantee.</param>
    private readonly record struct OperationStateTarget(
        SyncOperationState State,
        int Attempt,
        DeliveryGuarantee DeliveryGuarantee);

    /// <summary>One operation state write.</summary>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="State">The operation state.</param>
    /// <param name="Attempt">The attempt count.</param>
    /// <param name="ChangedAtUtc">The changed-at timestamp.</param>
    /// <param name="ReasonCode">The optional reason code.</param>
    private readonly record struct OperationStateWrite(
        OperationId OperationId,
        SyncOperationState State,
        int Attempt,
        DateTimeOffset ChangedAtUtc,
        string? ReasonCode);
}
