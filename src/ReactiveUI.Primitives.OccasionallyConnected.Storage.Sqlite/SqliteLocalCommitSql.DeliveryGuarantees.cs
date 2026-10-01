// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Owns exactly-once guarantee expiry and downgrade SQL.</summary>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The parameter carrying the durable downgrade marker.</summary>
    private const string DowngradedReasonCodeParameter = "$downgradedReasonCode";

    /// <summary>Moves a leased exactly-once operation to the guarantee-expired blocking state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="changedAtUtc">The state change timestamp.</param>
    /// <returns>The durable status after the transition.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own an eligible exactly-once operation.</exception>
    internal static SyncOperationStatus ExpireDeliveryGuarantee(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId,
        DateTimeOffset changedAtUtc)
    {
        ValidateGuaranteeTransition(ReadLeasedOperationState(connection, transaction, storeIdentity, leaseId, operationId));
        UpdateOperationState(
            connection,
            transaction,
            storeIdentity,
            operationId,
            SyncOperationState.GuaranteeExpired,
            changedAtUtc,
            SyncReasonCodes.GuaranteeExpired);
        return ReadOperationStatus(connection, transaction, storeIdentity, operationId)
            ?? throw new InvalidOperationException(MissingOperationStateMessage);
    }

    /// <summary>Records an explicit at-least-once downgrade and a fresh retry anchor for a leased exactly-once operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="retryState">The fresh at-least-once retry anchor.</param>
    /// <param name="changedAtUtc">The state change timestamp.</param>
    /// <returns>The durable status after the transition.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own an eligible exactly-once operation.</exception>
    internal static SyncOperationStatus DowngradeDeliveryGuarantee(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        Guid leaseId,
        OperationId operationId,
        RetryState retryState,
        DateTimeOffset changedAtUtc)
    {
        ValidateGuaranteeTransition(ReadLeasedOperationState(connection, transaction, storeIdentity, leaseId, operationId));
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            UPDATE oc_outbox_operation_states
            SET changed_at_utc = $changedAtUtc,
                reason_code = $reasonCode,
                retry_started_utc = $retryStartedUtc,
                retry_due_utc = $retryDueUtc,
                retry_previous_delay_ticks = $retryPreviousDelayTicks,
                retry_transient_attempt_count = $retryTransientAttemptCount,
                retry_authentication_state = $retryAuthenticationState,
                retry_credentials_version = $retryCredentialsVersion
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """);
        _ = command.Bind(ChangedAtUtcParameter, FormatDateTimeOffset(changedAtUtc));
        _ = command.Bind(ReasonCodeParameter, SyncReasonCodes.GuaranteeDowngraded);
        AddRetryStateParameters(command, retryState);
        _ = command.Bind(StoreIdentityParameter, storeIdentity);
        _ = command.Bind(OperationIdParameter, operationId.Value.ToString("D"));
        if (command.Execute() != 1)
        {
            throw new InvalidOperationException(MissingOperationStateMessage);
        }

        return ReadOperationStatus(connection, transaction, storeIdentity, operationId)
            ?? throw new InvalidOperationException(MissingOperationStateMessage);
    }

    /// <summary>Adds the parameter used to keep a durable downgrade marker across in-flight state changes.</summary>
    /// <param name="command">The command.</param>
    private static void AddDowngradedReasonCodeParameter(SqliteStatement command) =>
        _ = command.Bind(DowngradedReasonCodeParameter, SyncReasonCodes.GuaranteeDowngraded);

    /// <summary>Adds the persisted retry-state parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="retryState">The retry state.</param>
    private static void AddRetryStateParameters(SqliteStatement command, RetryState retryState)
    {
        _ = command.Bind("$retryStartedUtc", FormatDateTimeOffset(retryState.StartedUtc));
        _ = command.Bind("$retryDueUtc", (object?)FormatNullableDateTimeOffset(retryState.DueUtc) ?? DBNull.Value);
        _ = command.Bind(
            "$retryPreviousDelayTicks",
            retryState.PreviousDelay.HasValue ? retryState.PreviousDelay.GetValueOrDefault().Ticks : DBNull.Value);
        _ = command.Bind("$retryTransientAttemptCount", retryState.TransientAttemptCount);
        _ = command.Bind("$retryAuthenticationState", (int)retryState.AuthenticationState);
        _ = command.Bind("$retryCredentialsVersion", (object?)retryState.CredentialsVersion ?? DBNull.Value);
    }

    /// <summary>Validates that a leased operation may change its exactly-once guarantee.</summary>
    /// <param name="ownership">The leased operation state.</param>
    /// <exception cref="InvalidOperationException">The operation is not an in-flight exactly-once operation.</exception>
    private static void ValidateGuaranteeTransition(OperationStateTarget ownership)
    {
        if (ownership.DeliveryGuarantee != DeliveryGuarantee.ExactlyOnce)
        {
            throw new InvalidOperationException("Only exactly-once operations can expire or downgrade their delivery guarantee.");
        }

        if (IsTerminal(ownership.State) || ownership.State == SyncOperationState.Ambiguous)
        {
            throw new InvalidOperationException("The SQLite operation state is terminal.");
        }
    }
}
