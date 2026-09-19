// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes SQLite statements for snapshot recovery state changes.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Applies one snapshot recovery operation state and clears retry state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The operation state.</param>
    /// <param name="changedAtUtc">The change timestamp.</param>
    /// <param name="reasonCode">The optional terminal reason code.</param>
    /// <exception cref="InvalidOperationException">The operation state row is missing.</exception>
    internal static void ApplySnapshotRecoveryOperationState(
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
                reason_code = $reasonCode,
                retry_started_utc = NULL,
                retry_due_utc = NULL,
                retry_previous_delay_ticks = NULL,
                retry_transient_attempt_count = NULL,
                retry_authentication_state = NULL,
                retry_credentials_version = NULL
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue("$operationState", (int)state);
        _ = command.Parameters.AddWithValue("$changedAtUtc", FormatDateTimeOffset(changedAtUtc));
        _ = command.Parameters.AddWithValue("$reasonCode", (object?)reasonCode ?? DBNull.Value);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite operation state is missing.");
    }
}
