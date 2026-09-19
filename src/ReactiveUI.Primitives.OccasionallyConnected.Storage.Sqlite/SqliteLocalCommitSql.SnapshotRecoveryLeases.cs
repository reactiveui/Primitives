// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes SQLite statements for snapshot recovery lease checks.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Reads expired lease operation identifiers after rejecting active recovery ownership.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="pending">The pending operations.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <returns>The expired leased operation identifiers.</returns>
    /// <exception cref="InvalidOperationException">A pending operation is actively leased or its lease metadata is invalid.</exception>
    internal static HashSet<OperationId> ReadSnapshotRecoveryExpiredLeaseOperationIds(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        List<SqliteSnapshotRecoveryPendingOperation> pending,
        DateTimeOffset nowUtc)
    {
        HashSet<OperationId> expired = [];
        for (var index = 0; index < pending.Count; index++)
        {
            var operationId = pending[index].OperationId;
            var leaseExpiresAtUtc = ReadSnapshotRecoveryLeaseExpiry(connection, transaction, storeIdentity, operationId);
            if (!leaseExpiresAtUtc.HasValue)
            {
                continue;
            }

            if (leaseExpiresAtUtc.GetValueOrDefault() > nowUtc)
            {
                throw new InvalidOperationException("A snapshot recovery operation is still owned by an active lease.");
            }

            _ = expired.Add(operationId);
        }

        return expired;
    }

    /// <summary>Releases any lease row for one operation during snapshot recovery.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    internal static void ReleaseSnapshotRecoveryLeaseOperation(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads the lease expiry for an operation when the operation is leased.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The lease expiry, or <see langword="null" /> when the operation is not leased.</returns>
    /// <exception cref="InvalidOperationException">The durable lease expiry is invalid or inconsistent.</exception>
    private static DateTimeOffset? ReadSnapshotRecoveryLeaseExpiry(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        OperationId operationId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT lease_expires_at_utc
            FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        using var reader = command.ExecuteReader();
        return reader.Read()
            ? ReadDateTimeOffset(reader, 0, "The SQLite outbox lease expiry is invalid.")
            : null;
    }
}
