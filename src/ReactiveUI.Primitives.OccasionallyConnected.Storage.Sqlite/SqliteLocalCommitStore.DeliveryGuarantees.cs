// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists exactly-once guarantee expiry and downgrade decisions in SQLite.</summary>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Durably moves a leased exactly-once operation to the guarantee-expired state.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable status after the transition.</returns>
    /// <exception cref="ArgumentException">The lease or operation identifier is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store is not initialized, the lease is not current, or the operation is not eligible.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the operation.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal SyncOperationStatus ExpireDeliveryGuarantee(Guid leaseId, OperationId operationId, CancellationToken cancellationToken) =>
        ExecuteGuaranteeTransition(
            leaseId,
            operationId,
            (connection, transaction, storeIdentity, nowUtc) => SqliteLocalCommitSql.ExpireDeliveryGuarantee(
                connection,
                transaction,
                storeIdentity,
                leaseId,
                operationId,
                nowUtc),
            cancellationToken);

    /// <summary>Durably records an explicit at-least-once downgrade for a leased exactly-once operation.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="retryState">The fresh at-least-once retry anchor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable status after the transition.</returns>
    /// <exception cref="ArgumentException">The lease, operation identifier, or retry state is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store is not initialized, the lease is not current, or the operation is not eligible.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the operation.</exception>
    internal SyncOperationStatus DowngradeDeliveryGuarantee(
        Guid leaseId,
        OperationId operationId,
        RetryState retryState,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateRetryStateInput(operationId, retryState);
        return ExecuteGuaranteeTransition(
            leaseId,
            operationId,
            (connection, transaction, storeIdentity, nowUtc) => SqliteLocalCommitSql.DowngradeDeliveryGuarantee(
                connection,
                transaction,
                storeIdentity,
                leaseId,
                operationId,
                retryState,
                nowUtc),
            cancellationToken);
    }

    /// <summary>Runs one guarantee transition inside a write transaction owned by a current lease.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="transition">The SQL transition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable status after the transition.</returns>
    /// <exception cref="ArgumentException">The lease or operation identifier is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store is not initialized or the lease is expired.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    private SyncOperationStatus ExecuteGuaranteeTransition(
        Guid leaseId,
        OperationId operationId,
        Func<SqliteDatabase, SqliteTransaction, string, DateTimeOffset, SyncOperationStatus> transition,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateLeaseId(leaseId);
        SqliteLocalCommitValidation.ValidateOperationId(operationId, nameof(operationId));
        cancellationToken.ThrowIfCancellationRequested();
        var storeIdentity = GetInitializedStoreIdentityForOperation();
        return WithStoreConnection(
            storeIdentity,
            connection =>
            {
                SqliteLocalCommitConnection.ConfigureLockPolling(connection);
                SqliteConnectionSettings.ConfigureOperationalConnection(connection);
                using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
                var nowUtc = _timeProvider.GetUtcNow();
                var leaseExpiry = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
                SqliteLocalCommitSql.ThrowIfLeaseQuarantined(connection, transaction, storeIdentity, leaseId);
                SqliteLocalCommitSql.ThrowIfOperationStreamQuarantined(connection, transaction, storeIdentity, operationId);
                if (leaseExpiry <= nowUtc)
                {
                    throw new InvalidOperationException(ExpiredLeaseMessage);
                }

                var status = transition(connection, transaction, storeIdentity, nowUtc);
                cancellationToken.ThrowIfCancellationRequested();
                CommitWithOperationStateIntegrity(transaction);
                return status;
            },
            cancellationToken);
    }
}
