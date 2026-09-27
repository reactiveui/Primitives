// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Opens record-protecting connections and rotates record protection keys.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Re-encrypts every protected value that is not under the provider's current key.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of values re-encrypted.</returns>
    /// <exception cref="InvalidOperationException">The store does not protect records, is not initialized, or the key check fails.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal long RotateEncryptionKey(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            var protection = _protection
                ?? throw new InvalidOperationException("The SQLite local store does not encrypt records at rest.");
            var storeIdentity = GetInitializedStoreIdentity();
            using var connection = OpenStoreConnection(storeIdentity);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var rewritten = SqliteRecordProtectionMaintenance.RotateKeys(connection, transaction, protection, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            CommitAtCheckpoints(transaction, SqliteCommitCheckpoint.KeyRotationBeforeCommit, SqliteCommitCheckpoint.KeyRotationAfterCommit);
            SqliteRecordProtectionMaintenance.TruncateWriteAheadLog(connection);
            return rewritten;
        }
    }

    /// <summary>Opens a connection that carries the record cipher for a store identity when records are protected.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SqliteConnection OpenStoreConnection(string storeIdentity) =>
        SqliteLocalCommitConnection.OpenConnection(
            _databasePath,
            _protection is null ? null : new SqliteRecordCipher(_protection, storeIdentity));
}
