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
    /// <summary>Protects the observer connection and its trusted version.</summary>
    private readonly Lock _integrityGate = new();

    /// <summary>A connection that notices commits from every operational connection.</summary>
    private SqliteConnection? _integrityObserver;

    /// <summary>The observer version that was last fully authenticated or safely committed.</summary>
    private long? _trustedObserverVersion;

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
            using var connection = OpenStoreConnection(storeIdentity, forWrite: true);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var rewritten = SqliteRecordProtectionMaintenance.RotateKeys(connection, transaction, protection, cancellationToken);
            SqliteOperationStateIntegrity.Write(connection, transaction);
            ((SqliteProtectedConnection)connection).FullProofRewriteCompleted = true;
            cancellationToken.ThrowIfCancellationRequested();
            CommitAtCheckpoints(transaction, SqliteCommitCheckpoint.KeyRotationBeforeCommit, SqliteCommitCheckpoint.KeyRotationAfterCommit);
            SqliteRecordProtectionMaintenance.TruncateWriteAheadLog(connection);
            return rewritten;
        }
    }

    /// <summary>Commits state changes together with their authenticated proofs.</summary>
    /// <param name="transaction">The active transaction.</param>
    /// <exception cref="InvalidOperationException">The transaction has no connection.</exception>
    private static void CommitWithOperationStateIntegrity(SqliteTransaction transaction)
    {
        var connection = transaction.Connection ?? throw new InvalidOperationException("The SQLite transaction has no connection.");
        var protectedConnection = connection as SqliteProtectedConnection;
        var changed = protectedConnection is not null && HasChanges(connection, transaction);
        if (protectedConnection is not null
            && SqliteLocalCommitConnection.GetUserVersion(connection, transaction) == SqliteStoreSchema.LocalCommitSchemaVersion
            && changed
            && !protectedConnection.FullProofRewriteCompleted)
        {
            if (connection is SqliteProtectedConnection { JournalInstalled: true })
            {
                SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
            }
            else
            {
                SqliteOperationStateIntegrity.Write(connection, transaction);
            }
        }

        transaction.Commit();
        if (connection is SqliteProtectedConnection { ObserveAfterCommit: { } observeAfterCommit })
        {
            observeAfterCommit(changed);
        }
    }

    /// <summary>Reports whether the connection changed any rows during this operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether the connection made a change.</returns>
    private static bool HasChanges(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT total_changes();";
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) > 0;
    }

    /// <summary>Reads the observer's connection-local data version.</summary>
    /// <param name="connection">The observer connection.</param>
    /// <returns>The data version.</returns>
    private static long ReadObserverVersion(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA data_version;";
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Begins a read snapshot and authenticates operation states within it.</summary>
    /// <param name="connection">The connection.</param>
    /// <returns>The authenticated read transaction.</returns>
    private static SqliteTransaction BeginVerifiedReadTransaction(SqliteConnection connection)
    {
        var transaction = connection.BeginTransaction(System.Data.IsolationLevel.Serializable, deferred: true);
        try
        {
            SqliteOperationStateIntegrity.Verify(connection, transaction);
            return transaction;
        }
        catch
        {
            transaction.Dispose();
            throw;
        }
    }

    /// <summary>Opens a connection that carries the record cipher for a store identity when records are protected.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="forWrite">Whether the caller will acquire a writer transaction.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SqliteConnection OpenStoreConnection(string storeIdentity, bool forWrite = false)
    {
        var connection = SqliteLocalCommitConnection.OpenConnection(
            _databasePath,
            _protection is null ? null : new SqliteRecordCipher(_protection, storeIdentity));
        try
        {
            if (_storeIdentity is not null && _protection is not null && forWrite)
            {
                var protectedConnection = (SqliteProtectedConnection)connection;
                SqliteOperationStateIntegrity.InstallJournal(connection);
                protectedConnection.JournalInstalled = true;
                protectedConnection.VerifyBeforeWrite = VerifyProtectedWrite;
                protectedConnection.ObserveAfterCommit = ObserveProtectedCommit;
            }

            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Authenticates a writer snapshot when another connection changed the database.</summary>
    /// <param name="connection">The writer connection.</param>
    /// <param name="transaction">The locked write transaction.</param>
    private void VerifyProtectedWrite(SqliteConnection connection, SqliteTransaction transaction)
    {
        lock (_integrityGate)
        {
            _integrityObserver ??= SqliteLocalCommitConnection.OpenConnection(_databasePath);
            var observed = ReadObserverVersion(_integrityObserver);
            if (_trustedObserverVersion == observed)
            {
                return;
            }

            SqliteOperationStateIntegrity.Verify(connection, transaction);
            _trustedObserverVersion = observed;
        }
    }

    /// <summary>Advances the trusted observer only for the expected commit.</summary>
    /// <param name="changed">Whether this connection wrote any main database rows.</param>
    private void ObserveProtectedCommit(bool changed)
    {
        lock (_integrityGate)
        {
            if (_integrityObserver is null || _trustedObserverVersion is not long trusted)
            {
                return;
            }

            var observed = ReadObserverVersion(_integrityObserver);
            _trustedObserverVersion = observed == trusted + (changed ? 1 : 0) ? observed : null;
        }
    }

    /// <summary>Releases the observer when this store is disposed.</summary>
    private void DisposeIntegrityObserver()
    {
        lock (_integrityGate)
        {
            _integrityObserver?.Dispose();
            _integrityObserver = null;
            _trustedObserverVersion = null;
        }
    }
}
