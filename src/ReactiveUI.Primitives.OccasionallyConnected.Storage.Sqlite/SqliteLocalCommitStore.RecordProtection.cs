// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Opens record-protecting connections and rotates record protection keys.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>The brief gate wait before observing cancellation.</summary>
    private const int ConnectionGatePollMilliseconds = 10;

    /// <summary>Serializes native connection borrowers, including methods without the store gate.</summary>
    private readonly object _connectionGate = new();

    /// <summary>The operational connection owned under the connection gate.</summary>
    private SqliteDatabase? _operationalConnection;

    /// <summary>Re-encrypts every protected value that is not under the provider's current key.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of values re-encrypted.</returns>
    /// <exception cref="InvalidOperationException">The store does not protect records, is not initialized, or the key check fails.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the operation.</exception>
    internal long RotateEncryptionKey(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            var protection = _protection
                ?? throw new InvalidOperationException("The SQLite local store does not encrypt records at rest.");
            var storeIdentity = GetInitializedStoreIdentity();
            return WithStoreConnection(
                storeIdentity,
                connection =>
                {
                    SqliteLocalCommitConnection.ConfigureLockPolling(connection);
                    SqliteConnectionSettings.ConfigureOperationalConnection(connection);
                    using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
                    var rewritten = SqliteRecordProtectionMaintenance.RotateKeys(connection, transaction, protection, cancellationToken);
                    SqliteOutboxCapacityIntegrity.RefreshProofs(connection, transaction);
                    SqliteOperationStateIntegrity.Write(connection, transaction);
                    SqliteOperationStateIntegrity.Verify(connection, transaction);
                    ((SqliteRecordConnectionState)connection.Context!).FullProofRewriteCompleted = true;
                    cancellationToken.ThrowIfCancellationRequested();
                    CommitAtCheckpoints(transaction, SqliteCommitCheckpoint.KeyRotationBeforeCommit, SqliteCommitCheckpoint.KeyRotationAfterCommit);
                    SqliteRecordProtectionMaintenance.TruncateWriteAheadLog(connection);
                    return rewritten;
                },
                cancellationToken);
        }
    }

    /// <summary>Commits state changes together with their authenticated proofs.</summary>
    /// <param name="transaction">The active transaction.</param>
    /// <exception cref="InvalidOperationException">The transaction has no connection.</exception>
    private static void CommitWithOperationStateIntegrity(SqliteTransaction transaction)
    {
        var connection = transaction.Connection ?? throw new InvalidOperationException("The SQLite transaction has no connection.");
        var protectedConnection = connection.Context as SqliteRecordConnectionState;
        var changed = protectedConnection is not null && HasChanges(connection, transaction);
        if (protectedConnection is not null
            && SqliteLocalCommitConnection.GetUserVersion(connection, transaction) == SqliteStoreSchema.LocalCommitSchemaVersion
            && changed
            && !protectedConnection.FullProofRewriteCompleted)
        {
            if (connection.Context is SqliteRecordConnectionState { JournalInstalled: true })
            {
                SqliteOperationStateIntegrity.WriteChanges(connection, transaction);
            }
            else
            {
                SqliteOperationStateIntegrity.Write(connection, transaction);
            }
        }

        var trustedTotalChanges = protectedConnection is not null ? ReadTotalChanges(connection, transaction) : 0;
        transaction.Commit();
        connection.SetCancellation(CancellationToken.None);
        SqliteOutboxCapacityIntegrity.RecordTrustedCommit(connection);
        if (protectedConnection is not null)
        {
            protectedConnection.VerifiedTotalChanges = trustedTotalChanges;
        }
    }

    /// <summary>Reports whether the connection changed any rows during this operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether the connection made a change.</returns>
    private static bool HasChanges(SqliteDatabase connection, SqliteTransaction transaction) =>
        ReadTotalChanges(connection, transaction) > ((SqliteRecordConnectionState)connection.Context!).OperationStartChanges;

    /// <summary>Reads the connection's cumulative native change count, including its own commits.</summary>
    /// <param name="connection">The operational connection.</param>
    /// <param name="transaction">The active transaction, if any.</param>
    /// <returns>The cumulative change count.</returns>
    private static long ReadTotalChanges(SqliteDatabase connection, SqliteTransaction? transaction = null)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT total_changes();");
        return Convert.ToInt64(command.Scalar(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Begins a read snapshot and authenticates operation states within it.</summary>
    /// <param name="connection">The connection.</param>
    /// <returns>The authenticated read transaction.</returns>
    private static SqliteTransaction BeginVerifiedReadTransaction(SqliteDatabase connection)
    {
        var transaction = connection.BeginTransaction(deferred: true);
        try
        {
            VerifyProtectedWrite(connection, transaction);
            return transaction;
        }
        catch
        {
            transaction.Dispose();
            throw;
        }
    }

    /// <summary>Installs connection-local mutation tracking once the schema exists.</summary>
    /// <param name="connection">The store-owned connection.</param>
    private static void ConfigureProtectedOperationalConnection(SqliteDatabase connection)
    {
        if (connection.Context is not SqliteRecordConnectionState state || state.JournalInstalled)
        {
            return;
        }

        SqliteOperationStateIntegrity.InstallJournal(connection);
        state.JournalInstalled = true;
        state.VerifyBeforeWrite = VerifyProtectedWrite;
    }

    /// <summary>Authenticates a snapshot when another connection changed the database.</summary>
    /// <param name="connection">The operational connection.</param>
    /// <param name="transaction">The active transaction.</param>
    private static void VerifyProtectedWrite(SqliteDatabase connection, SqliteTransaction transaction)
    {
        if (connection.Context is not SqliteRecordConnectionState state)
        {
            return;
        }

        var version = SqliteLocalCommitConnection.GetDataVersion(connection, transaction);
        var changes = ReadTotalChanges(connection, transaction);
        if (state.VerifiedDataVersion == version && state.VerifiedTotalChanges == changes)
        {
            return;
        }

        SqliteOperationStateIntegrity.Verify(connection, transaction);
        SqliteOutboxCapacityIntegrity.VerifyWhenChanged(connection, transaction);
        state.VerifiedDataVersion = version;
        state.VerifiedTotalChanges = changes;
    }

    /// <summary>Opens a connection that carries the record cipher for a store identity when records are protected.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="cancellationToken">The token used to interrupt native operations.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SqliteDatabase OpenStoreConnection(string storeIdentity, CancellationToken cancellationToken)
    {
        var connection = SqliteLocalCommitConnection.OpenConnection(
            _databasePath,
            _protection is null ? null : new SqliteRecordCipher(_protection, storeIdentity),
            cancellationToken);
        try
        {
            if (_storeIdentity is not null && _protection is not null)
            {
                ConfigureProtectedOperationalConnection(connection);
            }

            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Runs a synchronous borrower while retaining lexical gate ownership through cleanup.</summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="operation">The synchronous connection borrower.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The operation result.</returns>
    private TResult WithStoreConnection<TResult>(
        string storeIdentity,
        Func<SqliteDatabase, TResult> operation,
        CancellationToken cancellationToken)
    {
        var gateTaken = false;
        try
        {
            while (!gateTaken)
            {
                Monitor.TryEnter(_connectionGate, ConnectionGatePollMilliseconds, ref gateTaken);
                cancellationToken.ThrowIfCancellationRequested();
            }

            using var scope = PrepareStoreConnection(storeIdentity, cancellationToken);
            return operation(scope.Connection);
        }
        finally
        {
            if (gateTaken)
            {
                Monitor.Exit(_connectionGate);
            }
        }
    }

    /// <summary>Runs a synchronous borrower that does not return a value.</summary>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="operation">The synchronous connection borrower.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WithStoreConnection(string storeIdentity, Action<SqliteDatabase> operation, CancellationToken cancellationToken) =>
        WithStoreConnection(
            storeIdentity,
            connection =>
            {
                operation(connection);
                return 0;
            },
            cancellationToken);

    /// <summary>Prepares the gate-owned connection and installs fresh operation state.</summary>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The scope that clears cancellation after rollback or commit.</returns>
    /// <exception cref="InvalidOperationException">The connection already has an active operation.</exception>
    private SqliteStoreConnectionScope PrepareStoreConnection(string storeIdentity, CancellationToken cancellationToken)
    {
        SqliteStoreConnectionScope? scope = null;
        try
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            _operationalConnection ??= OpenStoreConnection(storeIdentity, CancellationToken.None);
            var connection = _operationalConnection;
            if (connection.Transaction is not null)
            {
                throw new InvalidOperationException("The SQLite store connection already has an active operation.");
            }

            scope = new(connection, RetireOperationalConnection);
            connection.SetCancellation(cancellationToken);
            if (connection.Context is SqliteRecordConnectionState state)
            {
                if (state.VerifiedTotalChanges != ReadTotalChanges(connection))
                {
                    state.VerifiedDataVersion = null;
                }

                state.FullProofRewriteCompleted = false;
                if (state.JournalInstalled)
                {
                    connection.Execute("DELETE FROM temp.oc_state_journal;");
                }

                state.OperationStartChanges = ReadTotalChanges(connection);
                state.VerifiedTotalChanges = state.OperationStartChanges;
            }

            return scope;
        }
        catch
        {
            scope?.Dispose();

            throw;
        }
    }

    /// <summary>Removes a connection whose operation cleanup failed so it cannot be borrowed again.</summary>
    /// <param name="connection">The failed native connection.</param>
    private void RetireOperationalConnection(SqliteDatabase connection)
    {
        _operationalConnection = null;
        connection.Dispose();
    }
}
