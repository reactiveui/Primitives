// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Opens SQLite connections and starts bounded writer transactions.</summary>
internal static class SqliteLocalCommitConnection
{
    /// <summary>The SQLite busy error code.</summary>
    private const int SqliteBusy = 5;

    /// <summary>The SQLite locked error code.</summary>
    private const int SqliteLocked = 6;

    /// <summary>The maximum duration of one writer lock attempt in milliseconds.</summary>
    private const int WriterAttemptTimeoutMilliseconds = 1000;

    /// <summary>The retry delay used while waiting for a writer lock.</summary>
    private static readonly TimeSpan WriterRetryDelay = TimeSpan.FromMilliseconds(10);

    /// <summary>The maximum time spent waiting for a writer lock.</summary>
    private static readonly TimeSpan WriterTotalTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Validates existing ownership before durability settings are persisted.</summary>
    /// <param name="connection">The connection.</param>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    internal static void ValidateOwnershipBeforeDurability(SqliteDatabase connection)
    {
        using var transaction = connection.BeginTransaction(deferred: true);
        var userVersion = GetUserVersion(connection, transaction);
        if (userVersion == 0 && !HasUserTables(connection, transaction))
        {
            transaction.Commit();
            return;
        }

        SqliteStoreSchema.ValidateExistingSchemaForLocalCommit(connection, transaction, userVersion);
        transaction.Commit();
    }

    /// <summary>Returns whether user tables exist.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether user tables exist.</returns>
    internal static bool HasUserTables(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';");
        return SqliteIdentityStoreData.ReadHasUserTables(command.Scalar());
    }

    /// <summary>Gets the SQLite schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    internal static long GetUserVersion(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA user_version;");
        return SqliteIdentityStoreData.ReadUserVersion(command.Scalar());
    }

    /// <summary>Opens a SQLite connection with pooling disabled.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static SqliteDatabase OpenConnection(string databasePath) => OpenConnection(databasePath, cipher: null);

    /// <summary>Opens a SQLite connection with pooling disabled that carries an optional record cipher.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="cipher">The record cipher, or null for a plaintext store.</param>
    /// <param name="cancellationToken">The token used to interrupt native operations.</param>
    /// <returns>The open connection.</returns>
    internal static SqliteDatabase OpenConnection(
        string databasePath,
        SqliteRecordCipher? cipher,
        CancellationToken cancellationToken = default) =>
        new(databasePath, cancellationToken: cancellationToken) { Context = cipher is null ? null : new SqliteRecordConnectionState(cipher) };

    /// <summary>Configures short SQLite waits so cancellation can be observed while waiting for writers.</summary>
    /// <param name="connection">The connection.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ConfigureLockPolling(SqliteDatabase connection) => connection.SetBusyTimeout(WriterAttemptTimeoutMilliseconds);

    /// <summary>Applies verified durability settings after ownership validation, retrying only lock contention.</summary>
    /// <param name="connection">The validated SQLite connection.</param>
    /// <param name="cancellationToken">The token used to cancel lock waits.</param>
    /// <exception cref="OperationCanceledException">The durability wait is canceled.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the durability settings for a reason other than lock contention.</exception>
    /// <exception cref="TimeoutException">A writer holds the database past the bounded retry period.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ConfigureDurabilityAfterOwnershipValidation(SqliteDatabase connection, CancellationToken cancellationToken) =>
        RetryWhileBusyOrLocked(() => SqliteConnectionSettings.ConfigureDurability(connection), cancellationToken);

    /// <summary>Begins a write transaction, observing cancellation between lock attempts.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transaction.</returns>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    /// <exception cref="OperationCanceledException">The writer wait is canceled.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the write transaction.</exception>
    /// <exception cref="TimeoutException">The SQLite writer lock is held past the bounded wait.</exception>
    internal static SqliteTransaction BeginWriteTransaction(SqliteDatabase connection, CancellationToken cancellationToken)
    {
        connection.SetCancellation(cancellationToken);
        var startTimestamp = Stopwatch.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var transaction = connection.BeginTransaction();
                try
                {
                    if (connection.Context is SqliteRecordConnectionState { VerifyBeforeWrite: { } verifyBeforeWrite })
                    {
                        verifyBeforeWrite(connection, transaction);
                    }
                    else if (connection.Context is SqliteRecordConnectionState protectedConnection
                        && protectedConnection.VerifiedDataVersion is long verifiedVersion
                        && GetDataVersion(connection, transaction) != verifiedVersion)
                    {
                        SqliteOperationStateIntegrity.Verify(connection, transaction);
                        protectedConnection.VerifiedDataVersion = GetDataVersion(connection, transaction);
                    }

                    return transaction;
                }
                catch
                {
                    transaction.Dispose();
                    throw;
                }
            }
            catch (SqliteDatabaseException exception) when (IsBusyOrLocked(exception))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (GetElapsedSince(startTimestamp) >= WriterTotalTimeout)
                {
                    throw new TimeoutException("Timed out waiting for the SQLite writer lock.", exception);
                }

                _ = cancellationToken.WaitHandle.WaitOne(WriterRetryDelay);
            }
        }
    }

    /// <summary>Gets the connection-local version used to detect commits from other connections.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <returns>The connection-local data version.</returns>
    internal static long GetDataVersion(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA data_version;");
        return Convert.ToInt64(command.Scalar(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Gets elapsed time since a stopwatch timestamp.</summary>
    /// <param name="startTimestamp">The start timestamp.</param>
    /// <returns>The elapsed time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TimeSpan GetElapsedSince(long startTimestamp)
    {
#if NET8_0_OR_GREATER
        return Stopwatch.GetElapsedTime(startTimestamp);
#else
        var elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
        return TimeSpan.FromSeconds((double)elapsedTicks / Stopwatch.Frequency);
#endif
    }

    /// <summary>Returns whether a SQLite exception indicates lock contention.</summary>
    /// <param name="exception">The exception.</param>
    /// <returns>Whether the exception is retryable lock contention.</returns>
    internal static bool IsBusyOrLocked(SqliteDatabaseException exception) => exception.SqliteErrorCode == SqliteBusy || exception.SqliteErrorCode == SqliteLocked;

    /// <summary>Runs an operation while observing cancellation between bounded SQLite lock retries.</summary>
    /// <param name="operation">The operation that can report SQLite lock contention.</param>
    /// <param name="cancellationToken">The token used to cancel lock waits.</param>
    /// <exception cref="OperationCanceledException">The operation wait is canceled.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the operation for a reason other than lock contention.</exception>
    /// <exception cref="TimeoutException">A writer holds the database past the bounded retry period.</exception>
    private static void RetryWhileBusyOrLocked(Action operation, CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                operation();
                return;
            }
            catch (SqliteDatabaseException exception) when (IsBusyOrLocked(exception))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (GetElapsedSince(startTimestamp) >= WriterTotalTimeout)
                {
                    throw new TimeoutException("Timed out waiting for the SQLite writer lock.", exception);
                }

                _ = cancellationToken.WaitHandle.WaitOne(WriterRetryDelay);
            }
        }
    }
}
