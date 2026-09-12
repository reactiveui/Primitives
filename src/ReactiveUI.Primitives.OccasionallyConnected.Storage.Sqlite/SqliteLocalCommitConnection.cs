// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Opens SQLite connections and starts bounded writer transactions.</summary>
internal static class SqliteLocalCommitConnection
{
    /// <summary>The SQLite busy error code.</summary>
    private const int SqliteBusy = 5;

    /// <summary>The SQLite locked error code.</summary>
    private const int SqliteLocked = 6;

    /// <summary>The retry delay used while waiting for a writer lock.</summary>
    private static readonly TimeSpan WriterRetryDelay = TimeSpan.FromMilliseconds(10);

    /// <summary>The maximum time spent waiting for a writer lock.</summary>
    private static readonly TimeSpan WriterTotalTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Validates existing ownership before durability settings are persisted.</summary>
    /// <param name="connection">The connection.</param>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    internal static void ValidateOwnershipBeforeDurability(SqliteConnection connection)
    {
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
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
    internal static bool HasUserTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';";
        return SqliteIdentityStoreData.ReadHasUserTables(command.ExecuteScalar());
    }

    /// <summary>Gets the SQLite schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    internal static long GetUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version;";
        return SqliteIdentityStoreData.ReadUserVersion(command.ExecuteScalar());
    }

    /// <summary>Opens a SQLite connection with pooling disabled.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    internal static SqliteConnection OpenConnection(string databasePath)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = databasePath, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Configures short SQLite waits so cancellation can be observed while waiting for writers.</summary>
    /// <param name="connection">The connection.</param>
    internal static void ConfigureLockPolling(SqliteConnection connection) => connection.DefaultTimeout = 1;

    /// <summary>Begins a write transaction, observing cancellation between lock attempts.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transaction.</returns>
    /// <exception cref="InvalidOperationException">The SQLite ownership or locking state is invalid.</exception>
    /// <exception cref="OperationCanceledException">The writer wait is canceled.</exception>
    /// <exception cref="SqliteException">SQLite rejects the write transaction.</exception>
    /// <exception cref="TimeoutException">The SQLite writer lock is held past the bounded wait.</exception>
    internal static SqliteTransaction BeginWriteTransaction(SqliteConnection connection, CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
            }
            catch (SqliteException exception) when (IsBusyOrLocked(exception))
            {
                if (GetElapsedSince(startTimestamp) >= WriterTotalTimeout)
                {
                    throw new TimeoutException("Timed out waiting for the SQLite writer lock.", exception);
                }

                _ = cancellationToken.WaitHandle.WaitOne(WriterRetryDelay);
            }
        }
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
    internal static bool IsBusyOrLocked(SqliteException exception) => exception.SqliteErrorCode == SqliteBusy || exception.SqliteErrorCode == SqliteLocked;
}
