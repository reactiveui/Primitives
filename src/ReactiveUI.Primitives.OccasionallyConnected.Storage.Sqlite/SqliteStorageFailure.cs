// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Maps SQLite storage-medium errors to the typed, non-transient <see cref="DurableStorageException"/>.</summary>
/// <remarks>
/// SQLite rolls back the statement or transaction that hits <c>SQLITE_FULL</c> or <c>SQLITE_IOERR</c>, and the store
/// disposes its transaction without committing, so the mapped failure leaves no partial state.
/// </remarks>
internal static class SqliteStorageFailure
{
    /// <summary>The SQLite primary result code for an operating system I/O error.</summary>
    internal const int SqliteIoError = 10;

    /// <summary>The SQLite primary result code for a full disk or a reached <c>max_page_count</c>.</summary>
    internal const int SqliteFull = 13;

    /// <summary>The maximum exception chain depth inspected for a SQLite storage error.</summary>
    private const int MaximumDepth = 32;

    /// <summary>Runs a store command and maps a SQLite storage-medium error to <see cref="DurableStorageException"/>.</summary>
    /// <typeparam name="T">The command result type.</typeparam>
    /// <param name="command">The synchronous store command.</param>
    /// <param name="cancellationToken">The cancellation token passed to the command.</param>
    /// <returns>The command result.</returns>
    /// <exception cref="DurableStorageException">SQLite reported a full disk or an I/O error.</exception>
    internal static T Run<T>(Func<CancellationToken, T> command, CancellationToken cancellationToken)
    {
        try
        {
            return command(cancellationToken);
        }
        catch (Exception exception) when (FindStorageFailure(exception, out _) is not null)
        {
            var sqliteException = FindStorageFailure(exception, out var failure)!;
            throw Create(failure, sqliteException);
        }
    }

    /// <summary>Finds a SQLite storage-medium error in an exception chain.</summary>
    /// <param name="exception">The observed exception.</param>
    /// <param name="failure">The mapped failure kind.</param>
    /// <returns>The SQLite exception that reported an unmapped storage-medium error, or null.</returns>
    internal static SqliteException? FindStorageFailure(Exception exception, out DurableStorageFailure failure)
    {
        failure = DurableStorageFailure.Unknown;
        if (DurableStorageException.IsInChain(exception))
        {
            return null;
        }

        var current = (Exception?)exception;
        for (var depth = 0; current is not null && depth < MaximumDepth; depth++)
        {
            if (current is SqliteException sqlite && TryClassify(sqlite.SqliteErrorCode, out failure))
            {
                return sqlite;
            }

            current = current.InnerException;
        }

        return null;
    }

    /// <summary>Maps a SQLite primary result code to a storage failure kind.</summary>
    /// <param name="errorCode">The SQLite result code.</param>
    /// <param name="failure">The mapped failure kind.</param>
    /// <returns><see langword="true"/> when the code reports a storage-medium failure.</returns>
    internal static bool TryClassify(int errorCode, out DurableStorageFailure failure)
    {
        switch (errorCode & 0xFF)
        {
            case SqliteFull:
            {
                failure = DurableStorageFailure.StorageFull;
                return true;
            }

            case SqliteIoError:
            {
                failure = DurableStorageFailure.InputOutput;
                return true;
            }

            default:
            {
                failure = DurableStorageFailure.Unknown;
                return false;
            }
        }
    }

    /// <summary>Creates the typed storage failure.</summary>
    /// <param name="failure">The failure kind.</param>
    /// <param name="sqliteException">The SQLite exception that reported the failure.</param>
    /// <returns>The typed exception.</returns>
    internal static DurableStorageException Create(DurableStorageFailure failure, SqliteException sqliteException) =>
        new(
            failure == DurableStorageFailure.StorageFull
                ? "The SQLite store is full. The write was rolled back; free disk space before retrying."
                : "The SQLite store reported an I/O error. The write was rolled back.",
            failure,
            sqliteException);
}
