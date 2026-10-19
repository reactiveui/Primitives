// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Owns a native transaction and rolls it back unless committed.</summary>
internal sealed class SqliteTransaction : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="SqliteTransaction"/> class.</summary>
    /// <param name="database">The database with the active transaction.</param>
    internal SqliteTransaction(SqliteDatabase database) => Connection = database;

    /// <summary>Gets the database while this transaction is active.</summary>
    internal SqliteDatabase? Connection { get; private set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Connection is null)
        {
            return;
        }

        Rollback();
    }

    /// <summary>Rolls back any active native transaction, including after cancellation or automatic rollback.</summary>
    /// <exception cref="InvalidOperationException">The transaction has already ended.</exception>
    internal void Rollback()
    {
        var database = Connection ?? throw new InvalidOperationException("The SQLite transaction has already ended.");
        database.SetCancellation(CancellationToken.None);
        if (raw.sqlite3_get_autocommit(database.Handle) == 0)
        {
            database.Execute("ROLLBACK;");
        }

        database.Transaction = null;
        Connection = null;
    }

    /// <summary>Commits the transaction and releases its ownership.</summary>
    /// <exception cref="InvalidOperationException">The transaction has already ended.</exception>
    internal void Commit()
    {
        var database = Connection ?? throw new InvalidOperationException("The SQLite transaction has already ended.");
        database.Execute("COMMIT;");
        database.Transaction = null;
        Connection = null;
    }
}
