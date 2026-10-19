// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Clears operation-local native state without releasing the store-owned connection.</summary>
internal sealed class SqliteStoreConnectionScope : IDisposable
{
    /// <summary>Retires the connection if native rollback or cancellation cleanup fails.</summary>
    private readonly Action<SqliteDatabase> _retire;

    /// <summary>Whether this operation has ended.</summary>
    private int _disposeStarted;

    /// <summary>Initializes a new instance of the <see cref="SqliteStoreConnectionScope"/> class.</summary>
    /// <param name="connection">The gate-owned connection.</param>
    /// <param name="retire">The failed connection cleanup.</param>
    internal SqliteStoreConnectionScope(SqliteDatabase connection, Action<SqliteDatabase> retire)
    {
        Connection = connection;
        _retire = retire;
    }

    /// <summary>Gets the connection borrowed for this operation.</summary>
    internal SqliteDatabase Connection { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
        {
            return;
        }

        try
        {
            Connection.Transaction?.Dispose();
            Connection.SetCancellation(CancellationToken.None);
        }
        catch
        {
            _retire(Connection);
            throw;
        }
    }
}
