// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Releases an initialization connection unless a successful initialization transfers it to the store.</summary>
internal sealed class SqliteInitializationConnectionScope : IDisposable
{
    /// <summary>Whether the store adopted this connection.</summary>
    private bool _retained;

    /// <summary>Initializes a new instance of the <see cref="SqliteInitializationConnectionScope"/> class.</summary>
    /// <param name="connection">The validated initialization connection.</param>
    internal SqliteInitializationConnectionScope(SqliteDatabase connection) => Connection = connection;

    /// <summary>Gets the initialization connection.</summary>
    internal SqliteDatabase Connection { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_retained)
        {
            Connection.SetCancellation(CancellationToken.None);
        }
        else
        {
            Connection.Dispose();
        }
    }

    /// <summary>Transfers the connection lifetime to the store after a successful commit.</summary>
    /// <returns>The store-owned connection.</returns>
    internal SqliteDatabase Retain()
    {
        _retained = true;
        return Connection;
    }
}
