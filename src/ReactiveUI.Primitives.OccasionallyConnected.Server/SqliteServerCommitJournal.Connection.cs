// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Serializes native connection ownership across independently admitted command workers.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Protects complete native transaction scopes and connection disposal.</summary>
    private readonly object _connectionGate = new();

    /// <summary>The initialized native connection and its bounded prepared statement cache.</summary>
    private readonly SqliteDatabase _connection;

    /// <summary>Gets the number of native statements prepared by the owned connection.</summary>
    internal long PreparationCount
    {
        get
        {
            lock (_connectionGate)
            {
                ThrowIfDisposed();
                return _connection.PreparationCount;
            }
        }
    }

    /// <summary>Gets whether disposal released the owned native connection.</summary>
    internal bool IsConnectionDisposed
    {
        get
        {
            lock (_connectionGate)
            {
                return _connection.IsDisposed;
            }
        }
    }
}
