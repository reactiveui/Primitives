// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
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
            using var lease = AcquireConnection();
            return _connection.PreparationCount;
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

    /// <summary>Acquires sole native ownership until statements and transactions have been released.</summary>
    /// <returns>The synchronous connection ownership lease.</returns>
    private ConnectionLease AcquireConnection()
    {
        Monitor.Enter(_connectionGate);
        try
        {
            ThrowIfDisposed();
            return new(_connectionGate);
        }
        catch
        {
            Monitor.Exit(_connectionGate);
            throw;
        }
    }

    /// <summary>Releases synchronous connection ownership after a complete journal operation.</summary>
    private readonly struct ConnectionLease : IDisposable
    {
        /// <summary>The journal's native ownership gate.</summary>
        private readonly object _gate;

        /// <summary>Initializes a new instance of the <see cref="ConnectionLease"/> struct.</summary>
        /// <param name="gate">The acquired ownership gate.</param>
        internal ConnectionLease(object gate) => _gate = gate;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Monitor.Exit(_gate);
    }
}
