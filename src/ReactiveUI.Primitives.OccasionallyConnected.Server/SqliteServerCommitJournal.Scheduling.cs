// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Schedules synchronous journal work away from request threads.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Protects worker initialization and disposal.</summary>
    private readonly object _workerGate = new();

    /// <summary>The effect command worker.</summary>
    private SqliteSynchronousCommandWorker? _effectWorker;

    /// <summary>The independent acknowledgement command worker.</summary>
    private SqliteSynchronousCommandWorker? _acknowledgementWorker;

    /// <summary>The bounded admitted command count for each worker.</summary>
    private int _workerCapacity = 64;

    /// <summary>Configures command capacity before the hub admits requests.</summary>
    /// <param name="capacity">The hub's independently bounded effect and acknowledgement capacities.</param>
    internal void ConfigureWorkerCapacity(int capacity) => _workerCapacity = capacity;

    /// <summary>Schedules a command on the appropriate bounded FIFO worker.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="command">The bounded journal command.</param>
    /// <param name="cancellationToken">The cancellation token checked before dispatch.</param>
    /// <param name="acknowledgement">Whether to use independent acknowledgement admission.</param>
    /// <returns>The command result.</returns>
    internal Task<T> ExecuteAsync<T>(Func<T> command, CancellationToken cancellationToken, bool acknowledgement = false)
    {
        SqliteSynchronousCommandWorker worker;
        lock (_workerGate)
        {
            ThrowIfDisposed();
            worker = acknowledgement
                ? _acknowledgementWorker ??= new(_workerCapacity, _workerCapacity)
                : _effectWorker ??= new(_workerCapacity, _workerCapacity);
        }

        // Each command retains one request bounded by the hub's payload and item limits.
        return worker.ExecuteAsync(_ => command(), 1, cancellationToken);
    }

    /// <summary>Closes admission, cancels queued commands and joins active workers.</summary>
    /// <returns>The worker drain task.</returns>
    internal async ValueTask DisposeWorkersAsync()
    {
        Task effects;
        Task acknowledgements;
        lock (_workerGate)
        {
            _disposed = true;
            effects = _effectWorker?.DisposeAsync().AsTask() ?? Task.CompletedTask;
            acknowledgements = _acknowledgementWorker?.DisposeAsync().AsTask() ?? Task.CompletedTask;
        }

        await Task.WhenAll(effects, acknowledgements).ConfigureAwait(false);
        lock (_connectionGate)
        {
            _connection.Dispose();
        }
    }
}
