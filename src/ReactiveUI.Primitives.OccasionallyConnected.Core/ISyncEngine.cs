// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates local outbox admission, remote synchronization, and lifecycle state.</summary>
public interface ISyncEngine : IAsyncDisposable
{
    /// <summary>Gets synchronization lifecycle state changes.</summary>
    IObservable<SyncState> SyncStates { get; }

    /// <summary>Gets per-operation state changes.</summary>
    IObservable<SyncOperationStatus> OperationStates { get; }

    /// <summary>Gets non-payload operational faults.</summary>
    IObservable<OccasionallyConnectedFault> Faults { get; }

    /// <summary>Persists and admits a synchronization operation.</summary>
    /// <param name="operation">The operation to enqueue.</param>
    /// <param name="cancellationToken">The token used to cancel admission.</param>
    /// <returns>The durable publish receipt.</returns>
    ValueTask<PublishReceipt> EnqueueOperationAsync(
        SyncOperation operation,
        CancellationToken cancellationToken);

    /// <summary>Starts synchronization work.</summary>
    /// <param name="cancellationToken">The token used to cancel startup.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops synchronization work after in-flight local commits complete.</summary>
    /// <param name="cancellationToken">The token used to cancel shutdown waiting.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken);

    /// <summary>Requests an immediate synchronization attempt.</summary>
    /// <param name="cancellationToken">The token used to cancel the trigger request.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask TriggerSyncAsync(CancellationToken cancellationToken);
}
