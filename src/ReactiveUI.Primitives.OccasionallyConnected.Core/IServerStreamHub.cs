// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Applies synchronization batches and serves stream subscriptions on the remote side.</summary>
public interface IServerStreamHub
{
    /// <summary>Applies client operations and returns their terminal or retryable results.</summary>
    /// <param name="batch">The operation batch.</param>
    /// <param name="client">The authenticated server principal.</param>
    /// <param name="cancellationToken">The token used to cancel application.</param>
    /// <returns>The server synchronization result.</returns>
    ValueTask<ServerSyncResult> ApplyOperationsAsync(
        SyncBatch batch,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken);

    /// <summary>Persists a receive acknowledgement for an authorized client.</summary>
    /// <param name="acknowledgement">The acknowledgement for a durably applied receive cursor.</param>
    /// <param name="client">The authenticated server principal.</param>
    /// <param name="cancellationToken">The token used to cancel acknowledgement persistence.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Successful completion means the authorized acknowledgement was durably persisted, or an identical duplicate was
    /// already persisted. Rejection or persistence failure faults the returned task. This contract alone makes no
    /// capability claim.
    /// </remarks>
    ValueTask AcknowledgeAsync(
        ReceiveAcknowledgement acknowledgement,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken);

    /// <summary>Subscribes a client to remote stream batches.</summary>
    /// <param name="request">The remote subscription request.</param>
    /// <param name="client">The authenticated server principal.</param>
    /// <param name="cancellationToken">The token used to cancel subscription enumeration.</param>
    /// <returns>The remote event batches.</returns>
    IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
        RemoteSubscribeRequest request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken);
}
