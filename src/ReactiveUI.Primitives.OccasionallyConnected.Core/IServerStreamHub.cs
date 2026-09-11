// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Applies synchronization batches and serves stream subscriptions on the remote side.</summary>
public interface IServerStreamHub
{
    /// <summary>Applies client operations and returns their terminal or retryable results.</summary>
    /// <param name="batch">The operation batch.</param>
    /// <param name="client">The authenticated client identity.</param>
    /// <param name="cancellationToken">The token used to cancel application.</param>
    /// <returns>The server synchronization result.</returns>
    ValueTask<ServerSyncResult> ApplyOperationsAsync(
        SyncBatch batch,
        ClientIdentity client,
        CancellationToken cancellationToken);

    /// <summary>Subscribes a client to remote stream batches.</summary>
    /// <param name="request">The remote subscription request.</param>
    /// <param name="client">The authenticated client identity.</param>
    /// <param name="cancellationToken">The token used to cancel subscription enumeration.</param>
    /// <returns>The remote event batches.</returns>
    IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
        RemoteSubscribeRequest request,
        ClientIdentity client,
        CancellationToken cancellationToken);
}
