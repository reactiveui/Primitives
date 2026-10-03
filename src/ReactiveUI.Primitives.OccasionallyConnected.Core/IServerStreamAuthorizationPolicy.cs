// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Authorizes authenticated clients for server stream hub actions.</summary>
public interface IServerStreamAuthorizationPolicy
{
    /// <summary>Authorizes an incoming publish batch before any operation journal lookup.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="batch">The incoming synchronization batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted tenant and client scope for the batch.</returns>
    ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
        ServerAuthenticatedClient client,
        SyncBatch batch,
        CancellationToken cancellationToken);

    /// <summary>Authorizes an individual operation before any operation journal lookup.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="operation">The operation to authorize.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted tenant and client scope for the operation.</returns>
    ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
        ServerAuthenticatedClient client,
        SyncOperation operation,
        CancellationToken cancellationToken);

    /// <summary>Authorizes a receive subscription before subscription registration or journal lookup.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="request">The subscription request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted tenant and client scope for the subscription.</returns>
    ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
        ServerAuthenticatedClient client,
        RemoteSubscribeRequest request,
        CancellationToken cancellationToken);

    /// <summary>Authorizes a receive acknowledgement before subscription journal lookup.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="acknowledgement">The receive acknowledgement.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted tenant and client scope for the acknowledgement.</returns>
    ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
        ServerAuthenticatedClient client,
        ReceiveAcknowledgement acknowledgement,
        CancellationToken cancellationToken);
}
