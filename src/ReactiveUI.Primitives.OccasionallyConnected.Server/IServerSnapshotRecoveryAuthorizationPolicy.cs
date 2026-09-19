// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Authorizes authenticated clients for full-state server snapshot recovery.</summary>
public interface IServerSnapshotRecoveryAuthorizationPolicy
{
    /// <summary>Authorizes a snapshot recovery request before any journal lookup or materialization.</summary>
    /// <param name="client">The authenticated client identity supplied by the host.</param>
    /// <param name="request">The bounded snapshot recovery request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted tenant and client scope for the recovery request.</returns>
    ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
        ServerAuthenticatedClient client,
        RemoteSnapshotRecoveryRequest request,
        CancellationToken cancellationToken);
}
