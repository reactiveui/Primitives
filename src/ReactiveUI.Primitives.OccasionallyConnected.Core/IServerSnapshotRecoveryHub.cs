// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Serves authenticated snapshot recovery requests on the remote side.</summary>
public interface IServerSnapshotRecoveryHub
{
    /// <summary>Requests a coherent snapshot for an authenticated subscription whose cursor can no longer be replayed.</summary>
    /// <param name="request">The bounded snapshot recovery request.</param>
    /// <param name="client">The authenticated server principal.</param>
    /// <param name="cancellationToken">The token used to cancel recovery.</param>
    /// <returns>The remote snapshot recovery result.</returns>
    ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
        RemoteSnapshotRecoveryRequest request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken);
}
