// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Recovers a remote retained-history gap with a bounded snapshot checkpoint.</summary>
public interface IRemoteSnapshotRecoverySession
{
    /// <summary>Requests a coherent snapshot for a subscription whose remote cursor can no longer be replayed.</summary>
    /// <param name="request">The bounded snapshot recovery request.</param>
    /// <param name="cancellationToken">The token used to cancel the request.</param>
    /// <returns>The remote snapshot recovery result.</returns>
    ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
        RemoteSnapshotRecoveryRequest request,
        CancellationToken cancellationToken);
}
