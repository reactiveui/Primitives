// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IServerSnapshotRecoveryHub"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IServerSnapshotRecoveryHubExtensions
{
    /// <summary>Convenience overloads for a server snapshot recovery hub.</summary>
    /// <param name="hub">The server snapshot recovery hub.</param>
    extension(IServerSnapshotRecoveryHub hub)
    {
        /// <summary>Requests a coherent snapshot for an authenticated subscription whose cursor can no longer be replayed.</summary>
        /// <param name="request">The bounded snapshot recovery request.</param>
        /// <param name="client">The authenticated server principal.</param>
        /// <returns>The remote snapshot recovery result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client) =>
            hub.GetSnapshotAsync(request, client, CancellationToken.None);
    }
}
