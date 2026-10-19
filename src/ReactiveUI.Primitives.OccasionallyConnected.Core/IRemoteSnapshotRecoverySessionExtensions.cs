// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IRemoteSnapshotRecoverySession"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IRemoteSnapshotRecoverySessionExtensions
{
    /// <summary>Convenience overloads for a remote snapshot recovery session.</summary>
    /// <param name="session">The snapshot recovery session.</param>
    extension(IRemoteSnapshotRecoverySession session)
    {
        /// <summary>Requests a coherent snapshot for a subscription whose remote cursor can no longer be replayed.</summary>
        /// <param name="request">The bounded snapshot recovery request.</param>
        /// <returns>The remote snapshot recovery result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(RemoteSnapshotRecoveryRequest request) =>
            session.GetSnapshotAsync(request, CancellationToken.None);
    }
}
