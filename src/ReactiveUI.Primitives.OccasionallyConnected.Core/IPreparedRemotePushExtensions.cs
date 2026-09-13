// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IPreparedRemotePush"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IPreparedRemotePushExtensions
{
    /// <summary>Convenience overloads for a prepared remote push.</summary>
    /// <param name="prepared">The prepared remote push.</param>
    extension(IPreparedRemotePush prepared)
    {
        /// <summary>Sends the prepared batch to the remote peer.</summary>
        /// <returns>The remote synchronization result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> SendAsync() =>
            prepared.SendAsync(CancellationToken.None);
    }
}
