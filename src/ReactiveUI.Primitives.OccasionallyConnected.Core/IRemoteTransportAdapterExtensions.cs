// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IRemoteTransportAdapter"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IRemoteTransportAdapterExtensions
{
    /// <summary>Convenience overloads for a remote transport adapter.</summary>
    /// <param name="adapter">The remote transport adapter.</param>
    extension(IRemoteTransportAdapter adapter)
    {
        /// <summary>Connects to the remote peer.</summary>
        /// <param name="request">The connect request.</param>
        /// <returns>The connected remote session.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request) =>
            adapter.ConnectAsync(request, CancellationToken.None);
    }
}
