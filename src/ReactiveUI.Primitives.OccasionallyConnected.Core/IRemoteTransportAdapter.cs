// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Connects the synchronization engine to a remote occasionally-connected protocol peer.</summary>
public interface IRemoteTransportAdapter : IAsyncDisposable
{
    /// <summary>Gets the remote transport capabilities.</summary>
    RemoteTransportCapabilities Capabilities { get; }

    /// <summary>Connects to the remote peer.</summary>
    /// <param name="request">The connect request.</param>
    /// <param name="cancellationToken">The token used to cancel connection.</param>
    /// <returns>The connected remote session.</returns>
    ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken);
}
