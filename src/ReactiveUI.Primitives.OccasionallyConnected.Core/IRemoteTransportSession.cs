// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents an active remote protocol session.</summary>
public interface IRemoteTransportSession : IAsyncDisposable
{
    /// <summary>Gets the capabilities negotiated for this remote session.</summary>
    NegotiatedCapabilities NegotiatedCapabilities { get; }

    /// <summary>Pushes a batch of local operations to the remote peer.</summary>
    /// <param name="batch">The synchronization batch.</param>
    /// <param name="cancellationToken">The token used to cancel the push.</param>
    /// <returns>The remote synchronization result.</returns>
    ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken);

    /// <summary>Subscribes to remote event batches.</summary>
    /// <param name="request">The subscription request.</param>
    /// <param name="cancellationToken">The token used to cancel subscription enumeration.</param>
    /// <returns>The remote event batches.</returns>
    IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken);

    /// <summary>Acknowledges a durably applied remote receive cursor.</summary>
    /// <param name="acknowledgement">The receive acknowledgement.</param>
    /// <param name="cancellationToken">The token used to cancel acknowledgement.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken);
}
