// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IRemoteTransportSession"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IRemoteTransportSessionExtensions
{
    /// <summary>Convenience overloads for a remote transport session.</summary>
    /// <param name="session">The remote transport session.</param>
    extension(IRemoteTransportSession session)
    {
        /// <summary>Pushes a batch of local operations to the remote peer.</summary>
        /// <param name="batch">The synchronization batch.</param>
        /// <returns>The remote synchronization result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch) =>
            session.PushAsync(batch, CancellationToken.None);

        /// <summary>Subscribes to remote event batches.</summary>
        /// <param name="request">The subscription request.</param>
        /// <returns>The remote event batches.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request) =>
            session.SubscribeAsync(request, CancellationToken.None);

        /// <summary>Acknowledges a durably applied remote receive cursor.</summary>
        /// <param name="acknowledgement">The receive acknowledgement.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement) =>
            session.AcknowledgeAsync(acknowledgement, CancellationToken.None);
    }
}
