// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IServerStreamHub"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IServerStreamHubExtensions
{
    /// <summary>Convenience overloads for a server stream hub.</summary>
    /// <param name="hub">The server stream hub.</param>
    extension(IServerStreamHub hub)
    {
        /// <summary>Applies client operations.</summary>
        /// <param name="batch">The synchronization batch.</param>
        /// <param name="client">The client identity.</param>
        /// <returns>The server synchronization result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(SyncBatch batch, ClientIdentity client) =>
            hub.ApplyOperationsAsync(batch, client, CancellationToken.None);

        /// <summary>Subscribes a client to remote stream batches.</summary>
        /// <param name="request">The remote subscription request.</param>
        /// <param name="client">The client identity.</param>
        /// <returns>The remote event batches.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(RemoteSubscribeRequest request, ClientIdentity client) =>
            hub.SubscribeStreamAsync(request, client, CancellationToken.None);
    }
}
