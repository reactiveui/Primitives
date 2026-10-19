// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IRemoteTransportBatchPreparer"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IRemoteTransportBatchPreparerExtensions
{
    /// <summary>Convenience overloads for a remote transport batch preparer.</summary>
    /// <param name="preparer">The remote transport batch preparer.</param>
    extension(IRemoteTransportBatchPreparer preparer)
    {
        /// <summary>Validates and reserves capacity for a batch that can be sent later.</summary>
        /// <param name="batch">The synchronization batch.</param>
        /// <returns>The prepared remote push handle.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch) =>
            preparer.PreparePushAsync(batch, CancellationToken.None);
    }
}
