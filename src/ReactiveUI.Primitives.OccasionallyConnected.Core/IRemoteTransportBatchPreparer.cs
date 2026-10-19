// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Prepares remote synchronization batches before the durable attempt barrier is recorded.</summary>
public interface IRemoteTransportBatchPreparer
{
    /// <summary>Validates and reserves capacity for a batch that can be sent later.</summary>
    /// <param name="batch">The synchronization batch.</param>
    /// <param name="cancellationToken">The token used to cancel preparation.</param>
    /// <returns>The prepared remote push handle.</returns>
    ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken);
}
