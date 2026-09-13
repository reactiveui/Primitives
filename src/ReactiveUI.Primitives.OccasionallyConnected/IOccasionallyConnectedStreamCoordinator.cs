// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates context-owned lifecycle, identity, and scheduling hooks for a stream facade.</summary>
internal interface IOccasionallyConnectedStreamCoordinator
{
    /// <summary>Gets or creates the durable subscription identity after context-owned store initialization.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="preferredId">The optional caller-supplied subscription identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable subscription identity.</returns>
    ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken);

    /// <summary>Starts context-owned stream work without implying remote connectivity.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The start operation.</returns>
    ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken);

    /// <summary>Stops context-owned stream work without completing public observers.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stop operation.</returns>
    ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken);

    /// <summary>Nudges context-owned scheduling after a durable local commit.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="operation">The committed operation.</param>
    void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation);
}
