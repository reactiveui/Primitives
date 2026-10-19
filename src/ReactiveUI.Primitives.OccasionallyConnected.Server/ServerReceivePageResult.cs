// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Contains a bounded server receive page and cursor state.</summary>
/// <param name="Status">The page read outcome.</param>
/// <param name="Batch">The returned complete batch when available.</param>
/// <param name="NextGroupSequence">The next group sequence represented by the result.</param>
/// <param name="LastGroupSequence">The stream high-water group sequence.</param>
internal sealed record ServerReceivePageResult(
    ServerReceivePageStatus Status,
    RemoteEventBatch? Batch,
    long NextGroupSequence,
    long LastGroupSequence)
{
    /// <summary>Gets the batch for a page result and fails when an invariant was violated.</summary>
    /// <returns>The complete receive page batch.</returns>
    /// <exception cref="InvalidOperationException">The page result did not carry a batch.</exception>
    internal RemoteEventBatch RequireBatch() =>
        Batch ?? throw new InvalidOperationException("A receive page result must carry a batch.");
}
