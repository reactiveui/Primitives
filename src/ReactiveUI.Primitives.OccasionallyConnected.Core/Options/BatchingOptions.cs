// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Bounds outgoing batches by operation count, encoded bytes and dwell time.</summary>
[DebuggerDisplay("Count={MaximumOperations,nq}; Bytes={MaximumBytes,nq}")]
public sealed record BatchingOptions
{
    /// <summary>The default maximum number of operations in one batch.</summary>
    private const int DefaultMaximumOperations = 100;

    /// <summary>The default maximum number of encoded bytes in one batch.</summary>
    private const long DefaultMaximumBytes = 1024 * 1024;

    /// <summary>The default maximum batch dwell time in milliseconds.</summary>
    private const int DefaultDwellMilliseconds = 50;

    /// <summary>Gets the maximum number of operations in one batch.</summary>
    public int MaximumOperations { get; init; } = DefaultMaximumOperations;

    /// <summary>Gets the maximum encoded byte count, including the protocol envelope.</summary>
    public long MaximumBytes { get; init; } = DefaultMaximumBytes;

    /// <summary>Gets the longest time the first eligible operation waits for a fuller batch.</summary>
    public TimeSpan MaximumDwellTime { get; init; } = TimeSpan.FromMilliseconds(DefaultDwellMilliseconds);

    /// <summary>Gets the maximum concurrent batches for one stream; the default preserves serial delivery.</summary>
    public int MaxInFlightBatchesPerStream { get; init; } = 1;

    /// <summary>Validates the local batching limits before negotiation with the server.</summary>
    /// <exception cref="InvalidOperationException">A batching limit is not positive.</exception>
    public void Validate()
    {
        if (MaximumOperations <= 0)
        {
            throw new InvalidOperationException("MaximumOperations must be positive.");
        }

        if (MaximumBytes <= 0)
        {
            throw new InvalidOperationException("MaximumBytes must be positive.");
        }

        if (MaximumDwellTime <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("MaximumDwellTime must be positive.");
        }

        if (MaxInFlightBatchesPerStream > 0)
        {
            return;
        }

        throw new InvalidOperationException("MaxInFlightBatchesPerStream must be positive.");
    }
}
