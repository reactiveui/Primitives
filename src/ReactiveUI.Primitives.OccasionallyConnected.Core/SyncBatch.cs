// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a remote synchronization operation batch.</summary>
[System.Diagnostics.DebuggerDisplay("{BatchId,nq}")]
public sealed record SyncBatch
{
    /// <summary>Initializes a new instance of the <see cref="SyncBatch"/> class.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="operations">The operations in the batch.</param>
    public SyncBatch(Guid batchId, IReadOnlyList<SyncOperation> operations)
    {
        BatchId = batchId;
        Operations = CollectionCopy.List(operations);
    }

    /// <summary>Gets the batch identifier.</summary>
    public Guid BatchId { get; }

    /// <summary>Gets the operations in the batch.</summary>
    public IReadOnlyList<SyncOperation> Operations { get; }
}
