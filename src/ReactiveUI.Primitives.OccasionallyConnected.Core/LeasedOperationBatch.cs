// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a leased batch of local operations.</summary>
[System.Diagnostics.DebuggerDisplay("{LeaseId,nq}")]
public sealed record LeasedOperationBatch
{
    /// <summary>Initializes a new instance of the <see cref="LeasedOperationBatch"/> class.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="expiresAtUtc">The time the lease expires.</param>
    /// <param name="operations">The leased operations.</param>
    public LeasedOperationBatch(Guid leaseId, DateTimeOffset expiresAtUtc, IReadOnlyList<SyncOperation> operations)
    {
        LeaseId = leaseId;
        ExpiresAtUtc = expiresAtUtc;
        Operations = CollectionCopy.List(operations);
    }

    /// <summary>Gets the lease identifier.</summary>
    public Guid LeaseId { get; }

    /// <summary>Gets the time the lease expires.</summary>
    public DateTimeOffset ExpiresAtUtc { get; }

    /// <summary>Gets the leased operations.</summary>
    public IReadOnlyList<SyncOperation> Operations { get; }
}
