// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes retained server provenance for one requested pending operation.</summary>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {Kind,nq}")]
public sealed record SnapshotOperationDisposition
{
    /// <summary>Gets the operation identifier from the recovery request.</summary>
    public required OperationId OperationId { get; init; }

    /// <summary>Gets the snapshot recovery disposition.</summary>
    public required SnapshotOperationDispositionKind Kind { get; init; }

    /// <summary>Gets the retained server result when the disposition is proven.</summary>
    public OperationSyncResult? Result { get; init; }
}
