// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a durable local snapshot recovery commit.</summary>
[System.Diagnostics.DebuggerDisplay("{Snapshot,nq}")]
public sealed record LocalSnapshotRecoveryResult
{
    /// <summary>Gets the durable snapshot after recovery commit.</summary>
    public required LocalSnapshot Snapshot { get; init; }

    /// <summary>Gets the number of pending operations proven included in the checkpoint.</summary>
    public required int IncludedOperationCount { get; init; }

    /// <summary>Gets the number of pending operations proven terminal during recovery.</summary>
    public required int TerminalOperationCount { get; init; }

    /// <summary>Gets the number of pending operations preserved for later handling.</summary>
    public required int PreservedPendingOperationCount { get; init; }
}
