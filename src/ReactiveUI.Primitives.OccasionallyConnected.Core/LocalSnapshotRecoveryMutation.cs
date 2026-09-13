// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes one atomic local mutation that applies remote snapshot recovery.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SnapshotFormatVersion,nq}")]
public sealed record LocalSnapshotRecoveryMutation
{
    /// <summary>Gets the stream recovered by this mutation.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the durable subscription recovered by this mutation.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the durable snapshot revision observed before recovery projection.</summary>
    public required long ExpectedRevision { get; init; }

    /// <summary>Gets the durable cursor observed before recovery, or null for an initial frontier.</summary>
    public required string? ExpectedPreviousCursor { get; init; }

    /// <summary>Gets the remote checkpoint whose client state becomes authoritative locally.</summary>
    public required RemoteSnapshotCheckpoint Checkpoint { get; init; }

    /// <summary>Gets the rebuilt optimistic state after replaying unresolved local work.</summary>
    public required PayloadEnvelope OptimisticState { get; init; }

    /// <summary>Gets the snapshot format version used by the local optimistic state.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Gets exact dispositions for the current recovered pending operations.</summary>
    public required IReadOnlyList<SnapshotOperationDisposition> OperationDispositions
    {
        get;
        init => field = SnapshotRecoveryCollectionCopy.List(value, nameof(OperationDispositions));
    } = [];
}
