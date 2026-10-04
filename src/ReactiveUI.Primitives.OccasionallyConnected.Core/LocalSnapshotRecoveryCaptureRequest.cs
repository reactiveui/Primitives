// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the local stream fences and limits used to capture snapshot recovery state.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed record LocalSnapshotRecoveryCaptureRequest
{
    /// <summary>Gets the stream whose local recovery state is captured.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the durable subscription expected for the stream.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the finite capture limits.</summary>
    public required SnapshotRecoveryLimits Limits { get; init; }
}
