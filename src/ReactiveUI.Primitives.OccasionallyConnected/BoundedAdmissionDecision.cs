// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a deterministic decision returned by a custom overflow policy.</summary>
/// <param name="Kind">The decision kind.</param>
/// <param name="DropOldestCount">The number of oldest eligible queued items to drop.</param>
internal readonly record struct BoundedAdmissionDecision(BoundedAdmissionDecisionKind Kind, int DropOldestCount)
{
    /// <summary>Gets a decision that rejects the incoming item.</summary>
    internal static BoundedAdmissionDecision Reject { get; } = new(BoundedAdmissionDecisionKind.Reject, 0);

    /// <summary>Gets a decision that drops the incoming item.</summary>
    internal static BoundedAdmissionDecision DropNewest { get; } = new(BoundedAdmissionDecisionKind.DropNewest, 0);

    /// <summary>Gets a decision that waits for capacity without dropping work.</summary>
    internal static BoundedAdmissionDecision Block { get; } = new(BoundedAdmissionDecisionKind.Block, 0);

    /// <summary>Creates a decision that drops oldest eligible queued items.</summary>
    /// <param name="count">The number of eligible queued items to drop.</param>
    /// <returns>A drop-oldest decision.</returns>
    internal static BoundedAdmissionDecision DropOldest(int count) => new(BoundedAdmissionDecisionKind.DropOldest, count);
}
