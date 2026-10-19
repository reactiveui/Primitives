// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents the decision returned by an <see cref="IBufferOverflowPolicy"/>.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} {EvictOperationId,nq}")]
public sealed class BufferOverflowDecision
{
    /// <summary>Initializes a new instance of the <see cref="BufferOverflowDecision"/> class.</summary>
    /// <param name="kind">The decision kind.</param>
    /// <param name="evictOperationId">The selected candidate, when the decision evicts.</param>
    private BufferOverflowDecision(BufferOverflowDecisionKind kind, OperationId? evictOperationId)
    {
        Kind = kind;
        EvictOperationId = evictOperationId;
    }

    /// <summary>Gets a decision that waits for outbox capacity.</summary>
    public static BufferOverflowDecision Block { get; } = new(BufferOverflowDecisionKind.Block, null);

    /// <summary>Gets a decision that rejects the incoming publication.</summary>
    public static BufferOverflowDecision Reject { get; } = new(BufferOverflowDecisionKind.Reject, null);

    /// <summary>Gets the decision kind.</summary>
    public BufferOverflowDecisionKind Kind { get; }

    /// <summary>Gets the candidate selected for eviction, when <see cref="Kind"/> is <see cref="BufferOverflowDecisionKind.Evict"/>.</summary>
    public OperationId? EvictOperationId { get; }

    /// <summary>Creates a decision that evicts one listed candidate.</summary>
    /// <param name="operationId">The candidate operation identifier.</param>
    /// <returns>The eviction decision.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BufferOverflowDecision Evict(OperationId operationId) => new(BufferOverflowDecisionKind.Evict, operationId);
}
