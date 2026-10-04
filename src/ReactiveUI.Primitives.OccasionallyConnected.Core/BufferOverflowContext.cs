// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a publication that does not fit the outbox and the operations that may be evicted for it.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} Candidates={Candidates.Count,nq}")]
public sealed record BufferOverflowContext
{
    /// <summary>Initializes a new instance of the <see cref="BufferOverflowContext"/> class.</summary>
    /// <param name="streamId">The stream receiving the publication.</param>
    /// <param name="incomingDurable">Whether the incoming publication is durable.</param>
    /// <param name="incomingPriority">The incoming publication priority.</param>
    /// <param name="incomingRetainedBytes">The declared upper bound of bytes retained for the incoming publication.</param>
    /// <param name="candidates">The pending non-durable operations of the stream, oldest first.</param>
    public BufferOverflowContext(
        StreamId streamId,
        bool incomingDurable,
        int incomingPriority,
        long incomingRetainedBytes,
        IReadOnlyList<BufferOverflowCandidate> candidates)
    {
        StreamId = streamId;
        IncomingDurable = incomingDurable;
        IncomingPriority = incomingPriority;
        IncomingRetainedBytes = incomingRetainedBytes;
        Candidates = CollectionCopy.List(candidates);
    }

    /// <summary>Gets the stream receiving the publication.</summary>
    public StreamId StreamId { get; }

    /// <summary>Gets a value indicating whether the incoming publication is durable.</summary>
    public bool IncomingDurable { get; }

    /// <summary>Gets the incoming publication priority.</summary>
    public int IncomingPriority { get; }

    /// <summary>Gets the declared upper bound of bytes retained for the incoming publication.</summary>
    public long IncomingRetainedBytes { get; }

    /// <summary>Gets the bounded list of pending non-durable operations that may be evicted, oldest first.</summary>
    public IReadOnlyList<BufferOverflowCandidate> Candidates { get; }
}
