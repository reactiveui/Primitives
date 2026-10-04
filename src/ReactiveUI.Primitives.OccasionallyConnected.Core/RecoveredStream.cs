// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a recovered stream and its durable pending work.</summary>
[System.Diagnostics.DebuggerDisplay("{SubscriptionId,nq}")]
public sealed record RecoveredStream
{
    /// <summary>Initializes a new instance of the <see cref="RecoveredStream"/> class.</summary>
    /// <param name="subscriptionId">The recovered subscription identifier.</param>
    /// <param name="serverCursor">The recovered server cursor.</param>
    /// <param name="snapshot">The recovered local snapshot.</param>
    /// <param name="pendingOperations">The recovered pending operations.</param>
    /// <param name="deadLetters">The recovered dead-letter records.</param>
    /// <param name="nextClientSequence">The next client sequence to assign.</param>
    public RecoveredStream(
        SubscriptionId subscriptionId,
        string? serverCursor,
        LocalSnapshot? snapshot,
        IReadOnlyList<SyncOperation> pendingOperations,
        IReadOnlyList<DeadLetterRecord> deadLetters,
        long nextClientSequence)
    {
        SubscriptionId = subscriptionId;
        ServerCursor = serverCursor;
        Snapshot = snapshot;
        PendingOperations = CollectionCopy.List(pendingOperations);
        ReplayOperations = PendingOperations;
        DeadLetters = CollectionCopy.List(deadLetters);
        NextClientSequence = nextClientSequence;
    }

    /// <summary>Gets the recovered subscription identifier.</summary>
    public SubscriptionId SubscriptionId { get; }

    /// <summary>Gets the recovered server cursor.</summary>
    public string? ServerCursor { get; }

    /// <summary>Gets the recovered local snapshot.</summary>
    public LocalSnapshot? Snapshot { get; }

    /// <summary>Gets the recovered pending operations.</summary>
    public IReadOnlyList<SyncOperation> PendingOperations { get; }

    /// <summary>Gets the recovered operations that should be replayed into local projection.</summary>
    public IReadOnlyList<SyncOperation> ReplayOperations
    {
        get;
        init => field = CollectionCopy.List(value);
    }

    /// <summary>Gets the recovered dead-letter records.</summary>
    public IReadOnlyList<DeadLetterRecord> DeadLetters { get; }

    /// <summary>Gets the optional lower bound before which the recovered FIFO upload head cannot be leased.</summary>
    /// <remarks>
    /// Null means the store has no known future time constraint. Other ownership or policy blockers can still prevent leasing.
    /// </remarks>
    public DateTimeOffset? PendingUploadNotBeforeUtc { get; init; }

    /// <summary>Gets the recovered quarantine marker for this stream, when one exists.</summary>
    public LocalPayloadQuarantineRecord? Quarantine
    {
        get;
        init;
    }

    /// <summary>Gets the next client sequence to assign.</summary>
    public long NextClientSequence { get; }
}
