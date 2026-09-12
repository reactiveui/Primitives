// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a batch of remote events for one stream.</summary>
[System.Diagnostics.DebuggerDisplay("{BatchId,nq}")]
public sealed record RemoteEventBatch
{
    /// <summary>Initializes a new instance of the <see cref="RemoteEventBatch"/> class.</summary>
    /// <param name="batchId">The remote event batch identifier.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="previousCursor">The cursor preceding this batch.</param>
    /// <param name="nextCursor">The cursor after this batch.</param>
    /// <param name="events">The events in this batch.</param>
    public RemoteEventBatch(
        Guid batchId,
        StreamId streamId,
        string? previousCursor,
        string nextCursor,
        IReadOnlyList<RemoteEvent> events)
    {
        BatchId = batchId;
        StreamId = streamId;
        PreviousCursor = previousCursor;
        NextCursor = nextCursor;
        Events = CollectionCopy.List(events);
    }

    /// <summary>Gets the remote event batch identifier.</summary>
    public Guid BatchId { get; }

    /// <summary>Gets the stream identifier.</summary>
    public StreamId StreamId { get; }

    /// <summary>Gets the cursor preceding this batch.</summary>
    public string? PreviousCursor { get; }

    /// <summary>Gets the cursor after this batch.</summary>
    public string NextCursor { get; }

    /// <summary>Gets the events in this batch.</summary>
    public IReadOnlyList<RemoteEvent> Events { get; }

    /// <summary>Gets the operations whose complete effects are included at the next cursor.</summary>
    public IReadOnlyList<RemoteOperationCompletion> CompletedOperations
    {
        get;
        init => field = CollectionCopy.List(value);
    } = Array.Empty<RemoteOperationCompletion>();
}
