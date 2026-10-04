// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes an immutable server-originated event after durable inbox validation.</summary>
[DebuggerDisplay("{EventId,nq} {StreamId,nq}")]
public sealed record RemoteEvent
{
    /// <summary>Initializes a new instance of the <see cref="RemoteEvent"/> class.</summary>
    /// <param name="eventId">The server event identifier.</param>
    /// <param name="streamId">The stream that produced the event.</param>
    /// <param name="serverCursor">The server cursor assigned to the event.</param>
    /// <param name="committedAtUtc">The server commit timestamp.</param>
    /// <param name="causedByOperationId">The optional client operation that caused the event.</param>
    /// <param name="payload">The serialized event payload.</param>
    /// <param name="metadata">The event metadata.</param>
    public RemoteEvent(
        Guid eventId,
        StreamId streamId,
        string serverCursor,
        DateTimeOffset committedAtUtc,
        OperationId? causedByOperationId,
        PayloadEnvelope payload,
        IReadOnlyDictionary<string, string> metadata)
    {
        EventId = eventId;
        StreamId = streamId;
        ServerCursor = serverCursor;
        CommittedAtUtc = committedAtUtc;
        CausedByOperationId = causedByOperationId;
        Payload = payload;
        Metadata = CollectionCopy.Dictionary(metadata);
    }

    /// <summary>Gets the server event identifier.</summary>
    public Guid EventId { get; }

    /// <summary>Gets the stream that produced the event.</summary>
    public StreamId StreamId { get; }

    /// <summary>Gets the server cursor assigned to the event.</summary>
    public string ServerCursor { get; }

    /// <summary>Gets the server commit timestamp.</summary>
    public DateTimeOffset CommittedAtUtc { get; }

    /// <summary>Gets the optional client operation that caused the event.</summary>
    public OperationId? CausedByOperationId { get; }

    /// <summary>
    /// Gets the optional origin correlation. Origin data does not itself authenticate a caller; trusted server code populates it from authenticated context,
    /// and clients validate the transport that carries it.
    /// </summary>
    public RemoteEventOrigin? Origin
    {
        get;
        init
        {
            if (value is not null && (!CausedByOperationId.HasValue || value.OperationId != CausedByOperationId.Value))
            {
                throw new InvalidOperationException("A remote event origin must match its causing operation.");
            }

            field = value;
        }
    }

    /// <summary>Gets the serialized event payload.</summary>
    public PayloadEnvelope Payload { get; }

    /// <summary>Gets the event metadata.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; }
}
