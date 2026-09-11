// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a durable local snapshot.</summary>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="FormatVersion">The snapshot format version.</param>
/// <param name="ServerCursor">The server cursor covered by the snapshot.</param>
/// <param name="State">The serialized snapshot state.</param>
/// <param name="Revision">The committed snapshot revision; zero denotes no committed snapshot.</param>
/// <param name="SavedAtUtc">The time the snapshot was saved.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {FormatVersion,nq}")]
public sealed record LocalSnapshot(
    StreamId StreamId,
    int FormatVersion,
    string? ServerCursor,
    PayloadEnvelope State,
    long Revision,
    DateTimeOffset SavedAtUtc)
{
    /// <summary>Initializes a new instance of the <see cref="LocalSnapshot"/> class.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="formatVersion">The snapshot format version.</param>
    /// <param name="serverCursor">The server cursor covered by the snapshot.</param>
    /// <param name="state">The serialized snapshot state.</param>
    /// <param name="savedAtUtc">The time the snapshot was saved.</param>
    public LocalSnapshot(
        StreamId streamId,
        int formatVersion,
        string? serverCursor,
        PayloadEnvelope state,
        DateTimeOffset savedAtUtc)
        : this(streamId, formatVersion, serverCursor, state, Revision: 0, savedAtUtc)
    {
    }
}
