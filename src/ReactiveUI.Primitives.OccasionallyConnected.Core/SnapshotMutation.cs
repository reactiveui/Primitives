// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a snapshot replacement to commit transactionally.</summary>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="State">The serialized snapshot state.</param>
/// <param name="FormatVersion">The snapshot format version.</param>
/// <param name="ExpectedRevision">The snapshot revision observed before projection.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {FormatVersion,nq}")]
public sealed record SnapshotMutation(
    StreamId StreamId,
    PayloadEnvelope State,
    int FormatVersion,
    long ExpectedRevision)
{
    /// <summary>Initializes a new instance of the <see cref="SnapshotMutation"/> class.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="state">The serialized snapshot state.</param>
    /// <param name="formatVersion">The snapshot format version.</param>
    public SnapshotMutation(StreamId streamId, PayloadEnvelope state, int formatVersion)
        : this(streamId, state, formatVersion, ExpectedRevision: 0)
    {
    }

    /// <summary>Gets the serialized authoritative checkpoint replacement, or null to preserve the stored authoritative state.</summary>
    public PayloadEnvelope? AuthoritativeState { get; init; }
}
