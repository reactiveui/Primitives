// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the allowlisted client-state checkpoint returned by snapshot recovery.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {FrontierCursor,nq}")]
public sealed record RemoteSnapshotCheckpoint
{
    /// <summary>Gets the stream covered by the checkpoint.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the subscription binding offered this checkpoint.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the opaque server frontier cursor covered by the checkpoint.</summary>
    public required string FrontierCursor { get; init; }

    /// <summary>Gets the server version observed for the coherent checkpoint view.</summary>
    public required string ServerVersion { get; init; }

    /// <summary>Gets the snapshot format version used by the checkpoint payload.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Gets the allowlisted client-state payload. Opaque server state is never exposed here.</summary>
    public required PayloadEnvelope ClientState { get; init; }

    /// <summary>Gets the time the coherent checkpoint view was observed.</summary>
    public required DateTimeOffset ObservedAtUtc { get; init; }
}
