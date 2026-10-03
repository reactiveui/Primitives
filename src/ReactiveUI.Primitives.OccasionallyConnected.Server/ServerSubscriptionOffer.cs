// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores one offered complete receive position and optional snapshot proof for a subscription.</summary>
internal sealed record ServerSubscriptionOffer
{
    /// <summary>Gets the offered cursor.</summary>
    internal required string Cursor { get; init; }

    /// <summary>Gets the complete group sequence.</summary>
    internal required long GroupSequence { get; init; }

    /// <summary>Gets the monotonic server time when the cursor was offered.</summary>
    internal required DateTimeOffset OfferedAtUtc { get; init; }

    /// <summary>Gets the retained logical byte count.</summary>
    internal required long LogicalBytes { get; init; }

    /// <summary>Gets the captured stream revision for a snapshot offer proof.</summary>
    internal long? SnapshotStreamRevision { get; init; }

    /// <summary>Gets the captured stream event frontier for a snapshot offer proof.</summary>
    internal long? SnapshotLastEventSequence { get; init; }

    /// <summary>Gets the subscription generation bound to this offer proof.</summary>
    internal long? SnapshotSubscriptionGeneration { get; init; }

    /// <summary>Gets the subscription revision observed before creating this offer proof.</summary>
    internal long? SnapshotOriginatingSubscriptionRevision { get; init; }

    /// <summary>Gets the subscription revision assigned to this offer proof.</summary>
    internal long? SnapshotIssuedSubscriptionRevision { get; init; }

    /// <summary>Gets the snapshot format version bound to this offer proof.</summary>
    internal int? SnapshotFormatVersion { get; init; }

    /// <summary>Gets the trusted recovered client state bound to this offer proof.</summary>
    internal PayloadEnvelope? SnapshotClientState { get; init; }
}
