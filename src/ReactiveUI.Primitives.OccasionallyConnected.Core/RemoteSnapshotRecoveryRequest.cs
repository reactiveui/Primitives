// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Requests bounded snapshot recovery for a subscription whose cursor cannot be replayed.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed record RemoteSnapshotRecoveryRequest
{
    /// <summary>Gets the stream to recover.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the durable subscription identity to recover.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the expired cursor, or null when the gap is before the first resumable cursor.</summary>
    public required string? ExpiredCursor { get; init; }

    /// <summary>Gets the requested client-state contract identifier.</summary>
    public required string ClientStateContractId { get; init; }

    /// <summary>Gets the requested client-state schema version.</summary>
    public required int ClientStateSchemaVersion { get; init; }

    /// <summary>Gets the requested snapshot format version.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Gets the full pending operation intents copied before transport.</summary>
    public required IReadOnlyList<SyncOperation> PendingOperations
    {
        get;
        init => field = SnapshotRecoveryCollectionCopy.List(value, nameof(PendingOperations));
    } = [];

    /// <summary>Gets the maximum response bytes the caller will accept.</summary>
    public required long MaximumResponseBytes { get; init; }
}
