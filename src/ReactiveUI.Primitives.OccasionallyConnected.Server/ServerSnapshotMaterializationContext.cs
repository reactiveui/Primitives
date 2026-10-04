// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes the trusted and captured inputs for server snapshot materialization.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed record ServerSnapshotMaterializationContext
{
    /// <summary>Gets the trusted tenant identifier from the validated recovery authorization scope.</summary>
    public required string TenantId { get; init; }

    /// <summary>Gets the trusted client identifier from the validated recovery authorization scope.</summary>
    public required string ClientId { get; init; }

    /// <summary>Gets the stream being recovered.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the subscription being recovered.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the captured complete receive frontier cursor.</summary>
    public required string FrontierCursor { get; init; }

    /// <summary>Gets the captured canonical server state.</summary>
    public required ServerState CapturedServerState { get; init; }

    /// <summary>Gets the captured server version.</summary>
    public required string CapturedServerVersion { get; init; }

    /// <summary>Gets the requested client-state contract identifier.</summary>
    public required string ClientStateContractId { get; init; }

    /// <summary>Gets the requested client-state schema version.</summary>
    public required int ClientStateSchemaVersion { get; init; }

    /// <summary>Gets the requested snapshot format version.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Gets the maximum response bytes accepted by the caller.</summary>
    public required long MaximumResponseBytes { get; init; }

    /// <summary>Gets the time the coherent server view was observed.</summary>
    public required DateTimeOffset ObservedAtUtc { get; init; }
}
