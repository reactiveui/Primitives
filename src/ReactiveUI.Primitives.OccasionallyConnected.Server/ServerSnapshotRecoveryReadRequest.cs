// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Requests the internal retained server view for one snapshot recovery attempt.</summary>
internal sealed record ServerSnapshotRecoveryReadRequest
{
    /// <summary>Gets the authenticated stream key.</summary>
    internal required ServerStreamKey StreamKey { get; init; }

    /// <summary>Gets the trusted subscription identity.</summary>
    internal required ServerSubscriptionIdentity Subscription { get; init; }

    /// <summary>Gets the client supplied snapshot recovery request.</summary>
    internal required RemoteSnapshotRecoveryRequest RecoveryRequest { get; init; }

    /// <summary>Gets the configured validation limits.</summary>
    internal required SnapshotRecoveryLimits Limits { get; init; }
}
