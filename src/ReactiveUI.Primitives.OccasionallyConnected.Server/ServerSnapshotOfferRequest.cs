// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Requests a durable recovered snapshot cursor offer after a view has been validated.</summary>
internal sealed record ServerSnapshotOfferRequest
{
    /// <summary>Gets the authenticated stream key.</summary>
    internal required ServerStreamKey StreamKey { get; init; }

    /// <summary>Gets the trusted subscription identity.</summary>
    internal required ServerSubscriptionIdentity Subscription { get; init; }

    /// <summary>Gets the source view that produced the remote recovery result.</summary>
    internal required ServerSnapshotRecoveryView View { get; init; }

    /// <summary>Gets the request that produced the remote result.</summary>
    internal required RemoteSnapshotRecoveryRequest RecoveryRequest { get; init; }

    /// <summary>Gets the remote recovery result after structural validation.</summary>
    internal required RemoteSnapshotRecoveryResult RecoveryResult { get; init; }

    /// <summary>Gets the configured validation limits.</summary>
    internal required SnapshotRecoveryLimits Limits { get; init; }
}
