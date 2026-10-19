// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Describes a replay-protected HTTP request being authorized by the endpoint host.</summary>
[System.Diagnostics.DebuggerDisplay("{Operation,nq} {Client.ClientId,nq}")]
public sealed record HttpReplayAuthorizationContext
{
    /// <summary>Gets the authenticated transport principal supplied by the host.</summary>
    public required ServerAuthenticatedClient Client { get; init; }

    /// <summary>Gets the replay operation name.</summary>
    public required string Operation { get; init; }

    /// <summary>Gets every stream identifier decoded from the protected protocol request.</summary>
    public IReadOnlyList<StreamId> StreamIds { get; init; } = [];

    /// <summary>Gets the subscription identifier decoded from subscribe or acknowledgement requests.</summary>
    public SubscriptionId? SubscriptionId { get; init; }
}
