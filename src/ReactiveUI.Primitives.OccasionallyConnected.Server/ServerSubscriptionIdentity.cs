// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Binds a durable subscription to a trusted tenant, client and stream.</summary>
/// <param name="StreamKey">The authenticated stream key.</param>
/// <param name="ClientId">The authenticated client identifier.</param>
/// <param name="SubscriptionId">The durable subscription identifier.</param>
internal sealed record ServerSubscriptionIdentity(
    ServerStreamKey StreamKey,
    string ClientId,
    SubscriptionId SubscriptionId);
