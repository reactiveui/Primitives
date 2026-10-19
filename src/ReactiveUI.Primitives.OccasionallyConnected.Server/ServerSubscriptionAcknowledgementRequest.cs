// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Identifies an acknowledgement made by an authenticated client within a tenant boundary.</summary>
/// <param name="StreamKey">The authenticated stream key.</param>
/// <param name="ClientId">The authenticated client identifier.</param>
/// <param name="Acknowledgement">The protocol acknowledgement.</param>
internal sealed record ServerSubscriptionAcknowledgementRequest(
    ServerStreamKey StreamKey,
    string ClientId,
    ReceiveAcknowledgement Acknowledgement);
