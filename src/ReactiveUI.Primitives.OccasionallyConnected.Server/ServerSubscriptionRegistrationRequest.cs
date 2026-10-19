// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes a trusted durable subscription registration request.</summary>
/// <param name="Identity">The trusted subscription identity.</param>
/// <param name="StartPosition">The initial stream position.</param>
internal sealed record ServerSubscriptionRegistrationRequest(
    ServerSubscriptionIdentity Identity,
    StartPosition StartPosition);
