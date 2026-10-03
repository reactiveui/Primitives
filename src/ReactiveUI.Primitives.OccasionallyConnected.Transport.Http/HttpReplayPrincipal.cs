// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Names the trusted authenticated replay owner supplied by the host endpoint.</summary>
/// <param name="TenantId">The authenticated tenant identifier.</param>
/// <param name="ClientId">The authenticated client identifier.</param>
internal readonly record struct HttpReplayPrincipal(string TenantId, string ClientId);
