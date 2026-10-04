// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes trusted tenant and client scope returned by server stream authorization.</summary>
/// <param name="TenantId">The trusted tenant identifier.</param>
/// <param name="ClientId">The trusted client identifier.</param>
[System.Diagnostics.DebuggerDisplay("{TenantId,nq} {ClientId,nq}")]
public sealed record ServerStreamAuthorizationScope(string TenantId, string ClientId);
