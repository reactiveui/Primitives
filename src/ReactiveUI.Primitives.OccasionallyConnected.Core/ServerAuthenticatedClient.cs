// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a host-authenticated tenant and client without carrying credentials.</summary>
/// <param name="TenantId">The trusted tenant identifier supplied by the host authentication boundary.</param>
/// <param name="ClientId">The trusted client identifier supplied by the host authentication boundary.</param>
[DebuggerDisplay("{TenantId,nq} {ClientId,nq}")]
public sealed record ServerAuthenticatedClient(string TenantId, string ClientId);
