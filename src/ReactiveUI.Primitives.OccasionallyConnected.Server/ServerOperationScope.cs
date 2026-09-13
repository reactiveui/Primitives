// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes the trusted tenant and client identities admitted for one operation.</summary>
/// <param name="TenantId">The authenticated tenant identifier.</param>
/// <param name="ClientId">The authenticated client identifier.</param>
internal readonly record struct ServerOperationScope(string TenantId, string ClientId);
