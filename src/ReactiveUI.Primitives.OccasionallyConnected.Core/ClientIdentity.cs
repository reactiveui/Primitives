// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies an authenticated client or device without carrying credentials.</summary>
/// <param name="ClientId">The stable client identifier assigned by the host application or remote authority.</param>
/// <param name="TenantHint">The optional tenant routing hint supplied by the client.</param>
/// <remarks><see cref="TenantHint"/> is routing context supplied by the client. It is untrusted input and is not authorization.</remarks>
[DebuggerDisplay("{ClientId,nq}")]
public sealed record ClientIdentity(string ClientId, string? TenantHint = null);
