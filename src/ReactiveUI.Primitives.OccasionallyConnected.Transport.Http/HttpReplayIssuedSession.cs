// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Contains caller-owned replay session header values issued by the endpoint.</summary>
internal sealed record HttpReplayIssuedSession
{
    /// <summary>Gets the trusted replay tenant identifier.</summary>
    internal string TenantId { get; init; } = string.Empty;

    /// <summary>Gets the replay session identifier.</summary>
    internal required string SessionId { get; init; }

    /// <summary>Gets the replay session secret header value.</summary>
    internal required string SessionSecret { get; init; }

    /// <summary>Gets the inclusive expiry timestamp.</summary>
    internal required DateTimeOffset ExpiresAtUtc { get; init; }
}
