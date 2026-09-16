// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Confirms that a non-connect replay request proved a current endpoint replay session.</summary>
internal sealed record HttpReplaySessionProof
{
    /// <summary>Gets the verified replay session identifier.</summary>
    internal required string SessionId { get; init; }

    /// <summary>Gets the inclusive session expiry timestamp.</summary>
    internal required DateTimeOffset ExpiresAtUtc { get; init; }
}
