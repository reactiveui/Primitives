// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents a current authorization decision supplied by the hosting endpoint.</summary>
internal sealed record HttpReplayAuthorizationResult
{
    /// <summary>Gets an allowed replay authorization result.</summary>
    internal static HttpReplayAuthorizationResult Allowed { get; } = new() { IsAuthorized = true };

    /// <summary>Gets whether replay admission is currently authorized.</summary>
    internal required bool IsAuthorized { get; init; }

    /// <summary>Gets the safe failure to return when authorization is denied.</summary>
    internal HttpReplayFailure? Failure { get; init; }
}
