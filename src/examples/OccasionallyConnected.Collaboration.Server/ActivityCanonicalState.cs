// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Represents the server-canonical activity stream state.</summary>
internal sealed record ActivityCanonicalState
{
    /// <summary>Gets the accepted activity status.</summary>
    public string Status { get; init; } = string.Empty;

    /// <summary>Gets the accepted activity title, when supplied.</summary>
    public string? Title { get; init; }

    /// <summary>Gets the accepted activity details, when supplied.</summary>
    public string? Details { get; init; }

    /// <summary>Gets the trusted client identifier that submitted the accepted activity.</summary>
    public string AcceptedClientId { get; init; } = string.Empty;

    /// <summary>Gets the accepted operation identifier.</summary>
    public string AcceptedOperationId { get; init; } = string.Empty;

    /// <summary>Gets the server-assigned version produced for the accepted activity.</summary>
    public string AcceptedVersion { get; init; } = string.Empty;

    /// <summary>Gets the server-side acceptance timestamp.</summary>
    public string ServerAcceptedUtc { get; init; } = string.Empty;
}
