// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Describes a canonical HTTP request seeking replay admission.</summary>
internal sealed record HttpReplayRequest
{
    /// <summary>Gets the protected HTTP operation kind.</summary>
    internal required HttpReplayOperationKind Operation { get; init; }

    /// <summary>Gets the trusted authenticated request owner.</summary>
    internal required HttpReplayPrincipal Principal { get; init; }

    /// <summary>Gets the replay message identifier.</summary>
    internal required string MessageId { get; init; }

    /// <summary>Gets the replay nonce.</summary>
    internal required string Nonce { get; init; }

    /// <summary>Gets the replay timestamp covered by the envelope.</summary>
    internal required DateTimeOffset SentAtUtc { get; init; }

    /// <summary>Gets the replay session identifier for non-connect operations.</summary>
    internal string? ReplaySessionId { get; init; }

    /// <summary>Gets the replay MAC for non-connect operations.</summary>
    internal string? ReplayMac { get; init; }

    /// <summary>Gets the owned canonical request bytes and hashes.</summary>
    internal required HttpCanonicalRequest CanonicalRequest { get; init; }
}
