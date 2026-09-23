// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents the result of HTTP replay admission.</summary>
internal sealed record HttpReplayDecision
{
    /// <summary>Default safe failure for incomplete replay decisions.</summary>
    private static readonly HttpReplayFailure DefaultFailure = new(HttpStatusCode.ServiceUnavailable, HttpTransportFailureKind.Transient);

    /// <summary>Gets the replay admission kind.</summary>
    internal required HttpReplayAdmissionKind Kind { get; init; }

    /// <summary>Gets the first-execution owner when the request should execute.</summary>
    internal HttpReplayOwner? Owner { get; init; }

    /// <summary>Gets the cached response for a byte-identical replay.</summary>
    internal HttpReplayCachedResponse? CachedResponse { get; init; }

    /// <summary>Gets the safe replay failure.</summary>
    internal HttpReplayFailure Failure { get; init; } = DefaultFailure;
}
