// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Describes the outcome of an owner execution for replay retention.</summary>
internal sealed record HttpReplayCompletion
{
    /// <summary>Gets the HTTP status code produced by the endpoint.</summary>
    internal required HttpStatusCode StatusCode { get; init; }

    /// <summary>Gets the optional response content type.</summary>
    internal string? ContentType { get; init; }

    /// <summary>Gets the response body bytes to retain when they fit the replay cache.</summary>
    internal ReadOnlyMemory<byte> ResponseBytes { get; init; }

    /// <summary>Gets whether the operation failed before domain effects.</summary>
    internal bool FailedBeforeEffect { get; init; }

    /// <summary>Gets the replay session issued by a successful connect response.</summary>
    internal HttpReplayIssuedSession? ConnectSession { get; init; }
}
