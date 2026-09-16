// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Classifies HTTP status codes and retry hints.</summary>
internal static class HttpTransportStatus
{
    /// <summary>The HTTP Too Many Requests status code.</summary>
    internal const HttpStatusCode TooManyRequests = (HttpStatusCode)TooManyRequestsStatusCode;

    /// <summary>The first redirection status code.</summary>
    private const int RedirectionStatusCodeStart = 300;

    /// <summary>The first client error status code.</summary>
    private const int ClientErrorStatusCodeStart = 400;

    /// <summary>The first server error status code.</summary>
    private const int ServerErrorStatusCodeStart = 500;

    /// <summary>The HTTP Too Many Requests status code.</summary>
    private const int TooManyRequestsStatusCode = 429;

    /// <summary>The HTTP Payload Too Large status code.</summary>
    private const int PayloadTooLargeStatusCode = 413;

    /// <summary>Gets a bounded retry hint from a response.</summary>
    /// <param name="response">The HTTP response.</param>
    /// <param name="timeProvider">The clock used to sample observation time.</param>
    /// <returns>The retry hint when present and valid.</returns>
    internal static TimeSpan? GetRetryAfter(HttpResponseMessage response, TimeProvider timeProvider)
    {
        if (!response.Headers.TryGetValues("Retry-After", out var values))
        {
            return null;
        }

        using var enumerator = values.GetEnumerator();
        _ = enumerator.MoveNext();
        var value = enumerator.Current;
        return enumerator.MoveNext() ? null : HttpRetryAfterParser.Parse(value, timeProvider.GetUtcNow());
    }

    /// <summary>Classifies a non-success HTTP status code.</summary>
    /// <param name="statusCode">The status code.</param>
    /// <returns>The failure kind.</returns>
    internal static HttpTransportFailureKind Classify(HttpStatusCode statusCode)
    {
        var numeric = (int)statusCode;
        if (statusCode == HttpStatusCode.Unauthorized)
        {
            return HttpTransportFailureKind.Authentication;
        }

        if (statusCode == HttpStatusCode.Forbidden)
        {
            return HttpTransportFailureKind.AuthorizationDenied;
        }

        if (numeric == PayloadTooLargeStatusCode)
        {
            return HttpTransportFailureKind.PayloadTooLarge;
        }

        if (statusCode == HttpStatusCode.UnsupportedMediaType)
        {
            return HttpTransportFailureKind.SchemaIncompatible;
        }

        if (numeric == TooManyRequestsStatusCode || numeric >= ServerErrorStatusCodeStart)
        {
            return HttpTransportFailureKind.Transient;
        }

        return numeric is >= RedirectionStatusCodeStart and < ClientErrorStatusCodeStart
            ? HttpTransportFailureKind.ProtocolViolation
            : HttpTransportFailureKind.ValidationRejected;
    }
}
