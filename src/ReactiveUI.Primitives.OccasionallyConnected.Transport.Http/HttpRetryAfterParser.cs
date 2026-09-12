// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http.Headers;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Parses bounded HTTP Retry-After lower-bound hints.</summary>
internal static class HttpRetryAfterParser
{
    /// <summary>The inclusive maximum raw header length accepted by this parser.</summary>
    private const int MaximumHeaderLength = 128;

    /// <summary>Parses an HTTP Retry-After header according to RFC 9110 section 10.2.3.</summary>
    /// <param name="rawRetryAfter">The single raw Retry-After header value.</param>
    /// <param name="observedUtc">The caller-sampled UTC time at which the header was observed.</param>
    /// <returns>The server retry lower bound, zero for a past HTTP-date, or <see langword="null"/> when invalid.</returns>
    /// <remarks>Delay-seconds are accepted only within the range supported by <see cref="RetryConditionHeaderValue.TryParse(string, out RetryConditionHeaderValue?)"/>.</remarks>
    internal static TimeSpan? Parse(string? rawRetryAfter, DateTimeOffset observedUtc)
    {
        if (rawRetryAfter is null)
        {
            return null;
        }

        if (rawRetryAfter.Length == 0 || rawRetryAfter.Length > MaximumHeaderLength)
        {
            return null;
        }

        if (!RetryConditionHeaderValue.TryParse(rawRetryAfter, out var retryCondition))
        {
            return null;
        }

        ArgumentExceptionHelper.ThrowIfNull(retryCondition);

        if (retryCondition.Delta is { } delay)
        {
            return delay;
        }

        var remaining = retryCondition.Date.GetValueOrDefault() - observedUtc;
        return remaining < TimeSpan.Zero ? TimeSpan.Zero : remaining;
    }
}
