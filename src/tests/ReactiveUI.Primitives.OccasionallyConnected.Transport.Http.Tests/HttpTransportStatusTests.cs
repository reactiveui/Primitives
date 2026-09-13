// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpTransportStatus"/>.</summary>
public sealed class HttpTransportStatusTests
{
    /// <summary>The retry-after delay in seconds.</summary>
    private const int RetryAfterSeconds = 3;

    /// <summary>The retry-after header name.</summary>
    private const string RetryAfterHeader = "Retry-After";

    /// <summary>Verifies missing retry hints return no bounded delay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetRetryAfterReturnsNullWhenHeaderIsMissing()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

        var retryAfter = HttpTransportStatus.GetRetryAfter(response, TimeProvider.System);

        await Assert.That(retryAfter).IsNull();
    }

    /// <summary>Verifies empty retry hint collections return no bounded delay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetRetryAfterReturnsNullWhenHeaderHasNoValues()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        _ = response.Headers.TryAddWithoutValidation(RetryAfterHeader, []);

        var retryAfter = HttpTransportStatus.GetRetryAfter(response, TimeProvider.System);

        await Assert.That(retryAfter).IsNull();
    }

    /// <summary>Verifies ambiguous retry hint collections return no bounded delay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetRetryAfterReturnsNullWhenHeaderHasMultipleValues()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        _ = response.Headers.TryAddWithoutValidation(RetryAfterHeader, ["1", "2"]);

        var retryAfter = HttpTransportStatus.GetRetryAfter(response, TimeProvider.System);

        await Assert.That(retryAfter).IsNull();
    }

    /// <summary>Verifies one valid retry hint is parsed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetRetryAfterParsesSingleHeaderValue()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        _ = response.Headers.TryAddWithoutValidation(RetryAfterHeader, RetryAfterSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var retryAfter = HttpTransportStatus.GetRetryAfter(response, TimeProvider.System);

        await Assert.That(retryAfter).IsEqualTo(TimeSpan.FromSeconds(RetryAfterSeconds));
    }

    /// <summary>Verifies status codes map to stable failure kinds.</summary>
    /// <param name="statusCode">The status code.</param>
    /// <param name="expected">The expected failure kind.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(HttpStatusCode.Unauthorized, HttpTransportFailureKind.Authentication)]
    [Arguments(HttpStatusCode.Forbidden, HttpTransportFailureKind.AuthorizationDenied)]
    [Arguments(HttpStatusCode.RequestEntityTooLarge, HttpTransportFailureKind.PayloadTooLarge)]
    [Arguments(HttpStatusCode.UnsupportedMediaType, HttpTransportFailureKind.SchemaIncompatible)]
    [Arguments(HttpStatusCode.TooManyRequests, HttpTransportFailureKind.Transient)]
    [Arguments(HttpStatusCode.InternalServerError, HttpTransportFailureKind.Transient)]
    [Arguments(HttpStatusCode.TemporaryRedirect, HttpTransportFailureKind.ProtocolViolation)]
    [Arguments(HttpStatusCode.BadRequest, HttpTransportFailureKind.ValidationRejected)]
    public async Task ClassifyMapsHttpStatusCodes(HttpStatusCode statusCode, HttpTransportFailureKind expected)
    {
        var actual = HttpTransportStatus.Classify(statusCode);

        await Assert.That(actual).IsEqualTo(expected);
    }
}
