// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolContent"/>.</summary>
public sealed class HttpProtocolContentTests
{
    /// <summary>The protocol media type.</summary>
    private const string ProtocolMediaType = "application/vnd.reactiveui.occasionally-connected+json;v=1";

    /// <summary>The protocol media type with a missing version value.</summary>
    private const string MissingVersionValueMediaType = "application/vnd.reactiveui.occasionally-connected+json;v";

    /// <summary>The response body.</summary>
    private const string BodyText = "{}";

    /// <summary>A shared HTTP client for option instances.</summary>
    private static readonly HttpClient SharedHttpClient = new();

    /// <summary>Verifies declared content lengths above the configured bound are rejected before reading.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadBoundedBytesAsyncRejectsDeclaredLengthAboveLimit()
    {
        using var response = CreateResponse(BodyText);
        var options = CreateOptions(maximumResponseBytes: 1);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await HttpProtocolContent.ReadBoundedBytesAsync(response, options, CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(exception.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies a body exactly at the configured bound is accepted after the EOF probe.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadBoundedBytesAsyncAcceptsBodyExactlyAtLimit()
    {
        using var response = CreateResponse(BodyText);
        var options = CreateOptions(Encoding.UTF8.GetByteCount(BodyText));

        var bytes = await HttpProtocolContent.ReadBoundedBytesAsync(response, options, CancellationToken.None);

        await Assert.That(Encoding.UTF8.GetString(bytes)).IsEqualTo(BodyText);
    }

    /// <summary>Verifies a protocol version parameter without a value is rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadBoundedBytesAsyncRejectsMissingVersionValue()
    {
        using var response = CreateResponse(BodyText, MissingVersionValueMediaType);
        var options = CreateOptions(Encoding.UTF8.GetByteCount(BodyText));

        var exception = await CaptureHttpExceptionAsync(async () => _ = await HttpProtocolContent.ReadBoundedBytesAsync(response, options, CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Creates a protocol response.</summary>
    /// <param name="body">The response body.</param>
    /// <param name="mediaType">The content type.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateResponse(string body, string mediaType = ProtocolMediaType)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body)) };
        response.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(mediaType);
        return response;
    }

    /// <summary>Creates options with a bounded response size.</summary>
    /// <param name="maximumResponseBytes">The maximum response bytes.</param>
    /// <returns>The options.</returns>
    private static HttpRemoteTransportOptions CreateOptions(int maximumResponseBytes) =>
        new() { HttpClient = SharedHttpClient, BaseAddress = new("https://example.invalid/oc/"), MaximumResponseBytes = maximumResponseBytes };

    /// <summary>Captures a typed HTTP exception from an asynchronous action.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static async Task<HttpRemoteTransportException> CaptureHttpExceptionAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (HttpRemoteTransportException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected an HTTP transport exception.");
    }
}
