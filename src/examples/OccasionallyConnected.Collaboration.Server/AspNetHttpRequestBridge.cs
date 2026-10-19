// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Http.Features;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Adapts ASP.NET Core requests to the portable occasionally-connected HTTP endpoint.</summary>
[System.Diagnostics.DebuggerDisplay("ASP.NET portable endpoint bridge")]
public sealed class AspNetHttpRequestBridge
{
    /// <summary>Handles an ASP.NET Core request with local development authentication.</summary>
    /// <param name="context">The ASP.NET Core request context.</param>
    /// <param name="credentials">The development credential store.</param>
    /// <param name="dispatch">The portable endpoint dispatcher.</param>
    /// <returns>The asynchronous request operation.</returns>
    public async Task InvokeAsync(
        HttpContext context,
        DevelopmentCredentialStore credentials,
        PortableHttpEndpointDispatch dispatch)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(credentials);
        ArgumentNullException.ThrowIfNull(dispatch);

        if (!TryAuthenticate(context, credentials, out var authenticatedClient))
        {
            context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
            return;
        }

        using var request = CreatePortableRequest(context.Request);
        using var response = await dispatch(request, authenticatedClient, context.RequestAborted).ConfigureAwait(false);
        await CopyResponseAsync(response, context.Response, context.RequestAborted).ConfigureAwait(false);
    }

    /// <summary>Authenticates the request header against the configured development credential store.</summary>
    /// <param name="context">The ASP.NET request context.</param>
    /// <param name="credentials">The configured credential store.</param>
    /// <param name="authenticatedClient">The authenticated client when the token is known.</param>
    /// <returns>Whether authentication succeeded.</returns>
    private static bool TryAuthenticate(
        HttpContext context,
        DevelopmentCredentialStore credentials,
        out ServerAuthenticatedClient authenticatedClient)
    {
        if (context.Request.Headers.TryGetValue(DevelopmentCredentialStore.TokenHeaderName, out var values)
            && values.Count == 1
            && credentials.TryAuthenticate(values[0] ?? string.Empty, out authenticatedClient))
        {
            return true;
        }

        authenticatedClient = new(string.Empty, string.Empty);
        return false;
    }

    /// <summary>Creates the portable request passed to the transport endpoint.</summary>
    /// <param name="source">The ASP.NET Core request.</param>
    /// <returns>The portable request message.</returns>
    private static HttpRequestMessage CreatePortableRequest(HttpRequest source)
    {
        var request = new HttpRequestMessage(new HttpMethod(source.Method), source.GetEncodedUrl());
        if (HasRequestBody(source))
        {
            request.Content = new StreamContent(new NonOwningReadStream(source.Body));
        }

        CopyRequestHeaders(source, request);
        return request;
    }

    /// <summary>Determines whether the request carries a body.</summary>
    /// <param name="source">The ASP.NET Core request.</param>
    /// <returns>Whether request content should be forwarded.</returns>
    private static bool HasRequestBody(HttpRequest source) =>
        source.HttpContext.Features.Get<IHttpRequestBodyDetectionFeature>()?.CanHaveBody
        ?? (source.ContentLength.HasValue || source.Headers.ContainsKey("Transfer-Encoding"));

    /// <summary>Copies ASP.NET request headers onto the portable request.</summary>
    /// <param name="source">The ASP.NET Core request.</param>
    /// <param name="destination">The portable request.</param>
    private static void CopyRequestHeaders(HttpRequest source, HttpRequestMessage destination)
    {
        foreach (var header in source.Headers)
        {
            if (destination.Headers.TryAddWithoutValidation(header.Key, (IEnumerable<string>)header.Value))
            {
                continue;
            }

            _ = destination.Content?.Headers.TryAddWithoutValidation(header.Key, (IEnumerable<string>)header.Value);
        }
    }

    /// <summary>Copies the portable endpoint response back to ASP.NET Core.</summary>
    /// <param name="source">The portable response.</param>
    /// <param name="destination">The ASP.NET Core response.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous copy task.</returns>
    private static async Task CopyResponseAsync(
        HttpResponseMessage source,
        HttpResponse destination,
        CancellationToken cancellationToken)
    {
        destination.StatusCode = (int)source.StatusCode;
        CopyResponseHeaders(source, destination);
        await source.Content.CopyToAsync(destination.Body, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Copies response headers without revalidating values already accepted by the portable endpoint.</summary>
    /// <param name="source">The portable response.</param>
    /// <param name="destination">The ASP.NET Core response.</param>
    private static void CopyResponseHeaders(HttpResponseMessage source, HttpResponse destination)
    {
        foreach (var header in source.Headers)
        {
            destination.Headers[header.Key] = CopyHeaderValues(header.Value);
        }

        foreach (var header in source.Content.Headers)
        {
            destination.Headers[header.Key] = CopyHeaderValues(header.Value);
        }
    }

    /// <summary>Copies enumerable header values into a materialized array for ASP.NET Core.</summary>
    /// <param name="values">The source header values.</param>
    /// <returns>The copied header values.</returns>
    private static string[] CopyHeaderValues(IEnumerable<string> values)
    {
        var copy = new List<string>(values);
        return copy.ToArray();
    }
}
