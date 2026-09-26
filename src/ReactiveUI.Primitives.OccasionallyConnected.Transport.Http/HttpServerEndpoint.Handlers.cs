// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Dispatches a body-defined route after path matching.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="requestUri">The request URI.</param>
    /// <param name="expectedMethod">The expected HTTP method.</param>
    /// <param name="handler">The route handler.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private static ValueTask<HttpResponseMessage> DispatchBodyRouteAsync(
        HttpRequestMessage request,
        Uri requestUri,
        HttpMethod expectedMethod,
        Func<HttpRequestMessage, ServerAuthenticatedClient, CancellationToken, ValueTask<HttpResponseMessage>> handler,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        if (GetMethodStatus(request.Method, expectedMethod) != HttpStatusCode.OK)
        {
            return new(CreateResponse(HttpStatusCode.MethodNotAllowed));
        }

        return HasQuery(requestUri)
            ? new(CreateResponse(HttpStatusCode.BadRequest))
            : handler(request, authenticatedClient, cancellationToken);
    }

    /// <summary>Creates a safe response before route work when endpoint admission cannot proceed.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The early response, or <see langword="null"/> when route work may continue.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> or <paramref name="authenticatedClient"/> is <see langword="null"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled before endpoint work starts.</exception>
    private HttpResponseMessage? TryCreateEarlyResponse(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(authenticatedClient);
        cancellationToken.ThrowIfCancellationRequested();

        if (Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }

        return HasTrustedPrincipal(authenticatedClient)
            ? null
            : CreateResponse(HttpStatusCode.Unauthorized);
    }

    /// <summary>Dispatches a validated request inside a server activity parented to the incoming trace context.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> DispatchTracedAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var activity = HttpTraceContext.StartServerActivity(request);
        return await DispatchAsync(request, authenticatedClient, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Dispatches a validated request to its configured route.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private ValueTask<HttpResponseMessage> DispatchAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        var requestUri = request.RequestUri;
        if (requestUri is null)
        {
            return new(CreateResponse(HttpStatusCode.BadRequest));
        }

        var route = TryGetRoute(requestUri);
        if (route is null)
        {
            return new(CreateResponse(HttpStatusCode.BadRequest));
        }

        if (StringComparer.Ordinal.Equals(route, _connectPath))
        {
            return DispatchBodyRouteAsync(request, requestUri, HttpMethod.Post, HandleConnectAsync, authenticatedClient, cancellationToken);
        }

        if (StringComparer.Ordinal.Equals(route, _pushPath))
        {
            return DispatchBodyRouteAsync(request, requestUri, HttpMethod.Post, HandlePushAsync, authenticatedClient, cancellationToken);
        }

        if (StringComparer.Ordinal.Equals(route, _subscribePath))
        {
            return GetMethodStatus(request.Method, HttpMethod.Get) == HttpStatusCode.OK
                ? HandleSubscribeAsync(request, requestUri, authenticatedClient, cancellationToken)
                : new(CreateResponse(HttpStatusCode.MethodNotAllowed));
        }

        if (StringComparer.Ordinal.Equals(route, _snapshotRecoveryPath))
        {
            return DispatchBodyRouteAsync(request, requestUri, HttpMethod.Post, HandleSnapshotRecoveryAsync, authenticatedClient, cancellationToken);
        }

        return StringComparer.Ordinal.Equals(route, _acknowledgePath)
            ? DispatchBodyRouteAsync(request, requestUri, HttpMethod.Post, HandleAcknowledgeAsync, authenticatedClient, cancellationToken)
            : new(CreateResponse(NotFound));
    }

    /// <summary>Handles a connect request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> HandleConnectAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteReplayConnectAsync(request, authenticatedClient, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
    }

    /// <summary>Handles a snapshot recovery request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> HandleSnapshotRecoveryAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await ExecuteReplaySnapshotRecoveryAsync(request, authenticatedClient, cancellationToken).ConfigureAwait(false);
            return result.Response;
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
    }

    /// <summary>Handles a push request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> HandlePushAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await ExecuteReplayPushAsync(request, authenticatedClient, cancellationToken).ConfigureAwait(false);
            return result.Response;
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
    }

    /// <summary>Handles an acknowledgement request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> HandleAcknowledgeAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await ExecuteReplayAcknowledgeAsync(request, authenticatedClient, cancellationToken).ConfigureAwait(false);
            return result.Response;
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
    }

    /// <summary>Handles a subscribe request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="requestUri">The request URI captured before asynchronous endpoint work.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private async ValueTask<HttpResponseMessage> HandleSubscribeAsync(
        HttpRequestMessage request,
        Uri requestUri,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        if (HasBody(request))
        {
            return CreateResponse(HttpStatusCode.BadRequest);
        }

        try
        {
            var result = await ExecuteReplaySubscribeAsync(request, requestUri, authenticatedClient, cancellationToken).ConfigureAwait(false);
            return result.Response;
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
    }
}
