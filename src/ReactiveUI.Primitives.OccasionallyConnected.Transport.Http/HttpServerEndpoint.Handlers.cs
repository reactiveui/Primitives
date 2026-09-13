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
            return GetMethodStatus(request.Method, HttpMethod.Post) == HttpStatusCode.OK
                ? HandleConnectAsync(request, authenticatedClient, cancellationToken)
                : new(CreateResponse(HttpStatusCode.MethodNotAllowed));
        }

        if (StringComparer.Ordinal.Equals(route, _pushPath))
        {
            return GetMethodStatus(request.Method, HttpMethod.Post) == HttpStatusCode.OK
                ? HandlePushAsync(request, authenticatedClient, cancellationToken)
                : new(CreateResponse(HttpStatusCode.MethodNotAllowed));
        }

        if (StringComparer.Ordinal.Equals(route, _subscribePath))
        {
            return GetMethodStatus(request.Method, HttpMethod.Get) == HttpStatusCode.OK
                ? HandleSubscribeAsync(request, requestUri, authenticatedClient, cancellationToken)
                : new(CreateResponse(HttpStatusCode.MethodNotAllowed));
        }

        return StringComparer.Ordinal.Equals(route, _acknowledgePath)
            ? DispatchAcknowledgementAsync(request, authenticatedClient, cancellationToken)
            : new(CreateResponse(NotFound));
    }

    /// <summary>Dispatches an acknowledgement route after path matching.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    private ValueTask<HttpResponseMessage> DispatchAcknowledgementAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken) =>
        GetMethodStatus(request.Method, HttpMethod.Post) == HttpStatusCode.OK
            ? HandleAcknowledgeAsync(request, authenticatedClient, cancellationToken)
            : new(CreateResponse(HttpStatusCode.MethodNotAllowed));

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
            using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            var requestToken = requestSource.Token;
            using var lease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
            var connect = _codec.DeserializeConnectRequest(await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false));
            if (!string.Equals(connect.Client.ClientId, authenticatedClient.ClientId, StringComparison.Ordinal))
            {
                return CreateResponse(HttpStatusCode.Forbidden);
            }

            HttpRemoteTransportCapabilities.ValidateNegotiation(connect, _advertisedCapabilities, SupportedCapabilities);
            return CreateProtocolResponse(_codec.SerializeConnectResponse(_advertisedCapabilities));
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception, effectsPossible: false);
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
        var effectsPossible = false;
        try
        {
            using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            var requestToken = requestSource.Token;
            using var lease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
            var batch = _codec.DeserializePushRequest(await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false));
            effectsPossible = true;
            var result = await _hub.ApplyOperationsAsync(batch, authenticatedClient, requestToken).ConfigureAwait(false);
            return CreateProtocolResponse(_codec.SerializePushResponse(batch, result.Result));
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception, effectsPossible);
        }
        catch (ObjectDisposedException) when (!effectsPossible)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!effectsPossible && !cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (effectsPossible)
        {
            return CreateAmbiguousResponse();
        }
        catch (Exception) when (effectsPossible)
        {
            return CreateAmbiguousResponse();
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
        var effectsPossible = false;
        try
        {
            using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            var requestToken = requestSource.Token;
            using var lease = await _acknowledgementGate.EnterAsync(requestToken).ConfigureAwait(false);
            var acknowledgement = _codec.DeserializeAcknowledgement(await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false));
            effectsPossible = true;
            await _hub.AcknowledgeAsync(acknowledgement, authenticatedClient, requestToken).ConfigureAwait(false);
            return CreateResponse(HttpStatusCode.NoContent);
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception, effectsPossible);
        }
        catch (ObjectDisposedException) when (!effectsPossible)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!effectsPossible && !cancellationToken.IsCancellationRequested && Volatile.Read(ref _disposed) != 0)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (effectsPossible)
        {
            return CreateAmbiguousResponse();
        }
        catch (Exception) when (effectsPossible)
        {
            return CreateAmbiguousResponse();
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

        var effectsPossible = false;
        try
        {
            using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            var requestToken = requestSource.Token;
            using var requestLease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
            using var subscriptionLease = await _subscriptionGate.EnterAsync(requestToken).ConfigureAwait(false);
            var subscribe = ParseSubscribeRequest(requestUri);
            IReadOnlyList<RemoteEventBatch> batches;
            await using (var deadline = await PollDeadline.StartAsync(_timeProvider, _longPollTimeout, requestToken).ConfigureAwait(false))
            {
                effectsPossible = true;
                batches = await ReadOneBatchAsync(subscribe, authenticatedClient, deadline.Token).ConfigureAwait(false);
            }

            return batches.Count == 0 ? CreateResponse(HttpStatusCode.NoContent) : CreateProtocolResponse(_codec.SerializeSubscribeResponse(batches));
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateErrorResponse(exception, effectsPossible);
        }
        catch (ObjectDisposedException)
        {
            return CreateResponse(ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!effectsPossible)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return CreateAmbiguousResponse();
        }
        catch (OperationCanceledException)
        {
            return Volatile.Read(ref _disposed) != 0 ? CreateResponse(ServiceUnavailable) : CreateResponse(HttpStatusCode.NoContent);
        }
        catch (Exception) when (effectsPossible)
        {
            return CreateAmbiguousResponse();
        }
    }
}
