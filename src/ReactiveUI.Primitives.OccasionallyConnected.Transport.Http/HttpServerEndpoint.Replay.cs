// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles replay authentication helpers for the portable HTTP server endpoint.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Creates a response for a non-executing replay decision.</summary>
    /// <param name="decision">The replay decision.</param>
    /// <param name="response">The response, when available.</param>
    /// <param name="owner">The non-null execution owner when no response was created.</param>
    /// <returns>Whether a response was created.</returns>
    private static bool TryCreateReplayDecisionResponse(
        HttpReplayDecision decision,
        [NotNullWhen(true)] out HttpResponseMessage? response,
        [NotNullWhen(false)] out HttpReplayOwner? owner)
    {
        if (decision.Kind == HttpReplayAdmissionKind.ReplayCached && decision.CachedResponse is not null)
        {
            response = CreateCachedReplayResponse(decision.CachedResponse);
            owner = null;
            return true;
        }

        if (decision.Kind == HttpReplayAdmissionKind.Execute && decision.Owner is not null)
        {
            response = null;
            owner = decision.Owner;
            return false;
        }

        response = CreateReplayFailureResponse(decision.Failure);
        owner = null;
        return true;
    }

    /// <summary>Creates authorization details for a push batch.</summary>
    /// <param name="batch">The decoded batch.</param>
    /// <returns>The authorization details.</returns>
    private static ReplayAuthorizationDetails CreatePushAuthorizationDetails(SyncBatch batch)
    {
        var streamIds = new StreamId[batch.Operations.Count];
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            streamIds[index] = batch.Operations[index].StreamId;
        }

        return new(streamIds, null);
    }

    /// <summary>Creates authorization details for an acknowledgement.</summary>
    /// <param name="acknowledgement">The decoded acknowledgement.</param>
    /// <returns>The authorization details.</returns>
    private static ReplayAuthorizationDetails CreateAcknowledgementAuthorizationDetails(ReceiveAcknowledgement acknowledgement) =>
        new([acknowledgement.StreamId], acknowledgement.SubscriptionId);

    /// <summary>Creates authorization details for a subscribe request.</summary>
    /// <param name="request">The decoded subscribe request.</param>
    /// <returns>The authorization details.</returns>
    private static ReplayAuthorizationDetails CreateSubscribeAuthorizationDetails(RemoteSubscribeRequest request) =>
        new([request.StreamId], request.SubscriptionId);

    /// <summary>Creates authorization details for a snapshot recovery request.</summary>
    /// <param name="request">The decoded snapshot recovery request.</param>
    /// <returns>The authorization details.</returns>
    private static ReplayAuthorizationDetails CreateSnapshotRecoveryAuthorizationDetails(RemoteSnapshotRecoveryRequest request) =>
        new([request.StreamId], request.SubscriptionId);

    /// <summary>Gets one required replay header value.</summary>
    /// <param name="request">The request.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The header value.</returns>
    /// <exception cref="HttpRemoteTransportException">The header is missing or duplicated.</exception>
    private static string GetRequiredReplayHeader(HttpRequestMessage request, string name)
    {
        var count = HttpReplayHeaders.ReadValueCount(request.Headers, name, out var value);
        return count != 1
            ? throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest)
            : value!;
    }

    /// <summary>Executes a replay-protected connect request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The caller-owned response.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay admission or protocol validation fails.</exception>
    private async ValueTask<HttpResponseMessage> ExecuteReplayConnectAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var requestToken = requestSource.Token;
        using var lease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
        var body = await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false);
        var connect = _codec.DeserializeConnectRequest(body);
        if (!string.Equals(connect.Client.ClientId, authenticatedClient.ClientId, StringComparison.Ordinal))
        {
            return CreateResponse(HttpStatusCode.Forbidden);
        }

        var replayRequest = CreateReplayRequest(request, HttpReplayOperationKind.Connect, authenticatedClient, _connectPath, [], body);
        var decision = await AdmitReplayAsync(replayRequest, authenticatedClient, ReplayAuthorizationDetails.Empty, requestToken).ConfigureAwait(false);
        if (TryCreateReplayDecisionResponse(decision, out var replayResponse, out var owner))
        {
            return replayResponse;
        }

        try
        {
            HttpRemoteTransportCapabilities.ValidateNegotiation(connect, _advertisedCapabilities, SupportedCapabilities);
            return await CompleteReplayConnectAsync(owner, authenticatedClient, requestToken).ConfigureAwait(false);
        }
        catch
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
    }

    /// <summary>Executes a replay-protected push request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The push execution result.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay admission or protocol validation fails.</exception>
    private async ValueTask<ReplayRouteExecution> ExecuteReplayPushAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var requestToken = requestSource.Token;
        using var lease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
        var body = await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false);
        var batch = _codec.DeserializePushRequest(body);
        var details = CreatePushAuthorizationDetails(batch);
        var replayRequest = CreateReplayRequest(request, HttpReplayOperationKind.Push, authenticatedClient, _pushPath, [], body);
        var decision = await AdmitReplayAsync(replayRequest, authenticatedClient, details, requestToken).ConfigureAwait(false);
        if (TryCreateReplayDecisionResponse(decision, out var replayResponse, out var owner))
        {
            return new(replayResponse);
        }

        try
        {
            var result = await _hub.ApplyOperationsAsync(batch, authenticatedClient, requestToken).ConfigureAwait(false);
            var responseBytes = _codec.SerializePushResponse(batch, result.Result);
            _ = await CompleteReplayOwnerAsync(owner, HttpStatusCode.OK, HttpProtocolContent.MediaType, responseBytes).ConfigureAwait(false);
            return new(CreateProtocolResponse(responseBytes));
        }
        catch (Exception) when (!requestToken.IsCancellationRequested || _shutdown.IsCancellationRequested)
        {
            _replayCoordinator.Abandon(owner);
            return new(CreateAmbiguousResponse());
        }
        catch
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
    }

    /// <summary>Executes a replay-protected acknowledgement request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The acknowledgement execution result.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay admission or protocol validation fails.</exception>
    private async ValueTask<ReplayRouteExecution> ExecuteReplayAcknowledgeAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var requestToken = requestSource.Token;
        using var lease = await _acknowledgementGate.EnterAsync(requestToken).ConfigureAwait(false);
        var body = await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false);
        var acknowledgement = _codec.DeserializeAcknowledgement(body);
        var details = CreateAcknowledgementAuthorizationDetails(acknowledgement);
        var replayRequest = CreateReplayRequest(request, HttpReplayOperationKind.Acknowledge, authenticatedClient, _acknowledgePath, [], body);
        var decision = await AdmitReplayAsync(replayRequest, authenticatedClient, details, requestToken).ConfigureAwait(false);
        if (TryCreateReplayDecisionResponse(decision, out var replayResponse, out var owner))
        {
            return new(replayResponse);
        }

        try
        {
            await _hub.AcknowledgeAsync(acknowledgement, authenticatedClient, requestToken).ConfigureAwait(false);
            _ = await CompleteReplayOwnerAsync(owner, HttpStatusCode.NoContent, contentType: null, ReadOnlyMemory<byte>.Empty).ConfigureAwait(false);
            return new(CreateResponse(HttpStatusCode.NoContent));
        }
        catch (Exception) when (!requestToken.IsCancellationRequested || _shutdown.IsCancellationRequested)
        {
            _replayCoordinator.Abandon(owner);
            return new(CreateAmbiguousResponse());
        }
        catch
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
    }

    /// <summary>Executes a replay-protected subscribe request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="requestUri">The request URI captured before asynchronous endpoint work.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The subscribe execution result.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay admission or protocol validation fails.</exception>
    private async ValueTask<ReplayRouteExecution> ExecuteReplaySubscribeAsync(
        HttpRequestMessage request,
        Uri requestUri,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var requestToken = requestSource.Token;
        using var requestLease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
        using var subscriptionLease = await _subscriptionGate.EnterAsync(requestToken).ConfigureAwait(false);
        var parsedSubscribe = ParseSubscribeRequest(requestUri);
        var subscribe = parsedSubscribe.Request;
        var details = CreateSubscribeAuthorizationDetails(subscribe);
        var replayRequest = CreateReplayRequest(request, HttpReplayOperationKind.Subscribe, authenticatedClient, _subscribePath, parsedSubscribe.QueryFields, []);
        var decision = await AdmitReplayAsync(replayRequest, authenticatedClient, details, requestToken).ConfigureAwait(false);
        if (TryCreateReplayDecisionResponse(decision, out var replayResponse, out var owner))
        {
            return new(replayResponse);
        }

        var effectsPossible = false;
        try
        {
            IReadOnlyList<RemoteEventBatch> batches;
            await using (var deadline = await PollDeadline.StartAsync(_timeProvider, _longPollTimeout, requestToken).ConfigureAwait(false))
            {
                effectsPossible = true;
                batches = await ReadOneBatchAsync(subscribe, authenticatedClient, deadline.Token).ConfigureAwait(false);
            }

            return await CompleteReplaySubscribeAsync(owner, batches).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!effectsPossible)
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
        catch (OperationCanceledException) when (Volatile.Read(ref _disposed) != 0)
        {
            return AbandonReplayWithResponse(owner, ServiceUnavailable);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return await CompleteReplayNoContentAsync(owner).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return AbandonReplayAmbiguous(owner);
        }
        catch (Exception) when (!effectsPossible)
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
        catch
        {
            return AbandonReplayAmbiguous(owner);
        }
    }

    /// <summary>Executes a replay-protected snapshot recovery request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The snapshot recovery execution result.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay admission or protocol validation fails.</exception>
    private async ValueTask<ReplayRouteExecution> ExecuteReplaySnapshotRecoveryAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        using var requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        var requestToken = requestSource.Token;
        using var lease = await _requestGate.EnterAsync(requestToken).ConfigureAwait(false);
        var body = await ReadRequiredBodyAsync(request, requestToken).ConfigureAwait(false);
        var recoveryRequest = _codec.DeserializeSnapshotRecoveryRequest(body);
        var details = CreateSnapshotRecoveryAuthorizationDetails(recoveryRequest);
        var replayRequest = CreateReplayRequest(request, HttpReplayOperationKind.SnapshotRecovery, authenticatedClient, _snapshotRecoveryPath, [], body);
        var decision = await AdmitReplayAsync(replayRequest, authenticatedClient, details, requestToken).ConfigureAwait(false);
        if (TryCreateReplayDecisionResponse(decision, out var replayResponse, out var owner))
        {
            return new(replayResponse);
        }

        if (_snapshotRecoveryHub is null)
        {
            _replayCoordinator.Abandon(owner);
            throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible, HttpStatusCode.NotFound);
        }

        try
        {
            var result = await _snapshotRecoveryHub.GetSnapshotAsync(recoveryRequest, authenticatedClient, requestToken).ConfigureAwait(false);
            var responseBytes = _codec.SerializeSnapshotRecoveryResponse(recoveryRequest, result);
            _ = await CompleteReplayOwnerAsync(owner, HttpStatusCode.OK, HttpProtocolContent.MediaType, responseBytes).ConfigureAwait(false);
            return new(CreateProtocolResponse(responseBytes));
        }
        catch (HttpRemoteTransportException exception) when (!exception.IsTransient)
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
        catch (Exception) when (!requestToken.IsCancellationRequested || _shutdown.IsCancellationRequested)
        {
            _replayCoordinator.Abandon(owner);
            return new(CreateAmbiguousResponse());
        }
        catch
        {
            _replayCoordinator.Abandon(owner);
            throw;
        }
    }

    /// <summary>Completes a replay owner with a subscribe response.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="batches">The subscribed batches.</param>
    /// <returns>The route execution result.</returns>
    private async ValueTask<ReplayRouteExecution> CompleteReplaySubscribeAsync(HttpReplayOwner owner, IReadOnlyList<RemoteEventBatch> batches)
    {
        var responseBytes = batches.Count == 0 ? [] : _codec.SerializeSubscribeResponse(batches);
        var statusCode = batches.Count == 0 ? HttpStatusCode.NoContent : HttpStatusCode.OK;
        var contentType = batches.Count == 0 ? null : HttpProtocolContent.MediaType;
        _ = await CompleteReplayOwnerAsync(owner, statusCode, contentType, responseBytes).ConfigureAwait(false);
        return new(batches.Count == 0 ? CreateResponse(HttpStatusCode.NoContent) : CreateProtocolResponse(responseBytes));
    }

    /// <summary>Completes a replay owner with a no-content subscribe timeout response.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <returns>The route execution result.</returns>
    private async ValueTask<ReplayRouteExecution> CompleteReplayNoContentAsync(HttpReplayOwner owner)
    {
        _ = await CompleteReplayOwnerAsync(owner, HttpStatusCode.NoContent, contentType: null, ReadOnlyMemory<byte>.Empty).ConfigureAwait(false);
        return new(CreateResponse(HttpStatusCode.NoContent));
    }

    /// <summary>Abandons a replay owner and returns an explicit status response.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="statusCode">The response status code.</param>
    /// <returns>The route execution result.</returns>
    private ReplayRouteExecution AbandonReplayWithResponse(HttpReplayOwner owner, HttpStatusCode statusCode)
    {
        _replayCoordinator.Abandon(owner);
        return new(CreateResponse(statusCode));
    }

    /// <summary>Abandons a replay owner and returns an ambiguous response.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <returns>The route execution result.</returns>
    private ReplayRouteExecution AbandonReplayAmbiguous(HttpReplayOwner owner)
    {
        _replayCoordinator.Abandon(owner);
        return new(CreateAmbiguousResponse());
    }

    /// <summary>Admits one replay request.</summary>
    /// <param name="replayRequest">The replay request.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="details">The decoded authorization details.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The admission decision.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<HttpReplayDecision> AdmitReplayAsync(
        HttpReplayRequest replayRequest,
        ServerAuthenticatedClient authenticatedClient,
        ReplayAuthorizationDetails details,
        CancellationToken cancellationToken) =>
        _replayCoordinator.AdmitAsync(
            replayRequest,
            token => AuthorizeReplayAsync(authenticatedClient, replayRequest.Operation, details, token),
            cancellationToken);

    /// <summary>Completes a replay connect execution.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The connect response.</returns>
    private async ValueTask<HttpResponseMessage> CompleteReplayConnectAsync(
        HttpReplayOwner owner,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var responseBytes = _codec.SerializeConnectResponse(_advertisedCapabilities);
        var observedUtc = _replayTimeProvider.GetUtcNow();
        var issued = _replayCoordinator.Sessions.CreatePending(new(authenticatedClient.TenantId, authenticatedClient.ClientId), observedUtc);
        var response = CreateProtocolResponse(responseBytes);
        try
        {
            response.Headers.Add(HttpReplayHeaders.TenantId, HttpReplayBase64Url.Encode(Encoding.UTF8.GetBytes(issued.TenantId)));
            response.Headers.Add(HttpReplayHeaders.SessionId, issued.SessionId);
            response.Headers.Add(HttpReplayHeaders.SessionSecret, issued.SessionSecret);
            response.Headers.Add(HttpReplayHeaders.SessionExpires, issued.ExpiresAtUtc.ToString("O", CultureInfo.InvariantCulture));
            var failureStatus = await _replayCoordinator.CompleteAsync(
                owner,
                new() { StatusCode = HttpStatusCode.OK, ContentType = HttpProtocolContent.MediaType, ResponseBytes = responseBytes, ConnectSession = issued }).ConfigureAwait(false);
            if (failureStatus is not null)
            {
                response.Dispose();
                return CreateReplayFailureResponse(new(failureStatus.Value, HttpTransportFailureKind.Transient));
            }

            return response;
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    /// <summary>Completes a non-connect replay owner.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="statusCode">The response status code.</param>
    /// <param name="contentType">The optional content type.</param>
    /// <param name="responseBytes">The response body bytes.</param>
    /// <returns>The registration failure status, when connect session registration failed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<HttpStatusCode?> CompleteReplayOwnerAsync(
        HttpReplayOwner owner,
        HttpStatusCode statusCode,
        string? contentType,
        ReadOnlyMemory<byte> responseBytes) =>
        _replayCoordinator.CompleteAsync(owner, new() { StatusCode = statusCode, ContentType = contentType, ResponseBytes = responseBytes });

    /// <summary>Creates a replay request from validated endpoint material.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="operation">The replay operation.</param>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="relativePath">The canonical relative path.</param>
    /// <param name="query">The canonical query fields.</param>
    /// <param name="body">The exact request body.</param>
    /// <returns>The replay request.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay headers are missing or invalid.</exception>
    private HttpReplayRequest CreateReplayRequest(
        HttpRequestMessage request,
        HttpReplayOperationKind operation,
        ServerAuthenticatedClient authenticatedClient,
        string relativePath,
        IReadOnlyList<KeyValuePair<string, string>> query,
        byte[] body)
    {
        var messageId = GetRequiredReplayHeader(request, HttpReplayHeaders.MessageId);
        var nonce = GetRequiredReplayHeader(request, HttpReplayHeaders.Nonce);
        var sentAtText = GetRequiredReplayHeader(request, HttpReplayHeaders.SentAt);
        if (!DateTimeOffset.TryParseExact(sentAtText, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out var sentAtUtc)
            || sentAtUtc.Offset != TimeSpan.Zero)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        var sessionId = operation == HttpReplayOperationKind.Connect ? null : GetRequiredReplayHeader(request, HttpReplayHeaders.SessionId);
        var mac = operation == HttpReplayOperationKind.Connect ? null : GetRequiredReplayHeader(request, HttpReplayHeaders.Mac);
        var canonical = _canonicalRequestBuilder.Build(operation, request.Method.Method, relativePath, query, sentAtUtc, body);
        return new()
        {
            Operation = operation,
            Principal = new(authenticatedClient.TenantId, authenticatedClient.ClientId),
            MessageId = messageId,
            Nonce = nonce,
            SentAtUtc = sentAtUtc,
            ReplaySessionId = sessionId,
            ReplayMac = mac,
            CanonicalRequest = canonical,
        };
    }

    /// <summary>Authorizes one replay admission attempt through the host callback.</summary>
    /// <param name="authenticatedClient">The authenticated principal.</param>
    /// <param name="operation">The replay operation.</param>
    /// <param name="details">The decoded request details.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The authorization result.</returns>
    private async ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(
        ServerAuthenticatedClient authenticatedClient,
        HttpReplayOperationKind operation,
        ReplayAuthorizationDetails details,
        CancellationToken cancellationToken)
    {
        var context = new HttpReplayAuthorizationContext { Client = authenticatedClient, Operation = operation.ToString(), StreamIds = details.StreamIds, SubscriptionId = details.SubscriptionId };
        var authorized = await _replayAuthorizer.AuthorizeReplayAsync(context, cancellationToken).ConfigureAwait(false);
        return authorized
            ? HttpReplayAuthorizationResult.Allowed
            : new() { IsAuthorized = false, Failure = new(HttpStatusCode.Forbidden, HttpTransportFailureKind.AuthorizationDenied) };
    }

    /// <summary>Captures a replay-protected route response.</summary>
    /// <param name="Response">The response.</param>
    private sealed record ReplayRouteExecution(HttpResponseMessage Response);

    /// <summary>Captures decoded authorization details for replay authorization.</summary>
    /// <param name="StreamIds">The decoded stream identifiers.</param>
    /// <param name="SubscriptionId">The decoded subscription identifier.</param>
    private sealed record ReplayAuthorizationDetails(IReadOnlyList<StreamId> StreamIds, SubscriptionId? SubscriptionId)
    {
        /// <summary>Gets empty authorization details.</summary>
        internal static ReplayAuthorizationDetails Empty { get; } = new([], null);
    }
}
