// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Adapts common protocol requests to cancellation-aware single-result SignalR streaming RPCs.</summary>
/// <param name="connection">The borrowed connection.</param>
internal sealed class SignalRRpcHandler(HubConnection connection) : HttpMessageHandler
{
    /// <summary>The minimum valid response status.</summary>
    private const int MinimumStatusCode = 100;

    /// <summary>The maximum valid response status.</summary>
    private const int MaximumStatusCode = 599;

    /// <inheritdoc/>
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (connection.State != HubConnectionState.Connected)
        {
            throw new HttpRequestException("The SignalR connection is disconnected.");
        }

        var body = request.Content is null
            ? []
            : await request.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        var carrier = SignalRCarrier.Encode(new(
            request.Method.Method,
            request.RequestUri!.PathAndQuery,
            0,
            SignalRCarrier.GetHeaders(request.Headers, request.Content),
            body));
        using var invocation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            var reader = await connection.StreamAsChannelCoreAsync(
                "Exchange",
                typeof(byte[]),
                [carrier],
                invocation.Token).ConfigureAwait(false);
            if (!await reader.WaitToReadAsync(invocation.Token).ConfigureAwait(false)
                || !reader.TryRead(out var value)
                || value is not byte[] responseBytes)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            return DecodeResponse(responseBytes);
        }
        catch (HubException)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }
        catch (IOException exception)
        {
            throw new HttpRequestException("The SignalR connection failed during the invocation.", exception);
        }
        catch (InvalidOperationException exception) when (connection.State != HubConnectionState.Connected)
        {
            throw new HttpRequestException("The SignalR connection closed during the invocation.", exception);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new HttpRequestException("The SignalR connection closed during the invocation.");
        }
        finally
        {
            await invocation.CancelAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Validates and reconstructs a source-generated protocol response.</summary>
    /// <param name="bytes">The received carrier.</param>
    /// <returns>The owned response.</returns>
    /// <exception cref="HttpRemoteTransportException">The response shape is invalid.</exception>
    private static HttpResponseMessage DecodeResponse(byte[] bytes)
    {
        var carrier = SignalRCarrier.Decode(bytes);
        if (carrier.StatusCode is < MinimumStatusCode or > MaximumStatusCode
            || carrier.Method is not null || carrier.PathAndQuery is not null)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        var response = new HttpResponseMessage((HttpStatusCode)carrier.StatusCode)
        { Content = new ByteArrayContent(carrier.Body) };
        SignalRCarrier.SetHeaders(carrier.Headers, response.Headers, response.Content);
        return response;
    }
}
