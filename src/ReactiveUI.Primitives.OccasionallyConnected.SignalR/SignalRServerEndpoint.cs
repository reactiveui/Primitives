// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Composes the authorized, bounded protocol endpoint with an authenticated SignalR connection.</summary>
[System.Diagnostics.DebuggerDisplay("{_endpoint.DeclaredCapabilities}")]
public sealed class SignalRServerEndpoint : IAsyncDisposable
{
    /// <summary>The owned protocol endpoint, which borrows its core server hub.</summary>
    private readonly HttpServerEndpoint _endpoint;

    /// <summary>Maps a host-authenticated principal to the trusted server identity.</summary>
    private readonly Func<ClaimsPrincipal, ServerAuthenticatedClient?> _authenticate;

    /// <summary>Initializes a new instance of the <see cref="SignalRServerEndpoint"/> class.</summary>
    /// <param name="options">Core endpoint composition and security limits.</param>
    /// <param name="authenticate">Host mapping of authenticated claims, never client request fields.</param>
    public SignalRServerEndpoint(
        HttpServerEndpointOptions options,
        Func<ClaimsPrincipal, ServerAuthenticatedClient?> authenticate)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(authenticate);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(options.MaximumRequestBytes, SignalRCarrier.MaximumBodyBytes);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(options.MaximumResponseBytes, SignalRCarrier.MaximumBodyBytes);
        _endpoint = new(options);
        _authenticate = authenticate;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _endpoint.DisposeAsync();

    /// <summary>Dispatches one RPC to the common server protocol implementation.</summary>
    /// <param name="bytes">The bounded carrier.</param>
    /// <param name="principal">The host-authenticated connection principal.</param>
    /// <param name="cancellationToken">The canceled invocation or disconnected connection.</param>
    /// <returns>The bounded response carrier.</returns>
    internal async Task<byte[]> ExchangeAsync(byte[] bytes, ClaimsPrincipal? principal, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (principal?.Identity?.IsAuthenticated != true || _authenticate(principal) is not { } client)
        {
            return SignalRCarrier.Encode(new(null, null, (int)HttpStatusCode.Unauthorized, [], []));
        }

        SignalRCarrierMessage envelope;
        try
        {
            envelope = SignalRCarrier.Decode(bytes);
        }
        catch (HttpRemoteTransportException exception)
        {
            var status = exception.Kind == HttpTransportFailureKind.PayloadTooLarge
                ? HttpStatusCode.RequestEntityTooLarge
                : HttpStatusCode.BadRequest;
            return SignalRCarrier.Encode(new(null, null, (int)status, [], []));
        }

        if (!IsValidRequest(envelope))
        {
            return SignalRCarrier.Encode(new(null, null, (int)HttpStatusCode.BadRequest, [], []));
        }

        using var request = new HttpRequestMessage(new HttpMethod(envelope.Method!), new Uri($"https://signalr.invalid{envelope.PathAndQuery}"));
        if (envelope.Method == "POST" || envelope.Body.Length != 0)
        {
            request.Content = new ByteArrayContent(envelope.Body);
        }

        SignalRCarrier.SetHeaders(envelope.Headers, request.Headers, request.Content);
        using var response = await _endpoint.HandleAsync(request, client, cancellationToken).ConfigureAwait(false);
        var body = response.Content is null
            ? []
            : await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        return SignalRCarrier.Encode(new(
            null,
            null,
            (int)response.StatusCode,
            SignalRCarrier.GetHeaders(response.Headers, response.Content),
            body));
    }

    /// <summary>Rejects carrier routes that could change authority or escape the endpoint.</summary>
    /// <param name="envelope">The request carrier.</param>
    /// <returns>Whether the carrier route is valid.</returns>
    private static bool IsValidRequest(SignalRCarrierMessage envelope) =>
        envelope.StatusCode == 0
        && envelope.Method is "GET" or "POST"
        && envelope.PathAndQuery is { } path
        && path.StartsWith('/')
        && !path.StartsWith("//", StringComparison.Ordinal)
        && !path.Contains('#', StringComparison.Ordinal)
        && !path.Contains('\\', StringComparison.Ordinal);
}
