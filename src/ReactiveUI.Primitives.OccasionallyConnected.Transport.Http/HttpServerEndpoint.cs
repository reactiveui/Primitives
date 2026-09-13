// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
[System.Diagnostics.DebuggerDisplay("{DeclaredCapabilities,nq}")]
public sealed partial class HttpServerEndpoint : IAsyncDisposable
{
    /// <summary>The endpoint capabilities that can be preserved by the portable HTTP server surface.</summary>
    private const RemoteTransportCapabilities SupportedCapabilities =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge;

    /// <summary>The HTTP Service Unavailable status code.</summary>
    private const HttpStatusCode ServiceUnavailable = HttpStatusCode.ServiceUnavailable;

    /// <summary>The status returned for unknown routes.</summary>
    private const HttpStatusCode NotFound = HttpStatusCode.NotFound;

    /// <summary>The HTTP Payload Too Large status code.</summary>
    private const HttpStatusCode PayloadTooLarge = HttpStatusCode.RequestEntityTooLarge;

    /// <summary>The HTTP Too Many Requests status code.</summary>
#if NET8_0_OR_GREATER
    private const HttpStatusCode TooManyRequests = HttpStatusCode.TooManyRequests;
#else
    private const HttpStatusCode TooManyRequests = (HttpStatusCode)429;
#endif

    /// <summary>The supported protocol major version.</summary>
    private const int SupportedProtocolMajor = 1;

    /// <summary>The protocol version media type parameter value.</summary>
    private const string ProtocolVersionParameterValue = "1";

    /// <summary>The protocol media type without parameters.</summary>
    private const string ProtocolMediaType = "application/vnd.reactiveui.occasionally-connected+json";

    /// <summary>The protocol version media type parameter name.</summary>
    private const string ProtocolVersionParameterName = "v";

    /// <summary>The stream read buffer size.</summary>
    private const int ReadBufferSize = 8192;

    /// <summary>The initial route segment count capacity.</summary>
    private const int InitialRouteSegmentCapacity = 4;

    /// <summary>The hex high nibble shift.</summary>
    private const int HexHighNibbleShift = 4;

    /// <summary>The percent-encoded text width.</summary>
    private const int PercentEncodedWidth = 3;

    /// <summary>The percent-encoded first hex digit offset.</summary>
    private const int PercentEncodedHighOffset = 1;

    /// <summary>The percent-encoded second hex digit offset.</summary>
    private const int PercentEncodedLowOffset = 2;

    /// <summary>The strict UTF-8 encoding.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>The borrowed server hub.</summary>
    private readonly IServerStreamHub _hub;

    /// <summary>The protocol codec configured from endpoint limits.</summary>
    private readonly HttpProtocolCodec _codec;

    /// <summary>The conservative capabilities returned to connecting clients.</summary>
    private readonly NegotiatedCapabilities _advertisedCapabilities;

    /// <summary>The general request admission gate.</summary>
    private readonly HttpRequestGate _requestGate;

    /// <summary>The acknowledgement admission gate.</summary>
    private readonly HttpRequestGate _acknowledgementGate;

    /// <summary>The subscription admission gate.</summary>
    private readonly HttpRequestGate _subscriptionGate;

    /// <summary>The endpoint-owned shutdown source.</summary>
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>The finite long-poll timeout.</summary>
    private readonly TimeSpan _longPollTimeout;

    /// <summary>The endpoint clock.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The maximum encoded request body bytes.</summary>
    private readonly int _maximumRequestBytes;

    /// <summary>The maximum encoded query bytes.</summary>
    private readonly int _maximumQueryBytes;

    /// <summary>The normalized connect route.</summary>
    private readonly string _connectPath;

    /// <summary>The normalized push route.</summary>
    private readonly string _pushPath;

    /// <summary>The normalized subscribe route.</summary>
    private readonly string _subscribePath;

    /// <summary>The normalized acknowledgement route.</summary>
    private readonly string _acknowledgePath;

    /// <summary>The disposal completion signal.</summary>
    private readonly TaskCompletionSource<object?> _disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Whether disposal has started.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpServerEndpoint"/> class.</summary>
    /// <param name="options">The endpoint options.</param>
    public HttpServerEndpoint(HttpServerEndpointOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ValidateOptions(options);
        DeclaredCapabilities = options.DeclaredCapabilities;
        _advertisedCapabilities = CreateAdvertisedCapabilities(options);
        _hub = options.Hub;
        _codec = CreateCodec(options);
        _requestGate = new(options.MaximumConcurrentRequests);
        _acknowledgementGate = new(options.MaximumConcurrentAcknowledgements);
        _subscriptionGate = new(options.MaximumConcurrentSubscriptions);
        _longPollTimeout = options.LongPollTimeout;
        _timeProvider = options.TimeProvider;
        _maximumRequestBytes = options.MaximumRequestBytes;
        _maximumQueryBytes = options.MaximumQueryBytes;

        var pathBase = NormalizePathBase(options.PathBase);
        _connectPath = Combine(pathBase, options.ConnectPath);
        _pushPath = Combine(pathBase, options.PushPath);
        _subscribePath = Combine(pathBase, options.SubscribePath);
        _acknowledgePath = Combine(pathBase, options.AcknowledgePath);
    }

    /// <summary>Gets the capabilities declared by this endpoint.</summary>
    public NegotiatedCapabilities DeclaredCapabilities { get; }

    /// <summary>Handles a portable HTTP request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The caller-owned HTTP response.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> or <paramref name="authenticatedClient"/> is <see langword="null"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled before endpoint work starts.</exception>
    public ValueTask<HttpResponseMessage> HandleAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        var earlyResponse = TryCreateEarlyResponse(request, authenticatedClient, cancellationToken);
        return earlyResponse is null
            ? DispatchAsync(request, authenticatedClient, cancellationToken)
            : new(earlyResponse);
    }

    /// <summary>Handles a portable HTTP request without external cancellation.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <returns>The caller-owned HTTP response.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public ValueTask<HttpResponseMessage> HandleAsync(HttpRequestMessage request, ServerAuthenticatedClient authenticatedClient) =>
        HandleAsync(request, authenticatedClient, CancellationToken.None);

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _ = DisposeAsyncCore();
        }

        return new(_disposeCompleted.Task);
    }
}
