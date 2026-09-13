// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Connects the occasionally-connected synchronization engine to an HTTP protocol peer.</summary>
[System.Diagnostics.DebuggerDisplay("{Capabilities,nq}")]
public sealed class HttpRemoteTransportAdapter : IRemoteTransportAdapter
{
    /// <summary>The capabilities advertised by the reference HTTP transport.</summary>
    private const RemoteTransportCapabilities AdapterCapabilities =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency;

    /// <summary>The adapter options.</summary>
    private readonly HttpRemoteTransportOptions _options;

    /// <summary>The shared request gate for connect, push and subscribe polls.</summary>
    private readonly HttpRequestGate _requestGate;

    /// <summary>The independently reserved acknowledgement gate.</summary>
    private readonly HttpRequestGate _acknowledgementGate;

    /// <summary>The bounded active subscription gate.</summary>
    private readonly HttpRequestGate _subscriptionGate;

    /// <summary>The adapter shutdown source.</summary>
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>The adapter lifecycle lock.</summary>
    private readonly Lock _lifecycle = new();

    /// <summary>The task completed when no adapter-scoped connect operation is active.</summary>
    private readonly TaskCompletionSource<object?> _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The task completed when adapter disposal finishes.</summary>
    private readonly TaskCompletionSource<object?> _disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The active adapter-scoped connect operation count.</summary>
    private int _activeConnects;

    /// <summary>Whether adapter disposal has started.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportAdapter"/> class.</summary>
    /// <param name="options">The adapter options.</param>
    public HttpRemoteTransportAdapter(HttpRemoteTransportOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _requestGate = new(options.MaximumConcurrentRequests);
        _acknowledgementGate = new(options.MaximumConcurrentAcknowledgements);
        _subscriptionGate = new(options.MaximumConcurrentSubscriptions);
        Capabilities = AdapterCapabilities;
    }

    /// <inheritdoc/>
    public RemoteTransportCapabilities Capabilities { get; }

    /// <inheritdoc/>
    public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        using var operation = BeginOperation();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        HttpProtocolCodec codec = new(_options);
        using var admission = await _requestGate.EnterAsync(linked.Token).ConfigureAwait(false);
        var body = codec.SerializeConnectRequest(request);
        using var response = await SendAsync(HttpMethod.Post, _options.ConnectPath, body, linked.Token).ConfigureAwait(false);
        var responseBody = await HttpProtocolContent.ReadBoundedBytesAsync(response, _options, linked.Token).ConfigureAwait(false);
        var capabilities = codec.DeserializeConnectResponse(responseBody);
        HttpRemoteTransportCapabilities.ValidateNegotiation(request, capabilities, AdapterCapabilities);
        return new HttpRemoteTransportSession(_options, capabilities, _requestGate, _acknowledgementGate, _subscriptionGate, _shutdown.Token);
    }

    /// <summary>Connects to the remote peer without an external cancellation token.</summary>
    /// <param name="request">The connect request.</param>
    /// <returns>The connected remote session.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request) => ConnectAsync(request, CancellationToken.None);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompleted.Task.ConfigureAwait(false);
            return;
        }

        Exception? failure = null;
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        lock (_lifecycle)
        {
            if (_activeConnects == 0)
            {
                _ = _drained.TrySetResult(null);
            }
        }

        await _requestGate.DisposeAsync().ConfigureAwait(false);
        await _acknowledgementGate.DisposeAsync().ConfigureAwait(false);
        await _subscriptionGate.DisposeAsync().ConfigureAwait(false);
        await _drained.Task.ConfigureAwait(false);
        _shutdown.Dispose();
        if (failure is null)
        {
            _ = _disposeCompleted.TrySetResult(null);
        }
        else
        {
            _ = _disposeCompleted.TrySetException(failure);
        }

        await _disposeCompleted.Task.ConfigureAwait(false);
    }

    /// <summary>Starts an adapter-scoped connect operation.</summary>
    /// <returns>The operation lease.</returns>
    /// <exception cref="ObjectDisposedException">The adapter is disposed.</exception>
    private AdapterOperation BeginOperation()
    {
        lock (_lifecycle)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                ObjectDisposedExceptionHelper.ThrowIf(true, this);
            }

            _activeConnects++;
            return new(this);
        }
    }

    /// <summary>Ends an adapter-scoped connect operation.</summary>
    private void EndOperation()
    {
        lock (_lifecycle)
        {
            _activeConnects--;
            if (Volatile.Read(ref _disposed) == 0 || _activeConnects != 0)
            {
                return;
            }

            _ = _drained.TrySetResult(null);
        }
    }

    /// <summary>Sends a bounded HTTP request and validates its response status.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The relative endpoint path.</param>
    /// <param name="body">The optional serialized body.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The successful HTTP response.</returns>
    /// <exception cref="HttpRemoteTransportException">The request failed or returned a non-success status.</exception>
    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, byte[]? body, CancellationToken cancellationToken)
    {
        using var request = CreateRequest(method, path, body);
        HttpResponseMessage response;
        try
        {
            response = await _options.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (exception is HttpRequestException or IOException)
        {
            throw new HttpRemoteTransportException(
                HttpTransportFailureKind.AmbiguousTransportOutcome,
                statusCode: null,
                retryAfter: null,
                innerException: exception);
        }

        if (response.IsSuccessStatusCode)
        {
            return response;
        }

        var retryAfter = HttpTransportStatus.GetRetryAfter(response, _options.TimeProvider);
        var kind = HttpTransportStatus.Classify(response.StatusCode);
        response.Dispose();
        throw new HttpRemoteTransportException(kind, response.StatusCode, retryAfter);
    }

    /// <summary>Creates a request under the trusted base URI.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The configured relative path.</param>
    /// <param name="body">The optional serialized body.</param>
    /// <returns>The HTTP request.</returns>
    private HttpRequestMessage CreateRequest(HttpMethod method, string path, byte[]? body)
    {
        var uri = ResolveEndpoint(path);
        HttpRequestMessage request = new(method, uri);
        request.Headers.Accept.ParseAdd(HttpProtocolContent.MediaType);
        if (body is not null)
        {
            request.Content = new ByteArrayContent(body);
            request.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(HttpProtocolContent.MediaType);
        }

        return request;
    }

    /// <summary>Resolves and validates a configured endpoint.</summary>
    /// <param name="path">The relative path.</param>
    /// <returns>The endpoint URI.</returns>
    /// <exception cref="HttpRemoteTransportException">The configured endpoint escapes the trusted base address.</exception>
    private Uri ResolveEndpoint(string path)
    {
        Uri endpoint = new(_options.BaseAddress, path);
        if (!_options.BaseAddress.IsBaseOf(endpoint))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.Configuration);
        }

        return endpoint;
    }

    /// <summary>Represents one adapter-scoped operation lease.</summary>
    private readonly struct AdapterOperation : IDisposable
    {
        /// <summary>The owning adapter.</summary>
        private readonly HttpRemoteTransportAdapter _owner;

        /// <summary>Initializes a new instance of the <see cref="AdapterOperation"/> struct.</summary>
        /// <param name="owner">The owning adapter.</param>
        internal AdapterOperation(HttpRemoteTransportAdapter owner) => _owner = owner;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _owner.EndOperation();
    }
}
