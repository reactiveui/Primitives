// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;

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
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The maximum accepted replay token text length.</summary>
    private const int MaximumReplayTokenLength = 128;

    /// <summary>The maximum accepted encoded replay tenant header length.</summary>
    private const int MaximumReplayTenantHeaderLength = 4096;

    /// <summary>The base64 encoding quantum size.</summary>
    private const int Base64QuantumSize = 4;

    /// <summary>The replay token byte count.</summary>
    private const int ReplayTokenBytes = 16;

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
        using var response = await SendConnectAsync(body, linked.Token).ConfigureAwait(false);
        var responseBody = await HttpProtocolContent.ReadBoundedBytesAsync(response, _options, linked.Token).ConfigureAwait(false);
        var capabilities = codec.DeserializeConnectResponse(responseBody);
        HttpRemoteTransportCapabilities.ValidateNegotiation(request, capabilities, AdapterCapabilities);
        var replaySession = CreateReplaySession(response);
        return new HttpRemoteTransportSession(_options, capabilities, new(_requestGate, _acknowledgementGate, _subscriptionGate), replaySession, request.Client, _shutdown.Token);
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

    /// <summary>Creates a retained replay session from connect response headers.</summary>
    /// <param name="response">The connect response.</param>
    /// <returns>The replay session.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay session headers are missing or malformed.</exception>
    private static HttpReplayIssuedSession CreateReplaySession(HttpResponseMessage response)
    {
        var tenantId = GetOptionalHeader(response, HttpReplayHeaders.TenantId);
        var sessionId = GetOptionalHeader(response, HttpReplayHeaders.SessionId);
        var sessionSecret = GetOptionalHeader(response, HttpReplayHeaders.SessionSecret);
        var sessionExpires = GetOptionalHeader(response, HttpReplayHeaders.SessionExpires);
        if (tenantId is null || sessionId is null || sessionSecret is null || sessionExpires is null || !IsReplayTenantHeader(tenantId) || !IsReplayToken(sessionId) || !IsReplayToken(sessionSecret))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }

        if (!DateTimeOffset.TryParseExact(sessionExpires, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out var expires)
            || expires.Offset != TimeSpan.Zero)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }

        var decodedTenantId = DecodeReplayTenantHeader(tenantId);
        return new() { TenantId = decodedTenantId, SessionId = sessionId, SessionSecret = sessionSecret, ExpiresAtUtc = expires };
    }

    /// <summary>Gets a single optional response header.</summary>
    /// <param name="response">The response.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The header value, or <see langword="null"/>.</returns>
    /// <exception cref="HttpRemoteTransportException">The response repeats the header.</exception>
    private static string? GetOptionalHeader(HttpResponseMessage response, string name)
    {
        var count = HttpReplayHeaders.ReadValueCount(response.Headers, name, out var value);
        return count > 1 ? throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected) : value;
    }

    /// <summary>Checks a bounded replay tenant header before decoding it.</summary>
    /// <param name="value">The encoded tenant header.</param>
    /// <returns>Whether the tenant header is syntactically valid.</returns>
    private static bool IsReplayTenantHeader(string value)
    {
        if (value.Length is 0 or > MaximumReplayTenantHeaderLength)
        {
            return false;
        }

        for (var index = 0; index < value.Length; index++)
        {
            if (!IsReplayTokenCharacter(value[index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Decodes a trusted replay tenant header.</summary>
    /// <param name="value">The encoded tenant header.</param>
    /// <returns>The decoded tenant identifier.</returns>
    /// <exception cref="HttpRemoteTransportException">The tenant header is malformed.</exception>
    private static string DecodeReplayTenantHeader(string value)
    {
        var paddedLength = checked(value.Length + ((Base64QuantumSize - (value.Length % Base64QuantumSize)) % Base64QuantumSize));
        var padded = value.Replace('-', '+').Replace('_', '/').PadRight(paddedLength, '=');
        try
        {
            var bytes = Convert.FromBase64String(padded);
            var tenantId = new UTF8Encoding(false, true).GetString(bytes);
            return string.IsNullOrWhiteSpace(tenantId)
                ? throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected)
                : tenantId;
        }
        catch (Exception exception) when (exception is FormatException or DecoderFallbackException or ArgumentException or OverflowException)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }
    }

    /// <summary>Checks a bounded replay token before retaining it.</summary>
    /// <param name="value">The token text.</param>
    /// <returns>Whether the token is syntactically valid.</returns>
    private static bool IsReplayToken(string value)
    {
        if (value.Length is 0 or > MaximumReplayTokenLength)
        {
            return false;
        }

        for (var index = 0; index < value.Length; index++)
        {
            if (!IsReplayTokenCharacter(value[index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Checks one replay token character.</summary>
    /// <param name="character">The candidate character.</param>
    /// <returns>Whether the character is allowed.</returns>
    private static bool IsReplayTokenCharacter(char character) =>
        character is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_';

    /// <summary>Creates an unpredictable replay header token.</summary>
    /// <returns>The replay token.</returns>
    private static string CreateReplayToken()
    {
        var bytes = new byte[ReplayTokenBytes];
        using var generator = RandomNumberGenerator.Create();
        generator.GetBytes(bytes);
        return HttpReplayBase64Url.Encode(bytes);
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

    /// <summary>Sends a bounded connect request and validates its response status.</summary>
    /// <param name="body">The serialized connect body.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The successful HTTP response.</returns>
    /// <exception cref="HttpRemoteTransportException">The request failed or returned a non-success status.</exception>
    private async Task<HttpResponseMessage> SendConnectAsync(byte[] body, CancellationToken cancellationToken)
    {
        using var request = CreateConnectRequest(body);
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
        var kind = HttpTransportStatus.Classify(response);
        response.Dispose();
        throw new HttpRemoteTransportException(kind, response.StatusCode, retryAfter);
    }

    /// <summary>Creates a connect request under the trusted base URI.</summary>
    /// <param name="body">The serialized connect body.</param>
    /// <returns>The HTTP request.</returns>
    private HttpRequestMessage CreateConnectRequest(byte[] body)
    {
        var uri = ResolveConnectEndpoint();
        HttpRequestMessage request = new(HttpMethod.Post, uri);
        request.Headers.Accept.ParseAdd(HttpProtocolContent.MediaType);
        AddConnectReplayHeaders(request, body);
        request.Content = new ByteArrayContent(body);
        request.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(HttpProtocolContent.MediaType);
        return request;
    }

    /// <summary>Resolves and validates the configured connect endpoint.</summary>
    /// <returns>The endpoint URI.</returns>
    /// <exception cref="HttpRemoteTransportException">The configured endpoint escapes the trusted base address.</exception>
    private Uri ResolveConnectEndpoint()
    {
        Uri endpoint = new(_options.BaseAddress, _options.ConnectPath);
        if (!_options.BaseAddress.IsBaseOf(endpoint))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.Configuration);
        }

        return endpoint;
    }

    /// <summary>Adds replay freshness headers for the initial connect request.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="body">The exact request body.</param>
    private void AddConnectReplayHeaders(HttpRequestMessage request, byte[] body)
    {
        var observedUtc = _options.ReplayProtection.TimeProvider.GetUtcNow();
        request.Headers.Add(HttpReplayHeaders.MessageId, CreateReplayToken());
        request.Headers.Add(HttpReplayHeaders.Nonce, CreateReplayToken());
        request.Headers.Add(HttpReplayHeaders.SentAt, observedUtc.ToString("O", CultureInfo.InvariantCulture));
        _ = new HttpCanonicalRequestBuilder(_options.ReplayProtection)
            .Build(HttpReplayOperationKind.Connect, HttpMethod.Post.Method, _options.ConnectPath, [], observedUtc, body);
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
