// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents an active bounded HTTP remote transport session.</summary>
internal sealed partial class HttpRemoteTransportSession : IRemoteTransportSession, IRemoteTransportBatchPreparer, IRemoteSnapshotRecoverySession
{
    /// <summary>The adapter options.</summary>
    private readonly HttpRemoteTransportOptions _options;

    /// <summary>The negotiated capabilities.</summary>
    private readonly NegotiatedCapabilities _negotiatedCapabilities;

    /// <summary>The shared request gate.</summary>
    private readonly HttpRequestGate _requestGate;

    /// <summary>The reserved acknowledgement gate.</summary>
    private readonly HttpRequestGate _acknowledgementGate;

    /// <summary>The bounded active subscription gate.</summary>
    private readonly HttpRequestGate _subscriptionGate;

    /// <summary>The adapter shutdown token.</summary>
    private readonly CancellationToken _adapterShutdownToken;

    /// <summary>The session shutdown source.</summary>
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>The session lifecycle lock.</summary>
    private readonly Lock _lifecycle = new();

    /// <summary>The task completed when no session request is active.</summary>
    private readonly TaskCompletionSource<object?> _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The task completed when session disposal finishes.</summary>
    private readonly TaskCompletionSource<object?> _disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The protocol codec.</summary>
    private readonly HttpProtocolCodec _codec;

    /// <summary>The replay envelope hasher.</summary>
    private readonly HttpReplayEnvelopeHasher _replayHasher;

    /// <summary>The trusted replay tenant identifier returned by connect.</summary>
    private readonly string? _replayTenantId;

    /// <summary>The retained replay session identifier.</summary>
    private readonly string? _replaySessionId;

    /// <summary>The retained replay session secret.</summary>
    private readonly HttpReplaySessionSecretOwner? _replaySessionSecret;

    /// <summary>The client identity used for replay MAC input.</summary>
    private readonly ClientIdentity _clientIdentity;

    /// <summary>The active request count.</summary>
    private int _activeRequests;

    /// <summary>Whether disposal has started.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportSession"/> class.</summary>
    /// <param name="options">The adapter options.</param>
    /// <param name="negotiatedCapabilities">The negotiated capabilities.</param>
    /// <param name="gates">The bounded adapter request gates.</param>
    /// <param name="replaySession">The replay session issued by connect, when available.</param>
    /// <param name="clientIdentity">The client identity from connect.</param>
    /// <param name="adapterShutdownToken">The adapter shutdown token.</param>
    internal HttpRemoteTransportSession(
        HttpRemoteTransportOptions options,
        NegotiatedCapabilities negotiatedCapabilities,
        HttpRemoteTransportSessionGates gates,
        HttpReplayIssuedSession? replaySession,
        ClientIdentity clientIdentity,
        CancellationToken adapterShutdownToken)
    {
        _options = options;
        _negotiatedCapabilities = negotiatedCapabilities;
        _requestGate = gates.Request;
        _acknowledgementGate = gates.Acknowledgement;
        _subscriptionGate = gates.Subscription;
        _adapterShutdownToken = adapterShutdownToken;
        _clientIdentity = clientIdentity;
        _codec = new(options);
        _replayHasher = new(options.ReplayProtection);
        if (replaySession is null)
        {
            return;
        }

        _replayTenantId = replaySession.TenantId;
        _replaySessionId = replaySession.SessionId;
        _replaySessionSecret = new(Encoding.UTF8.GetBytes(replaySession.SessionSecret));
    }

    /// <inheritdoc/>
    public NegotiatedCapabilities NegotiatedCapabilities => _negotiatedCapabilities;

    /// <inheritdoc/>
    public async ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
    {
        await using var prepared = await PreparePushAsync(batch, cancellationToken).ConfigureAwait(false);
        return await prepared.SendAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken)
    {
        ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentExceptionHelper.ThrowIfNull(request);
        return new HttpRemoteSubscription(this, request, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(acknowledgement);
        using var operation = BeginOperation();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _shutdown.Token,
            _adapterShutdownToken);
        using var admission = await _acknowledgementGate.EnterAsync(linked.Token).ConfigureAwait(false);
        var body = _codec.SerializeAcknowledgement(acknowledgement);
        using var response = await SendAsync(HttpMethod.Post, _options.AcknowledgePath, body, linked.Token).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(RemoteSnapshotRecoveryRequest request, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        using var operation = BeginOperation();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _shutdown.Token,
            _adapterShutdownToken);
        try
        {
            if ((_negotiatedCapabilities.Features & RemoteTransportCapabilities.SnapshotRecovery) != RemoteTransportCapabilities.SnapshotRecovery)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Configuration);
            }

            using var admission = await _requestGate.EnterAsync(linked.Token).ConfigureAwait(false);
            var body = _codec.SerializeSnapshotRecoveryRequest(request);
            using var response = await SendAsync(
                HttpMethod.Post,
                _options.SnapshotRecoveryPath,
                body,
                linked.Token).ConfigureAwait(false);
            var responseBytes = await HttpProtocolContent.ReadBoundedBytesAsync(
                response,
                _options,
                linked.Token).ConfigureAwait(false);
            return _codec.DeserializeSnapshotRecoveryResponse(request, responseBytes);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
    }

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
            if (_activeRequests == 0)
            {
                _ = _drained.TrySetResult(null);
            }
        }

        await _drained.Task.ConfigureAwait(false);
        _replaySessionSecret?.Dispose();
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

    /// <summary>Gets the request path without query text for canonical signing.</summary>
    /// <param name="endpoint">The resolved endpoint URI.</param>
    /// <returns>The canonical path.</returns>
    /// <exception cref="HttpRemoteTransportException">The resolved endpoint path is malformed.</exception>
    private static string GetCanonicalPath(Uri endpoint)
    {
        var rawPath = endpoint.GetComponents(UriComponents.Path, UriFormat.UriEscaped);
        var path = rawPath.Trim('/');
        if (path.Length == 0)
        {
            return string.Empty;
        }

        var segments = path.Split('/');
        for (var index = 0; index < segments.Length; index++)
        {
            var segment = Uri.UnescapeDataString(segments[index]);
            if (segment.Length == 0 || ContainsRouteSeparator(segment))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
            }

            segments[index] = segment;
        }

        return string.Join("/", segments);
    }

    /// <summary>Determines whether a decoded route segment contains a route separator.</summary>
    /// <param name="value">The decoded route segment.</param>
    /// <returns><see langword="true"/> when the route segment contains a separator.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ContainsRouteSeparator(string value)
    {
#if NET8_0_OR_GREATER
        return value.Contains('/') || value.Contains('\\');
#else
        return value.Contains("/") || value.Contains("\\");
#endif
    }

    /// <summary>Gets decoded query fields from a resolved endpoint.</summary>
    /// <param name="endpoint">The resolved endpoint URI.</param>
    /// <returns>The decoded query fields.</returns>
    /// <exception cref="HttpRemoteTransportException">The query contains malformed fields.</exception>
    private static KeyValuePair<string, string>[] GetCanonicalQueryFields(Uri endpoint)
    {
        var queryText = endpoint.GetComponents(UriComponents.Query, UriFormat.UriEscaped);
        if (queryText.Length == 0)
        {
            return [];
        }

        var segments = queryText.Split('&');
        var fields = new KeyValuePair<string, string>[segments.Length];
        for (var index = 0; index < segments.Length; index++)
        {
            var separator = segments[index].IndexOf('=');
            if (separator <= 0)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
            }

#if NET9_0_OR_GREATER
            var key = Uri.UnescapeDataString(segments[index].AsSpan(0, separator));
#else
            var key = Uri.UnescapeDataString(segments[index].Substring(0, separator));
#endif
            var value = Uri.UnescapeDataString(segments[index].Remove(0, separator + 1));
            fields[index] = new(key, value);
        }

        return fields;
    }

    /// <summary>Creates an unpredictable replay header token.</summary>
    /// <returns>The replay token.</returns>
    private static string CreateReplayToken()
    {
        var bytes = new byte[16];
        using var generator = RandomNumberGenerator.Create();
        generator.GetBytes(bytes);
        return HttpReplayBase64Url.Encode(bytes);
    }

    /// <summary>Throws if this session has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Receives one subscription response using the supplied cursor.</summary>
    /// <param name="request">The subscription request.</param>
    /// <param name="cursor">The current receive cursor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded event batches, or an empty array for an empty long-poll response.</returns>
    private async Task<RemoteEventBatch[]> ReceiveSubscribeResponseAsync(RemoteSubscribeRequest request, string? cursor, CancellationToken cancellationToken)
    {
        using var operation = BeginOperation();
        using var admission = await _requestGate.EnterAsync(cancellationToken).ConfigureAwait(false);
        using var response = await SendAsync(HttpMethod.Get, CreateSubscribePath(request, cursor), body: null, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NoContent)
        {
            return [];
        }

        var responseBytes = await HttpProtocolContent.ReadBoundedBytesAsync(response, _options, cancellationToken).ConfigureAwait(false);
        return _codec.DeserializeSubscribeResponse(responseBytes, request.StreamId, cursor);
    }

    /// <summary>Sends a request and validates its response.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The relative endpoint path.</param>
    /// <param name="body">The optional request body.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The successful response.</returns>
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
        var kind = HttpTransportStatus.Classify(response);
        response.Dispose();
        throw new HttpRemoteTransportException(kind, response.StatusCode, retryAfter);
    }

    /// <summary>Creates an HTTP request under the configured base URI.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The relative endpoint path.</param>
    /// <param name="body">The optional body.</param>
    /// <returns>The HTTP request.</returns>
    /// <exception cref="HttpRemoteTransportException">The configured endpoint escapes the trusted base address.</exception>
    private HttpRequestMessage CreateRequest(HttpMethod method, string path, byte[]? body)
    {
        Uri endpoint = new(_options.BaseAddress, path);
        if (!_options.BaseAddress.IsBaseOf(endpoint))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.Configuration);
        }

        var request = new HttpRequestMessage(method, endpoint);
        request.Headers.Accept.ParseAdd(HttpProtocolContent.MediaType);
        AddReplaySessionHeaders(request, method, endpoint, body ?? []);
        if (body is not null)
        {
            request.Content = new ByteArrayContent(body);
            request.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(HttpProtocolContent.MediaType);
        }

        return request;
    }

    /// <summary>Adds session replay headers to a request when connect returned a replay session.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="endpoint">The resolved endpoint URI.</param>
    /// <param name="body">The exact body bytes.</param>
    private void AddReplaySessionHeaders(HttpRequestMessage request, HttpMethod method, Uri endpoint, byte[] body)
    {
        if (_replayTenantId is null || _replaySessionId is null || _replaySessionSecret is null)
        {
            return;
        }

        var observedUtc = _options.ReplayProtection.TimeProvider.GetUtcNow();
        var messageId = CreateReplayToken();
        var nonce = CreateReplayToken();
        var canonicalPath = GetCanonicalPath(endpoint);
        var query = GetCanonicalQueryFields(endpoint);
        var operation = GetReplayOperation(canonicalPath);
        var canonical = new HttpCanonicalRequestBuilder(_options.ReplayProtection)
            .Build(operation, method.Method, canonicalPath, query, observedUtc, body);
        var replayRequest = new HttpReplayRequest
        {
            Operation = operation,
            Principal = new(_replayTenantId, _clientIdentity.ClientId),
            MessageId = messageId,
            Nonce = nonce,
            SentAtUtc = observedUtc,
            ReplaySessionId = _replaySessionId,
            ReplayMac = "placeholder",
            CanonicalRequest = canonical,
        };
        var envelope = _replayHasher.Create(replayRequest);
        byte[]? secret = null;
        try
        {
            secret = _replaySessionSecret.Copy();
            var mac = _replayHasher.ComputeMac(secret, envelope.MacInput);
            request.Headers.Add(HttpReplayHeaders.MessageId, messageId);
            request.Headers.Add(HttpReplayHeaders.Nonce, nonce);
            request.Headers.Add(HttpReplayHeaders.SentAt, observedUtc.ToString("O", CultureInfo.InvariantCulture));
            request.Headers.Add(HttpReplayHeaders.SessionId, _replaySessionId);
            request.Headers.Add(HttpReplayHeaders.Mac, mac);
        }
        finally
        {
            if (secret is not null)
            {
                HttpReplayCryptography.ZeroMemory(secret);
            }
        }
    }

    /// <summary>Gets the replay operation represented by a configured path.</summary>
    /// <param name="path">The configured path without query text.</param>
    /// <returns>The replay operation.</returns>
    private HttpReplayOperationKind GetReplayOperation(string path)
    {
        if (StringComparer.Ordinal.Equals(path, GetConfiguredCanonicalPath(_options.PushPath)))
        {
            return HttpReplayOperationKind.Push;
        }

        if (StringComparer.Ordinal.Equals(path, GetConfiguredCanonicalPath(_options.SubscribePath)))
        {
            return HttpReplayOperationKind.Subscribe;
        }

        return StringComparer.Ordinal.Equals(path, GetConfiguredCanonicalPath(_options.SnapshotRecoveryPath))
            ? HttpReplayOperationKind.SnapshotRecovery
            : HttpReplayOperationKind.Acknowledge;
    }

    /// <summary>Gets the canonical path for a configured route resolved under the trusted base address.</summary>
    /// <param name="path">The configured route.</param>
    /// <returns>The canonical route path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetConfiguredCanonicalPath(string path) => GetCanonicalPath(new(_options.BaseAddress, path));

    /// <summary>Creates the subscription long-poll endpoint and query.</summary>
    /// <param name="request">The subscribe request.</param>
    /// <param name="cursor">The current receive cursor.</param>
    /// <returns>The relative endpoint.</returns>
    private string CreateSubscribePath(RemoteSubscribeRequest request, string? cursor)
    {
        StringBuilder builder = new(_options.SubscribePath);
        _ = builder
            .Append("?streamId=")
            .Append(Uri.EscapeDataString(request.StreamId.Value))
            .Append("&subscriptionId=")
            .Append(Uri.EscapeDataString(request.SubscriptionId.Value.ToString("D")));
        if (cursor is not null)
        {
            _ = builder
                .Append("&cursor=")
                .Append(Uri.EscapeDataString(cursor));
        }

        _ = builder
            .Append("&positionKind=")
            .Append((int)request.InitialPosition.Kind);
        if (request.InitialPosition.Timestamp.HasValue)
        {
            _ = builder
                .Append("&timestamp=")
                .Append(Uri.EscapeDataString(request.InitialPosition.Timestamp.Value.ToString("O", System.Globalization.CultureInfo.InvariantCulture)));
        }

        if (request.InitialPosition.Sequence.HasValue)
        {
            _ = builder
                .Append("&sequence=")
                .Append(request.InitialPosition.Sequence.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        if (request.InitialPosition.Cursor is not null)
        {
            _ = builder
                .Append("&initialCursor=")
                .Append(Uri.EscapeDataString(request.InitialPosition.Cursor));
        }

        return builder.ToString();
    }

    /// <summary>Starts a session-scoped request operation.</summary>
    /// <returns>The operation lease.</returns>
    /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
    private SessionOperation BeginOperation()
    {
        lock (_lifecycle)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                ObjectDisposedExceptionHelper.ThrowIf(true, this);
            }

            _activeRequests++;
            return new(this);
        }
    }

    /// <summary>Ends a session-scoped request operation.</summary>
    private void EndOperation()
    {
        lock (_lifecycle)
        {
            _activeRequests--;
            if (Volatile.Read(ref _disposed) == 0 || _activeRequests != 0)
            {
                return;
            }

            _ = _drained.TrySetResult(null);
        }
    }

    /// <summary>The bounded request gates shared with the adapter.</summary>
    /// <param name="Request">The shared request gate.</param>
    /// <param name="Acknowledgement">The reserved acknowledgement gate.</param>
    /// <param name="Subscription">The active subscription gate.</param>
    internal readonly record struct HttpRemoteTransportSessionGates(
        HttpRequestGate Request,
        HttpRequestGate Acknowledgement,
        HttpRequestGate Subscription);

    /// <summary>Represents one session-scoped request lease.</summary>
    private readonly struct SessionOperation : IDisposable
    {
        /// <summary>The owning session.</summary>
        private readonly HttpRemoteTransportSession _owner;

        /// <summary>Initializes a new instance of the <see cref="SessionOperation"/> struct.</summary>
        /// <param name="owner">The owning session.</param>
        internal SessionOperation(HttpRemoteTransportSession owner) => _owner = owner;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _owner.EndOperation();
    }

    /// <summary>Creates subscription enumerators that hold bounded lifetime admission.</summary>
    private sealed class HttpRemoteSubscription : IAsyncEnumerable<RemoteEventBatch>
    {
        /// <summary>The owning session.</summary>
        private readonly HttpRemoteTransportSession _owner;

        /// <summary>The immutable caller request.</summary>
        private readonly RemoteSubscribeRequest _request;

        /// <summary>The caller supplied subscription cancellation token.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>Initializes a new instance of the <see cref="HttpRemoteSubscription"/> class.</summary>
        /// <param name="owner">The owning session.</param>
        /// <param name="request">The immutable caller request.</param>
        /// <param name="cancellationToken">The caller supplied subscription cancellation token.</param>
        internal HttpRemoteSubscription(HttpRemoteTransportSession owner, RemoteSubscribeRequest request, CancellationToken cancellationToken)
        {
            _owner = owner;
            _request = request;
            _cancellationToken = cancellationToken;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            new HttpRemoteSubscriptionEnumerator(_owner, _request, _cancellationToken, cancellationToken);
    }

    /// <summary>Owns one HTTP subscription enumeration and its retained receive state.</summary>
    private sealed class HttpRemoteSubscriptionEnumerator : IAsyncEnumerator<RemoteEventBatch>
    {
        /// <summary>The empty buffered batch set.</summary>
        private static readonly RemoteEventBatch[] EmptyBatches = [];

        /// <summary>The owning session.</summary>
        private readonly HttpRemoteTransportSession _owner;

        /// <summary>The immutable caller request.</summary>
        private readonly RemoteSubscribeRequest _request;

        /// <summary>The subscription lifetime token source.</summary>
        private readonly CancellationTokenSource _linked;

        /// <summary>The buffered state lock.</summary>
        private readonly Lock _state = new();

        /// <summary>The cancellation registration that releases paused subscription state.</summary>
        private readonly CancellationTokenRegistration _disposeRegistration;

        /// <summary>The subscription lifetime admission lease.</summary>
        private readonly HttpRequestGate.Lease? _subscriptionAdmission;

        /// <summary>The current cursor for the next long poll.</summary>
        private string? _cursor;

        /// <summary>The currently retained response batches.</summary>
        private RemoteEventBatch[] _batches = EmptyBatches;

        /// <summary>The next buffered batch index.</summary>
        private int _batchIndex;

        /// <summary>The active move task, when a move is in progress.</summary>
        private Task? _activeMove;

        /// <summary>The stable disposal completion task.</summary>
        private Task? _disposeTask;

        /// <summary>The current yielded batch, when present.</summary>
        private RemoteEventBatch? _current;

        /// <summary>Whether the enumerator is currently moving.</summary>
        private int _moving;

        /// <summary>Whether the enumerator is disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="HttpRemoteSubscriptionEnumerator"/> class.</summary>
        /// <param name="owner">The owning session.</param>
        /// <param name="request">The immutable caller request.</param>
        /// <param name="subscriptionCancellationToken">The subscription cancellation token.</param>
        /// <param name="enumeratorCancellationToken">The enumerator cancellation token.</param>
        internal HttpRemoteSubscriptionEnumerator(
            HttpRemoteTransportSession owner,
            RemoteSubscribeRequest request,
            CancellationToken subscriptionCancellationToken,
            CancellationToken enumeratorCancellationToken)
        {
            owner.ThrowIfDisposed();
            _owner = owner;
            _request = request;
            _cursor = request.Cursor;
            _linked = CancellationTokenSource.CreateLinkedTokenSource(
                subscriptionCancellationToken,
                enumeratorCancellationToken,
                owner._shutdown.Token,
                owner._adapterShutdownToken);
            if (_linked.IsCancellationRequested)
            {
                _disposeRegistration = default;
                _subscriptionAdmission = null;
                return;
            }

            try
            {
                _subscriptionAdmission = owner._subscriptionGate.Enter(_linked.Token);
                _disposeRegistration = _linked.Token.Register(DisposeCore);
            }
            catch
            {
                _linked.Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        public RemoteEventBatch Current => _current ?? throw new InvalidOperationException("The HTTP subscription has no current batch.");

        /// <inheritdoc/>
        public ValueTask<bool> MoveNextAsync()
        {
            var completion = BeginMove();
            return completion is null ? new(false) : new(FinishMoveAsync(completion));
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            TaskCompletionSource<object?>? completion = null;
            Task? activeMove = null;
            Task disposal;
            lock (_state)
            {
                if (_disposeTask is null)
                {
                    completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _disposeTask = completion.Task;
                    activeMove = _activeMove;
                    Volatile.Write(ref _disposed, 1);
                }

                disposal = _disposeTask;
            }

            if (completion is not null)
            {
                _ = DisposeOwnedResourcesAsync(activeMove, completion);
            }

            return new(disposal);
        }

        /// <summary>Publishes move ownership before any transport callback can run.</summary>
        /// <returns>The move completion signal, or null when disposed.</returns>
        /// <exception cref="InvalidOperationException">Another move is already active.</exception>
        private TaskCompletionSource<object?>? BeginMove()
        {
            lock (_state)
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    return null;
                }

                if (_moving != 0)
                {
                    throw new InvalidOperationException("A subscription enumerator already has an active move.");
                }

                _moving = 1;
                TaskCompletionSource<object?> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _activeMove = completion.Task;
                return completion;
            }
        }

        /// <summary>Runs one move and resets the move ownership flag.</summary>
        /// <param name="completion">The signal published before transport callbacks can begin.</param>
        /// <returns>Whether a batch was yielded.</returns>
        private async Task<bool> FinishMoveAsync(TaskCompletionSource<object?> completion)
        {
            try
            {
                return await MoveNextCoreAsync().ConfigureAwait(false);
            }
            finally
            {
                lock (_state)
                {
                    _activeMove = null;
                    _moving = 0;
                }

                _ = completion.TrySetResult(null);
            }
        }

        /// <summary>Cancels and drains the owned move before releasing cancellation resources.</summary>
        /// <param name="activeMove">The move already published under the state lock.</param>
        /// <param name="completion">The shared disposal completion signal.</param>
        /// <returns>The cleanup task.</returns>
        private async Task DisposeOwnedResourcesAsync(Task? activeMove, TaskCompletionSource<object?> completion)
        {
            Exception? failure = null;
            try
            {
                await _linked.CancelAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            ClearOwnedState();
            if (activeMove is not null)
            {
                await activeMove.ConfigureAwait(false);
            }

#if NET5_0_OR_GREATER
            await _disposeRegistration.DisposeAsync().ConfigureAwait(false);
#else
            _disposeRegistration.Dispose();
#endif
            _linked.Dispose();
            if (failure is null)
            {
                _ = completion.TrySetResult(null);
            }
            else
            {
                _ = completion.TrySetException(failure);
            }
        }

        /// <summary>Moves to the next locally buffered or remotely received batch.</summary>
        /// <returns>Whether a batch was yielded.</returns>
        private async Task<bool> MoveNextCoreAsync()
        {
            try
            {
                if (TryTakeBufferedBatch())
                {
                    return true;
                }

                while (true)
                {
                    _linked.Token.ThrowIfCancellationRequested();
                    var batches = await _owner.ReceiveSubscribeResponseAsync(_request, _cursor, _linked.Token).ConfigureAwait(false);
                    if (batches.Length == 0)
                    {
                        continue;
                    }

                    StoreBatches(batches);
                    if (TryTakeBufferedBatch())
                    {
                        return true;
                    }
                }
            }
            catch (OperationCanceledException) when (_linked.IsCancellationRequested)
            {
            }
            catch
            {
                DisposeCore();
                throw;
            }

            return false;
        }

        /// <summary>Stores a decoded response while the subscription is still active.</summary>
        /// <param name="batches">The decoded response batches.</param>
        private void StoreBatches(RemoteEventBatch[] batches)
        {
            lock (_state)
            {
                _batches = batches;
                _batchIndex = 0;
            }
        }

        /// <summary>Moves to the next retained response batch.</summary>
        /// <returns>Whether a batch was yielded.</returns>
        private bool TryTakeBufferedBatch()
        {
            lock (_state)
            {
                if (Volatile.Read(ref _disposed) != 0 || _batchIndex >= _batches.Length)
                {
                    ClearBufferedBatches();
                    return false;
                }

                _current = _batches[_batchIndex];
                _batchIndex++;
                _cursor = _current.NextCursor;
                if (_batchIndex >= _batches.Length)
                {
                    ClearBufferedBatches();
                }

                return true;
            }
        }

        /// <summary>Releases subscription resources synchronously for cancellation callbacks.</summary>
        private void DisposeCore()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                _linked.Cancel();
            }
            finally
            {
                ClearOwnedState();
            }
        }

        /// <summary>Releases the retained response and subscription admission.</summary>
        private void ClearOwnedState()
        {
            lock (_state)
            {
                _current = null;
                ClearBufferedBatches();
            }

            _subscriptionAdmission?.Dispose();
        }

        /// <summary>Clears retained batch array references.</summary>
        private void ClearBufferedBatches()
        {
            _batches = EmptyBatches;
            _batchIndex = 0;
        }
    }
}
