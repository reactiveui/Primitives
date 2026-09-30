// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

/// <summary>Connects the synchronization engine to a bidirectional WebSocket protocol peer.</summary>
[DebuggerDisplay("{Capabilities}")]
public sealed class WebSocketRemoteTransportAdapter : IRemoteTransportAdapter
{
    /// <summary>The capabilities implemented by this adapter.</summary>
    private const RemoteTransportCapabilities AdapterCapabilities =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>The adapter configuration.</summary>
    private readonly WebSocketRemoteTransportOptions _options;

    /// <summary>Tracks whether the adapter has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportAdapter"/> class.</summary>
    /// <param name="options">The WebSocket adapter options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The options contain an invalid endpoint.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured size limit is invalid.</exception>
    public WebSocketRemoteTransportAdapter(WebSocketRemoteTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        Capabilities = AdapterCapabilities;
    }

    /// <inheritdoc />
    public RemoteTransportCapabilities Capabilities { get; }

    /// <inheritdoc />
    public async ValueTask<IRemoteTransportSession> ConnectAsync(
        TransportConnectRequest request,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var socket = new ClientWebSocket();
        try
        {
            await socket.ConnectAsync(_options.Endpoint, cancellationToken).ConfigureAwait(false);
            var session = new WebSocketRemoteTransportSession(socket, _options);
            await session.HandshakeAsync(request, cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _ = Interlocked.Exchange(ref _disposed, 1);
        return ValueTask.CompletedTask;
    }

    /// <summary>Manages a connected WebSocket protocol session.</summary>
    internal sealed class WebSocketRemoteTransportSession : IRemoteTransportSession
    {
        /// <summary>The bounded number of event batches held for a subscription.</summary>
        private const int SubscriptionCapacity = 64;

        /// <summary>The protocol code used for malformed responses.</summary>
        private const string ProtocolErrorCode = "protocol-error";

        /// <summary>The maximum time allowed for the peer to acknowledge a normal close handshake.</summary>
        private static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(5);

        /// <summary>The active WebSocket.</summary>
        private readonly WebSocket _socket;

        /// <summary>The adapter configuration.</summary>
        private readonly WebSocketRemoteTransportOptions _options;

        /// <summary>Serializes concurrent sends.</summary>
        private readonly SemaphoreSlim _sendGate = new(1, 1);

        /// <summary>Cancels pending receive operations during disposal.</summary>
        private readonly CancellationTokenSource _shutdown = new();

        /// <summary>Tracks outstanding protocol requests by message identifier.</summary>
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<WebSocketProtocol.Frame>> _pending = new();

        /// <summary>Tracks active stream subscriptions.</summary>
        private readonly ConcurrentDictionary<SubscriptionId, Channel<RemoteEventBatch>> _subscriptions = new();

        /// <summary>The background frame receive loop.</summary>
        private readonly Task _receiveLoop;

        /// <summary>Tracks whether this session has been disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportSession"/> class.</summary>
        /// <param name="socket">The connected WebSocket.</param>
        /// <param name="options">The adapter configuration.</param>
        internal WebSocketRemoteTransportSession(WebSocket socket, WebSocketRemoteTransportOptions options)
        {
            _socket = socket;
            _options = options;
            _receiveLoop = ReceiveLoopAsync();
        }

        /// <summary>Gets the capabilities negotiated with the peer.</summary>
        public NegotiatedCapabilities NegotiatedCapabilities { get; private set; } = null!;

        /// <inheritdoc />
        public async ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(batch);
            var frame = await RequestAsync("push", batch, cancellationToken).ConfigureAwait(false);
            ValidateResponseType(frame, "pushResponse");
            return DeserializeBody<RemoteSyncResult>(frame);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            cancellationToken.ThrowIfCancellationRequested();
            return SubscribeCoreAsync(request, cancellationToken);
        }

        /// <inheritdoc />
        public async ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(acknowledgement);
            var frame = await RequestAsync("acknowledge", acknowledgement, cancellationToken).ConfigureAwait(false);
            ValidateResponseType(frame, "acknowledgeResponse");
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            await _shutdown.CancelAsync().ConfigureAwait(false);
            foreach (var entry in _subscriptions)
            {
                _ = entry.Value.Writer.TryComplete();
            }

            foreach (var entry in _pending)
            {
                _ = entry.Value.TrySetException(new ObjectDisposedException(nameof(WebSocketRemoteTransportSession)));
            }

            // Cancellation can abort a managed WebSocket while its ReceiveAsync call unwinds. Wait for that sole
            // receiver before inspecting state or beginning a close handshake, because concurrent receive and close
            // operations are not supported by all WebSocket implementations.
            await _receiveLoop.ConfigureAwait(false);

            if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var closeTimeout = new CancellationTokenSource(CloseTimeout);
                try
                {
                    await _socket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "disposed",
                        closeTimeout.Token).ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                }
                catch (OperationCanceledException) when (closeTimeout.IsCancellationRequested)
                {
                }
            }

            _socket.Dispose();
            _sendGate.Dispose();
            _shutdown.Dispose();
        }

        /// <summary>Negotiates the connection and protocol version.</summary>
        /// <param name="request">The client connection request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that represents the handshake.</returns>
        /// <exception cref="WebSocketRemoteTransportException">The peer rejects the connection or protocol version.</exception>
        internal async ValueTask HandshakeAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            var frame = await RequestAsync("connect", request, cancellationToken).ConfigureAwait(false);
            ValidateResponseType(frame, "connectResponse");
            NegotiatedCapabilities = DeserializeBody<NegotiatedCapabilities>(frame);
            if ((NegotiatedCapabilities.Features & ~WebSocketRemoteTransportAdapterCapabilities()) != 0)
            {
                throw new WebSocketRemoteTransportException("capability-error", "The peer negotiated an unsupported capability.");
            }

            if (request.SupportedProtocolVersions.Minimum > NegotiatedCapabilities.ProtocolVersion
                || request.SupportedProtocolVersions.Maximum < NegotiatedCapabilities.ProtocolVersion)
            {
                throw new WebSocketRemoteTransportException("protocol-version", "The peer selected an unsupported protocol version.");
            }
        }

        /// <summary>Deserializes a typed body from a validated frame.</summary>
        /// <typeparam name="T">The expected body type.</typeparam>
        /// <param name="frame">The protocol frame.</param>
        /// <returns>The deserialized body.</returns>
        /// <exception cref="WebSocketRemoteTransportException">The body is missing or contains invalid JSON.</exception>
        private static T DeserializeBody<T>(WebSocketProtocol.Frame frame)
        {
            try
            {
                return frame.Body.Deserialize(WebSocketProtocol.GetTypeInfo<T>())
                    ?? throw new WebSocketRemoteTransportException(ProtocolErrorCode, "The frame body is missing.");
            }
            catch (JsonException exception)
            {
                throw new WebSocketRemoteTransportException(ProtocolErrorCode, exception.Message);
            }
        }

        /// <summary>Ensures a response frame has the expected protocol type.</summary>
        /// <param name="frame">The response frame.</param>
        /// <param name="expected">The expected frame type.</param>
        /// <exception cref="WebSocketRemoteTransportException">The response type does not match.</exception>
        private static void ValidateResponseType(WebSocketProtocol.Frame frame, string expected)
        {
            if (!string.Equals(frame.MessageType, expected, StringComparison.Ordinal))
            {
                throw new WebSocketRemoteTransportException(ProtocolErrorCode, $"Expected {expected}, received {frame.MessageType}.");
            }
        }

        /// <summary>Gets the capabilities supported by the WebSocket adapter.</summary>
        /// <returns>The supported transport capabilities.</returns>
        private static RemoteTransportCapabilities WebSocketRemoteTransportAdapterCapabilities() =>
            RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.CursorResume
            | RemoteTransportCapabilities.ReceiveAcknowledgements
            | RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
            | RemoteTransportCapabilities.StreamingReceive;

        /// <summary>Reads events for one registered remote subscription.</summary>
        /// <param name="request">The subscription request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The received event batches.</returns>
        /// <exception cref="InvalidOperationException">The subscription is already active.</exception>
        private async IAsyncEnumerable<RemoteEventBatch> SubscribeCoreAsync(
            RemoteSubscribeRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var channel = Channel.CreateBounded<RemoteEventBatch>(
                new BoundedChannelOptions(SubscriptionCapacity) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = false, });
            if (!_subscriptions.TryAdd(request.SubscriptionId, channel))
            {
                throw new InvalidOperationException("The subscription is already active.");
            }

            try
            {
                var frame = await RequestAsync("subscribe", request, cancellationToken).ConfigureAwait(false);
                ValidateResponseType(frame, "subscribeResponse");
                await foreach (var batch in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return batch;
                }
            }
            finally
            {
                _ = _subscriptions.TryRemove(request.SubscriptionId, out _);
            }
        }

        /// <summary>Sends a request and waits for the matching response frame.</summary>
        /// <typeparam name="TBody">The request body type.</typeparam>
        /// <param name="messageType">The protocol request type.</param>
        /// <param name="body">The request body.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The response frame.</returns>
        /// <exception cref="InvalidOperationException">A message identifier collides.</exception>
        /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
        /// <exception cref="WebSocketRemoteTransportException">The peer returns a protocol error.</exception>
        private async Task<WebSocketProtocol.Frame> RequestAsync<TBody>(
            string messageType,
            TBody body,
            CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            cancellationToken.ThrowIfCancellationRequested();
            var messageId = Guid.NewGuid();
            var completion = new TaskCompletionSource<WebSocketProtocol.Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_pending.TryAdd(messageId, completion))
            {
                throw new InvalidOperationException("The protocol message identifier collided.");
            }

            try
            {
                await SendAsync(WebSocketProtocol.Serialize(messageType, messageId, null, body), cancellationToken).ConfigureAwait(false);
                await using var registration = cancellationToken.UnsafeRegister(
                    static state =>
                    {
                        var request =
                            ((TaskCompletionSource<WebSocketProtocol.Frame> Completion, CancellationToken CancellationToken))state!;
                        _ = request.Completion.TrySetCanceled(request.CancellationToken);
                    },
                    (Completion: completion, CancellationToken: cancellationToken));
                return await completion.Task.ConfigureAwait(false);
            }
            finally
            {
                _ = _pending.TryRemove(messageId, out _);
            }
        }

        /// <summary>Sends a protocol message while preserving send order.</summary>
        /// <param name="bytes">The encoded message.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that represents the send.</returns>
        private async Task SendAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _socket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _sendGate.Release();
            }
        }

        /// <summary>Receives frames and completes matching requests or subscriptions.</summary>
        /// <returns>A task that represents the receive loop.</returns>
        private async Task ReceiveLoopAsync()
        {
            var buffer = new byte[_options.ReceiveBufferBytes];
            try
            {
                while (!_shutdown.IsCancellationRequested)
                {
                    var frame = await ReceiveFrameAsync(buffer).ConfigureAwait(false);
                    await DispatchFrameAsync(frame).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                foreach (var entry in _pending)
                {
                    _ = entry.Value.TrySetException(exception);
                }

                foreach (var entry in _subscriptions)
                {
                    _ = entry.Value.Writer.TryComplete(exception);
                }
            }
        }

        /// <summary>Receives one complete protocol frame from the socket.</summary>
        /// <param name="buffer">The bounded receive buffer.</param>
        /// <returns>The parsed frame.</returns>
        /// <exception cref="WebSocketRemoteTransportException">The peer closes the socket or sends an oversized message.</exception>
        private async Task<WebSocketProtocol.Frame> ReceiveFrameAsync(byte[] buffer)
        {
            await using var message = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await _socket.ReceiveAsync(buffer, _shutdown.Token).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    throw new WebSocketRemoteTransportException("closed", "The remote WebSocket closed the session.");
                }

                await message.WriteAsync(buffer.AsMemory(0, result.Count), _shutdown.Token).ConfigureAwait(false);
                if (message.Length > _options.MaximumMessageBytes)
                {
                    throw new WebSocketRemoteTransportException("message-too-large", "The WebSocket message exceeds the configured limit.");
                }
            }
            while (!result.EndOfMessage);

            if (message.Length == 0)
            {
                // Some platforms deliver an abrupt peer disconnect as a zero-byte, non-Close result rather than a
                // proper close frame. Treat it the same as an explicit close instead of failing JSON parsing.
                throw new WebSocketRemoteTransportException("closed", "The remote WebSocket closed the session.");
            }

            return WebSocketProtocol.Parse(message.ToArray(), _options.MaximumMessageBytes);
        }

        /// <summary>Routes a parsed frame to its pending request or subscription.</summary>
        /// <param name="frame">The received frame.</param>
        /// <returns>A task that completes when the frame has been dispatched.</returns>
        private async Task DispatchFrameAsync(WebSocketProtocol.Frame frame)
        {
            if (frame.MessageType == "error")
            {
                var error = DeserializeBody<ProtocolError>(frame);
                CompletePending(frame.CorrelationId ?? frame.MessageId, new WebSocketRemoteTransportException(error.Code, error.Message));
                return;
            }

            if (frame.MessageType == "event")
            {
                var eventEnvelope = DeserializeBody<EventEnvelope>(frame);
                if (_subscriptions.TryGetValue(eventEnvelope.SubscriptionId, out var subscription))
                {
                    await subscription.Writer.WriteAsync(eventEnvelope.Batch, _shutdown.Token).ConfigureAwait(false);
                }

                return;
            }

            CompletePending(frame.CorrelationId ?? frame.MessageId, frame);
        }

        /// <summary>Completes a request waiting for a response frame.</summary>
        /// <param name="messageId">The response correlation identifier.</param>
        /// <param name="result">The response frame or protocol error.</param>
        private void CompletePending(Guid messageId, object result)
        {
            if (_pending.TryGetValue(messageId, out var pending))
            {
                if (result is Exception exception)
                {
                    _ = pending.TrySetException(exception);
                }
                else
                {
                    _ = pending.TrySetResult((WebSocketProtocol.Frame)result);
                }
            }
        }

        /// <summary>Describes a protocol error frame.</summary>
        /// <param name="Code">The stable protocol error code.</param>
        /// <param name="Message">The error message.</param>
        internal sealed record ProtocolError(string Code, string Message);

        /// <summary>Wraps a remote event batch with its subscription identifier.</summary>
        /// <param name="SubscriptionId">The target subscription.</param>
        /// <param name="Batch">The received event batch.</param>
        internal sealed record EventEnvelope(SubscriptionId SubscriptionId, RemoteEventBatch Batch);
    }
}
