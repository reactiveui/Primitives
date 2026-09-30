// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests;

/// <summary>Tests the WebSocket remote transport adapter.</summary>
public sealed class WebSocketRemoteTransportAdapterTests
{
    /// <summary>The connect request message type.</summary>
    private const string ConnectMessageType = "connect";

    /// <summary>The successful connect response message type.</summary>
    private const string ConnectResponseMessageType = "connectResponse";

    /// <summary>The subscribe request message type.</summary>
    private const string SubscribeMessageType = "subscribe";

    /// <summary>The successful push response message type.</summary>
    private const string PushResponseMessageType = "pushResponse";

    /// <summary>The test stream identifier.</summary>
    private const string StreamName = "stream";

    /// <summary>The test event cursor.</summary>
    private const string EventCursor = "cursor-1";

    /// <summary>An endpoint used when a connected test socket is supplied directly.</summary>
    private const string UnreachableEndpoint = "ws://127.0.0.1:1/";

    /// <summary>The number of fields in the serialized WebSocket crash case.</summary>
    private const int CrashCaseFieldCount = 2;

    /// <summary>The expected number of push requests after retrying the same operation.</summary>
    private const int ExpectedCrashRetryPushCount = 2;

    /// <summary>The maximum number of operations accepted by the test peer.</summary>
    private const int TestMaximumBatchSize = 10;

    /// <summary>The maximum payload size accepted by the test peer.</summary>
    private const int TestMaximumPayloadBytes = 4096;

    /// <summary>The number of attempts used to bind a free local HTTP port.</summary>
    private const int HttpListenerStartAttempts = 8;

    /// <summary>The increment from the first listener attempt.</summary>
    private const int NextListenerAttemptOffset = 1;

    /// <summary>The environment variable carrying the WebSocket crash endpoint and operation ID.</summary>
    private const string WebSocketCrashCaseVariable = "RXUI_WEBSOCKET_TRANSPORT_CRASH_CASE";

    /// <summary>The fixed batch ID reused when the parent retries a push after the child crash.</summary>
    private const string WebSocketCrashBatchIdText = "00000000-0000-0000-0000-000000000301";

    /// <summary>The test tree filter for the child WebSocket transport crash test.</summary>
    private const string WebSocketCrashChildFilter = $"/*/*/*/{nameof(WhenWebSocketPushCrashChildBlocksBeforeAcknowledgement_ThenSignalsParent)}";

    /// <summary>The maximum time to wait for a subscription protocol error.</summary>
    private static readonly TimeSpan SubscriptionResponseTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The maximum time to wait for the child push to reach the peer.</summary>
    private static readonly TimeSpan WebSocketCrashSignalTimeout = TimeSpan.FromSeconds(25);

    /// <summary>The maximum time to wait for the killed child process to exit.</summary>
    private static readonly TimeSpan WebSocketCrashExitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Gets the empty JSON object used by responses without a body.</summary>
    private static JsonElement EmptyBody => JsonElement.Parse("{}");

    /// <summary>Verifies that the adapter claims only implemented features.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task CapabilitiesAdvertiseOnlyImplementedWebSocketFeatures()
    {
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(new(UnreachableEndpoint)));

        await Assert.That(adapter.Capabilities).IsEqualTo(
            RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.CursorResume
            | RemoteTransportCapabilities.ReceiveAcknowledgements
            | RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
            | RemoteTransportCapabilities.StreamingReceive);
        await Assert.That((adapter.Capabilities & RemoteTransportCapabilities.SnapshotRecovery) == 0).IsTrue();
    }

    /// <summary>Verifies the protocol handshake.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ConnectAsyncCompletesHandshakeAndReturnsNegotiatedCapabilities()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            await SendAsync(socket, ConnectResponseMessageType, request.MessageId, new NegotiatedCapabilities(
                new(1, 0),
                RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.StreamingReceive,
                TestMaximumBatchSize,
                TestMaximumPayloadBytes,
                null,
                null));
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(peer.LastMessageType).IsEqualTo(ConnectMessageType);
        await Assert.That(session.NegotiatedCapabilities.ProtocolVersion).IsEqualTo(new(1, 0));
    }

    /// <summary>Verifies correlated push and acknowledgement responses.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task PushAndAcknowledgeUseCorrelatedRequestResponses()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == "push")
            {
                var batch = request.Body.Deserialize(WebSocketProtocol.GetTypeInfo<SyncBatch>())!;
                await SendAsync(socket, PushResponseMessageType, request.MessageId, new RemoteSyncResult(batch.BatchId, [], null, null));
            }
            else if (request.MessageType == "acknowledge")
            {
                await SendAsync(socket, "acknowledgeResponse", request.MessageId, EmptyBody);
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var batch = new SyncBatch(Guid.NewGuid(), []);
        var result = await session.PushAsync(batch, CancellationToken.None);
        await session.AcknowledgeAsync(
            new(new(Guid.NewGuid()), new(StreamName), EventCursor),
            CancellationToken.None);

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(peer.MessageTypes).Contains("push");
        await Assert.That(peer.MessageTypes).Contains("acknowledge");
    }

    /// <summary>Verifies streaming event delivery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsyncDeliversStreamingEvents()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == SubscribeMessageType)
            {
                await SendAsync(socket, "subscribeResponse", request.MessageId, EmptyBody);
                var subscriptionId = request.Body.GetProperty("subscriptionId")
                    .Deserialize(WebSocketProtocol.GetTypeInfo<SubscriptionId>());
                var batch = new RemoteEventBatch(Guid.NewGuid(), new(StreamName), null, EventCursor, []);
                await SendAsync(
                    socket,
                    "event",
                    Guid.NewGuid(),
                    new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.EventEnvelope(subscriptionId, batch));
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = new RemoteSubscribeRequest(new(StreamName), new(Guid.NewGuid()), null, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(EventCursor);
    }

    /// <summary>Verifies subscriptions reject correlated responses of the wrong type before delivering events.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsyncRejectsUnexpectedResponseType()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == SubscribeMessageType)
            {
                await SendAsync(socket, PushResponseMessageType, request.MessageId, EmptyBody);
                var subscriptionId = request.Body.GetProperty("subscriptionId")
                    .Deserialize(WebSocketProtocol.GetTypeInfo<SubscriptionId>());
                await SendAsync(
                    socket,
                    "event",
                    Guid.NewGuid(),
                    new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.EventEnvelope(
                        subscriptionId,
                        new(Guid.NewGuid(), new(StreamName), null, EventCursor, [])));
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource(SubscriptionResponseTimeout);
        await using var enumerator = session.SubscribeAsync(
            new(new(StreamName), new(Guid.NewGuid()), null, StartPosition.Latest),
            cancellation.Token).GetAsyncEnumerator();

        var exception = await Assert.That(async () => await enumerator.MoveNextAsync())
            .ThrowsExactly<WebSocketRemoteTransportException>();

        await Assert.That(exception!.Code).IsEqualTo("protocol-error");
        await Assert.That(exception.Message).IsEqualTo("Expected subscribeResponse, received pushResponse.");
    }

    /// <summary>Verifies subscription cancellation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAsyncHonorsCancellation()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == SubscribeMessageType)
            {
                await SendAsync(socket, "subscribeResponse", request.MessageId, EmptyBody);
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await using var enumerator = session.SubscribeAsync(
            new(new(StreamName), new(Guid.NewGuid()), null, StartPosition.Latest),
            cancellation.Token).GetAsyncEnumerator();
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await cancellation.CancelAsync();

        await Assert.That(async () => await moveNext).Throws<OperationCanceledException>();
    }

    /// <summary>Verifies protocol errors are not retried.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ProtocolErrorsAreSurfacedWithoutRetry()
    {
        var pushCount = 0;
        await using var peer = await TestPeer.StartAsync(async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == "push")
            {
                pushCount++;
                await SendAsync(
                    socket,
                    "error",
                    request.MessageId,
                    new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.ProtocolError("rejected", "bad request"));
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = await Assert.That(
                async () => await session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None))
            .ThrowsExactly<WebSocketRemoteTransportException>();

        await Assert.That(exception!.Code).IsEqualTo("rejected");
        await Assert.That(pushCount).IsEqualTo(1);
    }

    /// <summary>Verifies session disposal closes request admission.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposalClosesSessionAndRejectsNewRequests()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await session.DisposeAsync();

        await Assert.That(async () => await session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposal waits for its cancelled receiver before it starts the close handshake.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForCancelledReceiveBeforeClosingSocket()
    {
        var socket = new CancelledReceiveWebSocket();
        var session = new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession(
            socket,
            CreateOptions(new(UnreachableEndpoint)));
        await socket.WaitForReceiveStartedAsync().WaitAsync(SubscriptionResponseTimeout);

        var dispose = session.DisposeAsync().AsTask();
        try
        {
            await socket.WaitForReceiveCancellationAsync().WaitAsync(SubscriptionResponseTimeout);
            await Assert.That(socket.GetCloseAsyncCallCount()).IsEqualTo(0);
        }
        finally
        {
            socket.CompleteReceiveCancellation();
            try
            {
                await dispose.WaitAsync(SubscriptionResponseTimeout);
            }
            catch (InvalidOperationException) when (socket.GetCloseAsyncCallCount() != 0)
            {
                // The pre-fix implementation calls CloseAsync while the receive is still unwinding.
            }
        }

        await Assert.That(socket.GetCloseAsyncCallCount()).IsEqualTo(1);
    }

    /// <summary>Verifies disposal bounds a close handshake when a peer does not acknowledge it.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncCompletesWhenCloseHandshakeDoesNotComplete()
    {
        var socket = new CancelledReceiveWebSocket(completeCloseHandshake: false);
        var session = new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession(
            socket,
            CreateOptions(new(UnreachableEndpoint)));
        await socket.WaitForReceiveStartedAsync().WaitAsync(SubscriptionResponseTimeout);

        var dispose = session.DisposeAsync().AsTask();
        try
        {
            await socket.WaitForReceiveCancellationAsync().WaitAsync(SubscriptionResponseTimeout);
        }
        finally
        {
            socket.CompleteReceiveCancellation();
        }

        await dispose.WaitAsync(SubscriptionResponseTimeout);
        await Assert.That(socket.GetCloseAsyncCallCount()).IsEqualTo(1);
    }

    /// <summary>Verifies the test peer accepts the client's close frame as normal connection shutdown.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task TestPeerStopsWhenClientSendsCloseFrame()
    {
        await using var peer = await TestPeer.StartAsync(static (_, _) => Task.CompletedTask);
        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(peer.Endpoint, CancellationToken.None);
        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        await peer.Completion.WaitAsync(SubscriptionResponseTimeout);

        await Assert.That(peer.Completion.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Verifies a process crash after server application can safely retry the same WebSocket operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The crash child does not reach the server within the timeout.</exception>
    [Test]
    [NotInParallel("websocket-transport-process-crash")]
    public async Task WhenProcessDiesDuringWebSocketPush_ThenRestartedTransportPreservesServerEffect()
    {
        await using var peer = await CrashPeer.StartAsync();
        var operationId = Guid.NewGuid();
        using var child = StartWebSocketCrashChild(peer.Endpoint, operationId);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        try
        {
            var childExit = child.WaitForExitAsync();
            var reached = await Task.WhenAny(
                peer.FirstPushApplied,
                childExit,
                Task.Delay(WebSocketCrashSignalTimeout)).ConfigureAwait(false);
            if (reached != peer.FirstPushApplied)
            {
                if (!child.HasExited)
                {
                    child.Kill(entireProcessTree: true);
                    await child.WaitForExitAsync().WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
                }

                var output = await standardOutput.WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
                var error = await standardError.WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
                throw new InvalidOperationException(
                    string.Join(Environment.NewLine, "The WebSocket crash child did not reach the server.", output, error));
            }

            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
            _ = await standardOutput.WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
            _ = await standardError.WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
            peer.ReleaseFirstPush();

            await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
            await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
            var retry = await session.PushAsync(CreateCrashBatch(operationId), CancellationToken.None);

            await Assert.That(retry.Operations).HasSingleItem();
            await Assert.That(retry.Operations[0].OperationId.Value).IsEqualTo(operationId);
            await Assert.That(retry.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
            await Assert.That(peer.AppliedOperationCount).IsEqualTo(1);
            await Assert.That(peer.PushCount).IsEqualTo(ExpectedCrashRetryPushCount);
        }
        finally
        {
            peer.ReleaseFirstPush();
            if (!child.HasExited)
            {
                child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(WebSocketCrashExitTimeout).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Runs the child transport push until its server peer applies the operation and blocks the ACK.</summary>
    /// <returns>The child test task, or a passing no-op during normal suite execution.</returns>
    /// <exception cref="InvalidOperationException">The child endpoint or operation ID is malformed.</exception>
    [Test]
    public async Task WhenWebSocketPushCrashChildBlocksBeforeAcknowledgement_ThenSignalsParent()
    {
        var encoded = Environment.GetEnvironmentVariable(WebSocketCrashCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != CrashCaseFieldCount || !Uri.TryCreate(fields[0], UriKind.Absolute, out var endpoint) || !Guid.TryParse(fields[1], out var operationId))
        {
            throw new InvalidOperationException("The WebSocket transport crash case is malformed.");
        }

        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        _ = await session.PushAsync(CreateCrashBatch(operationId), CancellationToken.None);
        throw new InvalidOperationException("The WebSocket crash child received an ACK before the parent terminated it.");
    }

    /// <summary>Creates options for the specified WebSocket endpoint.</summary>
    /// <param name="endpoint">The test peer endpoint.</param>
    /// <returns>The configured adapter options.</returns>
    private static WebSocketRemoteTransportOptions CreateOptions(Uri endpoint) => new() { Endpoint = endpoint };

    /// <summary>Creates the idempotent batch used by the process-crash test.</summary>
    /// <param name="operationId">The operation identifier to send.</param>
    /// <returns>The batch to retry after the process crash.</returns>
    private static SyncBatch CreateCrashBatch(Guid operationId) =>
        new(Guid.Parse(WebSocketCrashBatchIdText), [CreateCrashOperation(operationId)]);

    /// <summary>Creates the append operation used by the process-crash test.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The append operation.</returns>
    private static SyncOperation CreateCrashOperation(Guid operationId)
    {
        var payload = "websocket-crash-operation"u8.ToArray();
        return new()
        {
            OperationId = new(operationId),
            StreamId = new("websocket-crash"),
            ClientSequence = 1,
            TimestampUtc = new(2026, 9, 13, 0, 0, 0, TimeSpan.Zero),
            Type = SyncOperationType.Append,
            Payload = new("crash-test", 1, "text/plain", payload, Convert.ToHexString(SHA256.HashData(payload))),
        };
    }

    /// <summary>Starts the child test process that blocks on the first push.</summary>
    /// <param name="endpoint">The crash peer endpoint.</param>
    /// <param name="operationId">The operation identifier sent by the child.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child process cannot be started.</exception>
    private static Process StartWebSocketCrashChild(Uri endpoint, Guid operationId)
    {
        var assembly = Path.Combine(AppContext.BaseDirectory, "ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests.dll");
        ProcessStartInfo start = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false, };
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add("--treenode-filter");
        start.ArgumentList.Add(WebSocketCrashChildFilter);
        start.Environment[WebSocketCrashCaseVariable] = string.Join('\n', endpoint, operationId.ToString("D"));
        return Process.Start(start) ?? throw new InvalidOperationException("The WebSocket crash child did not start.");
    }

    /// <summary>Creates a connection request for the test client.</summary>
    /// <returns>The test connection request.</returns>
    private static TransportConnectRequest CreateConnectRequest() =>
        new(new(new(1, 0), new(1, 0)), new("client"), [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Creates a set of capabilities for the test peer.</summary>
    /// <returns>The negotiated test capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities() =>
        new(new(1, 0), RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.StreamingReceive, TestMaximumBatchSize, TestMaximumPayloadBytes, null, null);

    /// <summary>Sends a correlated JSON response frame to a WebSocket client.</summary>
    /// <typeparam name="TBody">The generated protocol body type.</typeparam>
    /// <param name="socket">The connected socket.</param>
    /// <param name="type">The frame type.</param>
    /// <param name="correlationId">The request identifier being answered.</param>
    /// <param name="body">The response body.</param>
    /// <returns>A task that represents the asynchronous send.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task SendAsync<TBody>(WebSocket socket, string type, Guid correlationId, TBody body) =>
        socket.SendAsync(
            WebSocketProtocol.Serialize(type, Guid.NewGuid(), correlationId, body),
            WebSocketMessageType.Text,
            true,
            CancellationToken.None);

    /// <summary>Starts an HTTP listener on a locally selected port, retrying bind races.</summary>
    /// <returns>The active listener and its port.</returns>
    /// <exception cref="InvalidOperationException">No test port could be bound.</exception>
    private static (HttpListener Listener, int Port) StartHttpListener()
    {
        for (var attempt = default(int); attempt < HttpListenerStartAttempts; attempt++)
        {
            var tcp = new TcpListener(IPAddress.Loopback, 0);
            tcp.Start();
            var port = ((IPEndPoint)tcp.LocalEndpoint).Port;
            tcp.Stop();

            var listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            try
            {
                listener.Start();
                return (listener, port);
            }
            catch (HttpListenerException) when (attempt < HttpListenerStartAttempts - NextListenerAttemptOffset)
            {
                listener.Close();
            }
        }

        throw new InvalidOperationException("The WebSocket test listener could not bind a local port.");
    }

    /// <summary>Models a receive cancellation that finishes only when the test permits it.</summary>
    private sealed class CancelledReceiveWebSocket : WebSocket
    {
        /// <summary>Signals that the receive loop has begun reading.</summary>
        private readonly TaskCompletionSource _receiveStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Signals that disposal cancelled the receive operation.</summary>
        private readonly TaskCompletionSource _receiveCancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Allows the cancelled receive operation to finish unwinding.</summary>
        private readonly TaskCompletionSource _completeReceiveCancellation = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Determines whether the close handshake completes without cancellation.</summary>
        private readonly bool _completeCloseHandshake;

        /// <summary>Tracks the socket state.</summary>
        private WebSocketState _state = WebSocketState.Open;

        /// <summary>Tracks close handshake calls.</summary>
        private int _closeAsyncCallCount;

        /// <summary>Tracks whether the cancelled receive operation has finished unwinding.</summary>
        private int _receiveCancellationCompleted;

        /// <summary>Initializes a new instance of the <see cref="CancelledReceiveWebSocket"/> class.</summary>
        /// <param name="completeCloseHandshake">Whether the simulated peer acknowledges the close handshake.</param>
        internal CancelledReceiveWebSocket(bool completeCloseHandshake = true) =>
            _completeCloseHandshake = completeCloseHandshake;

        /// <inheritdoc />
        public override WebSocketCloseStatus? CloseStatus => null;

        /// <inheritdoc />
        public override string? CloseStatusDescription => null;

        /// <inheritdoc />
        public override WebSocketState State => _state;

        /// <inheritdoc />
        public override string? SubProtocol => null;

        /// <inheritdoc />
        public override void Abort() => _state = WebSocketState.Aborted;

        /// <inheritdoc />
        public override Task CloseAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _closeAsyncCallCount);
            if (Volatile.Read(ref _receiveCancellationCompleted) == 0)
            {
                throw new InvalidOperationException("CloseAsync ran before the cancelled receive operation finished.");
            }

            if (!_completeCloseHandshake)
            {
                return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            _state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public override Task CloseOutputAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken) => Task.CompletedTask;

        /// <inheritdoc />
        public override void Dispose() => _state = WebSocketState.Closed;

        /// <inheritdoc />
        public override async Task<WebSocketReceiveResult> ReceiveAsync(
            ArraySegment<byte> buffer,
            CancellationToken cancellationToken)
        {
            _ = _receiveStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _ = _receiveCancelled.TrySetResult();
                await _completeReceiveCancellation.Task;
                Volatile.Write(ref _receiveCancellationCompleted, 1);
                throw;
            }

            throw new InvalidOperationException("The receive cancellation was not observed.");
        }

        /// <inheritdoc />
        public override Task SendAsync(
            ArraySegment<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken) => Task.CompletedTask;

        /// <summary>Lets the cancelled receive operation finish unwinding.</summary>
        internal void CompleteReceiveCancellation() => _ = _completeReceiveCancellation.TrySetResult();

        /// <summary>Gets the task that completes when receiving starts.</summary>
        /// <returns>A task that completes when the receive loop begins.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitForReceiveStartedAsync() => _receiveStarted.Task;

        /// <summary>Gets the task that completes when receiving is cancelled.</summary>
        /// <returns>A task that completes when disposal cancels the receive operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitForReceiveCancellationAsync() => _receiveCancelled.Task;

        /// <summary>Gets the number of close handshake calls.</summary>
        /// <returns>The number of close handshake calls.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetCloseAsyncCallCount() => Volatile.Read(ref _closeAsyncCallCount);
    }

    /// <summary>Hosts a local WebSocket endpoint for adapter tests.</summary>
    private sealed class TestPeer : IAsyncDisposable
    {
        /// <summary>JSON options that match the WebSocket protocol naming policy.</summary>
        /// <summary>The HTTP listener accepting test connections.</summary>
        private readonly HttpListener _listener;

        /// <summary>The task accepting and processing the test connection.</summary>
        private readonly Task _acceptTask;

        /// <summary>The callback that handles requests received from the adapter.</summary>
        private readonly Func<WebSocket, WebSocketProtocol.Frame, Task> _handler;

        /// <summary>Cancels listener processing during disposal.</summary>
        private readonly CancellationTokenSource _shutdown = new();

        /// <summary>The received message types.</summary>
        private readonly List<string> _messageTypes = [];

        /// <summary>Initializes a new instance of the <see cref="TestPeer"/> class.</summary>
        /// <param name="listener">The active listener.</param>
        /// <param name="handler">The request handler.</param>
        /// <param name="endpoint">The WebSocket endpoint.</param>
        private TestPeer(HttpListener listener, Func<WebSocket, WebSocketProtocol.Frame, Task> handler, Uri endpoint)
        {
            _listener = listener;
            _handler = handler;
            Endpoint = endpoint;
            _acceptTask = AcceptAsync();
        }

        /// <summary>Gets the WebSocket endpoint.</summary>
        internal Uri Endpoint { get; }

        /// <summary>Gets the message types received by the peer.</summary>
        internal IReadOnlyList<string> MessageTypes => _messageTypes;

        /// <summary>Gets the most recent message type received by the peer.</summary>
        internal string? LastMessageType => _messageTypes.LastOrDefault();

        /// <summary>Gets the task that completes when the peer stops accepting the client connection.</summary>
        internal Task Completion => _acceptTask;

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await _shutdown.CancelAsync();
            _listener.Stop();
            try
            {
                await _acceptTask.ConfigureAwait(false);
            }
            catch (HttpListenerException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        /// <summary>Starts a peer and accepts one WebSocket client.</summary>
        /// <param name="handler">The request handler.</param>
        /// <returns>The started test peer.</returns>
        internal static async Task<TestPeer> StartAsync(Func<WebSocket, WebSocketProtocol.Frame, Task> handler)
        {
            var (listener, port) = StartHttpListener();
            return await Task.FromResult(new TestPeer(listener, handler, new($"ws://127.0.0.1:{port}/")));
        }

        /// <summary>Reads one protocol frame and records it with the request handler.</summary>
        /// <param name="socket">The connected socket.</param>
        /// <param name="cancellationToken">The token that stops the peer.</param>
        /// <returns>The received protocol frame.</returns>
        private static async Task<WebSocketProtocol.Frame?> ReceiveAsync(WebSocket socket, CancellationToken cancellationToken)
        {
            var buffer = new byte[16_384];
            await using var message = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return null;
                }

                await message.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken).ConfigureAwait(false);
            }
            while (!result.EndOfMessage);
            return WebSocketProtocol.Parse(message.ToArray(), TestMaximumPayloadBytes);
        }

        /// <summary>Accepts the peer's WebSocket client and processes messages.</summary>
        /// <returns>A task that represents the listener loop.</returns>
        private async Task AcceptAsync()
        {
            try
            {
                var context = await _listener.GetContextAsync().ConfigureAwait(false);
                var webSocket = (await context.AcceptWebSocketAsync(null).ConfigureAwait(false)).WebSocket;
                while (!_shutdown.IsCancellationRequested && webSocket.State == WebSocketState.Open)
                {
                    var frame = await ReceiveAsync(webSocket, _shutdown.Token).ConfigureAwait(false);
                    if (frame is null)
                    {
                        return;
                    }

                    _messageTypes.Add(frame.MessageType);
                    await _handler(webSocket, frame).ConfigureAwait(false);
                }
            }
            catch (HttpListenerException) when (_shutdown.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (_shutdown.IsCancellationRequested)
            {
            }
            catch (WebSocketException)
            {
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
            {
            }
        }
    }

    /// <summary>Hosts a peer that deduplicates operations across a simulated client crash.</summary>
    private sealed class CrashPeer : IAsyncDisposable
    {
        /// <summary>The listener accepting restarted client connections.</summary>
        private readonly HttpListener _listener;

        /// <summary>Cancels listener processing during disposal.</summary>
        private readonly CancellationTokenSource _shutdown = new();

        /// <summary>Signals that the server applied the first push.</summary>
        private readonly TaskCompletionSource _firstPushApplied = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases the blocked response for the first push.</summary>
        private readonly TaskCompletionSource _releaseFirstPush = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks operation identifiers applied by the peer.</summary>
        private readonly HashSet<Guid> _appliedOperations = [];

        /// <summary>The task accepting and processing client connections.</summary>
        private readonly Task _acceptTask;

        /// <summary>The number of push requests received.</summary>
        private int _pushCount;

        /// <summary>Initializes a new instance of the <see cref="CrashPeer"/> class.</summary>
        /// <param name="listener">The active listener.</param>
        /// <param name="endpoint">The WebSocket endpoint.</param>
        private CrashPeer(HttpListener listener, Uri endpoint)
        {
            _listener = listener;
            Endpoint = endpoint;
            _acceptTask = AcceptAsync();
        }

        /// <summary>Gets the WebSocket endpoint.</summary>
        internal Uri Endpoint { get; }

        /// <summary>Gets the signal raised after applying the first push.</summary>
        internal Task FirstPushApplied => _firstPushApplied.Task;

        /// <summary>Gets the number of unique operations applied by the peer.</summary>
        internal int AppliedOperationCount => _appliedOperations.Count;

        /// <summary>Gets the number of push requests handled by the peer.</summary>
        internal int PushCount => Volatile.Read(ref _pushCount);

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            _ = _releaseFirstPush.TrySetResult();
            await _shutdown.CancelAsync();
            _listener.Stop();
            try
            {
                await _acceptTask.ConfigureAwait(false);
            }
            catch (HttpListenerException) when (_shutdown.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (_shutdown.IsCancellationRequested)
            {
            }
        }

        /// <summary>Starts a peer that holds the first push response until released.</summary>
        /// <returns>The started crash-test peer.</returns>
        internal static Task<CrashPeer> StartAsync()
        {
            var (listener, port) = StartHttpListener();
            return Task.FromResult(new CrashPeer(listener, new Uri($"ws://127.0.0.1:{port}/")));
        }

        /// <summary>Releases the first push response.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseFirstPush() => _ = _releaseFirstPush.TrySetResult();

        /// <summary>Reads one protocol frame from the crash-test client.</summary>
        /// <param name="socket">The connected client socket.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The decoded protocol frame.</returns>
        /// <exception cref="InvalidOperationException">The frame payload is empty.</exception>
        private static async Task<WebSocketProtocol.Frame> ReceiveFrameAsync(WebSocket socket, CancellationToken cancellationToken)
        {
            var buffer = new byte[16_384];
            await using var message = new MemoryStream();
            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                await message.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken).ConfigureAwait(false);
            }
            while (!result.EndOfMessage);

            return WebSocketProtocol.Parse(message.ToArray(), TestMaximumPayloadBytes);
        }

        /// <summary>Accepts connections until the peer is shut down.</summary>
        /// <returns>A task that represents the listener loop.</returns>
        private async Task AcceptAsync()
        {
            while (!_shutdown.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().ConfigureAwait(false);
                }
                catch (HttpListenerException) when (_shutdown.IsCancellationRequested)
                {
                    return;
                }
                catch (ObjectDisposedException) when (_shutdown.IsCancellationRequested)
                {
                    // Some platforms (e.g. macOS) surface a disposed listener as
                    // ObjectDisposedException instead of HttpListenerException when
                    // Stop() races with a pending GetContextAsync() call.
                    return;
                }

                var socket = (await context.AcceptWebSocketAsync(null).ConfigureAwait(false)).WebSocket;
                try
                {
                    await HandleConnectionAsync(socket).ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                }
                catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                {
                }
                finally
                {
                    socket.Dispose();
                }
            }
        }

        /// <summary>Handles requests from one WebSocket client.</summary>
        /// <param name="socket">The connected client socket.</param>
        /// <returns>A task that represents request processing.</returns>
        private async Task HandleConnectionAsync(WebSocket socket)
        {
            while (!_shutdown.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var frame = await ReceiveFrameAsync(socket, _shutdown.Token).ConfigureAwait(false);
                if (frame.MessageType == "connect")
                {
                    await SendAsync(socket, ConnectResponseMessageType, frame.MessageId, CreateCapabilities()).ConfigureAwait(false);
                }
                else if (frame.MessageType == "push")
                {
                    await HandlePushAsync(socket, frame).ConfigureAwait(false);
                }
            }
        }

        /// <summary>Applies operations once and sends a correlated push response.</summary>
        /// <param name="socket">The connected client socket.</param>
        /// <param name="frame">The push request frame.</param>
        /// <returns>A task that represents push processing.</returns>
        /// <exception cref="InvalidOperationException">The batch payload is empty.</exception>
        private async Task HandlePushAsync(WebSocket socket, WebSocketProtocol.Frame frame)
        {
            var batch = frame.Body.Deserialize(WebSocketProtocol.GetTypeInfo<SyncBatch>())
                ?? throw new InvalidOperationException("The crash peer received an empty WebSocket batch.");
            _ = Interlocked.Increment(ref _pushCount);
            var results = new OperationSyncResult[batch.Operations.Count];
            for (var index = 0; index < batch.Operations.Count; index++)
            {
                var operation = batch.Operations[index];
                if (_appliedOperations.Add(operation.OperationId.Value))
                {
                    _ = _firstPushApplied.TrySetResult();
                    await _releaseFirstPush.Task.WaitAsync(_shutdown.Token).ConfigureAwait(false);
                }

                results[index] = new(operation.OperationId, OperationResultKind.Accepted, null, "server-1");
            }

            await SendAsync(
                socket,
                PushResponseMessageType,
                frame.MessageId,
                new RemoteSyncResult(batch.BatchId, results, "cursor-1", null)).ConfigureAwait(false);
        }
    }
}
