// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests;

/// <summary>Tests cancellation while requests own or await send resources.</summary>
public sealed partial class WebSocketRemoteTransportAdapterTests
{
    /// <summary>Verifies disposal cancels in-flight and queued sends before releasing resources.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisposalCancelsActiveAndQueuedSendsBeforeDisposingSocket()
    {
        var socket = new BlockingSendWebSocket();
        var session = new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession(socket, CreateOptions(new(UnreachableEndpoint)));
        var active = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await socket.SendStarted.WaitAsync(SubscriptionResponseTimeout);
        var queued = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await session.DisposeAsync().AsTask().WaitAsync(SubscriptionResponseTimeout);

        await Assert.That(async () => await active.WaitAsync(SubscriptionResponseTimeout)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(async () => await queued.WaitAsync(SubscriptionResponseTimeout)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(socket.SendCancellationCompleted).IsTrue();
        await Assert.That(socket.DisposedAfterSend).IsTrue();
    }

    /// <summary>Verifies a receiver failure wins while sending is still in flight.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task ReceiverFailureCancelsSendAndPreservesOriginalFailure()
    {
        var socket = new BlockingSendWebSocket();
        await using var session = new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession(socket, CreateOptions(new(UnreachableEndpoint)));
        var pending = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await socket.SendStarted.WaitAsync(SubscriptionResponseTimeout);
        socket.FailReceive();
        var failure = await Assert.That(async () => await pending.WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        var future = await Assert.That(async () => await session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None))
            .ThrowsExactly<WebSocketRemoteTransportException>();

        await Assert.That(failure!.Code).IsEqualTo("transport-error");
        await Assert.That(failure.InnerException).IsTypeOf<WebSocketException>();
        await Assert.That(ReferenceEquals(failure, future)).IsTrue();
        await Assert.That(socket.SendCancellationCompleted).IsTrue();
    }

    /// <summary>Verifies caller cancellation releases the send gate without faulting the session.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task CallerCancellationDuringSendDoesNotTerminateSession()
    {
        var socket = new BlockingSendWebSocket();
        await using var session = new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession(socket, CreateOptions(new(UnreachableEndpoint)));
        using var cancellation = new CancellationTokenSource();
        var pending = session.PushAsync(new(Guid.NewGuid(), []), cancellation.Token).AsTask();
        await socket.SendStarted.WaitAsync(SubscriptionResponseTimeout);
        await cancellation.CancelAsync();
        await Assert.That(async () => await pending.WaitAsync(SubscriptionResponseTimeout)).Throws<OperationCanceledException>();
        var next = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await session.DisposeAsync().AsTask().WaitAsync(SubscriptionResponseTimeout);
        await Assert.That(async () => await next.WaitAsync(SubscriptionResponseTimeout)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Blocks sending until cancellation while allowing deterministic receiver failure.</summary>
    private sealed class BlockingSendWebSocket : WebSocket
    {
        /// <summary>Signals entry to the send operation.</summary>
        private readonly TaskCompletionSource _sendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Controls receiver failure independently of sending.</summary>
        private readonly TaskCompletionSource<WebSocketReceiveResult> _receive = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The simulated socket state.</summary>
        private WebSocketState _state = WebSocketState.Open;

        /// <inheritdoc />
        public override WebSocketCloseStatus? CloseStatus => null;

        /// <inheritdoc />
        public override string? CloseStatusDescription => null;

        /// <inheritdoc />
        public override WebSocketState State => _state;

        /// <inheritdoc />
        public override string? SubProtocol => null;

        /// <summary>Gets the signal that sending has started.</summary>
        internal Task SendStarted => _sendStarted.Task;

        /// <summary>Gets a value indicating whether the cancelled send has unwound.</summary>
        internal bool SendCancellationCompleted { get; private set; }

        /// <summary>Gets a value indicating whether disposal followed send cancellation.</summary>
        internal bool DisposedAfterSend { get; private set; }

        /// <inheritdoc />
        public override void Abort() => _state = WebSocketState.Aborted;

        /// <inheritdoc />
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            _state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        /// <inheritdoc />
        public override void Dispose()
        {
            DisposedAfterSend = SendCancellationCompleted;
            _state = WebSocketState.Closed;
        }

        /// <inheritdoc />
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) =>
            _receive.Task.WaitAsync(cancellationToken);

        /// <inheritdoc />
        public override async Task SendAsync(
            ArraySegment<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken)
        {
            SendCancellationCompleted = false;
            _ = _sendStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            finally
            {
                SendCancellationCompleted = true;
            }
        }

        /// <summary>Terminates the receiver while a send is in flight.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FailReceive() => _ = _receive.TrySetException(new WebSocketException("test receiver failure"));
    }
}
