// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests;

/// <summary>Tests bounded event lanes and terminal request admission.</summary>
public sealed partial class WebSocketRemoteTransportAdapterTests
{
    /// <summary>The number of batches needed to exceed the byte limit.</summary>
    private const int ByteOverflowEventCount = 8;

    /// <summary>The number of batches needed to exceed the count limit.</summary>
    private const int CountOverflowEventCount = 65;

    /// <summary>The smallest subscription byte budget.</summary>
    private const int SmallSubscriptionByteBudget = 1024;

    /// <summary>The number of subscriptions after resuming once.</summary>
    private const int ExpectedResumeSubscribeCount = 2;

    /// <summary>Verifies count and byte overflow cannot starve responses on a real socket.</summary>
    /// <param name="byteBound">Whether to exercise the byte limit instead of the batch count.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FullSubscriptionDoesNotBlockPushOrAcknowledgementAndResumesSavedCursor(bool byteBound)
    {
        var subscriptionId = new SubscriptionId(Guid.NewGuid());
        var subscribeCount = 0;
        string? resumedCursor = null;
        await using var peer = await TestPeer.StartAsync(async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == SubscribeMessageType)
            {
                subscribeCount++;
                if (request.Body.TryGetProperty("cursor", out var cursor))
                {
                    resumedCursor = cursor.GetString();
                }

                await SendAsync(socket, "subscribeResponse", request.MessageId, EmptyBody);
                await SendEventAsync(socket, subscriptionId, EventCursor);
            }
            else if (request.MessageType == "push")
            {
                // The consumer is paused after its first event, just as an engine awaiting a push ACK can be.
                await FillSubscriptionAsync(socket, subscriptionId, byteBound);
                var batch = request.Body.Deserialize(WebSocketProtocol.GetTypeInfo<SyncBatch>())!;
                await SendAsync(socket, PushResponseMessageType, request.MessageId, new RemoteSyncResult(batch.BatchId, [], null, null));
            }
            else if (request.MessageType == "acknowledge")
            {
                await SendAsync(socket, "acknowledgeResponse", request.MessageId, EmptyBody);
            }
        });
        var options = CreateOptions(peer.Endpoint);
        options = options with { MaximumBufferedSubscriptionBytes = byteBound ? SmallSubscriptionByteBudget : options.MaximumBufferedSubscriptionBytes };
        await using var adapter = new WebSocketRemoteTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(new(StreamName), subscriptionId, null, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync().AsTask().WaitAsync(SubscriptionResponseTimeout)).IsTrue();
        var savedCursor = enumerator.Current.NextCursor;
        var push = new SyncBatch(Guid.NewGuid(), []);

        var result = await session.PushAsync(push, CancellationToken.None).AsTask().WaitAsync(SubscriptionResponseTimeout);
        await session.AcknowledgeAsync(new(subscriptionId, new(StreamName), savedCursor), CancellationToken.None)
            .AsTask().WaitAsync(SubscriptionResponseTimeout);
        var overflow = await Assert.That(async () => await enumerator.MoveNextAsync().AsTask().WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        await Assert.That(result.BatchId).IsEqualTo(push.BatchId);
        await Assert.That(overflow!.Code).IsEqualTo("subscription-overflow");
        await Assert.That(overflow.RetryFailure.Kind).IsEqualTo(RetryFailureKind.Transient);

        await using var resumed = session.SubscribeAsync(subscribe with { Cursor = savedCursor }, CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await resumed.MoveNextAsync().AsTask().WaitAsync(SubscriptionResponseTimeout)).IsTrue();
        await Assert.That(subscribeCount).IsEqualTo(ExpectedResumeSubscribeCount);
        await Assert.That(resumedCursor).IsEqualTo(EventCursor);
        await Assert.That(resumed.Current.NextCursor).IsEqualTo(EventCursor);
    }

    /// <summary>Verifies receiver failures fault concurrent requests and close future admission.</summary>
    /// <param name="malformed">Whether the peer sends malformed JSON rather than a close frame.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReceiverFailureFaultsPendingAndFutureRequestsWithSameClassifiedFailure(bool malformed)
    {
        var pushReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var peer = await TestPeer.StartAsync((socket, request) =>
            HandleReceiverFailureRequestAsync(socket, request, malformed, pushReceived));
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var events = session.SubscribeAsync(
            new(new(StreamName), new(Guid.NewGuid()), null, StartPosition.Latest),
            CancellationToken.None).GetAsyncEnumerator();
        var pendingEvent = events.MoveNextAsync().AsTask();
        var pendingPush = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await pushReceived.Task.WaitAsync(SubscriptionResponseTimeout);
        var pendingAck = session.AcknowledgeAsync(new(new(Guid.NewGuid()), new(StreamName), EventCursor), CancellationToken.None).AsTask();
        var first = await Assert.That(async () => await pendingPush.WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        var second = await Assert.That(async () => await pendingAck.WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        var future = await Assert.That(async () => await session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None)
                .AsTask().WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        var eventFailure = await Assert.That(async () => await pendingEvent.WaitAsync(SubscriptionResponseTimeout))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        var subscribeRequest = new RemoteSubscribeRequest(new(StreamName), new(Guid.NewGuid()), null, StartPosition.Latest);
        var subscribe = await Assert.That(() => session.SubscribeAsync(subscribeRequest, CancellationToken.None))
            .ThrowsExactly<WebSocketRemoteTransportException>();

        await Assert.That(ReferenceEquals(first, second)).IsTrue();
        await Assert.That(ReferenceEquals(first, future)).IsTrue();
        await Assert.That(ReferenceEquals(first, eventFailure)).IsTrue();
        await Assert.That(ReferenceEquals(first, subscribe)).IsTrue();
        await Assert.That(first!.Code).IsEqualTo(malformed ? "protocol-error" : "closed");
        await Assert.That(first.RetryFailure.Kind).IsEqualTo(
            malformed ? RetryFailureKind.ValidationRejected : RetryFailureKind.AmbiguousTransportOutcome);
        await session.DisposeAsync();
        var afterDispose = await Assert.That(async () => await session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None))
            .ThrowsExactly<WebSocketRemoteTransportException>();
        await Assert.That(ReferenceEquals(first, afterDispose)).IsTrue();
    }

    /// <summary>Verifies disposal faults a pending request without relying on caller cancellation.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task DisposalCompletesPendingRequestsBeforeReleasingResources()
    {
        var pushReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var peer = await TestPeer.StartAsync(async (socket, request) =>
        {
            if (request.MessageType == ConnectMessageType)
            {
                await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
            }
            else if (request.MessageType == "push")
            {
                _ = pushReceived.TrySetResult();
            }
        });
        await using var adapter = new WebSocketRemoteTransportAdapter(CreateOptions(peer.Endpoint));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var pending = session.PushAsync(new(Guid.NewGuid(), []), CancellationToken.None).AsTask();
        await pushReceived.Task.WaitAsync(SubscriptionResponseTimeout);
        await session.DisposeAsync().AsTask().WaitAsync(SubscriptionResponseTimeout);
        await Assert.That(async () => await pending.WaitAsync(SubscriptionResponseTimeout)).ThrowsExactly<ObjectDisposedException>();
        await session.DisposeAsync();
    }

    /// <summary>Verifies stable codes classify retry policy without adding transport retries.</summary>
    /// <param name="code">The protocol error code.</param>
    /// <param name="kind">The expected classification.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments("transport-error", RetryFailureKind.AmbiguousTransportOutcome)]
    [Arguments("message-too-large", RetryFailureKind.PayloadTooLarge)]
    [Arguments("authentication", RetryFailureKind.Authentication)]
    [Arguments("authorization", RetryFailureKind.AuthorizationDenied)]
    [Arguments("rejected", RetryFailureKind.ValidationRejected)]
    public async Task ProtocolFailureClassifiesEngineRetry(string code, RetryFailureKind kind)
    {
        var failure = new WebSocketRemoteTransportException(code, "test");
        await Assert.That(failure.RetryFailure.Kind).IsEqualTo(kind);
    }

    /// <summary>Sends a test batch to the specified event lane.</summary>
    /// <param name="socket">The real connected server socket.</param>
    /// <param name="subscriptionId">The event lane identifier.</param>
    /// <param name="cursor">The batch cursor.</param>
    /// <returns>The send task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task SendEventAsync(WebSocket socket, SubscriptionId subscriptionId, string cursor) =>
        SendAsync(
            socket,
            "event",
            Guid.NewGuid(),
            new WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.EventEnvelope(
                subscriptionId,
                new(Guid.NewGuid(), new(StreamName), null, cursor, [])));

    /// <summary>Fills a paused consumer's subscription.</summary>
    /// <param name="socket">The connected server socket.</param>
    /// <param name="subscriptionId">The event lane.</param>
    /// <param name="byteBound">Whether to exceed the byte budget first.</param>
    /// <returns>The send task.</returns>
    private static async Task FillSubscriptionAsync(WebSocket socket, SubscriptionId subscriptionId, bool byteBound)
    {
        var eventCount = byteBound ? ByteOverflowEventCount : CountOverflowEventCount;
        for (var index = 0; index < eventCount; index++)
        {
            await SendEventAsync(socket, subscriptionId, $"undelivered-{index}");
        }
    }

    /// <summary>Fails receiving after both push and acknowledgement requests reach the real server.</summary>
    /// <param name="socket">The connected server socket.</param>
    /// <param name="request">The client request.</param>
    /// <param name="malformed">Whether to send malformed JSON.</param>
    /// <param name="pushReceived">The signal raised when a push is pending.</param>
    /// <returns>The peer handler task.</returns>
    private static async Task HandleReceiverFailureRequestAsync(
        WebSocket socket,
        WebSocketProtocol.Frame request,
        bool malformed,
        TaskCompletionSource pushReceived)
    {
        if (request.MessageType == ConnectMessageType)
        {
            await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities());
        }
        else if (request.MessageType == "push")
        {
            _ = pushReceived.TrySetResult();
        }
        else if (request.MessageType == SubscribeMessageType)
        {
            await SendAsync(socket, "subscribeResponse", request.MessageId, EmptyBody);
        }
        else if (request.MessageType == "acknowledge")
        {
            if (malformed)
            {
                await socket.SendAsync("not-json"u8.ToArray(), WebSocketMessageType.Text, true, CancellationToken.None);
            }
            else
            {
                await socket.CloseOutputAsync(WebSocketCloseStatus.InternalServerError, "receiver failed", CancellationToken.None);
            }
        }
    }
}
