// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests the HTTP remote transport adapter.</summary>
/// <content>Prepared uploads validate and own the exact body before a durable attempt begins.</content>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>Verifies preparation creates no remote effects and sends exactly its measured body once.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparePushAsyncMeasuresBodyBeforeSendingOnce()
    {
        var batch = CreateBatch();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath == ConnectRoute
            ? CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)
            : CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch)));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await Assert.That(session is IRemoteTransportBatchPreparer).IsTrue();
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);

        await Assert.That(handler.Requests.Count).IsEqualTo(1);
        await Assert.That(prepared.Batch).IsSameReferenceAs(batch);
        var measuredBytes = prepared.EncodedSizeBytes;
        var result = await prepared.SendAsync(CancellationToken.None);

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(measuredBytes).IsEqualTo(Encoding.UTF8.GetByteCount(handler.Requests[1].Body));
        await Assert.That(async () => await prepared.SendAsync(CancellationToken.None)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies negotiated operation and payload bounds are enforced before any upload is sent.</summary>
    /// <param name="maximumOperations">The negotiated operation count.</param>
    /// <param name="maximumBytes">The negotiated payload byte count.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(1, MetadataValueBytes)]
    [Arguments(NegotiatedBatchOperations, 1)]
    public async Task PreparePushAsyncRejectsNegotiatedBoundsBeforeSending(int maximumOperations, int maximumBytes)
    {
        var response = $$"""
            {"protocolVersion":"1.0","features":15,"maximumBatchOperations":{{maximumOperations}},"maximumBatchBytes":{{maximumBytes}},"serverIdempotencyRetentionMilliseconds":60000,"clientInboxRetentionRequiredMilliseconds":120000}
            """;
        var handler = new RecordingHttpHandler(request => CreateProtocolResponse(HttpStatusCode.OK, response));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var batch = new SyncBatch(CreateBatch().BatchId, [CreateOperation(1), CreateOperation(SecondSequence)]);

        var exception = await CaptureHttpExceptionAsync(async () =>
        {
            await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        });

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies missing application metadata values are rejected before HTTP body creation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparePushAsyncRejectsMissingApplicationMetadataValue()
    {
        var missingValues = new string[1];
        var operation = CreateOperation(1) with { Metadata = new Dictionary<string, string> { [StreamName] = missingValues[0] } };
        var batch = new SyncBatch(CreateBatch().BatchId, [operation]);
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var failure = await CaptureHttpExceptionAsync(async () => _ = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None));

        await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies an idle upload reserves request capacity without blocking acknowledgements.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparePushAsyncReservesCapacityUntilDisposedAndLeavesAckCapacity()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient, CreateBaseAddress(), static options => options with { MaximumConcurrentRequests = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var preparer = (IRemoteTransportBatchPreparer)session;
        var prepared = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None);
        var failure = await CaptureHttpExceptionAsync(async () => _ = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None));
        var subscribe = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);

        await session.AcknowledgeAsync(new(subscribe.SubscriptionId, subscribe.StreamId, PriorCursor), CancellationToken.None);
        await Assert.That(failure.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
        await prepared.DisposeAsync();
        await prepared.DisposeAsync();
        await Assert.That(() => prepared.Batch).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(async () => await prepared.SendAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
        await using var replacement = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None);
        await Assert.That(replacement.EncodedSizeBytes).IsGreaterThan(0);
    }

    /// <summary>Verifies owner disposal reclaims idle payloads without waiting for their callers.</summary>
    /// <param name="disposeAdapter">Whether adapter disposal initiates shutdown.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PreparePushAsyncOwnerDisposalReclaimsIdleBody(bool disposeAdapter)
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(CreateBatch(), CancellationToken.None);

        var disposal = disposeAdapter ? adapter.DisposeAsync() : session.DisposeAsync();
        await AwaitWithTimeoutAsync(disposal.AsTask());

        await Assert.That(() => prepared.Batch).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(async () => await prepared.SendAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation of completed preparation does not cancel a separately initiated send.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparePushAsyncCancellationOnlyControlsPreparation()
    {
        var batch = CreateBatch();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath == ConnectRoute
            ? CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)
            : CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch)));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, cancellation.Token);

        await cancellation.CancelAsync();
        var result = await prepared.SendAsync(CancellationToken.None);

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
    }

    /// <summary>Verifies active sends are published before a synchronous HTTP callback reenters disposal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparedPushDisposalDrainsSendStartedInsideHttpCallback()
    {
        var batch = CreateBatch();
        var release = CreateCompletionSource();
        IPreparedRemotePush? prepared = null;
        Task? disposal = null;
        var handler = new RecordingHttpHandler(async (request, _) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            disposal = (prepared ?? throw new InvalidOperationException("Prepared upload was not assigned.")).DisposeAsync().AsTask();
            await release.Task;
            return CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch));
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var send = prepared.SendAsync(CancellationToken.None).AsTask();
        try
        {
            await Assert.That(disposal).IsNotNull();
            await Assert.That(disposal?.IsCompleted).IsFalse();
            await Assert.That(prepared.DisposeAsync().AsTask()).IsSameReferenceAs(disposal);
        }
        finally
        {
            _ = release.TrySetResult(null);
            await Assert.That(async () => await send).Throws<OperationCanceledException>();
            await prepared.DisposeAsync();
        }
    }

    /// <summary>Verifies throwing request cancellation callbacks produce a shared disposal failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PreparedPushDisposalFailureIsSharedAfterCancellationCallbackThrows()
    {
        var batch = CreateBatch();
        var entered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            await using var registration = cancellationToken.Register(static () => throw new InvalidOperationException("Prepared upload cancellation callback failed."));
            _ = entered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch));
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var send = prepared.SendAsync(CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(entered.Task);

        Task DisposePreparedAsync() => prepared.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds));

        await Assert.That(DisposePreparedAsync).ThrowsExactly<AggregateException>();
        await Assert.That(async () => await send).Throws<OperationCanceledException>();
        await Assert.That(DisposePreparedAsync).ThrowsExactly<AggregateException>();
    }
}
