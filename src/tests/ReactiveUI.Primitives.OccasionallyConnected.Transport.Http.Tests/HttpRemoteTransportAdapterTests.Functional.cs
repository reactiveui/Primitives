// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Functional edge-case tests for <see cref="HttpRemoteTransportAdapter"/>.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The timestamp query marker.</summary>
    private const string TimestampQueryMarker = "timestamp=";

    /// <summary>The sequence query marker.</summary>
    private const string SequenceQueryMarker = "sequence=9";

    /// <summary>The initial cursor query marker.</summary>
    private const string InitialCursorQueryMarker = "initialCursor=start-cursor";

    /// <summary>The cursor preceding the current receive cursor.</summary>
    private const string PriorCursor = "cursor-0";

    /// <summary>The current receive cursor.</summary>
    private const string CurrentCursor = "cursor-1";

    /// <summary>The next receive cursor.</summary>
    private const string NextCursor = "cursor-2";

    /// <summary>A skipped previous cursor.</summary>
    private const string SkippedPreviousCursor = "cursor-3";

    /// <summary>A skipped next cursor.</summary>
    private const string SkippedNextCursor = "cursor-4";

    /// <summary>The sequence used by query tests.</summary>
    private const int SubscribeSequence = 9;

    /// <summary>The first subscribe attempt number.</summary>
    private const int FirstSubscribeAttempt = 1;

    /// <summary>The expected connect and subscribe request count.</summary>
    private const int ConnectAndSubscribeRequestCount = 2;

    /// <summary>Verifies the convenience overload routes through the cancellation-aware connect path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncWithoutCancellationTokenUsesConfiguredConnectEndpoint()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        _ = await adapter.ConnectAsync(CreateConnectRequest());

        await Assert.That(handler.Requests[0].RequestUri).IsEqualTo(new(ConnectEndpoint));
    }

    /// <summary>Verifies connect is rejected after adapter disposal starts.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncAfterDisposeThrowsObjectDisposedException()
    {
        using var httpClient = CreateHttpClient(new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)));
        var adapter = CreateAdapter(httpClient);
        await adapter.DisposeAsync();

        await Assert.That(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies configured connect routes cannot escape the trusted base path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsConfiguredPathEscapingBaseAddress()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient, CreateBaseAddress(), static options => options with { ConnectPath = "../connect" });

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Configuration);
        await Assert.That(handler.Requests).IsEmpty();
    }

    /// <summary>Verifies connect transport failures are reported as ambiguous outcomes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncMapsTransportExceptionToAmbiguousOutcome()
    {
        var handler = new RecordingHttpHandler(static request => throw new HttpRequestException("connection failed before response headers"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.AmbiguousTransportOutcome);
        await Assert.That(exception.InnerException).IsTypeOf<HttpRequestException>();
    }

    /// <summary>Verifies non-success push responses are classified without deserializing the body.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncMapsNonSuccessResponseToTransportException()
    {
        var handler = new RecordingHttpHandler(static request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            PushRoute => new(HttpStatusCode.Unauthorized),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Authentication);
        await Assert.That(exception.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
    }

    /// <summary>Verifies configured push routes cannot escape the trusted base path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsConfiguredPathEscapingBaseAddress()
    {
        var handler = new RecordingHttpHandler(static request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient, CreateBaseAddress(), static options => options with { PushPath = "../push" });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Configuration);
        await Assert.That(handler.Requests).Count().IsEqualTo(1);
    }

    /// <summary>Verifies subscription start positions are encoded into their protocol query values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncEncodesTimestampSequenceAndInitialCursorQueries()
    {
        var timestampQuery = await CaptureSubscribeQueryAsync(StartPosition.FromTimestamp(new(2026, 9, 13, 0, 0, 0, TimeSpan.Zero)));
        var sequenceQuery = await CaptureSubscribeQueryAsync(StartPosition.FromSequence(SubscribeSequence));
        var cursorQuery = await CaptureSubscribeQueryAsync(StartPosition.FromCursor("start-cursor"));

        await Assert.That(timestampQuery).Contains(TimestampQueryMarker);
        await Assert.That(sequenceQuery).Contains(SequenceQueryMarker);
        await Assert.That(cursorQuery).Contains(InitialCursorQueryMarker);
    }

    /// <summary>Verifies disposing a subscription enumerator before the first move sends no long-poll request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncEnumeratorDisposeBeforeFirstMoveSendsNoPoll()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(StartPosition.Latest);

        var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        await enumerator.DisposeAsync();

        await Assert.That(handler.Requests).Count().IsEqualTo(1);
    }

    /// <summary>Verifies a pre-canceled subscription completes without sending a long-poll request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncReturnsFalseWhenCancellationIsAlreadyRequested()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var subscribe = CreateSubscribeRequest(StartPosition.Latest);

        await using var enumerator = session.SubscribeAsync(subscribe, cancellation.Token).GetAsyncEnumerator(CancellationToken.None);
        var hasBatch = await enumerator.MoveNextAsync();

        await Assert.That(hasBatch).IsFalse();
        await Assert.That(handler.Requests).Count().IsEqualTo(1);
    }

    /// <summary>Verifies empty long-poll responses continue until a later batch is available.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncContinuesAfterNoContentResponse()
    {
        var subscribeResponse = SubscribeResponseJson();
        var subscribeAttempts = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            if (request.RequestUri?.AbsolutePath == SubscribeRoute)
            {
                subscribeAttempts++;
                return subscribeAttempts == FirstSubscribeAttempt ? new(HttpStatusCode.NoContent) : CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse);
            }

            return CreateProtocolResponse(HttpStatusCode.NotFound, "{}");
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(StartPosition.Latest);

        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var hasBatch = await enumerator.MoveNextAsync();

        await Assert.That(hasBatch).IsTrue();
        await Assert.That(subscribeAttempts).IsEqualTo(ConnectAndSubscribeRequestCount);
    }

    /// <summary>Verifies cancellation after a delivered batch stops the subscription on the next move.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncReturnsFalseAfterCancellationFollowingBatch()
    {
        var subscribeResponse = SubscribeResponseJson();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            SubscribeRoute => CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(StartPosition.Latest);
        using var cancellation = new CancellationTokenSource();

        await using var enumerator = session.SubscribeAsync(subscribe, cancellation.Token).GetAsyncEnumerator(CancellationToken.None);
        var hasBatch = await enumerator.MoveNextAsync();
        await cancellation.CancelAsync();
        var hasSecondBatch = await enumerator.MoveNextAsync();

        await Assert.That(hasBatch).IsTrue();
        await Assert.That(hasSecondBatch).IsFalse();
        await Assert.That(handler.Requests).Count().IsEqualTo(ConnectAndSubscribeRequestCount);
    }

    /// <summary>Verifies a duplicate batch at the current cursor is delivered for downstream idempotent storage handling.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncAllowsImmediateDuplicateAtCurrentCursorBeforeNewBatch()
    {
        var subscribeResponse = SubscribeResponseJson(
            SubscribeBatchJson("00000000-0000-0000-0000-000000000301", PriorCursor, CurrentCursor),
            SubscribeBatchJson("00000000-0000-0000-0000-000000000302", CurrentCursor, NextCursor));
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            SubscribeRoute => CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(CurrentCursor, StartPosition.Latest);

        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var hasDuplicate = await enumerator.MoveNextAsync();
        var duplicate = enumerator.Current;
        var hasNew = await enumerator.MoveNextAsync();
        var next = enumerator.Current;

        await Assert.That(hasDuplicate).IsTrue();
        await Assert.That(duplicate.NextCursor).IsEqualTo(CurrentCursor);
        await Assert.That(hasNew).IsTrue();
        await Assert.That(next.PreviousCursor).IsEqualTo(CurrentCursor);
        await Assert.That(next.NextCursor).IsEqualTo(NextCursor);
    }

    /// <summary>Verifies a new batch cannot skip the current receive cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncRejectsSkippedNewCursorChain()
    {
        var subscribeResponse = SubscribeResponseJson(SubscribeBatchJson("00000000-0000-0000-0000-000000000303", SkippedPreviousCursor, SkippedNextCursor));
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            SubscribeRoute => CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(CurrentCursor, StartPosition.Latest);

        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await enumerator.MoveNextAsync());

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies session operations reject work after disposal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncAfterSessionDisposeThrowsObjectDisposedException()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await session.DisposeAsync();

        await Assert.That(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposing a session cancels an active push request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCancelsActivePushRequest()
    {
        var pushEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = pushEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, "{}");
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var push = session.PushAsync(CreateBatch(), CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(pushEntered.Task);

        await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());

        await Assert.That(push.IsCanceled).IsTrue();
    }

    /// <summary>Verifies disposing a session cancels an active acknowledgement request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCancelsActiveAcknowledgementRequest()
    {
        var acknowledgementEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = acknowledgementEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new(HttpStatusCode.NoContent);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(StartPosition.Latest);
        var acknowledgement = new ReceiveAcknowledgement(subscribe.SubscriptionId, subscribe.StreamId, CurrentCursor);
        var acknowledge = session.AcknowledgeAsync(acknowledgement, CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(acknowledgementEntered.Task);

        await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());

        await Assert.That(acknowledge.IsCanceled).IsTrue();
    }

    /// <summary>Captures the subscribe query for one start position.</summary>
    /// <param name="position">The start position.</param>
    /// <returns>The captured query.</returns>
    private static async Task<string> CaptureSubscribeQueryAsync(StartPosition position)
    {
        var subscribeResponse = SubscribeResponseJson();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            SubscribeRoute => CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = CreateSubscribeRequest(position);

        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        _ = await enumerator.MoveNextAsync();

        return handler.Requests[1].RequestUri?.Query ?? string.Empty;
    }

    /// <summary>Creates a subscribe request.</summary>
    /// <param name="position">The start position.</param>
    /// <returns>The request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteSubscribeRequest CreateSubscribeRequest(StartPosition position) => CreateSubscribeRequest(null, position);

    /// <summary>Creates a subscribe request.</summary>
    /// <param name="cursor">The durable receive cursor.</param>
    /// <param name="position">The start position.</param>
    /// <returns>The request.</returns>
    private static RemoteSubscribeRequest CreateSubscribeRequest(string? cursor, StartPosition position) => new(
        CreateStreamId(),
        new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000099")),
        cursor,
        position);

    /// <summary>Creates a subscribe response containing the supplied batch JSON payloads.</summary>
    /// <param name="batches">The batch payloads.</param>
    /// <returns>The response JSON.</returns>
    private static string SubscribeResponseJson(params string[] batches) => $$"""{"batches":[{{string.Join(",", batches)}}]}""";

    /// <summary>Creates a minimal valid subscribe batch JSON payload.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <returns>The batch JSON.</returns>
    private static string SubscribeBatchJson(string batchId, string previousCursor, string nextCursor) =>
        $$"""{"batchId":"{{batchId}}","streamId":"stream-1","previousCursor":"{{previousCursor}}","nextCursor":"{{nextCursor}}","events":[],"completedOperations":[]}""";
}
