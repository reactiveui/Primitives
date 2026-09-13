// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests the HTTP remote transport adapter.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The trusted test base address.</summary>
    private const string BaseAddressText = "https://example.invalid/oc/";

    /// <summary>The connect route path.</summary>
    private const string ConnectRoute = "/oc/connect";

    /// <summary>The push route path.</summary>
    private const string PushRoute = "/oc/push";

    /// <summary>The subscribe route path.</summary>
    private const string SubscribeRoute = "/oc/subscribe";

    /// <summary>The acknowledge route path.</summary>
    private const string AcknowledgeRoute = "/oc/ack";

    /// <summary>The expected connect endpoint.</summary>
    private const string ConnectEndpoint = "https://example.invalid/oc/connect";

    /// <summary>The expected push endpoint.</summary>
    private const string PushEndpoint = "https://example.invalid/oc/push";

    /// <summary>The expected ACK endpoint.</summary>
    private const string AcknowledgeEndpoint = "https://example.invalid/oc/ack";

    /// <summary>The negotiated batch operation count.</summary>
    private const int NegotiatedBatchOperations = 10;

    /// <summary>The second operation sequence.</summary>
    private const int SecondSequence = 2;

    /// <summary>The shared stream identifier.</summary>
    private const string StreamName = "stream-1";

    /// <summary>The request byte limit used by one bounded serialization test.</summary>
    private const int SmallRequestBytes = 320;

    /// <summary>The metadata value limit used by one bounded serialization test.</summary>
    private const int MetadataValueBytes = 1024;

    /// <summary>The metadata size that should exceed the request byte budget once encoded.</summary>
    private const int LargeMetadataCharacters = 260;

    /// <summary>The fake chunked response character count.</summary>
    private const int LargeResponseCharacters = 80;

    /// <summary>The small response byte limit.</summary>
    private const int SmallResponseBytes = 16;

    /// <summary>The bounded async wait timeout.</summary>
    private const int AwaitTimeoutSeconds = 5;

    /// <summary>The retry-after delay used by status mapping tests.</summary>
    private const int RetryAfterSeconds = 7;

    /// <summary>The protocol content type header value.</summary>
    private const string ProtocolContentType = "application/vnd.reactiveui.occasionally-connected+json; v=1";

    /// <summary>The protocol media type.</summary>
    private const string ProtocolMediaType = "application/vnd.reactiveui.occasionally-connected+json;v=1";

    /// <summary>The common connect response JSON.</summary>
    private const string ConnectResponseJson = """
        {"protocolVersion":"1.0","features":15,"maximumBatchOperations":10,"maximumBatchBytes":1024,"serverIdempotencyRetentionMilliseconds":60000,"clientInboxRetentionRequiredMilliseconds":120000}
        """;

    /// <summary>Verifies the adapter can connect through the public transport contract.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncSendsRouteMediaTypeAndReturnsNegotiatedSession()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(session.NegotiatedCapabilities.ProtocolVersion).IsEqualTo(new(1, 0));
        await Assert.That(session.NegotiatedCapabilities.Features).IsEqualTo(
            RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.CursorResume
            | RemoteTransportCapabilities.ReceiveAcknowledgements
            | RemoteTransportCapabilities.ServerIdempotency);
        await Assert.That(handler.Requests[0].Method).IsEqualTo(HttpMethod.Post);
        await Assert.That(handler.Requests[0].RequestUri).IsEqualTo(new(ConnectEndpoint));
        await Assert.That(handler.Requests[0].ContentType).IsEqualTo(ProtocolContentType);
        await Assert.That(handler.Requests[0].Accept).IsEqualTo(ProtocolContentType);
        await Assert.That(handler.Requests[0].Body).Contains("\"clientId\":\"client-1\"");
    }

    /// <summary>Verifies client identity data is serialized as protocol input without fabricating transport credentials.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncDoesNotCreateAuthorizationHeadersFromClientIdentity()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(handler.Requests[0].Authorization).IsNull();
        await Assert.That(handler.Requests[0].Headers.Any(static header => header.Key.Contains("Tenant", StringComparison.OrdinalIgnoreCase))).IsFalse();
    }

    /// <summary>Verifies plain HTTP is rejected except for explicit loopback development endpoints.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionsRejectPlainHttpUnlessLoopbackOptedIn()
    {
        using var httpClient = CreateHttpClient(new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)));
        var remoteOptions = new HttpRemoteTransportOptions { HttpClient = httpClient, BaseAddress = new("http://example.invalid/oc/") };
        var loopbackOptions = new HttpRemoteTransportOptions { HttpClient = httpClient, BaseAddress = new("http://127.0.0.1:6553/oc/"), AllowInsecureLoopbackHttp = true };

        await Assert.That(remoteOptions.Validate).ThrowsExactly<ArgumentException>();
        loopbackOptions.Validate();
    }

    /// <summary>Verifies the explicit loopback HTTP opt-in works against a real local TCP listener.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncAllowsExplicitLoopbackHttpServer()
    {
        var server = await LoopbackHttpServer.StartAsync(ConnectResponseJson, CancellationToken.None);
        try
        {
            using var httpClient = CreateHttpClient();
            await using var adapter = CreateAdapter(
                httpClient,
                server.BaseAddress,
                static options => options with { AllowInsecureLoopbackHttp = true });

            var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

            await Assert.That(session.NegotiatedCapabilities.MaximumBatchOperations).IsEqualTo(NegotiatedBatchOperations);
            await Assert.That(server.RequestLine).IsEqualTo("POST /oc/connect HTTP/1.1");
        }
        finally
        {
            await server.StopAsync();
        }
    }

    /// <summary>Verifies push sends stable batch and operation identities and accepts exact results.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncSendsBatchAndAcceptsExactOperationResults()
    {
        var batch = CreateBatch();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            PushRoute => CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch)),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var result = await session.PushAsync(batch, CancellationToken.None);

        var push = handler.Requests[1];
        using var document = JsonDocument.Parse(push.Body);
        await Assert.That(push.RequestUri).IsEqualTo(new(PushEndpoint));
        await Assert.That(document.RootElement.GetProperty("batchId").GetGuid()).IsEqualTo(batch.BatchId);
        await Assert.That(document.RootElement.GetProperty("operations")[0].GetProperty("operationId").GetGuid())
            .IsEqualTo(batch.Operations[0].OperationId.Value);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(result.Operations[0].OperationId).IsEqualTo(batch.Operations[0].OperationId);
    }

    /// <summary>Verifies ambiguous send failures do not alter caller-owned retry identifiers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncDroppedResponseLeavesRetryIdentityToCaller()
    {
        var batch = CreateBatch();
        var pushAttempts = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            pushAttempts++;
            if (pushAttempts == 1)
            {
                throw new IOException("connection dropped after request body was sent");
            }

            return CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch));
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(batch, CancellationToken.None));
        var result = await session.PushAsync(batch, CancellationToken.None);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.AmbiguousTransportOutcome);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(handler.Requests[1].Body).IsEqualTo(handler.Requests[2].Body);
    }

    /// <summary>Verifies the adapter rejects push responses that do not exactly match the submitted batch.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsMismatchedResultMembership()
    {
        var batch = CreateBatch();
        var handler = new RecordingHttpHandler(static request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            PushRoute => CreateProtocolResponse(HttpStatusCode.OK, """
                {"batchId":"00000000-0000-0000-0000-000000000001","operations":[]}
                """),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        _ = await CaptureSyncBatchExceptionAsync(async () => _ = await session.PushAsync(batch, CancellationToken.None));
    }

    /// <summary>Verifies count admission occurs before the codec allocates operation DTOs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsTooManyOperationsBeforeSendingRequest()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumBatchOperations = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var batch = new SyncBatch(Guid.Parse("00000000-0000-0000-0000-000000000002"), [CreateOperation(1), CreateOperation(SecondSequence)]);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(batch, CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies encoded metadata contributes to the exact request byte bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsMetadataExpansionBeforeBodyEscapesBound()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumRequestBytes = SmallRequestBytes, MaximumMetadataValueBytes = MetadataValueBytes });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var operation = CreateOperation(1) with { Metadata = new Dictionary<string, string> { ["notes"] = new('x', LargeMetadataCharacters) } };
        var batch = new SyncBatch(Guid.Parse("00000000-0000-0000-0000-000000000003"), [operation]);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(batch, CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies malformed protocol JSON is rejected as a protocol violation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsMalformedJsonResponse()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, "{not-json"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies successful responses must use the protocol media type.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsUnexpectedMediaType()
    {
        var handler = new RecordingHttpHandler(static request => CreateJsonResponse(HttpStatusCode.OK, ConnectResponseJson, "application/json"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Verifies successful responses must include the v1 protocol media type parameter.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsMissingMediaTypeVersion()
    {
        var handler = new RecordingHttpHandler(static request => CreateJsonResponse(
            HttpStatusCode.OK,
            ConnectResponseJson,
            "application/vnd.reactiveui.occasionally-connected+json"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Verifies successful responses must not advertise an incompatible protocol media type version.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsIncompatibleMediaTypeVersion()
    {
        var handler = new RecordingHttpHandler(static request => CreateJsonResponse(
            HttpStatusCode.OK,
            ConnectResponseJson,
            "application/vnd.reactiveui.occasionally-connected+json; v=2"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Verifies successful responses must not include ambiguous duplicate media type versions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsDuplicateMediaTypeVersion()
    {
        var handler = new RecordingHttpHandler(static request => CreateJsonResponse(
            HttpStatusCode.OK,
            ConnectResponseJson,
            "application/vnd.reactiveui.occasionally-connected+json; v=1; v=1"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Verifies chunked responses are bounded even without a Content-Length header.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsOversizedChunkedResponseWithoutContentLength()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateProtocolResponse(HttpStatusCode.OK, new(' ', LargeResponseCharacters));
            response.Content.Headers.ContentLength = null;
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumResponseBytes = SmallResponseBytes });

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies non-success statuses are classified and Retry-After is preserved when valid.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncMapsRetryAfterStatus()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateProtocolResponse(HttpStatusCode.TooManyRequests, "{}");
            _ = response.Headers.TryAddWithoutValidation("Retry-After", RetryAfterSeconds.ToString(CultureInfo.InvariantCulture));
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
        await Assert.That(exception.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);
        await Assert.That(exception.RetryAfter).IsEqualTo(TimeSpan.FromSeconds(RetryAfterSeconds));
    }

    /// <summary>Verifies visible redirects are rejected when the supplied handler does not auto-follow.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsVisibleRedirectResponse()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateProtocolResponse(HttpStatusCode.Redirect, "{}");
            response.Headers.Location = new("https://other.example.invalid/oc/connect");
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);

        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.StatusCode).IsEqualTo(HttpStatusCode.Redirect);
    }

    /// <summary>Verifies long-poll subscribe returns complete batches and ACK posts through its reserved route.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncReturnsBatchAndAcknowledgePostsCursor()
    {
        var subscribeResponse = SubscribeResponseJson();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson),
            SubscribeRoute => CreateProtocolResponse(HttpStatusCode.OK, subscribeResponse),
            AcknowledgeRoute => new HttpResponseMessage(HttpStatusCode.NoContent),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000010")),
            "cursor-0",
            StartPosition.Latest);

        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var hasBatch = await enumerator.MoveNextAsync();
        await session.AcknowledgeAsync(new(subscribe.SubscriptionId, subscribe.StreamId, enumerator.Current.NextCursor), CancellationToken.None);

        await Assert.That(hasBatch).IsTrue();
        await Assert.That(enumerator.Current.Events).Count().IsEqualTo(1);
        await Assert.That(enumerator.Current.CompletedOperations).Count().IsEqualTo(1);
        await Assert.That(handler.Requests[1].RequestUri?.Query).Contains("cursor=cursor-0");
        await Assert.That(handler.Requests[2].RequestUri).IsEqualTo(new(AcknowledgeEndpoint));
        await Assert.That(handler.Requests[2].Body).Contains("\"cursor\":\"cursor-1\"");
    }

    /// <summary>Verifies ACK capacity remains available while the long-poll slot is occupied.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AcknowledgeAsyncProgressesWhileSubscribePollIsBlocked()
    {
        var subscribeEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            if (request.RequestUri?.AbsolutePath == AcknowledgeRoute)
            {
                return new(HttpStatusCode.NoContent);
            }

            _ = subscribeEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new(HttpStatusCode.NoContent);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumConcurrentRequests = 1, MaximumConcurrentAcknowledgements = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000011")),
            null,
            StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        await AwaitWithTimeoutAsync(session.AcknowledgeAsync(new(subscribe.SubscriptionId, subscribe.StreamId, "cursor-1"), CancellationToken.None).AsTask());
        await session.DisposeAsync();

        await Assert.That(handler.Requests.Exists(static request => request.RequestUri?.AbsolutePath == AcknowledgeRoute)).IsTrue();
        await AssertCompletesAsync(moveNext);
    }

    /// <summary>Verifies saturated request admission fails fast before another request is sent.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncFailsFastWhenRequestSlotsAreSaturated()
    {
        var subscribeEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = subscribeEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new(HttpStatusCode.NoContent);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumConcurrentRequests = 1, MaximumConcurrentAcknowledgements = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000014")),
            null,
            StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        var exception = await CaptureHttpExceptionAsync(async () => await session.PushAsync(CreateBatch(), CancellationToken.None));
        await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
        await Assert.That(handler.Requests.Exists(static request => request.RequestUri?.AbsolutePath == PushRoute)).IsFalse();
        await AssertCompletesAsync(moveNext);
    }

    /// <summary>Verifies disposing the adapter cancels and drains an active connect request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCancelsActiveConnectRequest()
    {
        var connectEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            _ = connectEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
        });
        using var httpClient = CreateHttpClient(handler);
        var adapter = CreateAdapter(httpClient);
        var connect = adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(connectEntered.Task);

        await AwaitWithTimeoutAsync(adapter.DisposeAsync().AsTask());

        await Assert.That(connect.IsCanceled).IsTrue();
    }

    /// <summary>Verifies disposing a session cancels a paused subscription consumer.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCancelsPausedSubscribePoll()
    {
        var subscribeEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = subscribeEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new(HttpStatusCode.NoContent);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000012")),
            null,
            StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());

        await AssertCompletesAsync(moveNext);
    }

    /// <summary>Verifies disposing the adapter cancels and drains an active session subscription.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdapterDisposeAsyncCancelsActiveSessionSubscribePoll()
    {
        var subscribeEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = subscribeEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new(HttpStatusCode.NoContent);
        });
        using var httpClient = CreateHttpClient(handler);
        var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new SubscriptionId(Guid.Parse("00000000-0000-0000-0000-000000000013")),
            null,
            StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var moveNext = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        await AwaitWithTimeoutAsync(adapter.DisposeAsync().AsTask());

        await AssertCompletesAsync(moveNext);
        await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());
    }

    /// <summary>Creates the shared connect request.</summary>
    /// <returns>The connect request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TransportConnectRequest CreateConnectRequest() => new(
        new VersionRange(new Version(1, 0), new Version(1, 0)),
        new ClientIdentity("client-1", "tenant-1"),
        [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Creates an adapter under test.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateAdapter(HttpClient httpClient) =>
        CreateAdapter(httpClient, CreateBaseAddress(), static options => options);

    /// <summary>Creates an adapter under test.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="baseAddress">The trusted base address.</param>
    /// <param name="configure">The option customizer.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateAdapter(
        HttpClient httpClient,
        Uri baseAddress,
        Func<HttpRemoteTransportOptions, HttpRemoteTransportOptions> configure)
    {
        var options = new HttpRemoteTransportOptions { HttpClient = httpClient, BaseAddress = baseAddress };
        return new(configure(options));
    }

    /// <summary>Creates an HTTP client for a deterministic handler.</summary>
    /// <param name="handler">The handler.</param>
    /// <returns>The HTTP client.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpClient CreateHttpClient(HttpMessageHandler handler) => new(handler);

    /// <summary>Creates an HTTP client for loopback integration tests.</summary>
    /// <returns>The HTTP client.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpClient CreateHttpClient() => new();

    /// <summary>Creates the shared base address.</summary>
    /// <returns>The URI.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Uri CreateBaseAddress() => new(BaseAddressText);

    /// <summary>Creates one valid sync batch.</summary>
    /// <returns>The batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncBatch CreateBatch() =>
        new(Guid.Parse("00000000-0000-0000-0000-000000000100"), [CreateOperation(1)]);

    /// <summary>Creates one valid operation.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long sequence) => new()
    {
        OperationId = new(Guid.Parse($"00000000-0000-0000-0000-{sequence:000000000000}")),
        StreamId = CreateStreamId(),
        ClientSequence = sequence,
        TimestampUtc = new DateTimeOffset(2026, 9, 13, 0, 0, 0, TimeSpan.Zero).AddSeconds(sequence),
        Type = SyncOperationType.Append,
        Payload = new("contract", 1, "application/json", "{}"u8.ToArray(), "sha256-test"),
        Metadata = new Dictionary<string, string> { ["trace"] = sequence.ToString(CultureInfo.InvariantCulture) },
    };

    /// <summary>Creates the shared stream identifier.</summary>
    /// <returns>The stream id.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StreamId CreateStreamId() => new(StreamName);

    /// <summary>Creates a push response for a batch.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <returns>The response JSON.</returns>
    private static string PushResponseJson(SyncBatch batch) => $$"""
        {"batchId":"{{batch.BatchId:D}}","operations":[{"operationId":"{{batch.Operations[0].OperationId.Value:D}}","kind":0,"serverVersion":"v1"}],"serverCursor":"server-1"}
        """;

    /// <summary>Creates a subscribe response with one complete operation group.</summary>
    /// <returns>The response JSON.</returns>
    private static string SubscribeResponseJson()
    {
        var operationId = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var eventId = Guid.Parse("00000000-0000-0000-0000-000000000201");
        var operation = operationId.ToString("D", CultureInfo.InvariantCulture);
        var remoteEvent = eventId.ToString("D", CultureInfo.InvariantCulture);
        return "{\"batches\":[{\"batchId\":\"00000000-0000-0000-0000-000000000200\","
            + "\"streamId\":\"stream-1\",\"previousCursor\":\"cursor-0\",\"nextCursor\":\"cursor-1\",\"events\":[{"
            + "\"eventId\":\"" + remoteEvent + "\",\"streamId\":\"stream-1\",\"serverCursor\":\"cursor-1\","
            + "\"committedAtUtc\":\"2026-09-13T00:00:00+00:00\",\"causedByOperationId\":\"" + operation + "\","
            + "\"origin\":{\"clientId\":\"client-1\",\"operationId\":\"" + operation + "\"},"
            + "\"payload\":{\"contractId\":\"contract\",\"schemaVersion\":1,\"contentType\":\"application/json\","
            + "\"payload\":\"e30=\",\"payloadHash\":\"sha256-test\"},\"metadata\":{\"trace\":\"1\"}}],"
            + "\"completedOperations\":[{\"origin\":{\"clientId\":\"client-1\",\"operationId\":\"" + operation + "\"},"
            + "\"eventIds\":[\"" + remoteEvent + "\"]}]}]}";
    }

    /// <summary>Creates a protocol media type response.</summary>
    /// <param name="statusCode">The response status code.</param>
    /// <param name="json">The JSON response.</param>
    /// <returns>The response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateProtocolResponse(HttpStatusCode statusCode, string json) =>
        CreateJsonResponse(statusCode, json, ProtocolMediaType);

    /// <summary>Creates a JSON response.</summary>
    /// <param name="statusCode">The response status code.</param>
    /// <param name="json">The JSON response.</param>
    /// <param name="mediaType">The media type.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateJsonResponse(HttpStatusCode statusCode, string json, string mediaType)
    {
        var response = new HttpResponseMessage(statusCode) { Content = new ByteArrayContent(Encoding.UTF8.GetBytes(json)) };
        response.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(mediaType);
        return response;
    }

    /// <summary>Captures a typed HTTP exception from an asynchronous action.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static async Task<HttpRemoteTransportException> CaptureHttpExceptionAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (HttpRemoteTransportException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected an HTTP transport exception.");
    }

    /// <summary>Captures a sync batch validation exception from an asynchronous action.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static async Task<SyncBatchValidationException> CaptureSyncBatchExceptionAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (SyncBatchValidationException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected a sync batch validation exception.");
    }

    /// <summary>Awaits a task with a bounded timeout.</summary>
    /// <param name="task">The task.</param>
    /// <returns>The asynchronous wait operation.</returns>
    private static async Task AwaitWithTimeoutAsync(Task task)
    {
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(AwaitTimeoutSeconds), CancellationToken.None));
        await Assert.That(completed).IsEqualTo(task);
        await task.ConfigureAwait(false);
    }

    /// <summary>Verifies a task completes within the bounded test timeout without observing its result.</summary>
    /// <param name="task">The task.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertCompletesAsync(Task task)
    {
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(AwaitTimeoutSeconds), CancellationToken.None));
        await Assert.That(completed).IsEqualTo(task);
    }

    /// <summary>Creates a continuation-safe completion source.</summary>
    /// <returns>The completion source.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TaskCompletionSource<object?> CreateCompletionSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Records requests and returns deterministic responses.</summary>
    private sealed class RecordingHttpHandler : HttpMessageHandler
    {
        /// <summary>The synchronous responder.</summary>
        private readonly Func<HttpRequestMessage, HttpResponseMessage>? _responder;

        /// <summary>The asynchronous responder.</summary>
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? _asyncResponder;

        /// <summary>Initializes a new instance of the <see cref="RecordingHttpHandler"/> class.</summary>
        /// <param name="responder">The response factory.</param>
        internal RecordingHttpHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) => _responder = responder;

        /// <summary>Initializes a new instance of the <see cref="RecordingHttpHandler"/> class.</summary>
        /// <param name="asyncResponder">The async response factory.</param>
        internal RecordingHttpHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> asyncResponder) =>
            _asyncResponder = asyncResponder;

        /// <summary>Gets captured requests.</summary>
        internal List<RequestRecord> Requests { get; } = [];

        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(await CaptureAsync(request).ConfigureAwait(false));
            if (_asyncResponder is not null)
            {
                return await _asyncResponder(request, cancellationToken).ConfigureAwait(false);
            }

            if (_responder is not null)
            {
                return _responder(request);
            }

            throw new InvalidOperationException("No response factory was configured.");
        }

        /// <summary>Captures the outgoing request without retaining disposable request objects.</summary>
        /// <param name="request">The request.</param>
        /// <returns>The request record.</returns>
        private static async Task<RequestRecord> CaptureAsync(HttpRequestMessage request)
        {
            var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync().ConfigureAwait(false);
            return new(
                request.Method,
                request.RequestUri,
                request.Headers.Accept.Count == 0 ? null : request.Headers.Accept.First().ToString(),
                request.Content?.Headers.ContentType?.ToString(),
                request.Headers.Authorization?.Scheme,
                request.Headers.ToArray(),
                body);
        }
    }

    /// <summary>A one-request loopback HTTP server used for opt-in plain HTTP testing.</summary>
    private sealed class LoopbackHttpServer
    {
        /// <summary>The listener.</summary>
        private readonly TcpListener _listener;

        /// <summary>The server task.</summary>
        private Task _serverTask = Task.CompletedTask;

        /// <summary>Initializes a new instance of the <see cref="LoopbackHttpServer"/> class.</summary>
        /// <param name="listener">The listener.</param>
        /// <param name="baseAddress">The base address.</param>
        private LoopbackHttpServer(TcpListener listener, Uri baseAddress)
        {
            _listener = listener;
            BaseAddress = baseAddress;
        }

        /// <summary>Gets the trusted base address.</summary>
        internal Uri BaseAddress { get; }

        /// <summary>Gets the captured request line.</summary>
        internal string RequestLine { get; private set; } = string.Empty;

        /// <summary>Starts the server.</summary>
        /// <param name="json">The response JSON.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The running server.</returns>
        internal static Task<LoopbackHttpServer> StartAsync(string json, CancellationToken cancellationToken)
        {
            var listener = new TcpListener(IPAddress.Loopback, port: 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var server = new LoopbackHttpServer(listener, new Uri($"http://127.0.0.1:{port}/oc/"));
            server._serverTask = Task.Run(async () => await server.ServeOneAsync(json, cancellationToken).ConfigureAwait(false), cancellationToken);
            return Task.FromResult(server);
        }

        /// <summary>Stops the server.</summary>
        /// <returns>The asynchronous stop operation.</returns>
        internal async Task StopAsync()
        {
            _listener.Stop();
            try
            {
                await _serverTask.ConfigureAwait(false);
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        /// <summary>Reads the request headers.</summary>
        /// <param name="stream">The stream.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The decoded header text.</returns>
        private static async Task<string> ReadHeadersAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            var buffer = new byte[1024];
            await using var memory = new MemoryStream();
            while (true)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    break;
                }

                memory.Write(buffer, 0, read);
                var text = Encoding.ASCII.GetString(memory.ToArray());
                if (text.Contains("\r\n\r\n"))
                {
                    return text;
                }
            }

            return Encoding.ASCII.GetString(memory.ToArray());
        }

        /// <summary>Serves one request.</summary>
        /// <param name="json">The response JSON.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The asynchronous operation.</returns>
        private async Task ServeOneAsync(string json, CancellationToken cancellationToken)
        {
            using var client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            await using var stream = client.GetStream();
            var request = await ReadHeadersAsync(stream, cancellationToken).ConfigureAwait(false);
            var lineEnd = request.IndexOf("\r\n", StringComparison.Ordinal);
            RequestLine = lineEnd < 0 ? request : request[..lineEnd];
            var body = Encoding.UTF8.GetBytes(json);
            var header = string.Create(
                CultureInfo.InvariantCulture,
                $"HTTP/1.1 200 OK\r\nContent-Type: {ProtocolMediaType}\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n");
            var headerBytes = Encoding.ASCII.GetBytes(header);
            await stream.WriteAsync(headerBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(body.AsMemory(), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Records one outgoing request.</summary>
    /// <param name="Method">The HTTP method.</param>
    /// <param name="RequestUri">The URI.</param>
    /// <param name="Accept">The accept media type.</param>
    /// <param name="ContentType">The request content media type.</param>
    /// <param name="Authorization">The authorization scheme.</param>
    /// <param name="Headers">The outgoing headers.</param>
    /// <param name="Body">The UTF-8 body.</param>
    private sealed record RequestRecord(
        HttpMethod Method,
        Uri? RequestUri,
        string? Accept,
        string? ContentType,
        string? Authorization,
        IReadOnlyList<KeyValuePair<string, IEnumerable<string>>> Headers,
        string Body);
}
