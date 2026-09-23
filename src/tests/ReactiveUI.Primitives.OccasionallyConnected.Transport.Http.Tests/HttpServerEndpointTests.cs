// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Net;
using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpServerEndpoint"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The expected protocol content type.</summary>
    private const string ProtocolMediaType = "application/vnd.reactiveui.occasionally-connected+json;v=1";

    /// <summary>The trusted tenant identifier.</summary>
    private const string TenantId = "tenant-1";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "client-1";

    /// <summary>The forged client identifier.</summary>
    private const string ForgedClientId = "client-2";

    /// <summary>The shared stream identifier.</summary>
    private const string StreamName = "stream-1";

    /// <summary>The first operation identifier text.</summary>
    private const string OperationIdText = "00000000-0000-0000-0000-000000000001";

    /// <summary>The batch identifier text.</summary>
    private const string BatchIdText = "00000000-0000-0000-0000-000000000100";

    /// <summary>The subscription identifier text.</summary>
    private const string SubscriptionIdText = "00000000-0000-0000-0000-000000000301";

    /// <summary>The successful server cursor.</summary>
    private const string ServerCursor = "server-1";

    /// <summary>The first receive cursor.</summary>
    private const string CursorOne = "cursor-1";

    /// <summary>The second receive cursor.</summary>
    private const string CursorTwo = "cursor-2";

    /// <summary>The shared content contract.</summary>
    private const string ContractName = "contract";

    /// <summary>The shared payload content type.</summary>
    private const string PayloadContentType = "application/json";

    /// <summary>The shared payload hash.</summary>
    private const string PayloadHash = "sha256-test";

    /// <summary>The mounted route base used by route tests.</summary>
    private const string MountedPathBase = "/oc";

    /// <summary>The connect endpoint URI.</summary>
    private const string ConnectUri = "https://example.invalid/connect";

    /// <summary>The push endpoint URI.</summary>
    private const string PushUri = "https://example.invalid/push";

    /// <summary>The acknowledgement endpoint URI.</summary>
    private const string AcknowledgeUri = "https://example.invalid/ack";

    /// <summary>The subscribe endpoint URI.</summary>
    private const string SubscribeUri = "https://example.invalid/subscribe";

    /// <summary>The push endpoint URI with a percent-encoded route segment.</summary>
    private const string EscapedPushUri = "https://example.invalid/%70ush";

    /// <summary>The push endpoint URI with an escaped route separator.</summary>
    private const string EscapedPushSeparatorUri = "https://example.invalid/push%2Fextra";

    /// <summary>The connect route expressed as a relative request target.</summary>
    private const string RelativeConnectUri = "/connect";

    /// <summary>The subscribe route expressed as a relative request target.</summary>
    private const string RelativeSubscribeUri = "/subscribe";

    /// <summary>The duplicate protocol version content type used by media-type validation tests.</summary>
    private const string DuplicateProtocolVersionMediaType =
        "application/vnd.reactiveui.occasionally-connected+json;v=1;v=1";

    /// <summary>The GET method name used by method validation tests.</summary>
    private const string GetMethodName = "GET";

    /// <summary>The POST method name used by method validation tests.</summary>
    private const string PostMethodName = "POST";

    /// <summary>The duplicate route path used by option validation tests.</summary>
    private const string DuplicatePushPath = "connect";

    /// <summary>The absolute route base used by option validation tests.</summary>
    private const string AbsolutePathBase = "https://example.invalid/oc";

    /// <summary>The root URI used by empty route path tests.</summary>
    private const string RootUri = "https://example.invalid/";

    /// <summary>The malformed single-percent route URI used by validation tests.</summary>
    private const string MalformedPercentRouteUri = "/%";

    /// <summary>The malformed hex escape route URI used by validation tests.</summary>
    private const string MalformedHexRouteUri = "/push%GG";

    /// <summary>The subscribe query for a latest-position request.</summary>
    private const string SubscribeQuery =
        "?streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=0";

    /// <summary>An undefined transport failure kind value.</summary>
    private const int UndefinedFailureKindValue = 65_535;

    /// <summary>The protocol major version used by fixtures.</summary>
    private const int ProtocolMajorVersion = 1;

    /// <summary>The protocol minor version used by fixtures.</summary>
    private const int ProtocolMinorVersion = 0;

    /// <summary>The declared maximum operation count used by fixtures.</summary>
    private const int DeclaredMaximumBatchOperations = 10;

    /// <summary>The declared maximum batch byte count used by fixtures.</summary>
    private const int DeclaredMaximumBatchBytes = 1024;

    /// <summary>The effective exactly-once window minutes used by fixtures.</summary>
    private const int EffectiveExactlyOnceWindowMinutes = 1;

    /// <summary>The retry TTL minutes used by fixtures.</summary>
    private const int RetryTtlMinutes = 2;

    /// <summary>The byte count in one kibibyte.</summary>
    private const int BytesPerKibibyte = 1024;

    /// <summary>The second byte in an intentionally oversized two-byte body.</summary>
    private const byte OversizedBodySecondByte = 2;

    /// <summary>The request and response limit kibibytes used by fixtures.</summary>
    private const int TestBodyLimitKibibytes = 32;

    /// <summary>The payload limit kibibytes used by fixtures.</summary>
    private const int TestPayloadLimitKibibytes = 8;

    /// <summary>The metadata entry and batch count limit used by fixtures.</summary>
    private const int SmallCollectionLimit = 8;

    /// <summary>The metadata key byte limit used by fixtures.</summary>
    private const int MetadataKeyByteLimit = 64;

    /// <summary>The metadata value byte limit used by fixtures.</summary>
    private const int MetadataValueByteLimit = 256;

    /// <summary>The JSON depth limit used by fixtures.</summary>
    private const int JsonDepthLimit = 32;

    /// <summary>The bounded asynchronous wait timeout used by coordination tests.</summary>
    private const int AsyncWaitTimeoutSeconds = 5;

    /// <summary>The stable startup cleanup failure reason key used by endpoint diagnostics.</summary>
    private const string StartupCleanupFailureReasonKey =
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.PollDeadline.StartupCleanupFailureReason";

    /// <summary>The stable startup cleanup failure type key used by endpoint diagnostics.</summary>
    private const string StartupCleanupFailureTypeKey =
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.PollDeadline.StartupCleanupFailureType";

    /// <summary>The stable startup cleanup failure reason code used by endpoint diagnostics.</summary>
    private const string StartupCleanupFailureReasonCode = "StartupCleanupFailed";

    /// <summary>The secret-bearing cleanup failure message used to verify diagnostics remain sanitized.</summary>
    private const string SecretCleanupMessage = "secret-client-token=do-not-leak";

    /// <summary>Verifies construction validates endpoint-only capability claims.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsUnsupportedStreamingCapability()
    {
        var options = CreateOptions(new RecordingHub()) with
        {
            DeclaredCapabilities = CreateCapabilities(RemoteTransportCapabilities.StreamingReceive),
        };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies exactly-once retention cannot be advertised without atomic apply-and-acknowledge semantics.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsExactlyOnceWindowWithoutAtomicApplyAndAcknowledge()
    {
        const RemoteTransportCapabilities features = RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ReceiveAcknowledgements;
        var capabilities = CreateCapabilities(features) with { EffectiveExactlyOnceWindow = TimeSpan.FromMinutes(EffectiveExactlyOnceWindowMinutes) };
        var options = CreateOptions(new RecordingHub()) with { DeclaredCapabilities = capabilities };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies duplicate route configuration is rejected after endpoint route normalization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsDuplicateRoutes()
    {
        var options = CreateOptions(new RecordingHub()) with { PushPath = DuplicatePushPath };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies route options cannot collapse to empty paths or traverse configured endpoints.</summary>
    /// <param name="path">The unsafe route path.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("?")]
    [Arguments("?streamId=stream-1")]
    [Arguments(".")]
    [Arguments("./push")]
    [Arguments("push/.")]
    [Arguments("..")]
    [Arguments("../push")]
    [Arguments("push/../ack")]
    public async Task ConstructorRejectsUnsafeRouteOptions(string path)
    {
        var options = CreateOptions(new RecordingHub()) with { PushPath = path };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies route bases must remain local to the hosting pipeline.</summary>
    /// <param name="pathBase">The absolute URI that cannot identify a local mount.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(AbsolutePathBase)]
    [Arguments("file:///oc")]
    [Arguments("mailto:user@example.invalid")]
    public async Task ConstructorRejectsAbsolutePathBase(string pathBase)
    {
        var options = CreateOptions(new RecordingHub()) with { PathBase = pathBase };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies long-poll deadlines must be bounded by a finite timeout.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsInfiniteLongPollTimeout()
    {
        var options = CreateOptions(new RecordingHub()) with { LongPollTimeout = TimeSpan.MaxValue };
        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies construction accepts normalized mounted routes and exposes the declared capabilities.</summary>
    /// <param name="pathBase">The local route mount, independent of file-system URI rules.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(MountedPathBase)]
    [Arguments("oc")]
    [Arguments("/oc/")]
    public async Task ConstructorExposesDeclaredCapabilitiesForMountedRoutes(string pathBase)
    {
        var capabilities = CreateCapabilities(RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.CursorResume);
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with
        {
            PathBase = pathBase,
            DeclaredCapabilities = capabilities,
        });
        await Assert.That(endpoint.DeclaredCapabilities).IsSameReferenceAs(capabilities);
    }

    /// <summary>Verifies connect returns a bounded copy of the declared endpoint capabilities.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectReturnsDeclaredCapabilities()
    {
        var codec = CreateCodec();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        var requestBody = codec.SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, requestBody);
        AddConnectReplayHeaders(request, requestBody);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        var responseBody = await ReadResponseBodyAsync(response);
        var capabilities = codec.DeserializeConnectResponse(responseBody);
        await Assert.That(capabilities.Features).IsEqualTo(CreateCapabilities().Features);
        await Assert.That(capabilities.MaximumBatchOperations).IsEqualTo(CreateCapabilities().MaximumBatchOperations);
    }

    /// <summary>Verifies connect binds the wire client claim to the host-authenticated identity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectRejectsForgedWireClient()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ForgedClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
    }

    /// <summary>Verifies push decodes a real protocol batch, supplies the trusted principal, and returns exact results.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushInvokesHubWithTrustedPrincipalAndReturnsResult()
    {
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = static (incoming, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.FromResult(CreateServerResult(incoming, client));
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, batch);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
        var result = CreateCodec().DeserializePushResponse(batch, await ReadResponseBodyAsync(response), retryAfter: null);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(result.Operations[0].OperationId).IsEqualTo(batch.Operations[0].OperationId);
    }

    /// <summary>Verifies the no-cancellation overload dispatches a real push request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncWithoutCancellationDispatchesRequest()
    {
        var batch = CreateBatch();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, batch);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient());
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies an untrusted host principal is rejected before route work can reach the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsUntrustedPrincipalBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));
        using var response = await endpoint.HandleAsync(request, new(string.Empty, ClientId), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a missing request URI is rejected before route dispatch.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsMissingRequestUri()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = new HttpRequestMessage { Method = HttpMethod.Post };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
    }

    /// <summary>Verifies malformed escaped route segments are rejected before matching endpoint routes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsMalformedEscapedRoute()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(MalformedPercentRouteUri, UriKind.Relative));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
    }

    /// <summary>Verifies a root request path is treated as an unmatched route.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRootPathReturnsNotFound()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = new HttpRequestMessage(HttpMethod.Get, RootUri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Verifies malformed hex escapes are rejected while normalizing endpoint routes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsMalformedHexEscapedRoute()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(MalformedHexRouteUri, UriKind.Relative));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
    }

    /// <summary>Verifies escaped route separators cannot smuggle additional path segments into endpoint routes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsEscapedRouteSeparatorBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Post, EscapedPushSeparatorUri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies valid percent-encoded route segments normalize to their configured endpoint route.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcceptsPercentEncodedRouteSegment()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch(), EscapedPushUri, "push");
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies a push body is required before any hub effects are possible.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsMissingBodyBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies missing protocol media types are rejected before push effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsMissingContentTypeBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = new ByteArrayContent(CreateCodec().SerializePushRequest(CreateBatch()));
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies duplicated protocol version parameters are rejected before push effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsDuplicateProtocolVersionBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = new ByteArrayContent(CreateCodec().SerializePushRequest(CreateBatch()));
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(DuplicateProtocolVersionMediaType);
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies compressed protocol bodies are rejected before decoding or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsCompressedBodyBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateProtocolContent(CreateCodec().SerializePushRequest(CreateBatch()));
        content.Headers.ContentEncoding.Add("gzip");
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies declared oversized bodies are rejected from headers before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsDeclaredOversizedBodyBeforeHub()
    {
        var hub = new RecordingHub();
        var connectBody = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { MaximumRequestBytes = connectBody.Length });
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies chunked bodies are still bounded when no content length is available.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsUndeclaredOversizedBodyBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { MaximumRequestBytes = 1 });
        using var content = CreateProtocolNonSeekableStreamContent([1, OversizedBodySecondByte]);
        await Assert.That(content.Headers.ContentLength).IsNull();
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies chunked bodies at the exact endpoint limit are decoded rather than rejected by the probe read.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectAcceptsUndeclaredBodyAtLimit()
    {
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with { MaximumRequestBytes = body.Length });
        using var content = CreateProtocolNonSeekableStreamContent(body);
        await Assert.That(content.Headers.ContentLength).IsNull();
        using var request = new HttpRequestMessage(HttpMethod.Post, ConnectUri) { Content = content };
        AddConnectReplayHeaders(request, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies caller cancellation during connect body reads remains caller-observable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectPropagatesCallerCancellationDuringBodyReadBeforeHub()
    {
        using var cancellation = new CancellationTokenSource();
        var readStarted = CreateSignal();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = CreateBodyReadRequest(ConnectBodyReadTarget, CreateBlockingProtocolContent(readStarted));
        var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), cancellation.Token).AsTask();
        await readStarted.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
        await cancellation.CancelAsync().ConfigureAwait(false);
        await Assert.That(async () => _ = await AwaitResultAsync(responseTask).ConfigureAwait(false)).Throws<OperationCanceledException>();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies unknown pre-effect typed transport failures fail closed without reaching the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectMapsUnknownPreEffectTransportFailureToBadRequest()
    {
        const HttpTransportFailureKind failureKind = (HttpTransportFailureKind)UndefinedFailureKindValue;
        var failure = new HttpRemoteTransportException(failureKind);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateThrowingTransportFailureProtocolContent(failure);
        using var request = new HttpRequestMessage(HttpMethod.Post, ConnectUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies pre-effect typed transport failures keep their safe client-visible status.</summary>
    /// <param name="failureKind">The typed transport failure kind raised before endpoint effects.</param>
    /// <param name="expectedStatusCode">The expected HTTP response status.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(HttpTransportFailureKind.Authentication, HttpStatusCode.Unauthorized)]
    [Arguments(HttpTransportFailureKind.AuthorizationDenied, HttpStatusCode.Forbidden)]
    [Arguments(HttpTransportFailureKind.Transient, HttpStatusCode.TooManyRequests)]
    [Arguments(HttpTransportFailureKind.AmbiguousTransportOutcome, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest)]
    public async Task HandleAsyncConnectMapsTypedPreEffectTransportFailureToSafeStatus(
        HttpTransportFailureKind failureKind,
        HttpStatusCode expectedStatusCode)
    {
        var failure = new HttpRemoteTransportException(failureKind);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateThrowingTransportFailureProtocolContent(failure);
        using var request = new HttpRequestMessage(HttpMethod.Post, ConnectUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(expectedStatusCode);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies unknown hub failures after push effects are reported as retryable ambiguity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushMapsPostEffectHubFailureToAmbiguousResponse()
    {
        var hub = new RecordingHub { ApplyHandler = static (_, _, _) => throw new InvalidOperationException("Apply failed after possible effects.") };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies hub disposal after decoded push effects is treated as an ambiguous retryable outcome.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushMapsPostEffectObjectDisposedToAmbiguousResponse()
    {
        var hub = new RecordingHub { ApplyHandler = static (_, _, _) => throw new ObjectDisposedException("committed-push") };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies hub cancellation after push effects is reported as retryable ambiguity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushMapsPostEffectCancellationToAmbiguousResponse()
    {
        var hub = new RecordingHub { ApplyHandler = static (_, _, cancellationToken) => throw new OperationCanceledException(cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies caller cancellation after push reaches the hub propagates instead of returning an ambiguous response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushCallerCancellationAfterHubEntryPropagatesCancellation()
    {
        using var callerCancellation = new CancellationTokenSource();
        var hub = new RecordingHub
        {
            ApplyHandler = async (batch, client, cancellationToken) =>
            {
                await callerCancellation.CancelAsync().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                return CreateServerResult(batch, client);
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        await Assert.That(async () => _ = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), callerCancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies typed post-effect push transport failures keep only safe permanent classifications.</summary>
    /// <param name="failureKind">The typed transport failure kind reported by the hub.</param>
    /// <param name="expectedStatusCode">The HTTP status expected from endpoint response mapping.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(HttpTransportFailureKind.Authentication, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.AuthorizationDenied, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.Transient, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.AmbiguousTransportOutcome, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.SchemaIncompatible, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.InternalServerError)]
    [Arguments(HttpTransportFailureKind.Configuration, HttpStatusCode.InternalServerError)]
    public async Task HandleAsyncPushMapsTypedPostEffectTransportFailureToSafeStatus(
        HttpTransportFailureKind failureKind,
        HttpStatusCode expectedStatusCode)
    {
        var hub = new RecordingHub { ApplyHandler = (_, _, _) => throw new HttpRemoteTransportException(failureKind) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(expectedStatusCode);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies acknowledgement responds only after the hub has durably accepted the ACK.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeInvokesHubBeforeNoContent()
    {
        var acknowledgement = new ReceiveAcknowledgement(new(Guid.Parse(SubscriptionIdText)), new(StreamName), CursorOne);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, acknowledgement);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(hub.Acknowledgement).IsEqualTo(acknowledgement);
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies unknown hub failures after ACK effects are reported as retryable ambiguity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeMapsPostEffectHubFailureToAmbiguousResponse()
    {
        var hub = new RecordingHub { AcknowledgeHandler = static (_, _, _) => throw new InvalidOperationException("ACK failed after possible effects.") };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, CreateAcknowledgement());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies hub disposal after decoded ACK effects is treated as an ambiguous retryable outcome.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeMapsPostEffectObjectDisposedToAmbiguousResponse()
    {
        var hub = new RecordingHub { AcknowledgeHandler = static (_, _, _) => throw new ObjectDisposedException("committed-ack") };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, CreateAcknowledgement());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies hub cancellation after ACK effects is reported as retryable ambiguity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeMapsPostEffectCancellationToAmbiguousResponse()
    {
        var hub = new RecordingHub { AcknowledgeHandler = static (_, _, cancellationToken) => throw new OperationCanceledException(cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, CreateAcknowledgement());
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies caller cancellation after ACK reaches the hub propagates instead of returning an ambiguous response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeCallerCancellationAfterHubEntryPropagatesCancellation()
    {
        using var callerCancellation = new CancellationTokenSource();
        var hub = new RecordingHub
        {
            AcknowledgeHandler = async (acknowledgement, client, cancellationToken) =>
            {
                await callerCancellation.CancelAsync().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, CreateAcknowledgement());
        await Assert.That(async () => _ = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), callerCancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies endpoint disposal during an active subscribe maps to a service-unavailable response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeEndpointDisposeDuringHubReadReturnsServiceUnavailable()
    {
        var hubEntered = CreateSignal();
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockUntilCancelledAsync(hubEntered, cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
        await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
        await endpoint.DisposeAsync().ConfigureAwait(false);
        using var response = await responseTask.ConfigureAwait(false);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies shutdown cancellation from subscribe work maps to service unavailable without caller cancellation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeMapsShutdownOperationCancellationToServiceUnavailable()
    {
        using var callerCancellation = new CancellationTokenSource();
        var hubEntered = CreateSignal();
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => ThrowWhenCancelledAsync(hubEntered, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        Task<HttpResponseMessage>? responseTask = null;
        Task? disposeTask = null;
        HttpResponseMessage? response = null;
        try
        {
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), callerCancellation.Token).AsTask();
            await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            disposeTask = endpoint.DisposeAsync().AsTask();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(callerCancellation.IsCancellationRequested).IsFalse();
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
        }
        finally
        {
            disposeTask ??= endpoint.DisposeAsync().AsTask();
            try
            {
                if (responseTask is not null && response is null)
                {
                    response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                }
            }
            finally
            {
                try
                {
                    response?.Dispose();
                }
                finally
                {
                    await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Verifies one complete subscription batch is encoded into a successful long-poll response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeReturnsOneCompleteBatch()
    {
        var batch = CreateReceiveBatch();
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => YieldBatches([batch], cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
        var batches = CreateCodec().DeserializeSubscribeResponse(await ReadResponseBodyAsync(response), expectedStreamId: null);
        await Assert.That(batches.Length).IsEqualTo(1);
        await Assert.That(batches[0].NextCursor).IsEqualTo(CursorTwo);
    }

    /// <summary>Verifies relative subscribe request targets use the supplied query without losing authentication context.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeAcceptsRelativeRequestTargetQuery()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedRelativeSubscribeRequestAsync(endpoint);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies a GET poll carrying a body is rejected before subscription effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsUnexpectedBodyBeforeHub()
    {
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => YieldBatches([CreateReceiveBatch()], cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}") { Content = CreateProtocolContent("{}"u8.ToArray()) };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies oversized subscription queries are rejected before hub subscription work.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsOversizedQueryBeforeHub()
    {
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => YieldBatches([CreateReceiveBatch()], cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { MaximumQueryBytes = 1 });
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies malformed subscription queries are rejected before hub subscription work.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsMalformedQueryBeforeHub()
    {
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => YieldBatches([CreateReceiveBatch()], cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}?streamId=stream-1&subscriptionId=x&positionKind=0");
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies subscription hub failures after polling starts are reported as retryable ambiguity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeMapsPostEffectHubFailureToAmbiguousResponse()
    {
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => ThrowSubscriptionAsync(cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies caller cancellation after polling starts is reported as an ambiguous subscription outcome.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeMapsPostEffectCallerCancellationToAmbiguousResponse()
    {
        using var cancellation = new CancellationTokenSource();
        var hubEntered = CreateSignal();
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockUntilCancelledAsync(hubEntered, cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), cancellation.Token).AsTask();
        await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
        await cancellation.CancelAsync().ConfigureAwait(false);
        using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
    }

    /// <summary>Verifies method mismatches are rejected using the route-specific status.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncWrongMethodReturnsMethodNotAllowed()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        using var request = new HttpRequestMessage(HttpMethod.Get, PushUri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.MethodNotAllowed);
    }

    /// <summary>Verifies every known endpoint route rejects methods outside its contract before hub work.</summary>
    /// <param name="uri">The endpoint URI to call.</param>
    /// <param name="method">The unsupported HTTP method.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ConnectUri, GetMethodName)]
    [Arguments(SubscribeUri, PostMethodName)]
    [Arguments(AcknowledgeUri, GetMethodName)]
    [Arguments(RelativeConnectUri, GetMethodName)]
    public async Task HandleAsyncKnownRoutesRejectUnsupportedMethods(string uri, string method)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(new HttpMethod(method), uri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.MethodNotAllowed);
        await Assert.That(hub.ApplyClient).IsNull();
        await Assert.That(hub.AcknowledgeClient).IsNull();
        await Assert.That(hub.SubscribeClient).IsNull();
    }
}
