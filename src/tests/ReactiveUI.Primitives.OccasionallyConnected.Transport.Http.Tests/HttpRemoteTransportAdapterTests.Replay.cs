// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests HTTP replay authentication on the client adapter.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The replay message identifier header.</summary>
    private const string ReplayMessageIdHeader = "X-ReactiveUI-Replay-Message-Id";

    /// <summary>The replay nonce header.</summary>
    private const string ReplayNonceHeader = "X-ReactiveUI-Replay-Nonce";

    /// <summary>The replay sent timestamp header.</summary>
    private const string ReplaySentAtHeader = "X-ReactiveUI-Replay-Sent-At";

    /// <summary>The trusted replay tenant identifier header.</summary>
    private const string ReplayTenantIdHeader = "X-ReactiveUI-Replay-Tenant-Id";

    /// <summary>The replay session identifier header.</summary>
    private const string ReplaySessionIdHeader = "X-ReactiveUI-Replay-Session-Id";

    /// <summary>The replay session secret header.</summary>
    private const string ReplaySessionSecretHeader = "X-ReactiveUI-Replay-Session-Secret";

    /// <summary>The replay session expiry header.</summary>
    private const string ReplaySessionExpiresHeader = "X-ReactiveUI-Replay-Session-Expires";

    /// <summary>The replay MAC header.</summary>
    private const string ReplayMacHeader = "X-ReactiveUI-Replay-Mac";

    /// <summary>The trusted replay tenant returned by connect fixtures.</summary>
    private const string ReplayTenantId = "dGVuYW50LTE";

    /// <summary>The replay session identifier returned by connect fixtures.</summary>
    private const string ReplaySessionId = "replay-session-1";

    /// <summary>The replay session secret returned by connect fixtures.</summary>
    private const string ReplaySessionSecret = "replay-secret-1";

    /// <summary>The replay session expiry returned by connect fixtures.</summary>
    private const string ReplaySessionExpires = "2026-09-17T23:00:00.0000000+00:00";

    /// <summary>The replay endpoint tenant identifier.</summary>
    private const string ReplayEndpointTenantId = "tenant-1";

    /// <summary>The replay endpoint client identifier.</summary>
    private const string ReplayEndpointClientId = "client-1";

    /// <summary>The replay endpoint trusted base address used by integration tests.</summary>
    private const string ReplayEndpointBaseAddressText = "https://example.invalid/api/";

    /// <summary>The configured connect route used by replay endpoint integration tests.</summary>
    private const string ReplayEndpointConnectPath = "sync/connect";

    /// <summary>The configured push route used by replay endpoint integration tests.</summary>
    private const string ReplayEndpointPushPath = "sync/push";

    /// <summary>The configured subscribe route used by replay endpoint integration tests.</summary>
    private const string ReplayEndpointSubscribePath = "sync/subscribe";

    /// <summary>The configured acknowledgement route used by replay endpoint integration tests.</summary>
    private const string ReplayEndpointAcknowledgePath = "sync/ack";

    /// <summary>The first replay cursor fixture.</summary>
    private const string ReplayCursorOne = "cursor-1";

    /// <summary>An invalid replay session secret that cannot be decoded as a strict base64url token.</summary>
    private const string InvalidReplaySessionSecret = "not-base64url!*";

    /// <summary>An overlong replay session token length.</summary>
    private const int OverlongReplaySessionTokenLength = 129;

    /// <summary>The request count after connect and one push.</summary>
    private const int RequestsAfterConnectAndPush = 2;

    /// <summary>The maximum batch bytes advertised by the replay endpoint fixture.</summary>
    private const int ReplayEndpointMaximumBatchBytes = 1024;

    /// <summary>The client inbox retention minutes advertised by the replay endpoint fixture.</summary>
    private const int ReplayEndpointClientInboxRetentionMinutes = 2;

    /// <summary>The deterministic replay clock instant.</summary>
    private static readonly DateTimeOffset ReplayObservedUtc = DateTimeOffset.Parse("2026-09-17T22:00:00+00:00", CultureInfo.InvariantCulture);

    /// <summary>Verifies connect sends freshness headers before the server can issue a replay session.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncSendsReplayFreshnessHeaders()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var headers = handler.Requests[0].Headers;
        await Assert.That(GetSingleHeader(headers, ReplayMessageIdHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplayNonceHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplaySentAtHeader)).IsEqualTo(ReplayObservedUtc.ToString("O", CultureInfo.InvariantCulture));
        await Assert.That(GetSingleHeader(headers, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetSingleHeader(headers, ReplayMacHeader)).IsNull();
    }

    /// <summary>Verifies connect captures the issued replay session and signs a later push request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncUsesConnectReplaySessionToSignRequest()
    {
        var batch = CreateBatch();
        var handler = new RecordingHttpHandler(request => request.RequestUri?.AbsolutePath switch
        {
            ConnectRoute => CreateReplayConnectResponse(),
            PushRoute => CreateProtocolResponse(HttpStatusCode.OK, PushResponseJson(batch)),
            _ => CreateProtocolResponse(HttpStatusCode.NotFound, "{}"),
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        _ = await session.PushAsync(batch, CancellationToken.None);
        var headers = handler.Requests[1].Headers;
        await Assert.That(GetSingleHeader(headers, ReplayMessageIdHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplayNonceHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplaySentAtHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplaySessionIdHeader)).IsEqualTo(ReplaySessionId);
        await Assert.That(GetSingleHeader(headers, ReplayMacHeader)).IsNotNull();
        await Assert.That(GetSingleHeader(headers, ReplaySessionSecretHeader)).IsNull();
    }

    /// <summary>Verifies invalid replay session secret text is rejected before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsInvalidReplaySessionSecretEncoding()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(InvalidReplaySessionSecret));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies replay session tokens accept the full base64url token alphabet before retention.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncAcceptsBase64UrlReplaySessionTokenCharacters()
    {
        const string TokenWithFullAlphabet = "Aa0-_";
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(TokenWithFullAlphabet, ReplayTenantId, ReplaySessionExpires, TokenWithFullAlphabet));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await Assert.That(session).IsNotNull();
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies duplicated replay session headers are rejected before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsDuplicateReplaySessionHeader()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateReplayConnectResponse();
            response.Headers.Add(ReplaySessionIdHeader, "replay-session-2");
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies empty replay session header collections are treated as missing before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsEmptyReplaySessionHeaderCollection()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateReplayConnectResponse();
            _ = response.Headers.Remove(ReplaySessionIdHeader);
            _ = response.Headers.TryAddWithoutValidation(ReplaySessionIdHeader, []);
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies malformed replay session expiry headers are rejected before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsMalformedReplaySessionExpiry()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(ReplaySessionSecret, ReplayTenantId, "not-an-instant"));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies empty replay tenant headers are rejected before tenant decoding allocates.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsEmptyReplayTenantHeader()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(ReplaySessionSecret, string.Empty, ReplaySessionExpires));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies malformed replay tenant base64url is rejected before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsMalformedReplayTenantEncoding()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(ReplaySessionSecret, "____", ReplaySessionExpires));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies decoded blank replay tenant identifiers are rejected before a session is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsBlankDecodedReplayTenant()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse(ReplaySessionSecret, "IA", ReplaySessionExpires));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies connect fails closed when a replay-enabled server omits session headers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsMissingReplaySessionHeaders()
    {
        var handler = new RecordingHttpHandler(static request => CreateJsonResponse(HttpStatusCode.OK, ConnectResponseJson, ProtocolMediaType));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies partially returned replay session headers do not create an unsigned session fallback.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsPartialReplaySessionHeaders()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateJsonResponse(HttpStatusCode.OK, ConnectResponseJson, ProtocolMediaType);
            response.Headers.Add(ReplayTenantIdHeader, ReplayTenantId);
            response.Headers.Add(ReplaySessionIdHeader, ReplaySessionId);
            response.Headers.Add(ReplaySessionExpiresHeader, ReplaySessionExpires);
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies replay tenant headers with non-token characters are rejected before tenant decoding.</summary>
    /// <param name="tenantToken">The malformed encoded tenant identifier.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("tenant!*")]
    [Arguments("tenant+")]
    [Arguments("tenant/")]
    [Arguments("tenant=")]
    [Arguments("tenant[")]
    [Arguments("tenant:")]
    [Arguments("tenant`")]
    [Arguments("tenant{")]
    public async Task ConnectAsyncRejectsInvalidReplayTenantTokenCharacter(string tenantToken)
    {
        var handler = new RecordingHttpHandler(request => CreateReplayConnectResponse(ReplaySessionSecret, tenantToken, ReplaySessionExpires));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies overlong replay session tokens are rejected before retaining credential material.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncRejectsOverlongReplaySessionToken()
    {
        var handler = new RecordingHttpHandler(static request =>
        {
            var response = CreateReplayConnectResponse();
            _ = response.Headers.Remove(ReplaySessionIdHeader);
            response.Headers.Add(ReplaySessionIdHeader, new string('a', OverlongReplaySessionTokenLength));
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateReplayAdapter(httpClient);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies replay signing uses the resolved base path when talking to a real endpoint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncSignsResolvedBasePathForReplayEndpoint()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new ReplayEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            new(ReplayEndpointBaseAddressText),
            options => options with
            {
                ConnectPath = ReplayEndpointConnectPath,
                PushPath = ReplayEndpointPushPath,
                SubscribePath = ReplayEndpointSubscribePath,
                AcknowledgePath = ReplayEndpointAcknowledgePath,
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = timeProvider },
            });
        var batch = CreateBatch();
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var result = await session.PushAsync(batch, CancellationToken.None);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(hub.ApplyClient).IsEqualTo(new(ReplayEndpointTenantId, ReplayEndpointClientId));
        await Assert.That(handler.Requests[1].RequestUri).IsEqualTo(new("https://example.invalid/api/sync/push"));
    }

    /// <summary>Verifies replay signing preserves arbitrary tenant identifiers returned by the trusted endpoint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncSignsReplayRequestWithUnicodeTenantReturnedByEndpoint()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var authenticatedClient = new ServerAuthenticatedClient("tenant-\u2603-\uD83D\uDE80", ReplayEndpointClientId);
        var hub = new ReplayEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint, authenticatedClient);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateResolvedReplayAdapter(httpClient, timeProvider);
        var batch = CreateBatch();
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var result = await session.PushAsync(batch, CancellationToken.None);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(hub.ApplyClient).IsEqualTo(authenticatedClient);
        await Assert.That(handler.Requests[1].RequestUri).IsEqualTo(new("https://example.invalid/api/sync/push"));
    }

    /// <summary>Verifies replay signing uses the resolved ACK base path when talking to a real endpoint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AcknowledgeAsyncSignsResolvedBasePathForReplayEndpoint()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new ReplayEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateResolvedReplayAdapter(httpClient, timeProvider);
        var acknowledgement = new ReceiveAcknowledgement(new(Guid.Parse("00000000-0000-0000-0000-000000000201")), CreateStreamId(), ReplayCursorOne);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await session.AcknowledgeAsync(acknowledgement, CancellationToken.None);
        await Assert.That(hub.AcknowledgeClient).IsEqualTo(new(ReplayEndpointTenantId, ReplayEndpointClientId));
        await Assert.That(hub.Acknowledgement).IsEqualTo(acknowledgement);
        await Assert.That(handler.Requests[1].RequestUri).IsEqualTo(new("https://example.invalid/api/sync/ack"));
    }

    /// <summary>Verifies replay signing uses the resolved subscribe base path and query when talking to a real endpoint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncSignsResolvedBasePathAndQueryForReplayEndpoint()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new ReplayEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateResolvedReplayAdapter(httpClient, timeProvider);
        var subscribe = new RemoteSubscribeRequest(
            CreateStreamId(),
            new(Guid.Parse("00000000-0000-0000-0000-000000000202")),
            "cursor-0",
            StartPosition.Latest);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var enumerator = session.SubscribeAsync(subscribe, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var hasBatch = await enumerator.MoveNextAsync();
        await Assert.That(hasBatch).IsTrue();
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(ReplayCursorOne);
        await Assert.That(hub.SubscribeClient).IsEqualTo(new(ReplayEndpointTenantId, ReplayEndpointClientId));
        await Assert.That(hub.SubscribeRequest).IsEqualTo(subscribe);
        await Assert.That(handler.Requests[1].RequestUri?.AbsoluteUri).StartsWith("https://example.invalid/api/sync/subscribe?");
        await Assert.That(handler.Requests[1].RequestUri?.Query).Contains("cursor=cursor-0");
    }

    /// <summary>Verifies replay signing rejects a resolved route segment that decodes to a slash before sending the push.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsResolvedPathSegmentContainingEncodedSlashBeforeSend()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with
            {
                PushPath = "safe/%2F",
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplayObservedUtc) },
            });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies replay signing rejects a configured push route that escapes the trusted base address before sending.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncRejectsRouteEscapingTrustedBaseAddressBeforeSend()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            new(ReplayEndpointBaseAddressText),
            static options => options with
            {
                PushPath = "%2E%2E/push",
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplayObservedUtc) },
            });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Configuration);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies malformed configured signing routes are rejected before an upload reaches the network.</summary>
    /// <param name="path">The malformed configured push path.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("push?missing")]
    [Arguments("push?=value")]
    [Arguments("push?key=value&")]
    [Arguments("#fragment")]
    [Arguments("safe//segment")]
    [Arguments("safe/%5C")]
    public async Task PushAsyncRejectsMalformedConfiguredSigningRouteBeforeSend(string path)
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            new("https://example.invalid/"),
            options => options with
            {
                PushPath = path,
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplayObservedUtc) },
            });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var exception = await CaptureHttpExceptionAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

#if NET8_0_OR_GREATER
    /// <summary>Verifies dangerous URI canonicalization signs the normalized route used by the endpoint.</summary>
    /// <param name="path">The configured route containing an encoded dot segment.</param>
    /// <param name="endpointPushPath">The endpoint route after URI normalization.</param>
    /// <param name="expectedRequestUri">The expected normalized request URI.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("safe/%2E/push", "safe/push", "https://example.invalid/api/safe/push")]
    [Arguments("safe/%2E%2E/push", "push", "https://example.invalid/api/push")]
    public async Task PushAsyncWithDangerousCanonicalizationDotSegmentSignsNormalizedRoute(
        string path,
        string endpointPushPath,
        string expectedRequestUri)
    {
        var creationOptions = new UriCreationOptions { DangerousDisablePathAndQueryCanonicalization = true };
        var baseAddress = new Uri("https://example.invalid/api/", creationOptions);
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new ReplayEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider) with { PushPath = endpointPushPath });
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            baseAddress,
            options => options with
            {
                ConnectPath = ReplayEndpointConnectPath,
                PushPath = path,
                SubscribePath = ReplayEndpointSubscribePath,
                AcknowledgePath = ReplayEndpointAcknowledgePath,
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = timeProvider },
            });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var batch = CreateBatch();
        var result = await session.PushAsync(batch, CancellationToken.None);
        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(hub.ApplyClient).IsEqualTo(new(ReplayEndpointTenantId, ReplayEndpointClientId));
        await Assert.That(handler.Requests.Count).IsEqualTo(RequestsAfterConnectAndPush);
        await Assert.That(handler.Requests[1].RequestUri).IsEqualTo(new(expectedRequestUri));
    }

#endif

    /// <summary>Creates a connect response that includes replay session headers.</summary>
    /// <returns>The response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateReplayConnectResponse() => CreateReplayConnectResponse(ReplaySessionSecret, ReplayTenantId, ReplaySessionExpires);

    /// <summary>Creates a connect response that includes replay session headers.</summary>
    /// <param name="sessionSecret">The replay session secret header value.</param>
    /// <returns>The response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateReplayConnectResponse(string sessionSecret) =>
        CreateReplayConnectResponse(sessionSecret, ReplayTenantId, ReplaySessionExpires);

    /// <summary>Creates a connect response that includes replay session headers.</summary>
    /// <param name="sessionSecret">The replay session secret header value.</param>
    /// <param name="replayTenantId">The replay tenant identifier header value.</param>
    /// <param name="sessionExpires">The replay session expiry header value.</param>
    /// <returns>The response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateReplayConnectResponse(
        string sessionSecret,
        string replayTenantId,
        string sessionExpires) =>
        CreateReplayConnectResponse(sessionSecret, replayTenantId, sessionExpires, ReplaySessionId);

    /// <summary>Creates a connect response that includes replay session headers.</summary>
    /// <param name="sessionSecret">The replay session secret header value.</param>
    /// <param name="replayTenantId">The replay tenant identifier header value.</param>
    /// <param name="sessionExpires">The replay session expiry header value.</param>
    /// <param name="sessionId">The replay session identifier header value.</param>
    /// <returns>The response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateReplayConnectResponse(
        string sessionSecret,
        string replayTenantId,
        string sessionExpires,
        string sessionId)
    {
        var response = CreateJsonResponse(HttpStatusCode.OK, ConnectResponseJson, ProtocolMediaType);
        _ = response.Headers.TryAddWithoutValidation(ReplayTenantIdHeader, replayTenantId);
        response.Headers.Add(ReplaySessionIdHeader, sessionId);
        response.Headers.Add(ReplaySessionSecretHeader, sessionSecret);
        response.Headers.Add(ReplaySessionExpiresHeader, sessionExpires);
        return response;
    }

    /// <summary>Creates an adapter with deterministic replay time.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateReplayAdapter(HttpClient httpClient) =>
        CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with
            {
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplayObservedUtc) },
            });

    /// <summary>Creates a replay adapter with custom routes under a trusted base path.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="timeProvider">The deterministic replay clock.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateResolvedReplayAdapter(HttpClient httpClient, TimeProvider timeProvider) =>
        CreateAdapter(
            httpClient,
            new(ReplayEndpointBaseAddressText),
            options => options with
            {
                ConnectPath = ReplayEndpointConnectPath,
                PushPath = ReplayEndpointPushPath,
                SubscribePath = ReplayEndpointSubscribePath,
                AcknowledgePath = ReplayEndpointAcknowledgePath,
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = timeProvider },
            });

    /// <summary>Gets a single captured request header.</summary>
    /// <param name="headers">The captured headers.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The single header value, or <see langword="null"/>.</returns>
    private static string? GetSingleHeader(IReadOnlyList<KeyValuePair<string, IEnumerable<string>>> headers, string name)
    {
        for (var index = 0; index < headers.Count; index++)
        {
            if (string.Equals(headers[index].Key, name, StringComparison.Ordinal))
            {
                return headers[index].Value.Single();
            }
        }

        return null;
    }

    /// <summary>Creates replay-enabled endpoint options for adapter-to-endpoint tests.</summary>
    /// <param name="hub">The recording hub.</param>
    /// <param name="timeProvider">The replay clock.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateReplayEndpointOptions(IServerStreamHub hub, TimeProvider timeProvider) =>
        new()
        {
            Hub = hub,
            DeclaredCapabilities = new(
                new(1, 0),
                RemoteTransportCapabilities.BatchPush
                | RemoteTransportCapabilities.CursorResume
                | RemoteTransportCapabilities.ReceiveAcknowledgements
                | RemoteTransportCapabilities.ServerIdempotency,
                NegotiatedBatchOperations,
                ReplayEndpointMaximumBatchBytes,
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(ReplayEndpointClientInboxRetentionMinutes)),
            ReplayAuthorizer = ReplayEndpointAuthorizer.Instance,
            ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = timeProvider },
            PathBase = "api",
            ConnectPath = ReplayEndpointConnectPath,
            PushPath = ReplayEndpointPushPath,
            SubscribePath = ReplayEndpointSubscribePath,
            AcknowledgePath = ReplayEndpointAcknowledgePath,
        };

    /// <summary>Forwards adapter HTTP requests into a real replay endpoint.</summary>
    /// <param name="endpoint">The endpoint under test.</param>
    /// <param name="authenticatedClient">The authenticated client supplied by the host transport.</param>
    private sealed class ReplayEndpointHandler(HttpServerEndpoint endpoint, ServerAuthenticatedClient? authenticatedClient = null) : HttpMessageHandler
    {
        /// <summary>The trusted authenticated principal supplied by the host transport.</summary>
        private readonly ServerAuthenticatedClient _authenticatedClient = authenticatedClient ?? new(ReplayEndpointTenantId, ReplayEndpointClientId);

        /// <summary>Gets captured request records.</summary>
        internal List<RequestRecord> Requests { get; } = [];

        /// <summary>Gets the replay session id negotiated by the latest connect response.</summary>
        internal string? ConnectReplaySessionId { get; private set; }

        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await CaptureAsync(request).ConfigureAwait(false);
            var response = await endpoint.HandleAsync(request, _authenticatedClient, cancellationToken).ConfigureAwait(false);
            CaptureConnectReplaySessionId(response);
            return response;
        }

        /// <summary>Captures the negotiated replay session id without retaining a disposable response object.</summary>
        /// <param name="response">The endpoint response.</param>
        private void CaptureConnectReplaySessionId(HttpResponseMessage response)
        {
            if (response.Headers.TryGetValues(ReplaySessionIdHeader, out var values))
            {
                ConnectReplaySessionId = values.SingleOrDefault();
            }
        }

        /// <summary>Captures the outgoing request without retaining disposable request objects.</summary>
        /// <param name="request">The request.</param>
        /// <returns>The asynchronous capture operation.</returns>
        private async Task CaptureAsync(HttpRequestMessage request)
        {
            var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync().ConfigureAwait(false);
            Requests.Add(new(
                request.Method,
                request.RequestUri,
                request.Headers.Accept.Count == 0 ? null : request.Headers.Accept.First().ToString(),
                request.Content?.Headers.ContentType?.ToString(),
                request.Headers.Authorization?.Scheme,
                request.Headers.ToArray(),
                body));
        }
    }

    /// <summary>Records endpoint hub calls made by replay integration tests.</summary>
    private sealed class ReplayEndpointHub : IServerStreamHub
    {
        /// <summary>Gets the last trusted apply principal.</summary>
        internal ServerAuthenticatedClient? ApplyClient { get; private set; }

        /// <summary>Gets the last trusted acknowledge principal.</summary>
        internal ServerAuthenticatedClient? AcknowledgeClient { get; private set; }

        /// <summary>Gets the last acknowledgement.</summary>
        internal ReceiveAcknowledgement? Acknowledgement { get; private set; }

        /// <summary>Gets the last trusted subscribe principal.</summary>
        internal ServerAuthenticatedClient? SubscribeClient { get; private set; }

        /// <summary>Gets the last subscribe request.</summary>
        internal RemoteSubscribeRequest? SubscribeRequest { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ApplyClient = client;
            var result = new RemoteSyncResult(
                batch.BatchId,
                [new(batch.Operations[0].OperationId, OperationResultKind.Accepted, "v1", client.TenantId)],
                "server-1",
                null);
            return ValueTask.FromResult(new ServerSyncResult(result, []));
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AcknowledgeClient = client;
            Acknowledgement = acknowledgement;
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SubscribeClient = client;
            SubscribeRequest = request;
            await Task.CompletedTask.ConfigureAwait(false);
            yield return new(
                Guid.Parse("00000000-0000-0000-0000-000000000401"),
                request.StreamId,
                request.Cursor,
                ReplayCursorOne,
                []);
        }
    }

    /// <summary>Allows endpoint replay authorization for adapter integration tests.</summary>
    private sealed class ReplayEndpointAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets the shared allow authorizer.</summary>
        internal static ReplayEndpointAuthorizer Instance { get; } = new();

        /// <inheritdoc/>
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(true);
        }
    }

    /// <summary>Provides deterministic replay timestamps.</summary>
    /// <param name="utcNow">The UTC instant.</param>
    private sealed class ReplayTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
