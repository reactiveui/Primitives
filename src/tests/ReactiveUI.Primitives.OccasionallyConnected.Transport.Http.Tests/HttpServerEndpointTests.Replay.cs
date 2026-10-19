// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests HTTP replay authentication on the portable server endpoint.</summary>
public sealed partial class HttpServerEndpointTests
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

    /// <summary>The replay message identifier used by server replay fixtures.</summary>
    private const string ReplayMessageId = "message-1";

    /// <summary>The replay nonce used by server replay fixtures.</summary>
    private const string ReplayNonce = "nonce-1";

    /// <summary>The placeholder replay session identifier used by invalid-header fixtures.</summary>
    private const string InvalidHeaderReplaySessionId = "session-1";

    /// <summary>The placeholder replay MAC used by invalid-header fixtures.</summary>
    private const string InvalidHeaderReplayMac = "mac-1";

    /// <summary>The expected connect plus first push plus duplicate replay authorization call count.</summary>
    private const int ExpectedReplayRevocationAuthorizationCalls = 3;

    /// <summary>The expected replay authorization call count for a connect failure and its duplicate retry.</summary>
    private const int ExpectedDuplicateConnectFailureAuthorizationCalls = 2;

    /// <summary>The replay session lifetime in minutes used by server replay fixtures.</summary>
    private const int ReplaySessionLifetimeMinutes = 30;

    /// <summary>The small replay canonical request budget used by endpoint rejection tests.</summary>
    private const int SmallReplayCanonicalRequestBytes = 1024;

    /// <summary>The small subscribe query byte limit used by malformed query tests.</summary>
    private const int SmallSubscribeQueryBytes = 32;

    /// <summary>The canonical subscribe route used by server replay fixtures.</summary>
    private const string CanonicalSubscribePath = "subscribe";

    /// <summary>The stable replay sent timestamp.</summary>
    private static readonly DateTimeOffset ReplaySentAtUtc = DateTimeOffset.Parse("2026-09-17T22:00:00+00:00", CultureInfo.InvariantCulture);

    /// <summary>Verifies unsigned state-changing requests are rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsUnsignedReplayRequestBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies empty replay header collections are rejected before replay authorization or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsEmptyReplayHeaderCollectionBeforeAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        _ = request.Headers.TryAddWithoutValidation(ReplayMessageIdHeader, []);
        request.Headers.Add(ReplayNonceHeader, ReplayNonce);
        request.Headers.Add(ReplaySentAtHeader, ReplaySentAtUtc.ToString("O", CultureInfo.InvariantCulture));
        request.Headers.Add(ReplaySessionIdHeader, InvalidHeaderReplaySessionId);
        request.Headers.Add(ReplayMacHeader, InvalidHeaderReplayMac);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(authorizer.Calls).IsEqualTo(0);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies subscribe startup cancellation after replay admission abandons ownership before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeStartupCancellationAfterReplayAdmissionDoesNotReachHub()
    {
        var armFailure = new OperationCanceledException("Timer arm canceled.");
        var timeProvider = new ArmFailureTimeProvider(returnsFalse: false, changeFailure: armFailure);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer) with { TimeProvider = timeProvider });
        OperationCanceledException? exception = null;
        try
        {
            using var request = await CreateSignedSubscribeRequestAsync(endpoint);
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);
            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (OperationCanceledException thrown)
            {
                exception = thrown;
            }

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(exception).IsSameReferenceAs(armFailure);
            const int ExpectedAuthorizeCalls = 2;

            await Assert.That(authorizer.Calls).IsEqualTo(ExpectedAuthorizeCalls);
            await Assert.That(hub.SubscribeClient).IsNull();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies connect returns a replay session that can authenticate later requests.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectReturnsReplaySessionHeaders()
    {
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(request, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(GetResponseHeader(response, ReplayTenantIdHeader)).IsEqualTo("dGVuYW50LTE");
        await Assert.That(GetResponseHeader(response, ReplaySessionIdHeader)).IsNotNull();
        await Assert.That(GetResponseHeader(response, ReplaySessionSecretHeader)).IsNotNull();
        await Assert.That(GetResponseHeader(response, ReplaySessionExpiresHeader)).IsNotNull();
    }

    /// <summary>Verifies a byte-identical signed push is replayed without applying hub effects twice.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushReplaysDuplicateWithoutDuplicateHubEffect()
    {
        var applyCount = 0;
        var hub = new RecordingHub
        {
            ApplyHandler = (incoming, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                applyCount++;
                return ValueTask.FromResult(CreateServerResult(incoming, client));
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var first = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(applyCount).IsEqualTo(1);
        await Assert.That(await ReadResponseBodyAsync(duplicateResponse))
            .IsEquivalentTo(await ReadResponseBodyAsync(firstResponse), EqualityComparer<byte>.Default);
    }

    /// <summary>Verifies an expired replay session is rejected without hub effects while current host authorization is checked.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsExpiredReplaySessionWithoutHubEffects()
    {
        var replayClock = new ReplayTimeProvider(ReplaySentAtUtc);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer) with
        {
            ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = replayClock },
        });
        var session = await ConnectReplaySessionAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        var expiredButFreshRequestUtc = ReplaySentAtUtc.AddMinutes(ReplaySessionLifetimeMinutes).AddTicks(1);
        replayClock.SetUtcNow(expiredButFreshRequestUtc);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, new ReplaySessionSigning(session, expiredButFreshRequestUtc));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect + 1);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies cached replay re-runs host authorization and does not invoke hub effects after revocation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushReplayRejectsAfterAuthorizationRevokedWithoutDuplicateHubEffect()
    {
        var applyCount = 0;
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub
        {
            ApplyHandler = (incoming, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                applyCount++;
                return ValueTask.FromResult(CreateServerResult(incoming, client));
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var first = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        authorizer.Allow = false;
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(applyCount).IsEqualTo(1);
        await Assert.That(authorizer.Calls).IsEqualTo(ExpectedReplayRevocationAuthorizationCalls);
    }

    /// <summary>Verifies a duplicate nonce with changed body is rejected without a second hub effect.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsChangedBodyReplayWithoutDuplicateHubEffect()
    {
        var applyCount = 0;
        var hub = new RecordingHub
        {
            ApplyHandler = (incoming, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                applyCount++;
                return ValueTask.FromResult(CreateServerResult(incoming, client));
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var firstBody = CreateCodec().SerializePushRequest(CreateBatch());
        var changedOperation = CreateOperation() with { Metadata = new Dictionary<string, string> { ["trace"] = "changed" } };
        var changedBody = CreateCodec().SerializePushRequest(new(Guid.Parse(BatchIdText), [changedOperation]));
        using var first = CreateProtocolRequest(HttpMethod.Post, PushUri, firstBody);
        using var changed = CreateProtocolRequest(HttpMethod.Post, PushUri, changedBody);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.Push, "POST", "push", [], firstBody, session);
        AddSessionReplayHeaders(changed, HttpReplayOperationKind.Push, "POST", "push", [], changedBody, session);
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var changedResponse = await endpoint.HandleAsync(changed, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(changedResponse.StatusCode).IsEqualTo(HttpStatusCode.Conflict);
        await Assert.That(applyCount).IsEqualTo(1);
    }

    /// <summary>Verifies initial replay authorization denial maps to forbidden before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsInitialAuthorizationDenialBeforeHub()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var session = await ConnectReplaySessionAsync(endpoint);
        authorizer.Allow = false;
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies non-UTC replay timestamps are rejected before authorization and hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsNonUtcReplayTimestampBeforeAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var session = await ConnectReplaySessionAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        _ = request.Headers.Remove(ReplaySentAtHeader);
        request.Headers.Add(ReplaySentAtHeader, ReplaySentAtUtc.ToOffset(TimeSpan.FromHours(1)).ToString("O", CultureInfo.InvariantCulture));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies duplicate replay headers are rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsDuplicateReplayHeadersBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        request.Headers.Add(ReplayNonceHeader, "nonce-2");
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies oversized replay headers are rejected before replay authorization or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsOversizedReplayHeaderBeforeReplayAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        var options = CreateReplayOptions(hub, authorizer) with
        {
            ReplayProtection = new() { MaximumCanonicalRequestBytes = SmallReplayCanonicalRequestBytes, TimeProvider = new ReplayTimeProvider(ReplaySentAtUtc) },
        };
        await using var endpoint = new HttpServerEndpoint(options);
        var session = await ConnectReplaySessionAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        _ = request.Headers.Remove(ReplayMessageIdHeader);
        _ = request.Headers.TryAddWithoutValidation(ReplayMessageIdHeader, new string('a', SmallReplayCanonicalRequestBytes + 1));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies the returned secret header text is the byte material used for MAC signing.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushAcceptsMacComputedFromReturnedSecretHeaderBytes()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies a replay session cannot be used by another authenticated principal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsReplaySessionFromDifferentTenantBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        var substituted = new ServerAuthenticatedClient("tenant-2", ClientId);
        using var response = await endpoint.HandleAsync(request, substituted, CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies unsigned acknowledgements are rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeRejectsUnsignedReplayRequestBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var body = CreateCodec().SerializeAcknowledgement(CreateAcknowledgement());
        using var request = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies duplicate acknowledgements replay cached no-content responses without duplicate hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeReplaysDuplicateNoContentWithoutDuplicateHubEffect()
    {
        var acknowledgeCount = 0;
        var hub = new RecordingHub
        {
            AcknowledgeHandler = (acknowledgement, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                acknowledgeCount++;
                return ValueTask.CompletedTask;
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeAcknowledgement(CreateAcknowledgement());
        using var first = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.Acknowledge, "POST", "ack", [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.Acknowledge, "POST", "ack", [], body, session);
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(await ReadResponseBodyAsync(duplicateResponse)).IsEmpty();
        await Assert.That(acknowledgeCount).IsEqualTo(1);
    }

    /// <summary>Verifies unsigned subscribe polls are rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsUnsignedReplayRequestBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies connect route query text is rejected instead of ignored by replay canonicalization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectRejectsQueryBeforeReplayAdmission()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, $"{ConnectUri}?ignored=1", body);
        AddConnectReplayHeaders(request, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies push route query text is rejected instead of being omitted from the MAC.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsQueryBeforeHubEffects()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, $"{PushUri}?ignored=1", body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies acknowledgement route query text is rejected instead of being omitted from the MAC.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeRejectsQueryBeforeHubEffects()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeAcknowledgement(CreateAcknowledgement());
        using var request = CreateProtocolRequest(HttpMethod.Post, $"{AcknowledgeUri}?ignored=1", body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Acknowledge, "POST", "ack", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies a MAC signed for a default path cannot authorize a configured non-default path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsMacSignedForDifferentConfiguredPathBeforeHub()
    {
        const string CustomPushPath = "replay-push";
        const string CustomPushUri = "https://example.invalid/replay-push";
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub) with { PushPath = CustomPushPath });
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, CustomPushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies connect session expiry is issued from the trusted replay clock instead of the client timestamp.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectIssuesSessionExpiryFromTrustedReplayClock()
    {
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(request, body, ReplaySentAtUtc.AddMinutes(-1));
        var expected = ReplaySentAtUtc.AddMinutes(ReplaySessionLifetimeMinutes).ToString("O", CultureInfo.InvariantCulture);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(GetRequiredResponseHeader(response, ReplaySessionExpiresHeader)).IsEqualTo(expected);
    }

    /// <summary>Verifies connect session capacity failure is reported instead of returning unusable credentials.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectSessionCapacityFailureDoesNotReturnReplayCredentials()
    {
        var options = CreateReplayOptions(new RecordingHub()) with
        {
            ReplayProtection = new() { MaximumReplaySessions = 1, TimeProvider = new ReplayTimeProvider(ReplaySentAtUtc) },
        };
        await using var endpoint = new HttpServerEndpoint(options);
        var existing = await ConnectReplaySessionAsync(endpoint);
        await Assert.That(existing.SessionId).IsNotNull();
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var first = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(first, body, ReplaySentAtUtc, "capacity-message", "capacity-nonce");
        AddConnectReplayHeaders(duplicate, body, ReplaySentAtUtc, "capacity-message", "capacity-nonce");
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(GetResponseHeader(firstResponse, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetResponseHeader(firstResponse, ReplaySessionSecretHeader)).IsNull();
        await Assert.That(GetResponseHeader(duplicateResponse, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetResponseHeader(duplicateResponse, ReplaySessionSecretHeader)).IsNull();
    }

    /// <summary>Verifies a signed connect admitted by replay protection abandons ownership when negotiation fails.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectAbandonsReplayOwnerWhenNegotiationFails()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var capabilities = CreateCapabilities(RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge);
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub(), authorizer) with { DeclaredCapabilities = capabilities });
        var connect = new TransportConnectRequest(new(new(1, 0), new(1, 0)), new(ClientId, "forged-tenant"), [DeliveryGuarantee.ExactlyOnce]);
        var body = CreateCodec().SerializeConnectRequest(connect);
        using var first = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(first, body, ReplaySentAtUtc, "negotiation-message", "negotiation-nonce");
        AddConnectReplayHeaders(duplicate, body, ReplaySentAtUtc, "negotiation-message", "negotiation-nonce");
        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(GetResponseHeader(firstResponse, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetResponseHeader(firstResponse, ReplaySessionSecretHeader)).IsNull();
        await Assert.That(GetResponseHeader(duplicateResponse, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetResponseHeader(duplicateResponse, ReplaySessionSecretHeader)).IsNull();
        await Assert.That(authorizer.Calls).IsEqualTo(ExpectedDuplicateConnectFailureAuthorizationCalls);
    }

    /// <summary>Verifies replay authorization receives decoded acknowledgement rights.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeAuthorizationReceivesDecodedRights()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var session = await ConnectReplaySessionAsync(endpoint);
        var acknowledgement = CreateAcknowledgement();
        var body = CreateCodec().SerializeAcknowledgement(acknowledgement);
        using var request = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Acknowledge, "POST", "ack", [], body, session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(authorizer.Contexts.Exists(context => context.Operation == nameof(HttpReplayOperationKind.Acknowledge)
            && context.SubscriptionId == acknowledgement.SubscriptionId
            && context.StreamIds.SequenceEqual([acknowledgement.StreamId]))).IsTrue();
    }

    /// <summary>Verifies subscribe query fields are MAC-bound and visible to replay authorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsChangedQueryAndAuthorizesDecodedRights()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var session = await ConnectReplaySessionAsync(endpoint);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
        AddSessionReplayHeaders(
            request,
            HttpReplayOperationKind.Subscribe,
            "GET",
            CanonicalSubscribePath,
            [new("streamId", StreamName), new("subscriptionId", SubscriptionIdText), new("positionKind", "1")],
            [],
            session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.SubscribeClient).IsNull();
        await Assert.That(authorizer.Contexts.Exists(static context => context.Operation == nameof(HttpReplayOperationKind.Subscribe)
            && context.SubscriptionId == new SubscriptionId(Guid.Parse(SubscriptionIdText))
            && context.StreamIds.SequenceEqual([new StreamId(StreamName)]))).IsTrue();
    }

    /// <summary>Verifies caller cancellation during subscribe replay authorization remains caller-observable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeCallerCancellationDuringReplayAuthorizationPropagatesCancellation()
    {
        using var callerCancellation = new CancellationTokenSource();
        var authorizer = new CallerCancelingSubscribeReplayAuthorizer(callerCancellation);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;

        async Task Act()
        {
            using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), callerCancellation.Token);
        }

        await Assert.That(Act).Throws<OperationCanceledException>();
        await Assert.That(callerCancellation.IsCancellationRequested).IsTrue();
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect + 1);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies oversized malformed subscribe queries are rejected before replay authorization or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsOversizedMalformedQueryBeforeReplayAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer) with { MaximumQueryBytes = SmallSubscribeQueryBytes });
        var session = await ConnectReplaySessionAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        var query = $"broken&{string.Join('&', Enumerable.Range(0, SmallSubscribeQueryBytes).Select(static value => $"field{value}=value{value}"))}";
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}?{query}");
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Subscribe, "GET", CanonicalSubscribePath, [], [], session);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Creates a signed push request for a valid replay session.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <param name="batch">The push batch.</param>
    /// <returns>The signed request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<HttpRequestMessage> CreateSignedPushRequestAsync(HttpServerEndpoint endpoint, SyncBatch batch) =>
        CreateSignedPushRequestAsync(endpoint, batch, PushUri, "push");

    /// <summary>Creates a signed push request for a valid replay session.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <param name="batch">The push batch.</param>
    /// <param name="uri">The request URI.</param>
    /// <param name="canonicalPath">The canonical route path.</param>
    /// <returns>The signed request.</returns>
    private static async Task<HttpRequestMessage> CreateSignedPushRequestAsync(HttpServerEndpoint endpoint, SyncBatch batch, string uri, string canonicalPath)
    {
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(batch);
        var request = CreateProtocolRequest(HttpMethod.Post, uri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", canonicalPath, [], body, session);
        return request;
    }

    /// <summary>Creates a signed acknowledgement request for a valid replay session.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <param name="acknowledgement">The acknowledgement.</param>
    /// <returns>The signed request.</returns>
    private static async Task<HttpRequestMessage> CreateSignedAcknowledgeRequestAsync(HttpServerEndpoint endpoint, ReceiveAcknowledgement acknowledgement)
    {
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeAcknowledgement(acknowledgement);
        var request = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Acknowledge, "POST", "ack", [], body, session);
        return request;
    }

    /// <summary>Creates a signed subscribe request for a valid replay session.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <returns>The signed request.</returns>
    private static async Task<HttpRequestMessage> CreateSignedSubscribeRequestAsync(HttpServerEndpoint endpoint)
    {
        var session = await ConnectReplaySessionAsync(endpoint);
        var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Subscribe, "GET", CanonicalSubscribePath, CreateSubscribeQueryFields(), [], session);
        return request;
    }

    /// <summary>Creates a signed relative subscribe request for a valid replay session.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <returns>The signed request.</returns>
    private static async Task<HttpRequestMessage> CreateSignedRelativeSubscribeRequestAsync(HttpServerEndpoint endpoint)
    {
        var session = await ConnectReplaySessionAsync(endpoint);
        var request = new HttpRequestMessage(HttpMethod.Get, new Uri($"{RelativeSubscribeUri}{SubscribeQuery}", UriKind.Relative));
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Subscribe, "GET", CanonicalSubscribePath, CreateSubscribeQueryFields(), [], session);
        return request;
    }

    /// <summary>Creates canonical subscribe query fields for the shared subscribe fixture.</summary>
    /// <returns>The query fields.</returns>
    private static IReadOnlyList<KeyValuePair<string, string>> CreateSubscribeQueryFields() =>
        [new("streamId", StreamName), new("subscriptionId", SubscriptionIdText), new("positionKind", "0")];

    /// <summary>Connects and returns the replay session headers.</summary>
    /// <param name="endpoint">The endpoint under test.</param>
    /// <returns>The issued replay session.</returns>
    private static async Task<ReplaySession> ConnectReplaySessionAsync(HttpServerEndpoint endpoint)
    {
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(request, body);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        return new(
            GetRequiredResponseHeader(response, ReplaySessionIdHeader),
            GetRequiredResponseHeader(response, ReplaySessionSecretHeader));
    }

    /// <summary>Adds replay headers for a connect request.</summary>
    /// <param name="request">The request.</param>
    /// <param name="body">The exact body bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AddConnectReplayHeaders(HttpRequestMessage request, byte[] body) =>
        AddConnectReplayHeaders(request, body, ReplaySentAtUtc);

    /// <summary>Adds replay headers for a connect request.</summary>
    /// <param name="request">The request.</param>
    /// <param name="body">The exact body bytes.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AddConnectReplayHeaders(HttpRequestMessage request, byte[] body, DateTimeOffset sentAtUtc) =>
        AddConnectReplayHeaders(request, body, sentAtUtc, ReplayMessageId, ReplayNonce);

    /// <summary>Adds replay headers for a connect request.</summary>
    /// <param name="request">The request.</param>
    /// <param name="body">The exact body bytes.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="messageId">The replay message identifier.</param>
    /// <param name="nonce">The replay nonce.</param>
    private static void AddConnectReplayHeaders(HttpRequestMessage request, byte[] body, DateTimeOffset sentAtUtc, string messageId, string nonce)
    {
        AddFreshnessHeaders(request, messageId, nonce, sentAtUtc);
        _ = CreateCanonicalRequest(HttpReplayOperationKind.Connect, "POST", "connect", [], body, sentAtUtc);
    }

    /// <summary>Adds signed replay headers for a session request.</summary>
    /// <param name="request">The request.</param>
    /// <param name="operation">The operation kind.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The normalized relative path.</param>
    /// <param name="query">The query fields.</param>
    /// <param name="body">The exact request body.</param>
    /// <param name="session">The replay session.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AddSessionReplayHeaders(
        HttpRequestMessage request,
        HttpReplayOperationKind operation,
        string method,
        string path,
        IReadOnlyList<KeyValuePair<string, string>> query,
        byte[] body,
        ReplaySession session) =>
        AddSessionReplayHeaders(request, operation, method, path, query, body, new ReplaySessionSigning(session, ReplaySentAtUtc));

    /// <summary>Adds signed replay headers for a session request.</summary>
    /// <param name="request">The request.</param>
    /// <param name="operation">The operation kind.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The normalized relative path.</param>
    /// <param name="query">The query fields.</param>
    /// <param name="body">The exact request body.</param>
    /// <param name="signing">The replay session signing context.</param>
    private static void AddSessionReplayHeaders(
        HttpRequestMessage request,
        HttpReplayOperationKind operation,
        string method,
        string path,
        IReadOnlyList<KeyValuePair<string, string>> query,
        byte[] body,
        ReplaySessionSigning signing)
    {
        AddFreshnessHeaders(request, ReplayMessageId, ReplayNonce, signing.SentAtUtc);
        request.Headers.Add(ReplaySessionIdHeader, signing.Session.SessionId);
        var canonical = CreateCanonicalRequest(operation, method, path, query, body, signing.SentAtUtc);
        var replayRequest = new HttpReplayRequest
        {
            Operation = operation,
            Principal = new(TenantId, ClientId),
            MessageId = ReplayMessageId,
            Nonce = ReplayNonce,
            SentAtUtc = signing.SentAtUtc,
            ReplaySessionId = signing.Session.SessionId,
            ReplayMac = "placeholder",
            CanonicalRequest = canonical,
        };
        var envelope = new HttpReplayEnvelopeHasher().Create(replayRequest);
        var mac = new HttpReplayEnvelopeHasher().ComputeMac(Encoding.UTF8.GetBytes(signing.Session.SessionSecret), envelope.MacInput);
        _ = request.Headers.Remove(ReplayMacHeader);
        request.Headers.Add(ReplayMacHeader, mac);
    }

    /// <summary>Adds common replay freshness headers.</summary>
    /// <param name="request">The request.</param>
    /// <param name="messageId">The replay message identifier.</param>
    /// <param name="nonce">The replay nonce.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    private static void AddFreshnessHeaders(HttpRequestMessage request, string messageId, string nonce, DateTimeOffset sentAtUtc)
    {
        request.Headers.Add(ReplayMessageIdHeader, messageId);
        request.Headers.Add(ReplayNonceHeader, nonce);
        request.Headers.Add(ReplaySentAtHeader, sentAtUtc.ToString("O", CultureInfo.InvariantCulture));
    }

    /// <summary>Creates endpoint options with deterministic replay protection.</summary>
    /// <param name="hub">The borrowed hub.</param>
    /// <param name="authorizer">The host replay authorizer.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateReplayOptions(IServerStreamHub hub, IHttpReplayAuthorizer? authorizer = null) =>
        CreateOptions(hub) with
        {
            ReplayAuthorizer = authorizer ?? AllowReplayAuthorizer.Instance,
            ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplaySentAtUtc) },
        };

    /// <summary>Creates canonical request material through the production builder.</summary>
    /// <param name="operation">The operation kind.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The normalized path.</param>
    /// <param name="query">The query fields.</param>
    /// <param name="body">The request body.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <returns>The canonical request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpCanonicalRequest CreateCanonicalRequest(
        HttpReplayOperationKind operation,
        string method,
        string path,
        IReadOnlyList<KeyValuePair<string, string>> query,
        byte[] body,
        DateTimeOffset sentAtUtc) =>
        new HttpCanonicalRequestBuilder(new()).Build(operation, method, path, query, sentAtUtc, body);

    /// <summary>Gets an optional response header value.</summary>
    /// <param name="response">The response.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The value, or <see langword="null"/>.</returns>
    private static string? GetResponseHeader(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? values.Single() : null;

    /// <summary>Gets a required response header value.</summary>
    /// <param name="response">The response.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">The response does not include the required header.</exception>
    private static string GetRequiredResponseHeader(HttpResponseMessage response, string name) =>
        GetResponseHeader(response, name) ?? throw new InvalidOperationException($"Missing {name}.");

    /// <summary>Groups replay session material with the signing timestamp.</summary>
    /// <param name="Session">The replay session.</param>
    /// <param name="SentAtUtc">The replay timestamp.</param>
    private readonly record struct ReplaySessionSigning(ReplaySession Session, DateTimeOffset SentAtUtc);

    /// <summary>Provides deterministic replay timestamps.</summary>
    /// <param name="utcNow">The UTC instant.</param>
    private sealed class ReplayTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC instant.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Updates the current UTC instant.</summary>
        /// <param name="utcNow">The new UTC instant.</param>
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }

    /// <summary>Allows connect authorization, then cancels the caller during subscribe authorization.</summary>
    /// <param name="callerCancellation">The caller cancellation source.</param>
    private sealed class CallerCancelingSubscribeReplayAuthorizer(CancellationTokenSource callerCancellation) : IHttpReplayAuthorizer
    {
        /// <summary>Whether the connect replay authorization has been allowed.</summary>
        private bool _connectAuthorized;

        /// <summary>Gets the number of authorization calls.</summary>
        internal int Calls { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            if (!_connectAuthorized)
            {
                _connectAuthorized = true;
                return true;
            }

            await callerCancellation.CancelAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return true;
        }
    }

    /// <summary>Allows every replay authorization request.</summary>
    private sealed class AllowReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets the shared allow authorizer.</summary>
        internal static AllowReplayAuthorizer Instance { get; } = new();

        /// <inheritdoc/>
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(true);
        }
    }

    /// <summary>Records replay authorization calls and allows tests to revoke permission.</summary>
    private sealed class RecordingReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets or sets whether authorization is allowed.</summary>
        internal bool Allow { get; set; } = true;

        /// <summary>Gets the number of authorization calls.</summary>
        internal int Calls { get; private set; }

        /// <summary>Gets captured authorization contexts.</summary>
        internal List<HttpReplayAuthorizationContext> Contexts { get; } = [];

        /// <inheritdoc/>
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            Contexts.Add(context);
            return ValueTask.FromResult(Allow);
        }
    }

    /// <summary>Represents the replay session returned from connect.</summary>
    /// <param name="SessionId">The session identifier.</param>
    /// <param name="SessionSecret">The session secret.</param>
    private sealed record ReplaySession(string SessionId, string SessionSecret);
}
