// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Net;
using System.Net.Http;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests replay freshness and tamper rejection on the portable server endpoint.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The default replay freshness window in minutes.</summary>
    private const int DefaultFreshnessWindowMinutes = 5;

    /// <summary>The clock advance, in minutes, that keeps stale requests after replay session issuance.</summary>
    private const int FreshnessClockAdvanceMinutes = 10;

    /// <summary>The replacement message identifier used by header tamper tests.</summary>
    private const string TamperedReplayMessageId = "message-2";

    /// <summary>Verifies a signed request older than the default five-minute freshness window is rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsReplayTimestampOlderThanFreshnessWindowBeforeHub()
    {
        var clock = new ReplayTimeProvider(ReplaySentAtUtc);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateFreshnessOptions(hub, clock));
        var session = await ConnectReplaySessionAsync(endpoint);
        var nowUtc = ReplaySentAtUtc.AddMinutes(FreshnessClockAdvanceMinutes);
        clock.SetUtcNow(nowUtc);
        var staleSentAtUtc = nowUtc.AddMinutes(-DefaultFreshnessWindowMinutes).AddTicks(-1);

        using var response = await SendSignedPushAsync(endpoint, session, staleSentAtUtc);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a signed request dated beyond the future skew allowance is rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsReplayTimestampBeyondFutureSkewBeforeHub()
    {
        var clock = new ReplayTimeProvider(ReplaySentAtUtc);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateFreshnessOptions(hub, clock));
        var session = await ConnectReplaySessionAsync(endpoint);
        var futureSentAtUtc = ReplaySentAtUtc.AddMinutes(DefaultFreshnessWindowMinutes).AddTicks(1);

        using var response = await SendSignedPushAsync(endpoint, session, futureSentAtUtc);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a signed request exactly at the freshness boundary is still accepted.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushAcceptsReplayTimestampAtFreshnessBoundary()
    {
        var clock = new ReplayTimeProvider(ReplaySentAtUtc);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateFreshnessOptions(hub, clock));
        var session = await ConnectReplaySessionAsync(endpoint);
        var nowUtc = ReplaySentAtUtc.AddMinutes(FreshnessClockAdvanceMinutes);
        clock.SetUtcNow(nowUtc);

        using var response = await SendSignedPushAsync(endpoint, session, nowUtc.AddMinutes(-DefaultFreshnessWindowMinutes));

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies a byte-identical replay captured inside the window is rejected once it becomes stale.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsByteIdenticalReplayAfterFreshnessWindowWithoutSecondHubEffect()
    {
        var clock = new ReplayTimeProvider(ReplaySentAtUtc);
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
        await using var endpoint = new HttpServerEndpoint(CreateFreshnessOptions(hub, clock));
        var session = await ConnectReplaySessionAsync(endpoint);
        using var first = await SendSignedPushAsync(endpoint, session, ReplaySentAtUtc);
        clock.SetUtcNow(ReplaySentAtUtc.AddMinutes(DefaultFreshnessWindowMinutes).AddTicks(1));

        using var replayed = await SendSignedPushAsync(endpoint, session, ReplaySentAtUtc);

        await Assert.That(first.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(replayed.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(applyCount).IsEqualTo(1);
    }

    /// <summary>Verifies a captured signed request whose message identifier is rewritten is rejected as tampered.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsRewrittenReplayMessageIdWithoutHubEffect()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        _ = request.Headers.Remove(ReplayMessageIdHeader);
        request.Headers.Add(ReplayMessageIdHeader, TamperedReplayMessageId);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a captured signed request whose body is modified after signing is rejected as tampered.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsBodyModifiedAfterSigningWithoutHubEffect()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var signedBody = CreateCodec().SerializePushRequest(CreateBatch());
        var tamperedOperation = CreateOperation() with { Payload = new(ContractName, 1, PayloadContentType, "{\"tampered\":true}"u8.ToArray(), PayloadHash) };
        var tamperedBody = CreateCodec().SerializePushRequest(new(Guid.Parse(BatchIdText), [tamperedOperation]));
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, tamperedBody);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], signedBody, session);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a forged tenant routing header cannot replace the host-authenticated tenant.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushIgnoresForgedTenantHeaderAndUsesAuthenticatedPrincipal()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());
        request.Headers.Add(ReplayTenantIdHeader, HttpReplayBase64Url.Encode("forged-tenant"u8));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies a forged tenant hint in the connect body is replaced by the host-authenticated tenant.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectBindsReplaySessionToAuthenticatedTenantInsteadOfBodyHint()
    {
        var authorizer = new RecordingReplayAuthorizer();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub(), authorizer));
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(request, body);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        var tenantHeader = GetRequiredResponseHeader(response, ReplayTenantIdHeader);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(tenantHeader).IsEqualTo(HttpReplayBase64Url.Encode("tenant-1"u8));
        await Assert.That(authorizer.Contexts[0].Client).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Creates endpoint options whose replay clock is controlled by the test.</summary>
    /// <param name="hub">The borrowed hub.</param>
    /// <param name="clock">The replay clock.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateFreshnessOptions(IServerStreamHub hub, ReplayTimeProvider clock) =>
        CreateReplayOptions(hub) with { ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = clock } };

    /// <summary>Signs and sends the shared push batch with the supplied replay timestamp.</summary>
    /// <param name="endpoint">The endpoint under test.</param>
    /// <param name="session">The replay session.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <returns>The caller-owned response.</returns>
    private static async Task<HttpResponseMessage> SendSignedPushAsync(HttpServerEndpoint endpoint, ReplaySession session, DateTimeOffset sentAtUtc)
    {
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, new ReplaySessionSigning(session, sentAtUtc));
        return await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
    }
}
