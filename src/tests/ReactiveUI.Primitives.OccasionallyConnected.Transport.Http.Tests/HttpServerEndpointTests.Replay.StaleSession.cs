// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests stale replay session marker behavior on the portable server endpoint.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The stale replay session marker header.</summary>
    private const string StaleReplaySessionHeader = "X-ReactiveUI-Replay-Session-State";

    /// <summary>The stale replay session marker value.</summary>
    private const string StaleReplaySessionValue = "stale";

    /// <summary>The fixed unpadded SHA-256 MAC length used by replay headers.</summary>
    private const int ReplayMacLength = 43;

    /// <summary>A syntactically malformed replay MAC value.</summary>
    private const string MalformedReplayMac = "bad\u0001mac";

    /// <summary>A short Base64URL replay MAC value with invalid MAC length.</summary>
    private const string ShortReplayMac = "bad-mac";

    /// <summary>A full-length replay MAC value with an invalid character before uppercase letters.</summary>
    private const string InvalidBeforeUppercaseReplayMac = "@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value with an invalid character after uppercase letters.</summary>
    private const string InvalidAfterUppercaseReplayMac = "[AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value with an invalid character before lowercase letters.</summary>
    private const string InvalidBeforeLowercaseReplayMac = "`AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value with an invalid character after lowercase letters.</summary>
    private const string InvalidAfterLowercaseReplayMac = "{AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value with a standard Base64 slash character.</summary>
    private const string SlashReplayMac = "/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value with an invalid character after the digit upper bound.</summary>
    private const string ColonReplayMac = ":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value using the Base64URL uppercase lower-bound character.</summary>
    private const string UpperAReplayMac = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A full-length replay MAC value using the Base64URL uppercase upper-bound character.</summary>
    private const string UpperZReplayMac = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";

    /// <summary>A full-length replay MAC value using the Base64URL lowercase lower-bound character.</summary>
    private const string LowerAReplayMac = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// <summary>A full-length replay MAC value using the Base64URL lowercase upper-bound character.</summary>
    private const string LowerZReplayMac = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    /// <summary>A full-length replay MAC value using the Base64URL digit lower-bound character.</summary>
    private const string DigitZeroReplayMac = "0000000000000000000000000000000000000000000";

    /// <summary>A full-length replay MAC value using the Base64URL digit upper-bound character.</summary>
    private const string DigitNineReplayMac = "9999999999999999999999999999999999999999999";

    /// <summary>A full-length replay MAC value using the Base64URL hyphen character.</summary>
    private const string HyphenReplayMac = "-------------------------------------------";

    /// <summary>A full-length replay MAC value using the Base64URL underscore character.</summary>
    private const string UnderscoreReplayMac = "___________________________________________";

    /// <summary>Verifies a host-authenticated request with an absent replay session receives a bounded stale marker.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithMissingReplaySessionReturnsStaleMarkerWithoutHubEffect()
    {
        await using var issuer = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var session = await ConnectReplaySessionAsync(issuer);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var receiver = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await receiver.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(await ReadResponseBodyAsync(response)).IsEmpty();
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsEqualTo(StaleReplaySessionValue);
        await Assert.That(authorizer.Calls).IsEqualTo(1);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies plain host authentication failure does not advertise stale replay-session renewal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithUntrustedPrincipalReturnsUnauthorizedWithoutStaleMarker()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        using var response = await endpoint.HandleAsync(request, new(string.Empty, ClientId), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies expired replay sessions advertise stale replay-session renewal without hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithExpiredReplaySessionReturnsStaleMarkerWithoutHubEffect()
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
        AddSessionReplayHeaders(
            request,
            HttpReplayOperationKind.Push,
            "POST",
            "push",
            [],
            body,
            new ReplaySessionSigning(session, expiredButFreshRequestUtc));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(await ReadResponseBodyAsync(response)).IsEmpty();
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsEqualTo(StaleReplaySessionValue);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect + 1);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies replay sessions owned by another client do not advertise stale replay-session renewal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithReplaySessionOwnedByAnotherClientReturnsUnauthorizedWithoutStaleMarker()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        using var response = await endpoint.HandleAsync(request, new(TenantId, ForgedClientId), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a bad replay MAC does not advertise stale replay-session renewal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithBadReplayMacReturnsUnauthorizedWithoutStaleMarker()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        ReplaceReplayMacWithDifferentValidValue(request);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies Base64URL-only MAC alphabet variants fail as bad proofs, not syntax errors.</summary>
    /// <param name="replayMac">The syntactically valid but incorrect replay MAC.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(UpperAReplayMac)]
    [Arguments(UpperZReplayMac)]
    [Arguments(LowerAReplayMac)]
    [Arguments(LowerZReplayMac)]
    [Arguments(DigitZeroReplayMac)]
    [Arguments(DigitNineReplayMac)]
    [Arguments(HyphenReplayMac)]
    [Arguments(UnderscoreReplayMac)]
    public async Task HandleAsyncPushWithBase64UrlAlphabetWrongMacReturnsUnauthorizedWithoutStaleMarker(string replayMac)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        await Assert.That(replayMac.Length).IsEqualTo(ReplayMacLength);
        _ = request.Headers.Remove(ReplayMacHeader);
        request.Headers.Add(ReplayMacHeader, replayMac);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies malformed replay MAC syntax is rejected before stale-session lookup classification.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithMissingReplaySessionAndMalformedMacReturnsBadRequestWithoutStaleMarker()
    {
        await using var issuer = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var session = await ConnectReplaySessionAsync(issuer);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var receiver = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        _ = request.Headers.Remove(ReplayMacHeader);
        var addedMalformedMac = request.Headers.TryAddWithoutValidation(ReplayMacHeader, MalformedReplayMac);
        await Assert.That(addedMalformedMac).IsTrue();

        using var response = await receiver.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a short replay MAC is rejected before stale-session lookup classification.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushWithMissingReplaySessionAndShortMacReturnsBadRequestWithoutStaleMarker()
    {
        await using var issuer = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var session = await ConnectReplaySessionAsync(issuer);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var receiver = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        _ = request.Headers.Remove(ReplayMacHeader);
        var addedShortMac = request.Headers.TryAddWithoutValidation(ReplayMacHeader, ShortReplayMac);
        await Assert.That(addedShortMac).IsTrue();
        using var response = await receiver.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a full-length replay MAC with invalid alphabet text is rejected before stale-session lookup.</summary>
    /// <param name="replayMac">The syntactically invalid full-length replay MAC.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(InvalidBeforeUppercaseReplayMac)]
    [Arguments(InvalidAfterUppercaseReplayMac)]
    [Arguments(InvalidBeforeLowercaseReplayMac)]
    [Arguments(InvalidAfterLowercaseReplayMac)]
    [Arguments(SlashReplayMac)]
    [Arguments(ColonReplayMac)]
    public async Task HandleAsyncPushWithMissingReplaySessionAndInvalidAlphabetMacReturnsBadRequestWithoutStaleMarker(string replayMac)
    {
        await using var issuer = new HttpServerEndpoint(CreateReplayOptions(new RecordingHub()));
        var session = await ConnectReplaySessionAsync(issuer);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var receiver = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        var body = CreateCodec().SerializePushRequest(CreateBatch());
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);
        await Assert.That(replayMac.Length).IsEqualTo(ReplayMacLength);
        _ = request.Headers.Remove(ReplayMacHeader);
        var addedInvalidAlphabetMac = request.Headers.TryAddWithoutValidation(ReplayMacHeader, replayMac);
        await Assert.That(addedInvalidAlphabetMac).IsTrue();
        using var response = await receiver.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies replay authorization denial does not advertise stale replay-session renewal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushDeniedByReplayAuthorizerReturnsForbiddenWithoutStaleMarker()
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
        await Assert.That(GetResponseHeader(response, StaleReplaySessionHeader)).IsNull();
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Replaces a valid replay MAC with a different canonical Base64URL value.</summary>
    /// <param name="request">The request to mutate.</param>
    private static void ReplaceReplayMacWithDifferentValidValue(HttpRequestMessage request)
    {
        var original = GetRequiredRequestHeader(request, ReplayMacHeader);
        var replacement = original[0] == 'A' ? 'B' + original[1..] : 'A' + original[1..];
        _ = request.Headers.Remove(ReplayMacHeader);
        request.Headers.Add(ReplayMacHeader, replacement);
    }

    /// <summary>Gets a required request header value.</summary>
    /// <param name="request">The request.</param>
    /// <param name="name">The header name.</param>
    /// <returns>The header value.</returns>
    /// <exception cref="InvalidOperationException">The request does not include the required header.</exception>
    private static string GetRequiredRequestHeader(HttpRequestMessage request, string name) =>
        request.Headers.TryGetValues(name, out var values) ? values.Single() : throw new InvalidOperationException($"Missing {name}.");
}
