// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayCoordinator"/>.</summary>
public sealed partial class HttpReplayCoordinatorTests
{
    /// <summary>The expected single authorization call count.</summary>
    private const int SingleAuthorizationCall = 1;

    /// <summary>The expected double authorization call count.</summary>
    private const int DoubleAuthorizationCall = 2;

    /// <summary>The short bounded observation delay in milliseconds.</summary>
    private const int ObservationDelayMilliseconds = 50;

    /// <summary>The bounded wait timeout in milliseconds.</summary>
    private const int WaitTimeoutMilliseconds = 1000;

    /// <summary>The large content type length for constrained cache-reserve tests.</summary>
    private const int LargeContentTypeLength = 1024;

    /// <summary>The retained byte budget that admits request state but not a large response header.</summary>
    private const int ReexecuteRetainedBytes = 1024;

    /// <summary>The constrained retained-byte budget for representation accounting.</summary>
    private const int ConstrainedRetainedBytes = 384;

    /// <summary>The shared replay message identifier.</summary>
    private const string MessageId = "11111111-1111-1111-1111-111111111111";

    /// <summary>The replay nonce.</summary>
    private const string Nonce = "nonce";

    /// <summary>The alternate replay nonce.</summary>
    private const string AlternateNonce = "nonce-b";

    /// <summary>The fresh replay nonce.</summary>
    private const string FreshNonce = "nonce-fresh";

    /// <summary>The second fresh replay nonce.</summary>
    private const string SecondFreshNonce = "nonce-fresh-2";

    /// <summary>The replay session identifier.</summary>
    private const string ReplaySessionId = "session";

    /// <summary>The replay MAC placeholder.</summary>
    private const string ReplayMac = "mac";

    /// <summary>The pending MAC marker.</summary>
    private const string PendingMac = "pending";

    /// <summary>The tenant identifier.</summary>
    private const string TenantId = "tenant";

    /// <summary>The client identifier.</summary>
    private const string ClientId = "client";

    /// <summary>The session secret.</summary>
    private const string SessionSecret = "secret-1";

    /// <summary>The HTTP GET method.</summary>
    private const string GetMethod = "GET";

    /// <summary>The HTTP POST method.</summary>
    private const string PostMethod = "POST";

    /// <summary>The canonical empty query.</summary>
    private const string EmptyQuery = "";

    /// <summary>The replay session identifier response header.</summary>
    private const string ReplaySessionIdHeader = "X-ReactiveUI-Replay-Session-Id";

    /// <summary>The replay session secret response header.</summary>
    private const string ReplaySessionSecretHeader = "X-ReactiveUI-Replay-Session-Secret";

    /// <summary>The replay session expiry response header.</summary>
    private const string ReplaySessionExpiresHeader = "X-ReactiveUI-Replay-Session-Expires";

    /// <summary>The replay freshness window.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(5);

    /// <summary>The replay timestamp.</summary>
    private static readonly DateTimeOffset SentAtUtc = new(2026, 9, 13, 12, 15, 0, TimeSpan.Zero);

    /// <summary>The empty request body hash.</summary>
    private static readonly byte[] EmptyBodyHash = [0];

    /// <summary>The small response bytes.</summary>
    private static readonly byte[] SmallResponseBytes = [1, 2, 3];

    /// <summary>The oversized response bytes for the configured cache limit.</summary>
    private static readonly byte[] OversizedResponseBytes = [1, 2];

    /// <summary>The second response bytes.</summary>
    private static readonly byte[] AlternateResponseBytes = [4, 5, 6];

    /// <summary>Verifies first admission invokes current authorization before execution ownership is returned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncInvokesCurrentAuthorizationBeforeExecution()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var calls = 0;

        var decision = await coordinator.AdmitAsync(CreateRequest(HttpReplayOperationKind.Connect), AuthorizeAsync, CancellationToken.None);

        await Assert.That(calls).IsEqualTo(SingleAuthorizationCall);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(decision.Owner).IsNotNull();

        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            calls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies authorization denial on duplicate replay suppresses an existing cached success.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReauthorizesBeforeServingCachedReplay()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var replay = await coordinator.AdmitAsync(request, static _ => new(CreateDenied()), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(replay.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.AuthorizationDenied);
    }

    /// <summary>Verifies oversized post-effect connect responses become uncached transient replay records.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncStoresUncachedTransientWhenResponseIsTooLargeAfterEffects()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumCachedResponseBytes = SingleAuthorizationCall };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = OversizedResponseBytes });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies oversized post-effect push responses can reexecute after mandatory reauthorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncAllowsPushReexecuteWhenResponseIsTooLargeAfterEffects()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumCachedResponseBytes = SingleAuthorizationCall };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateAuthenticatedRequest(coordinator, HttpReplayOperationKind.Push);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var authorizationCalls = 0;
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = OversizedResponseBytes });
        var replay = await coordinator.AdmitAsync(request, AuthorizeReplayAsync, CancellationToken.None);

        await Assert.That(authorizationCalls).IsEqualTo(SingleAuthorizationCall);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(replay.Owner).IsNotNull();

        ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            authorizationCalls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies future-skewed envelopes remain retained through their inclusive freshness expiry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRetainsFutureTimestampThroughInclusiveFreshnessExpiry()
    {
        var observedNow = SentAtUtc;
        ManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions { TimeProvider = clock, FreshnessWindow = Window, NonceRetention = Window, ReplaySessionRetention = Window + Window };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateAuthenticatedRequest(coordinator, HttpReplayOperationKind.Push, observedNow.Add(Window));
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        clock.SetUtcNow(observedNow.Add(Window).Add(Window));
        var inclusiveReplay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(inclusiveReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);

        clock.SetUtcNow(observedNow.Add(Window).Add(Window).AddTicks(SingleAuthorizationCall));
        var expiredReplay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(expiredReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(expiredReplay.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies a connect replay after the bound cache/session lifetime fails instead of issuing a new session.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsConnectReplayAfterBoundSessionCacheLifetime()
    {
        var observedNow = SentAtUtc;
        ManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions { TimeProvider = clock, FreshnessWindow = Window, NonceRetention = Window, ReplaySessionRetention = Window + Window };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect, observedNow.Add(Window));
        var session = new HttpReplayIssuedSession { SessionId = ReplaySessionId, SessionSecret = SessionSecret, ExpiresAtUtc = observedNow.Add(Window).Add(Window) };
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes, ConnectSession = session });
        clock.SetUtcNow(observedNow.Add(Window).Add(Window).AddTicks(SingleAuthorizationCall));
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.Conflict);
    }

    /// <summary>Verifies foreign owners cannot close or complete a matching local entry id.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncWithForeignOwnerDoesNotCloseOrMutateMatchingEntry()
    {
        await using HttpReplayCoordinator firstCoordinator = new(CreateOptions(SentAtUtc));
        await using HttpReplayCoordinator secondCoordinator = new(CreateOptions(SentAtUtc));
        var firstRequest = CreateRequest(HttpReplayOperationKind.Connect);
        var secondRequest = CreateRequest(HttpReplayOperationKind.Connect) with { Nonce = AlternateNonce };
        var firstAdmission = await firstCoordinator.AdmitAsync(firstRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var secondAdmission = await secondCoordinator.AdmitAsync(secondRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(firstAdmission.Owner).IsNotNull();
        await Assert.That(secondAdmission.Owner).IsNotNull();
        if (firstAdmission.Owner is null || secondAdmission.Owner is null)
        {
            return;
        }

        await secondCoordinator.CompleteAsync(firstAdmission.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = AlternateResponseBytes });

        await Assert.That(firstAdmission.Owner.IsClosed).IsFalse();

        await secondCoordinator.CompleteAsync(secondAdmission.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var secondReplay = await secondCoordinator.AdmitAsync(secondRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(secondReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await AssertByteArrayEqualsAsync(secondReplay.CachedResponse?.Body.ToArray(), SmallResponseBytes);
    }

    /// <summary>Verifies foreign owners cannot abandon a matching local entry id.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AbandonAsyncWithForeignOwnerDoesNotCloseOrMutateMatchingEntry()
    {
        await using HttpReplayCoordinator firstCoordinator = new(CreateOptions(SentAtUtc));
        await using HttpReplayCoordinator secondCoordinator = new(CreateOptions(SentAtUtc));
        var firstRequest = CreateRequest(HttpReplayOperationKind.Connect);
        var secondRequest = CreateRequest(HttpReplayOperationKind.Connect) with { Nonce = AlternateNonce };
        var firstAdmission = await firstCoordinator.AdmitAsync(firstRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var secondAdmission = await secondCoordinator.AdmitAsync(secondRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(firstAdmission.Owner).IsNotNull();
        await Assert.That(secondAdmission.Owner).IsNotNull();
        if (firstAdmission.Owner is null || secondAdmission.Owner is null)
        {
            return;
        }

        await secondCoordinator.AbandonAsync(firstAdmission.Owner, CancellationToken.None);

        await Assert.That(firstAdmission.Owner.IsClosed).IsFalse();

        await secondCoordinator.CompleteAsync(secondAdmission.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var secondReplay = await secondCoordinator.AdmitAsync(secondRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(secondReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
    }

    /// <summary>Verifies expired in-flight entries cannot be evicted to admit a second owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncPreservesExpiredInFlightEntryWithoutSecondOwner()
    {
        var observedNow = SentAtUtc;
        ManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            MaximumEntries = SingleAuthorizationCall,
            TimeProvider = clock,
            FreshnessWindow = Window,
            NonceRetention = Window,
            ReplaySessionRetention = Window + Window,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        var freshNow = observedNow.Add(Window).AddTicks(SingleAuthorizationCall);
        clock.SetUtcNow(freshNow);
        var freshRequest = CreateRequest(HttpReplayOperationKind.Connect, freshNow) with { Nonce = FreshNonce };
        var second = await coordinator.AdmitAsync(freshRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(first.Owner.IsClosed).IsFalse();
        await Assert.That(second.Kind).IsNotEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(second.Owner).IsNull();

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var anotherFreshRequest = CreateRequest(HttpReplayOperationKind.Connect, freshNow) with { Nonce = SecondFreshNonce };
        var afterCompletion = await coordinator.AdmitAsync(anotherFreshRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(afterCompletion.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(afterCompletion.Owner).IsNotNull();
    }

    /// <summary>Verifies a stale duplicate is rejected by freshness validation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsStaleDuplicateByFreshnessValidation()
    {
        var observedNow = SentAtUtc;
        ManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions { TimeProvider = clock, FreshnessWindow = Window, NonceRetention = Window, ReplaySessionRetention = Window + Window };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        clock.SetUtcNow(observedNow.Add(Window).AddTicks(SingleAuthorizationCall));
        var staleDuplicate = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(staleDuplicate.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(staleDuplicate.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies admission reobserves the high-water clock after a delayed authorization callback.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReobservesClockAfterDelayedAuthorization()
    {
        ManualTimeProvider clock = new(SentAtUtc);
        var options = new HttpReplayProtectionOptions { TimeProvider = clock, FreshnessWindow = Window, NonceRetention = Window, ReplaySessionRetention = Window + Window };
        await using HttpReplayCoordinator coordinator = new(options);
        var authorization = new TaskCompletionSource<HttpReplayAuthorizationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var admission = coordinator.AdmitAsync(CreateRequest(HttpReplayOperationKind.Connect), AuthorizeAsync, CancellationToken.None).AsTask();

        clock.SetUtcNow(SentAtUtc.Add(Window).AddTicks(SingleAuthorizationCall));
        authorization.SetResult(HttpReplayAuthorizationResult.Allowed);
        var decision = await admission;

        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(decision.Owner).IsNull();
        await Assert.That(decision.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);

        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(authorization.Task);
        }
    }

    /// <summary>Verifies duplicate callers wait for shared cached completion after reauthorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncWaitsForInFlightCachedCompletionAfterReauthorization()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        using var replayTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var replayAuthorizationCalls = 0;
        var replayAdmission = coordinator.AdmitAsync(request, AuthorizeReplayAsync, replayTimeout.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await Assert.That(replayAuthorizationCalls).IsEqualTo(SingleAuthorizationCall);
        await AssertNotCompletedWithinObservationAsync(replayAdmission);

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var replay = await AwaitWithTimeoutAsync(replayAdmission);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await AssertByteArrayEqualsAsync(replay.CachedResponse?.Body.ToArray(), SmallResponseBytes);

        ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            replayAuthorizationCalls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies abandoned push entries can reexecute after mandatory reauthorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncAllowsPushReexecuteAfterAbandonedOwner()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var request = CreateAuthenticatedRequest(coordinator, HttpReplayOperationKind.Push);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var authorizationCalls = 0;
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await first.Owner.AbandonAsync(CancellationToken.None);
        var replay = await coordinator.AdmitAsync(request, AuthorizeReplayAsync, CancellationToken.None);

        await Assert.That(authorizationCalls).IsEqualTo(SingleAuthorizationCall);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(replay.Owner).IsNotNull();

        ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            authorizationCalls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies abandoned connect entries do not reexecute after reauthorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncDoesNotReexecuteAbandonedConnectOwner()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var authorizationCalls = 0;
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await first.Owner.AbandonAsync(CancellationToken.None);
        var replay = await coordinator.AdmitAsync(request, AuthorizeReplayAsync, CancellationToken.None);

        await Assert.That(authorizationCalls).IsEqualTo(SingleAuthorizationCall);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.Owner).IsNull();

        ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            authorizationCalls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies cached connect replay carries exact proof headers and the original session remains usable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReplaysConnectProofHeadersAndKeepsOriginalSessionUsable()
    {
        var observedNow = SentAtUtc;
        await using HttpReplayCoordinator coordinator = new(CreateOptions(observedNow));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var session = new HttpReplayIssuedSession { SessionId = ReplaySessionId, SessionSecret = SessionSecret, ExpiresAtUtc = observedNow.Add(Window + Window) };
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes, ConnectSession = session });
        var cachedConnect = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var push = CreateAuthenticatedRequestWithSession(HttpReplayOperationKind.Push, session);
        var pushAdmission = await coordinator.AdmitAsync(push, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(cachedConnect.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await Assert.That(GetHeader(cachedConnect.CachedResponse, ReplaySessionIdHeader)).IsEqualTo(session.SessionId);
        await Assert.That(GetHeader(cachedConnect.CachedResponse, ReplaySessionSecretHeader)).IsEqualTo(session.SessionSecret);
        await Assert.That(GetHeader(cachedConnect.CachedResponse, ReplaySessionExpiresHeader)).IsEqualTo(session.ExpiresAtUtc.ToString("O", CultureInfo.InvariantCulture));
        await Assert.That(pushAdmission.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
    }

    /// <summary>Verifies connect cache publication charges proof headers before publishing cached replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncChargesFullConnectProofRepresentationBeforeCaching()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumRetainedBytes = ConstrainedRetainedBytes };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var session = new HttpReplayIssuedSession { SessionId = ReplaySessionId, SessionSecret = SessionSecret, ExpiresAtUtc = SentAtUtc.Add(Window + Window) };
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes, ConnectSession = session });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.CachedResponse).IsNull();
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies in-flight connect waiters cannot observe cached headers when session registration fails.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncDoesNotPublishConnectCacheToWaiterBeforeSessionRegistration()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumReplaySessions = SingleAuthorizationCall };
        await using HttpReplayCoordinator coordinator = new(options);
        using var replayTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        _ = coordinator.Sessions.Issue(new(TenantId, ClientId), SentAtUtc);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var session = new HttpReplayIssuedSession { SessionId = ReplaySessionId, SessionSecret = SessionSecret, ExpiresAtUtc = SentAtUtc.Add(Window + Window) };
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var replayAdmission = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), replayTimeout.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertNotCompletedWithinObservationAsync(replayAdmission);

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes, ConnectSession = session });
        var replay = await AwaitWithTimeoutAsync(replayAdmission);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.CachedResponse).IsNull();
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies reauthorization runs for every duplicate admission path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReauthorizesInitialWaiterAndReexecutePaths()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var request = CreateAuthenticatedRequest(coordinator, HttpReplayOperationKind.Acknowledge);
        var calls = 0;
        var first = await coordinator.AdmitAsync(request, AuthorizeAsync, CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await first.Owner.AbandonAsync(CancellationToken.None);
        var replay = await coordinator.AdmitAsync(request, AuthorizeAsync, CancellationToken.None);

        await Assert.That(calls).IsEqualTo(DoubleAuthorizationCall);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);

        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            calls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies cache reserve failure leaves acknowledge replay available for safe reexecution.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncAllowsAcknowledgeReexecuteWhenResponseCacheReserveFails()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumRetainedBytes = ReexecuteRetainedBytes };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateAuthenticatedRequest(coordinator, HttpReplayOperationKind.Acknowledge);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var authorizationCalls = 0;
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(
            first.Owner,
            new() { StatusCode = HttpStatusCode.OK, ContentType = new('a', LargeContentTypeLength), ResponseBytes = SmallResponseBytes });
        var replay = await coordinator.AdmitAsync(request, AuthorizeReplayAsync, CancellationToken.None);

        await Assert.That(authorizationCalls).IsEqualTo(SingleAuthorizationCall);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(replay.Owner).IsNotNull();

        ValueTask<HttpReplayAuthorizationResult> AuthorizeReplayAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            authorizationCalls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies duplicate waiter cancellation releases bounded waiter capacity for a later replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncCancelledDuplicateWaiterReleasesWaiterCapacity()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumActiveReplayWaiters = SingleAuthorizationCall };
        await using HttpReplayCoordinator coordinator = new(options);
        using var firstReplayTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        using var secondReplayTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var firstReplay = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), firstReplayTimeout.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertNotCompletedWithinObservationAsync(firstReplay);

        var saturated = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(saturated.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(saturated.Failure?.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);

        await firstReplayTimeout.CancelAsync();
        await Assert.That(async () => await firstReplay).Throws<OperationCanceledException>();

        var secondReplay = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), secondReplayTimeout.Token).AsTask();
        await AssertNotCompletedWithinObservationAsync(secondReplay);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var replay = await AwaitWithTimeoutAsync(secondReplay);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await AssertByteArrayEqualsAsync(replay.CachedResponse?.Body.ToArray(), SmallResponseBytes);
    }

    /// <summary>Verifies pre-effect oversized responses are retained as permanent 413 replay rejections.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncRejectsOversizedResponseWhenFailureHappenedBeforeEffect()
    {
        var options = CreateOptions(SentAtUtc) with { MaximumCachedResponseBytes = SingleAuthorizationCall };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = OversizedResponseBytes, FailedBeforeEffect = true });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(replay.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
    }

    /// <summary>Verifies a fresh duplicate with different canonical bytes is rejected as an ambiguous replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsFreshDuplicateWithDifferentEnvelopeFingerprint()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes });
        var ambiguousRequest = request with { CanonicalRequest = CreateCanonicalRequest(AlternateResponseBytes) };
        var replay = await coordinator.AdmitAsync(ambiguousRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(replay.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.Conflict);
    }

    /// <summary>Verifies disposal drains duplicate waiters before a late connect completion returns.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncDrainsInFlightWaitersBeforeLateConnectCompletion()
    {
        await using HttpReplayCoordinator coordinator = new(CreateOptions(SentAtUtc));
        using var replayTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        var request = CreateRequest(HttpReplayOperationKind.Connect);
        var session = new HttpReplayIssuedSession { SessionId = ReplaySessionId, SessionSecret = SessionSecret, ExpiresAtUtc = SentAtUtc.Add(Window + Window) };
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var replayAdmission = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), replayTimeout.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertNotCompletedWithinObservationAsync(replayAdmission);
        await coordinator.DisposeAsync();
        var replay = await AwaitWithTimeoutAsync(replayAdmission);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = SmallResponseBytes, ConnectSession = session });

        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Asserts a task does not complete during the short observation window.</summary>
    /// <param name="task">The observed task.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertNotCompletedWithinObservationAsync(Task task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(ObservationDelayMilliseconds));
        var completed = await Task.WhenAny(task, delay);
        await Assert.That(completed).IsSameReferenceAs(delay);
    }

    /// <summary>Awaits a replay admission with a bounded timeout.</summary>
    /// <param name="task">The replay admission task.</param>
    /// <returns>The replay decision.</returns>
    private static async Task<HttpReplayDecision> AwaitWithTimeoutAsync(Task<HttpReplayDecision> task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(WaitTimeoutMilliseconds));
        var completed = await Task.WhenAny(task, delay);
        await Assert.That(completed).IsSameReferenceAs(task);
        return await task;
    }

    /// <summary>Asserts byte arrays have identical length and values.</summary>
    /// <param name="actual">The actual bytes.</param>
    /// <param name="expected">The expected bytes.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertByteArrayEqualsAsync(byte[]? actual, byte[] expected)
    {
        await Assert.That(actual).IsNotNull();
        if (actual is null)
        {
            return;
        }

        await Assert.That(actual.Length).IsEqualTo(expected.Length);
        for (var index = 0; index < actual.Length; index++)
        {
            await Assert.That(actual[index]).IsEqualTo(expected[index]);
        }
    }

    /// <summary>Gets a cached response header by ordinal name.</summary>
    /// <param name="response">The cached response.</param>
    /// <param name="headerName">The header name.</param>
    /// <returns>The header value, when present.</returns>
    private static string? GetHeader(HttpReplayCachedResponse? response, string headerName)
    {
        if (response is null)
        {
            return null;
        }

        var headers = response.Headers;
        for (var index = 0; index < headers.Count; index++)
        {
            if (string.Equals(headers[index].Key, headerName, StringComparison.Ordinal))
            {
                return headers[index].Value;
            }
        }

        return null;
    }

    /// <summary>Creates a replay request.</summary>
    /// <param name="operation">The operation kind.</param>
    /// <param name="sentAtUtc">The optional replay timestamp.</param>
    /// <returns>The replay request.</returns>
    private static HttpReplayRequest CreateRequest(HttpReplayOperationKind operation, DateTimeOffset? sentAtUtc = null)
    {
        var method = operation == HttpReplayOperationKind.Subscribe ? GetMethod : PostMethod;
        var canonical = CreateCanonicalRequest(SmallResponseBytes, method, operation);
        return new()
        {
            Operation = operation,
            Principal = new(TenantId, ClientId),
            MessageId = MessageId,
            Nonce = Nonce,
            SentAtUtc = sentAtUtc ?? SentAtUtc,
            ReplaySessionId = operation == HttpReplayOperationKind.Connect ? null : ReplaySessionId,
            ReplayMac = operation == HttpReplayOperationKind.Connect ? null : ReplayMac,
            CanonicalRequest = canonical,
        };
    }

    /// <summary>Creates a canonical request with controlled body bytes.</summary>
    /// <param name="body">The canonical body bytes.</param>
    /// <param name="method">The HTTP method.</param>
    /// <param name="operation">The replay operation.</param>
    /// <returns>The canonical request.</returns>
    private static HttpCanonicalRequest CreateCanonicalRequest(
        ReadOnlyMemory<byte> body,
        string method = PostMethod,
        HttpReplayOperationKind operation = HttpReplayOperationKind.Connect) =>
        new(method, operation.ToString(), EmptyQuery, EmptyBodyHash, body);

    /// <summary>Creates a replay request with a valid issued session and MAC when required.</summary>
    /// <param name="coordinator">The replay coordinator.</param>
    /// <param name="operation">The operation kind.</param>
    /// <param name="sentAtUtc">The optional replay timestamp.</param>
    /// <returns>The replay request.</returns>
    /// <exception cref="HttpRemoteTransportException">The initial implementation has not yet added session issuance.</exception>
    private static HttpReplayRequest CreateAuthenticatedRequest(HttpReplayCoordinator coordinator, HttpReplayOperationKind operation, DateTimeOffset? sentAtUtc = null)
    {
        var request = CreateRequest(operation, sentAtUtc);
        if (operation == HttpReplayOperationKind.Connect)
        {
            return request;
        }

        var issued = coordinator.Sessions.Issue(request.Principal, sentAtUtc ?? SentAtUtc);
        var sessionRequest = request with { ReplaySessionId = issued.SessionId, ReplayMac = PendingMac };
        HttpReplayEnvelopeHasher hasher = new();
        var envelope = hasher.Create(sessionRequest);
        var mac = hasher.ComputeMac(Encoding.UTF8.GetBytes(issued.SessionSecret), envelope.MacInput);
        return sessionRequest with { ReplayMac = mac };
    }

    /// <summary>Creates a replay request signed by a supplied replay session.</summary>
    /// <param name="operation">The operation kind.</param>
    /// <param name="session">The replay session.</param>
    /// <returns>The replay request.</returns>
    private static HttpReplayRequest CreateAuthenticatedRequestWithSession(HttpReplayOperationKind operation, HttpReplayIssuedSession session)
    {
        var request = CreateRequest(operation);
        var sessionRequest = request with { ReplaySessionId = session.SessionId, ReplayMac = PendingMac };
        HttpReplayEnvelopeHasher hasher = new();
        var envelope = hasher.Create(sessionRequest);
        var mac = hasher.ComputeMac(Encoding.UTF8.GetBytes(session.SessionSecret), envelope.MacInput);
        return sessionRequest with { ReplayMac = mac };
    }

    /// <summary>Creates a denied authorization result.</summary>
    /// <returns>The denied authorization result.</returns>
    private static HttpReplayAuthorizationResult CreateDenied() => new() { IsAuthorized = false, Failure = new(HttpStatusCode.Forbidden, HttpTransportFailureKind.AuthorizationDenied) };

    /// <summary>Creates replay options with a deterministic clock.</summary>
    /// <param name="utcNow">The current UTC instant.</param>
    /// <returns>The replay options.</returns>
    private static HttpReplayProtectionOptions CreateOptions(DateTimeOffset utcNow) => new() { TimeProvider = new ManualTimeProvider(utcNow) };

    /// <summary>A manually advanced replay clock for freshness tests.</summary>
    /// <param name="utcNow">The initial UTC instant.</param>
    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC instant.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Sets the current UTC instant.</summary>
        /// <param name="utcNow">The new UTC instant.</param>
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }
}
