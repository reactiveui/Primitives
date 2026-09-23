// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Net;
using System.Text;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests replay coordinator lifecycle edge behavior.</summary>
public sealed partial class HttpReplayCoordinatorTests
{
    /// <summary>The short bounded observation delay in milliseconds.</summary>
    private const int LifecycleObservationDelayMilliseconds = 50;

    /// <summary>The bounded wait timeout in milliseconds.</summary>
    private const int LifecycleWaitTimeoutMilliseconds = 1000;

    /// <summary>The expected double authorization call count.</summary>
    private const int LifecycleDoubleAuthorizationCall = 2;

    /// <summary>The single byte limit.</summary>
    private const int LifecycleSingleByteLimit = 1;

    /// <summary>The unknown replay owner entry identifier.</summary>
    private const long LifecycleUnknownOwnerEntryId = long.MaxValue;

    /// <summary>The shared replay message identifier.</summary>
    private const string LifecycleMessageId = "22222222-2222-2222-2222-222222222222";

    /// <summary>The replay nonce.</summary>
    private const string LifecycleNonce = "lifecycle-nonce";

    /// <summary>The alternate replay nonce.</summary>
    private const string LifecycleAlternateNonce = "lifecycle-nonce-b";

    /// <summary>The third replay nonce.</summary>
    private const string LifecycleThirdNonce = "lifecycle-nonce-c";

    /// <summary>The replay session identifier.</summary>
    private const string LifecycleReplaySessionId = "lifecycle-session";

    /// <summary>The pending MAC marker.</summary>
    private const string LifecyclePendingMac = "pending";

    /// <summary>The tenant identifier.</summary>
    private const string LifecycleTenantId = "tenant";

    /// <summary>The client identifier.</summary>
    private const string LifecycleClientId = "client";

    /// <summary>The session secret.</summary>
    private const string LifecycleSessionSecret = "secret-1";

    /// <summary>The HTTP POST method.</summary>
    private const string LifecyclePostMethod = "POST";

    /// <summary>The canonical empty query.</summary>
    private const string LifecycleEmptyQuery = "";

    /// <summary>The replay freshness window.</summary>
    private static readonly TimeSpan LifecycleWindow = TimeSpan.FromMinutes(5);

    /// <summary>The replay timestamp.</summary>
    private static readonly DateTimeOffset LifecycleSentAtUtc = new(2026, 9, 13, 12, 45, 0, TimeSpan.Zero);

    /// <summary>The empty request body hash.</summary>
    private static readonly byte[] LifecycleEmptyBodyHash = [0];

    /// <summary>The small response bytes.</summary>
    private static readonly byte[] LifecycleSmallResponseBytes = [1, 2, 3];

    /// <summary>Verifies canceling a duplicate waiter does not close the active owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncCancelsDuplicateWaiterBeforeOwnerCompletes()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        using var replayCancellation = new CancellationTokenSource();
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var duplicate = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), replayCancellation.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertLifecycleNotCompletedWithinObservationAsync(duplicate);
        await replayCancellation.CancelAsync();
        await Assert.That(async () => await duplicate).Throws<OperationCanceledException>();
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
    }

    /// <summary>Verifies a duplicate waiter completed before cancellation is not later removed from the cache entry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncIgnoresLateDuplicateWaiterCancellationAfterCompletion()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        using var replayCancellation = new CancellationTokenSource();
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var duplicate = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), replayCancellation.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertLifecycleNotCompletedWithinObservationAsync(duplicate);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var duplicateReplay = await AwaitLifecycleWithTimeoutAsync(duplicate);
        await replayCancellation.CancelAsync();
        var laterReplay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(duplicateReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await Assert.That(laterReplay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
    }

    /// <summary>Verifies disposed coordinators reject admission without invoking current authorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncAfterDisposeReturnsTransientWithoutAuthorization()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var calls = 0;
        await coordinator.DisposeAsync();
        var replay = await coordinator.AdmitAsync(CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce), AuthorizeAsync, CancellationToken.None);
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        return;
        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken _)
        {
            calls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Verifies authorization denial without a supplied failure returns the default forbidden denial.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncAuthorizationDeniedWithoutFailureUsesForbiddenDefault()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var decision = await coordinator.AdmitAsync(
            CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce),
            static _ => new(new HttpReplayAuthorizationResult { IsAuthorized = false }),
            CancellationToken.None);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(decision.Failure?.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(decision.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.AuthorizationDenied);
    }

    /// <summary>Verifies disposal after authorization still returns transient before replay state changes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncAfterAuthorizationObservesConcurrentDispose()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        TaskCompletionSource authorizationEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<HttpReplayAuthorizationResult> authorizationRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var admission = coordinator
            .AdmitAsync(CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce), AuthorizeAsync, CancellationToken.None)
            .AsTask();
        await AwaitLifecycleSignalWithTimeoutAsync(authorizationEntered.Task);
        await coordinator.DisposeAsync();
        authorizationRelease.SetResult(HttpReplayAuthorizationResult.Allowed);
        var replay = await AwaitLifecycleWithTimeoutAsync(admission);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        return;
        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken _)
        {
            authorizationEntered.SetResult();
            return new(authorizationRelease.Task);
        }
    }

    /// <summary>Verifies stale duplicates are rejected while retained by a longer nonce window.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsStaleDuplicateBeforeRetentionExpiry()
    {
        var observedNow = LifecycleSentAtUtc;
        LifecycleManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            TimeProvider = clock,
            FreshnessWindow = LifecycleWindow,
            NonceRetention = LifecycleWindow + LifecycleWindow,
            ReplaySessionRetention = LifecycleWindow + LifecycleWindow,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, observedNow, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        clock.SetUtcNow(observedNow.Add(LifecycleWindow).AddTicks(LifecycleSingleByteLimit));
        var stale = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(stale.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(stale.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies stale expired in-flight duplicates are rejected instead of joined.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsStaleExpiredInFlightDuplicate()
    {
        var observedNow = LifecycleSentAtUtc;
        LifecycleManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            TimeProvider = clock,
            FreshnessWindow = LifecycleWindow,
            NonceRetention = LifecycleWindow,
            ReplaySessionRetention = LifecycleWindow + LifecycleWindow,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, observedNow, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        clock.SetUtcNow(observedNow.Add(LifecycleWindow).AddTicks(LifecycleSingleByteLimit));
        var stale = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(stale.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(stale.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies a fresh duplicate request is throttled while the expired owner is still in flight.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReturnsTooManyRequestsWhenFreshDuplicateFindsExpiredInFlightEntry()
    {
        var observedNow = LifecycleSentAtUtc;
        LifecycleManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            TimeProvider = clock,
            FreshnessWindow = LifecycleWindow,
            NonceRetention = LifecycleWindow,
            ReplaySessionRetention = LifecycleWindow + LifecycleWindow,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var staleRequest = CreateLifecycleRequest(HttpReplayOperationKind.Connect, observedNow, LifecycleNonce);
        var first = await coordinator.AdmitAsync(staleRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        clock.SetUtcNow(observedNow.Add(LifecycleWindow).AddTicks(LifecycleSingleByteLimit));
        var freshRequest = CreateLifecycleRequest(HttpReplayOperationKind.Connect, clock.GetUtcNow(), LifecycleNonce);
        var fresh = await coordinator.AdmitAsync(freshRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(fresh.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(fresh.Failure?.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);
        await Assert.That(fresh.Owner).IsNull();
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        await Assert.That(first.Owner.IsClosed).IsTrue();
    }

    /// <summary>Verifies duplicate waiters without cancellation observe owner completion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncCompletesDuplicateWaiterWithoutCancellationRegistration()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var duplicate = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertLifecycleNotCompletedWithinObservationAsync(duplicate);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var replay = await AwaitLifecycleWithTimeoutAsync(duplicate);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
    }

    /// <summary>Verifies a late duplicate waiter cancellation does not override completed replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncLateDuplicateWaiterCancellationKeepsCompletedReplay()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        using var replayCancellation = new CancellationTokenSource();
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var duplicate = coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), replayCancellation.Token).AsTask();
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await AssertLifecycleNotCompletedWithinObservationAsync(duplicate);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        await replayCancellation.CancelAsync();
        var replay = await AwaitLifecycleWithTimeoutAsync(duplicate);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
    }

    /// <summary>Verifies retained-byte admission failure returns a bounded transient result.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReturnsTooManyRequestsWhenEntryReservationFails()
    {
        var options = new HttpReplayProtectionOptions { TimeProvider = new LifecycleManualTimeProvider(LifecycleSentAtUtc), MaximumRetainedBytes = LifecycleSingleByteLimit };
        await using HttpReplayCoordinator coordinator = new(options);
        var decision = await coordinator.AdmitAsync(
            CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce),
            static _ => new(HttpReplayAuthorizationResult.Allowed),
            CancellationToken.None);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(decision.Failure?.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);
        await Assert.That(decision.Owner).IsNull();
    }

    /// <summary>Verifies disposed session registry failures become transient admission decisions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncReturnsTransientWhenSessionRegistryIsDisposed()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var request = CreateLifecycleAuthenticatedRequest(coordinator, HttpReplayOperationKind.Push, LifecycleSentAtUtc, LifecycleNonce);
        await coordinator.Sessions.DisposeAsync();
        var decision = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayTransient);
        await Assert.That(decision.Failure?.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies completion after coordinator disposal returns without mutating disposed state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncAfterCoordinatorDisposeReturnsWithoutPublishing()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.DisposeAsync();
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        await Assert.That(first.Owner.IsClosed).IsTrue();
    }

    /// <summary>Verifies completion after entry expiry closes only the stale owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncAfterEntryExpiryReturnsWithoutPublishing()
    {
        var observedNow = LifecycleSentAtUtc;
        LifecycleManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            TimeProvider = clock,
            FreshnessWindow = LifecycleWindow,
            NonceRetention = LifecycleWindow,
            ReplaySessionRetention = LifecycleWindow + LifecycleWindow,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var expiredRequest = CreateLifecycleRequest(HttpReplayOperationKind.Connect, observedNow, LifecycleNonce);
        var first = await coordinator.AdmitAsync(expiredRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        clock.SetUtcNow(observedNow.Add(LifecycleWindow).AddTicks(LifecycleSingleByteLimit));
        var prunerRequest = CreateLifecycleRequest(HttpReplayOperationKind.Connect, clock.GetUtcNow(), LifecycleAlternateNonce);
        var pruner = await coordinator.AdmitAsync(prunerRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var freshRequest = CreateLifecycleRequest(HttpReplayOperationKind.Connect, clock.GetUtcNow(), LifecycleThirdNonce);
        var fresh = await coordinator.AdmitAsync(freshRequest, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner.IsClosed).IsTrue();
        await Assert.That(fresh.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await DisposeOwnerIfPresentAsync(pruner.Owner);
        await DisposeOwnerIfPresentAsync(fresh.Owner);
    }

    /// <summary>Verifies same-coordinator completion with an unknown entry id closes only that owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncWithUnknownOwnerEntryReturnsWithoutPublishing()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        HttpReplayOwner owner = new(coordinator, LifecycleUnknownOwnerEntryId);
        await coordinator.CompleteAsync(owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        await Assert.That(owner.IsClosed).IsTrue();
    }

    /// <summary>Verifies abandon after coordinator disposal returns without publishing replay state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AbandonAfterCoordinatorDisposeReturnsWithoutPublishing()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.DisposeAsync();
        coordinator.Abandon(first.Owner);
        await Assert.That(first.Owner.IsClosed).IsTrue();
    }

    /// <summary>Verifies same-coordinator abandon with an unknown entry id closes only that owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AbandonWithUnknownOwnerEntryReturnsWithoutPublishing()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        HttpReplayOwner owner = new(coordinator, LifecycleUnknownOwnerEntryId);
        coordinator.Abandon(owner);
        await Assert.That(owner.IsClosed).IsTrue();
    }

    /// <summary>Verifies connect completion extends retained entry lifetime to the issued session.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompleteAsyncExtendsConnectEntryExpiryToIssuedSession()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var session = new HttpReplayIssuedSession
        {
            SessionId = LifecycleReplaySessionId,
            SessionSecret = LifecycleSessionSecret,
            ExpiresAtUtc = LifecycleSentAtUtc.Add(LifecycleWindow + LifecycleWindow + LifecycleWindow),
        };
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, LifecycleSentAtUtc, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes, ConnectSession = session });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.ReplayCached);
        await Assert.That(GetHeader(replay.CachedResponse, ReplaySessionExpiresHeader)).IsEqualTo(session.ExpiresAtUtc.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Verifies cached response snapshots survive eviction after retained buffers are retired.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExpiredCachedConnectRetiresRetainedBuffersWithoutMutatingSnapshot()
    {
        var observedNow = LifecycleSentAtUtc;
        LifecycleManualTimeProvider clock = new(observedNow);
        var options = new HttpReplayProtectionOptions
        {
            TimeProvider = clock,
            FreshnessWindow = LifecycleWindow,
            NonceRetention = LifecycleWindow,
            ReplaySessionRetention = LifecycleWindow + LifecycleWindow,
        };
        await using HttpReplayCoordinator coordinator = new(options);
        var request = CreateLifecycleRequest(HttpReplayOperationKind.Connect, observedNow, LifecycleNonce);
        var first = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(first.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var cached = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        var snapshot = cached.CachedResponse;
        await Assert.That(snapshot).IsNotNull();
        if (snapshot is null)
        {
            return;
        }

        clock.SetUtcNow(observedNow.Add(LifecycleWindow).AddTicks(LifecycleSingleByteLimit));
        var fresh = CreateLifecycleRequest(HttpReplayOperationKind.Connect, clock.GetUtcNow(), LifecycleAlternateNonce);
        _ = await coordinator.AdmitAsync(fresh, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(snapshot.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await AssertByteArrayEqualsAsync(snapshot.Body.ToArray(), LifecycleSmallResponseBytes);
    }

    /// <summary>Verifies signed requests are accepted at session expiry and rejected one tick after.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncHonorsInclusiveSessionExpiryBoundary()
    {
        var expiry = LifecycleSentAtUtc.Add(LifecycleWindow);
        LifecycleManualTimeProvider clock = new(expiry);
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(clock));
        var session = new HttpReplayIssuedSession { SessionId = LifecycleReplaySessionId, SessionSecret = LifecycleSessionSecret, ExpiresAtUtc = expiry };
        coordinator.Sessions.RegisterIssued(new(LifecycleTenantId, LifecycleClientId), session, LifecycleSentAtUtc);
        var boundary = CreateLifecycleAuthenticatedRequestWithSession(HttpReplayOperationKind.Push, session, expiry, LifecycleNonce);
        var boundaryDecision = await coordinator.AdmitAsync(boundary, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(boundaryDecision.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(boundaryDecision.Owner).IsNotNull();
        if (boundaryDecision.Owner is not null)
        {
            await boundaryDecision.Owner.DisposeAsync();
        }

        clock.SetUtcNow(expiry.AddTicks(1));
        var expired = CreateLifecycleAuthenticatedRequestWithSession(HttpReplayOperationKind.Push, session, expiry, LifecycleAlternateNonce);
        var expiredDecision = await coordinator.AdmitAsync(expired, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(expiredDecision.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(expiredDecision.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.StaleReplaySession);
        await Assert.That(expiredDecision.Failure?.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
    }

    /// <summary>Verifies high-water time prevents session reuse after the clock rolls backward.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncRejectsSignedRequestWhenClockRollsBackAfterSessionExpiry()
    {
        var expiry = LifecycleSentAtUtc.Add(LifecycleWindow);
        LifecycleManualTimeProvider clock = new(expiry.AddTicks(1));
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(clock));
        var session = new HttpReplayIssuedSession { SessionId = LifecycleReplaySessionId, SessionSecret = LifecycleSessionSecret, ExpiresAtUtc = expiry };
        coordinator.Sessions.RegisterIssued(new(LifecycleTenantId, LifecycleClientId), session, LifecycleSentAtUtc);
        var highWater = await coordinator.AdmitAsync(
            CreateLifecycleRequest(HttpReplayOperationKind.Connect, expiry.AddTicks(1), LifecycleAlternateNonce),
            static _ => new(HttpReplayAuthorizationResult.Allowed),
            CancellationToken.None);
        await Assert.That(highWater.Owner).IsNotNull();
        if (highWater.Owner is not null)
        {
            await highWater.Owner.DisposeAsync();
        }

        clock.SetUtcNow(LifecycleSentAtUtc);
        var request = CreateLifecycleAuthenticatedRequestWithSession(HttpReplayOperationKind.Push, session, LifecycleSentAtUtc, LifecycleNonce);
        var decision = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(decision.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.StaleReplaySession);
    }

    /// <summary>Verifies a signed request admitted before expiry is retained only until the verified session expiry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdmitAsyncCapsSignedRequestEntryAtVerifiedSessionExpiry()
    {
        var expiry = LifecycleSentAtUtc.AddTicks(LifecycleSingleByteLimit);
        LifecycleAdvancingTimeProvider clock = new(LifecycleSentAtUtc, expiry.AddTicks(LifecycleSingleByteLimit));
        var options = new HttpReplayProtectionOptions { TimeProvider = clock };
        await using HttpReplayCoordinator coordinator = new(options);
        var session = new HttpReplayIssuedSession { SessionId = LifecycleReplaySessionId, SessionSecret = LifecycleSessionSecret, ExpiresAtUtc = expiry };
        coordinator.Sessions.RegisterIssued(new(LifecycleTenantId, LifecycleClientId), session, LifecycleSentAtUtc);
        var request = CreateLifecycleAuthenticatedRequestWithSession(HttpReplayOperationKind.Push, session, LifecycleSentAtUtc, LifecycleNonce);
        var decision = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(decision.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        await Assert.That(decision.Owner).IsNotNull();
        if (decision.Owner is null)
        {
            return;
        }

        await coordinator.CompleteAsync(decision.Owner, new() { StatusCode = HttpStatusCode.OK, ResponseBytes = LifecycleSmallResponseBytes });
        var replay = await coordinator.AdmitAsync(request, static _ => new(HttpReplayAuthorizationResult.Allowed), CancellationToken.None);
        await Assert.That(replay.Kind).IsEqualTo(HttpReplayAdmissionKind.Reject);
        await Assert.That(replay.Failure?.Kind).IsEqualTo(HttpTransportFailureKind.StaleReplaySession);
        await Assert.That(replay.Failure?.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
    }

    /// <summary>Verifies repeated owner disposal performs one safe abandon and permits one reexecution.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncOnOwnerAbandonsOnlyOnce()
    {
        await using HttpReplayCoordinator coordinator = new(CreateLifecycleOptions(LifecycleSentAtUtc));
        var request = CreateLifecycleAuthenticatedRequest(coordinator, HttpReplayOperationKind.Push, LifecycleSentAtUtc, LifecycleNonce);
        var calls = 0;
        var first = await coordinator.AdmitAsync(request, AuthorizeAsync, CancellationToken.None);
        await Assert.That(first.Owner).IsNotNull();
        if (first.Owner is null)
        {
            return;
        }

        await first.Owner.DisposeAsync();
        await first.Owner.DisposeAsync();
        var reexecute = await coordinator.AdmitAsync(request, AuthorizeAsync, CancellationToken.None);
        await Assert.That(reexecute.Owner).IsNotNull();
        if (reexecute.Owner is not null)
        {
            await reexecute.Owner.DisposeAsync();
        }

        await Assert.That(calls).IsEqualTo(LifecycleDoubleAuthorizationCall);
        await Assert.That(reexecute.Kind).IsEqualTo(HttpReplayAdmissionKind.Execute);
        return;
        ValueTask<HttpReplayAuthorizationResult> AuthorizeAsync(CancellationToken _)
        {
            calls++;
            return new(HttpReplayAuthorizationResult.Allowed);
        }
    }

    /// <summary>Asserts a task does not complete during the short observation window.</summary>
    /// <param name="task">The observed task.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertLifecycleNotCompletedWithinObservationAsync(Task task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(LifecycleObservationDelayMilliseconds));
        var completed = await Task.WhenAny(task, delay);
        await Assert.That(completed).IsSameReferenceAs(delay);
    }

    /// <summary>Awaits a replay admission with a bounded timeout.</summary>
    /// <param name="task">The replay admission task.</param>
    /// <returns>The replay decision.</returns>
    private static async Task<HttpReplayDecision> AwaitLifecycleWithTimeoutAsync(Task<HttpReplayDecision> task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(LifecycleWaitTimeoutMilliseconds));
        var completed = await Task.WhenAny(task, delay);
        await Assert.That(completed).IsSameReferenceAs(task);
        return await task;
    }

    /// <summary>Awaits a signal with a bounded timeout.</summary>
    /// <param name="task">The signal task.</param>
    /// <returns>The asynchronous wait operation.</returns>
    private static async Task AwaitLifecycleSignalWithTimeoutAsync(Task task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(LifecycleWaitTimeoutMilliseconds));
        var completed = await Task.WhenAny(task, delay);
        await Assert.That(completed).IsSameReferenceAs(task);
        await task;
    }

    /// <summary>Creates a replay request.</summary>
    /// <param name="operation">The operation kind.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="nonce">The replay nonce.</param>
    /// <returns>The replay request.</returns>
    private static HttpReplayRequest CreateLifecycleRequest(HttpReplayOperationKind operation, DateTimeOffset sentAtUtc, string nonce)
    {
        var canonical = new HttpCanonicalRequest(LifecyclePostMethod, operation.ToString(), LifecycleEmptyQuery, LifecycleEmptyBodyHash, LifecycleSmallResponseBytes);
        return new()
        {
            Operation = operation,
            Principal = new(LifecycleTenantId, LifecycleClientId),
            MessageId = LifecycleMessageId,
            Nonce = nonce,
            SentAtUtc = sentAtUtc,
            ReplaySessionId = operation == HttpReplayOperationKind.Connect ? null : LifecycleReplaySessionId,
            ReplayMac = operation == HttpReplayOperationKind.Connect ? null : LifecyclePendingMac,
            CanonicalRequest = canonical,
        };
    }

    /// <summary>Creates a replay request with a valid issued session and MAC when required.</summary>
    /// <param name="coordinator">The replay coordinator.</param>
    /// <param name="operation">The operation kind.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="nonce">The replay nonce.</param>
    /// <returns>The replay request.</returns>
    private static HttpReplayRequest CreateLifecycleAuthenticatedRequest(
        HttpReplayCoordinator coordinator,
        HttpReplayOperationKind operation,
        DateTimeOffset sentAtUtc,
        string nonce)
    {
        var request = CreateLifecycleRequest(operation, sentAtUtc, nonce);
        var issued = coordinator.Sessions.Issue(request.Principal, sentAtUtc);
        return CreateLifecycleSignedRequest(request, issued);
    }

    /// <summary>Creates a replay request signed by a supplied replay session.</summary>
    /// <param name="operation">The replay operation.</param>
    /// <param name="session">The replay session.</param>
    /// <param name="sentAtUtc">The replay timestamp.</param>
    /// <param name="nonce">The replay nonce.</param>
    /// <returns>The signed request.</returns>
    private static HttpReplayRequest CreateLifecycleAuthenticatedRequestWithSession(
        HttpReplayOperationKind operation,
        HttpReplayIssuedSession session,
        DateTimeOffset sentAtUtc,
        string nonce)
    {
        var request = CreateLifecycleRequest(operation, sentAtUtc, nonce);
        return CreateLifecycleSignedRequest(request, session);
    }

    /// <summary>Signs a replay request with the supplied session.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="session">The replay session.</param>
    /// <returns>The signed replay request.</returns>
    private static HttpReplayRequest CreateLifecycleSignedRequest(HttpReplayRequest request, HttpReplayIssuedSession session)
    {
        var sessionRequest = request with { ReplaySessionId = session.SessionId, ReplayMac = LifecyclePendingMac };
        HttpReplayEnvelopeHasher hasher = new();
        var envelope = hasher.Create(sessionRequest);
        var mac = hasher.ComputeMac(Encoding.UTF8.GetBytes(session.SessionSecret), envelope.MacInput);
        return sessionRequest with { ReplayMac = mac };
    }

    /// <summary>Creates replay options with a deterministic clock.</summary>
    /// <param name="utcNow">The current UTC instant.</param>
    /// <returns>The replay options.</returns>
    private static HttpReplayProtectionOptions CreateLifecycleOptions(DateTimeOffset utcNow) => new() { TimeProvider = new LifecycleManualTimeProvider(utcNow) };

    /// <summary>Creates replay options with a deterministic clock.</summary>
    /// <param name="clock">The manual clock.</param>
    /// <returns>The replay options.</returns>
    private static HttpReplayProtectionOptions CreateLifecycleOptions(LifecycleManualTimeProvider clock) => new() { TimeProvider = clock };

    /// <summary>Disposes an optional replay owner.</summary>
    /// <param name="owner">The optional replay owner.</param>
    /// <returns>The asynchronous dispose operation.</returns>
    private static async ValueTask DisposeOwnerIfPresentAsync(HttpReplayOwner? owner)
    {
        if (owner is null)
        {
            return;
        }

        await owner.DisposeAsync();
    }

    /// <summary>A manually advanced replay clock for freshness tests.</summary>
    /// <param name="utcNow">The initial UTC instant.</param>
    private sealed class LifecycleManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC instant.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Sets the current UTC instant.</summary>
        /// <param name="utcNow">The new UTC instant.</param>
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }

    /// <summary>A replay clock that advances after its first observation.</summary>
    /// <param name="firstUtcNow">The first observed UTC instant.</param>
    /// <param name="laterUtcNow">The later observed UTC instant.</param>
    private sealed class LifecycleAdvancingTimeProvider(DateTimeOffset firstUtcNow, DateTimeOffset laterUtcNow) : TimeProvider
    {
        /// <summary>The observed call count.</summary>
        private int _calls;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => Interlocked.Increment(ref _calls) == LifecycleSingleByteLimit ? firstUtcNow : laterUtcNow;
    }
}
