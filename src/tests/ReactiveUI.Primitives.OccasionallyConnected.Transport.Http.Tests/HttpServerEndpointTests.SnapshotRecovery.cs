// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests HTTP server snapshot recovery routing.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The snapshot recovery endpoint URI.</summary>
    private const string SnapshotRecoveryUri = "https://example.invalid/snapshot-recovery";

    /// <summary>The encoded connect alias route.</summary>
    private const string EncodedConnectAliasPath = "%63onnect";

    /// <summary>The canonical snapshot recovery route used for replay signing.</summary>
    private const string SnapshotRecoveryCanonicalPath = "snapshot-recovery";

    /// <summary>The snapshot recovery logical byte limit fixture.</summary>
    private const int SnapshotRecoveryLogicalByteLimit = 4096;

    /// <summary>The expected authorization calls after connect and one recovery request.</summary>
    private const int SnapshotRecoveryExpectedAuthorizationCalls = 2;

    /// <summary>The replay operation sequence fixture.</summary>
    private const int SnapshotRecoveryReplayOperationSequence = 2;

    /// <summary>The first byte in an oversized request body fixture.</summary>
    private const byte OversizedSnapshotRecoveryFirstByte = 1;

    /// <summary>The second byte in an oversized request body fixture.</summary>
    private const byte OversizedSnapshotRecoverySecondByte = 2;

    /// <summary>The oversized request body fixture.</summary>
    private static readonly byte[] OversizedSnapshotRecoveryBody = [OversizedSnapshotRecoveryFirstByte, OversizedSnapshotRecoverySecondByte];

    /// <summary>Verifies snapshot recovery route aliases must be distinct after route decoding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsDuplicateSnapshotRecoveryRoute()
    {
        var options = CreateSnapshotRecoveryOptions(new()) with
        {
            SnapshotRecoveryPath = DuplicatePushPath,
        };

        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies encoded snapshot recovery route aliases cannot shadow existing routes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsEncodedSnapshotRecoveryRouteAlias()
    {
        var options = CreateSnapshotRecoveryOptions(new()) with
        {
            SnapshotRecoveryPath = EncodedConnectAliasPath,
        };

        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies snapshot recovery capability is rejected unless a recovery hub is configured.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorRejectsSnapshotRecoveryCapabilityWithoutHub()
    {
        var options = CreateOptions(new RecordingHub()) with
        {
            DeclaredCapabilities = CreateCapabilitiesWithSnapshotRecovery(),
            SnapshotRecoveryHub = null,
        };

        await Assert.That(() => new HttpServerEndpoint(options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies unsigned snapshot recovery requests are rejected before the recovery hub is called.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsUnsignedRequest()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(CreateEndpointSnapshotRecoveryRequest());
        using var request = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies replay authorization sees decoded snapshot recovery stream and subscription rights.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryAuthorizationReceivesDecodedRights()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub, authorizer));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, recoveryRequest);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(authorizer.Contexts.Exists(context => context.Operation == nameof(HttpReplayOperationKind.SnapshotRecovery)
            && context.SubscriptionId == recoveryRequest.SubscriptionId
            && context.StreamIds.SequenceEqual([recoveryRequest.StreamId]))).IsTrue();
    }

    /// <summary>Verifies signed snapshot recovery requests bind replay operations to the hub request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The hub request is absent.</exception>
    [Test]
    public async Task HandleAsyncSnapshotRecoverySignedRequestIncludesReplayOperations()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequestWithReplay();
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, recoveryRequest);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
        await Assert.That(hub.RecoveryRequest).IsNotNull();
        var actualRequest = hub.RecoveryRequest ?? throw new InvalidOperationException("Expected snapshot recovery request.");
        await Assert.That(actualRequest.ReplayOperations).Count().IsEqualTo(1);
        await Assert.That(actualRequest.ReplayOperations[0].OperationId).IsEqualTo(recoveryRequest.ReplayOperations[0].OperationId);
    }

    /// <summary>Verifies client substitution is rejected before snapshot recovery reaches the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsClientSubstitution()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, CreateEndpointSnapshotRecoveryRequest());

        using var response = await endpoint.HandleAsync(request, new(TenantId, ForgedClientId), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies tenant substitution is rejected before snapshot recovery reaches the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsTenantSubstitution()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, CreateEndpointSnapshotRecoveryRequest());
        var substituted = new ServerAuthenticatedClient("tenant-2", ClientId);

        using var response = await endpoint.HandleAsync(request, substituted, CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies subscription substitution is rejected by replay authorization before the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsSubscriptionSubstitution()
    {
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var substitutedRequest = recoveryRequest with
        {
            SubscriptionId = new(Guid.Parse("00000000-0000-0000-0000-000000000999")),
        };
        var hub = new RecordingSnapshotRecoveryHub();
        var authorizer = new SubscriptionBindingReplayAuthorizer(recoveryRequest.SubscriptionId);
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub, authorizer));
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, substitutedRequest);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(authorizer.Calls).IsEqualTo(SnapshotRecoveryExpectedAuthorizationCalls);
        await Assert.That(authorizer.Contexts[1].SubscriptionId).IsEqualTo(substitutedRequest.SubscriptionId);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies duplicate recovery after a lost response replays the cached answer without calling the hub again.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryReplaysLostResponseWithoutCallingHubAgain()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);

        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        var firstBody = await ReadResponseBodyAsync(firstResponse);
        var duplicateBody = await ReadResponseBodyAsync(duplicateResponse);
        var firstResult = CreateCodec().DeserializeSnapshotRecoveryResponse(recoveryRequest, firstBody);
        var duplicateResult = CreateCodec().DeserializeSnapshotRecoveryResponse(recoveryRequest, duplicateBody);

        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(duplicateBody.SequenceEqual(firstBody)).IsTrue();
        await AssertSnapshotRecoveryResultsEqualAsync(duplicateResult, firstResult);
        await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
    }

    /// <summary>Verifies malformed hub results are rejected without caching a successful recovery response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryReturnsValidationRejectedForBadHubResult()
    {
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var hub = new RecordingSnapshotRecoveryHub
        {
            Result = CreateEndpointSnapshotRecoveryResult(recoveryRequest) with
            {
                Checkpoint = CreateEndpointSnapshotCheckpoint(recoveryRequest) with
                {
                    StreamId = new("other-stream"),
                },
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        using var request = await CreateSignedSnapshotRecoveryRequestAsync(endpoint, recoveryRequest);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
    }

    /// <summary>Verifies an invalid hub result keeps the replay entry non-reexecutable for the same signed envelope.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryDoesNotReinvokeHubForDuplicateAfterInvalidHubResult()
    {
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var hub = new RecordingSnapshotRecoveryHub
        {
            Result = CreateEndpointSnapshotRecoveryResult(recoveryRequest) with
            {
                Checkpoint = CreateEndpointSnapshotCheckpoint(recoveryRequest) with
                {
                    StreamId = new("other-stream"),
                },
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);

        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
    }

    /// <summary>Verifies signed snapshot recovery without a hub fails closed and does not reexecute duplicates.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryWithoutHubFailsClosedAndDoesNotReexecuteDuplicate()
    {
        var options = CreateReplayOptions(new RecordingHub()) with
        {
            SnapshotRecoveryPath = HttpRemoteTransportOptions.DefaultSnapshotRecoveryPath,
            SnapshotRecoveryHub = null,
        };
        await using var endpoint = new HttpServerEndpoint(options);
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);

        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies oversized recovery bodies fail before replay authorization or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsOversizedBodyBeforeReplayAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingSnapshotRecoveryHub();
        var options = CreateSnapshotRecoveryOptions(hub, authorizer) with { MaximumRequestBytes = 1 };
        await using var endpoint = new HttpServerEndpoint(options);
        using var request = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, OversizedSnapshotRecoveryBody);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(authorizer.Calls).IsEqualTo(0);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies expired replay sessions are rejected for snapshot recovery before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryRejectsExpiredReplaySessionWithoutHubEffects()
    {
        var replayClock = new ReplayTimeProvider(ReplaySentAtUtc);
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub, authorizer) with
        {
            ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = replayClock },
        });
        var session = await ConnectReplaySessionAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        var expiredButFreshRequestUtc = ReplaySentAtUtc.AddMinutes(ReplaySessionLifetimeMinutes).AddTicks(1);
        replayClock.SetUtcNow(expiredButFreshRequestUtc);
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var request = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(
            request,
            HttpReplayOperationKind.SnapshotRecovery,
            PostMethodName,
            SnapshotRecoveryCanonicalPath,
            [],
            body,
            new ReplaySessionSigning(session, expiredButFreshRequestUtc));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect + 1);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Verifies unexpected snapshot recovery hub failures return a bodyless server error and do not reexecute duplicates.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryPostEffectHubFailureReturnsBodylessServerErrorAndDoesNotReexecuteDuplicate()
    {
        var hub = new RecordingSnapshotRecoveryHub { Failure = new InvalidOperationException("Snapshot recovery failed after admission.") };
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);

        using var firstResponse = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        using var duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
        await Assert.That(await firstResponse.Content.ReadAsStringAsync().ConfigureAwait(false)).IsEmpty();
        await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
    }

    /// <summary>Verifies caller-canceled snapshot recovery effects abandon replay without allowing duplicate hub invocation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryCallerCancellationAfterHubEntryDoesNotReexecuteDuplicate()
    {
        using var callerCancellation = new CancellationTokenSource();
        var hubEntered = CreateSignal();
        var hub = CreateBlockingSnapshotRecoveryHub(hubEntered);
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        Task<HttpResponseMessage>? responseTask = null;
        HttpResponseMessage? duplicateResponse = null;
        OperationCanceledException? observedCancellation = null;
        try
        {
            responseTask = endpoint.HandleAsync(first, CreateAuthenticatedClient(), callerCancellation.Token).AsTask();
            await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await callerCancellation.CancelAsync().ConfigureAwait(false);
            observedCancellation = await CaptureOperationCanceledExceptionAsync(responseTask).ConfigureAwait(false);
            await Assert.That(callerCancellation.IsCancellationRequested).IsTrue();
            duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
            await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
        }
        finally
        {
            await callerCancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                HttpResponseMessage? firstResponse = null;
                try
                {
                    if (responseTask is not null)
                    {
                        firstResponse = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException exception) when (ReferenceEquals(exception, observedCancellation))
                {
                    // The main test already proved caller cancellation; cleanup only observes the same canceled request task.
                }
                finally
                {
                    firstResponse?.Dispose();
                }
            }
            finally
            {
                duplicateResponse?.Dispose();
            }
        }
    }

    /// <summary>Verifies shutdown-canceled snapshot recovery effects abandon replay without allowing duplicate hub invocation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryShutdownAfterHubEntryDoesNotReexecuteDuplicate()
    {
        var hubEntered = CreateSignal();
        var hub = CreateBlockingSnapshotRecoveryHub(hubEntered);
        var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        var recoveryRequest = CreateEndpointSnapshotRecoveryRequest();
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        using var first = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(first, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        AddSessionReplayHeaders(duplicate, HttpReplayOperationKind.SnapshotRecovery, PostMethodName, SnapshotRecoveryCanonicalPath, [], body, session);
        Task<HttpResponseMessage>? responseTask = null;
        Task? disposeTask = null;
        HttpResponseMessage? firstResponse = null;
        HttpResponseMessage? duplicateResponse = null;
        try
        {
            responseTask = endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            disposeTask = endpoint.DisposeAsync().AsTask();
            firstResponse = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            duplicateResponse = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
            await Assert.That(firstResponse.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
            await Assert.That(await firstResponse.Content.ReadAsStringAsync().ConfigureAwait(false)).IsEmpty();
            await Assert.That(duplicateResponse.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.RecoveryCalls).IsEqualTo(1);
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
        finally
        {
            disposeTask ??= endpoint.DisposeAsync().AsTask();
            try
            {
                if (responseTask is not null && firstResponse is null)
                {
                    firstResponse = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                }
            }
            finally
            {
                try
                {
                    firstResponse?.Dispose();
                    duplicateResponse?.Dispose();
                }
                finally
                {
                    await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Verifies disposed snapshot recovery body reads fail before replay admission or hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryDisposedBodyReadReturnsServiceUnavailableBeforeHub()
    {
        var hub = new RecordingSnapshotRecoveryHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Post, SnapshotRecoveryUri) { Content = CreateDisposedReadProtocolContent() };

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.RecoveryClient).IsNull();
    }

    /// <summary>Creates endpoint options with snapshot recovery enabled.</summary>
    /// <param name="hub">The recovery hub.</param>
    /// <returns>The options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpServerEndpointOptions CreateSnapshotRecoveryOptions(
        RecordingSnapshotRecoveryHub hub) =>
        CreateSnapshotRecoveryOptions(hub, AllowReplayAuthorizer.Instance);

    /// <summary>Creates endpoint options with snapshot recovery enabled.</summary>
    /// <param name="hub">The recovery hub.</param>
    /// <param name="authorizer">The replay authorizer.</param>
    /// <returns>The options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpServerEndpointOptions CreateSnapshotRecoveryOptions(
        RecordingSnapshotRecoveryHub hub,
        IHttpReplayAuthorizer authorizer) =>
        CreateReplayOptions(new RecordingHub(), authorizer) with
        {
            DeclaredCapabilities = CreateCapabilitiesWithSnapshotRecovery(),
            SnapshotRecoveryHub = hub,
            SnapshotRecoveryPath = HttpRemoteTransportOptions.DefaultSnapshotRecoveryPath,
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryLogicalByteLimit },
        };

    /// <summary>Creates snapshot recovery endpoint capabilities.</summary>
    /// <returns>The capabilities.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static NegotiatedCapabilities CreateCapabilitiesWithSnapshotRecovery() =>
        CreateCapabilities(CreateCapabilities().Features | RemoteTransportCapabilities.SnapshotRecovery);

    /// <summary>Creates a valid endpoint snapshot recovery request.</summary>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateEndpointSnapshotRecoveryRequest() => new()
    {
        StreamId = new(StreamName),
        SubscriptionId = new(Guid.Parse(SubscriptionIdText)),
        ExpiredCursor = CursorOne,
        ClientStateContractId = ContractName,
        ClientStateSchemaVersion = 1,
        SnapshotFormatVersion = 1,
        PendingOperations = [CreateOperation()],
        MaximumResponseBytes = SnapshotRecoveryLogicalByteLimit,
    };

    /// <summary>Creates a valid endpoint snapshot recovery request with a replay operation.</summary>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateEndpointSnapshotRecoveryRequestWithReplay() => CreateEndpointSnapshotRecoveryRequest() with
    {
        ReplayOperations = [CreateOperation() with
        {
            OperationId = new(Guid.Parse("00000000-0000-0000-0000-000000000602")),
            ClientSequence = SnapshotRecoveryReplayOperationSequence,
        }],
    };

    /// <summary>Creates a valid endpoint snapshot recovery result.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The result.</returns>
    private static RemoteSnapshotRecoveryResult CreateEndpointSnapshotRecoveryResult(RemoteSnapshotRecoveryRequest request) => new()
    {
        Status = RemoteSnapshotRecoveryStatus.Recovered,
        Checkpoint = CreateEndpointSnapshotCheckpoint(request),
        OperationDispositions =
        [
            new()
            {
                OperationId = request.PendingOperations[0].OperationId,
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(request.PendingOperations[0].OperationId, OperationResultKind.Accepted, null, ServerCursor),
            },
            .. request.ReplayOperations.Select(static operation => new SnapshotOperationDisposition
            {
                OperationId = operation.OperationId,
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(operation.OperationId, OperationResultKind.Accepted, null, ServerCursor),
            }),
        ],
    };

    /// <summary>Creates a valid endpoint snapshot recovery checkpoint.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The checkpoint.</returns>
    private static RemoteSnapshotCheckpoint CreateEndpointSnapshotCheckpoint(RemoteSnapshotRecoveryRequest request) => new()
    {
        StreamId = request.StreamId,
        SubscriptionId = request.SubscriptionId,
        FrontierCursor = CursorTwo,
        ServerVersion = ServerCursor,
        SnapshotFormatVersion = request.SnapshotFormatVersion,
        ClientState = new(ContractName, 1, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
        ObservedAtUtc = DateTimeOffset.Parse("2026-09-18T00:00:00+00:00", System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Creates a signed snapshot recovery request.</summary>
    /// <param name="endpoint">The endpoint used to issue the replay session.</param>
    /// <param name="recoveryRequest">The recovery request.</param>
    /// <returns>The signed request.</returns>
    private static async Task<HttpRequestMessage> CreateSignedSnapshotRecoveryRequestAsync(
        HttpServerEndpoint endpoint,
        RemoteSnapshotRecoveryRequest recoveryRequest)
    {
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = CreateCodec().SerializeSnapshotRecoveryRequest(recoveryRequest);
        var request = CreateProtocolRequest(HttpMethod.Post, SnapshotRecoveryUri, body);
        AddSessionReplayHeaders(
            request,
            HttpReplayOperationKind.SnapshotRecovery,
            PostMethodName,
            SnapshotRecoveryCanonicalPath,
            [],
            body,
            session);
        return request;
    }

    /// <summary>Asserts two snapshot recovery results have the same checkpoint and operation disposition fields.</summary>
    /// <param name="actual">The actual result.</param>
    /// <param name="expected">The expected result.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    /// <exception cref="InvalidOperationException">The expected checkpoint is absent.</exception>
    private static async Task AssertSnapshotRecoveryResultsEqualAsync(
        RemoteSnapshotRecoveryResult actual,
        RemoteSnapshotRecoveryResult expected)
    {
        await Assert.That(actual.Status).IsEqualTo(expected.Status);
        await Assert.That(actual.Checkpoint).IsNotNull();
        await Assert.That(expected.Checkpoint).IsNotNull();
        var actualCheckpoint = actual.Checkpoint ?? throw new InvalidOperationException("Expected actual snapshot checkpoint.");
        var expectedCheckpoint = expected.Checkpoint ?? throw new InvalidOperationException("Expected expected snapshot checkpoint.");
        await Assert.That(actualCheckpoint.StreamId).IsEqualTo(expectedCheckpoint.StreamId);
        await Assert.That(actualCheckpoint.SubscriptionId).IsEqualTo(expectedCheckpoint.SubscriptionId);
        await Assert.That(actualCheckpoint.FrontierCursor).IsEqualTo(expectedCheckpoint.FrontierCursor);
        await Assert.That(actualCheckpoint.ServerVersion).IsEqualTo(expectedCheckpoint.ServerVersion);
        await Assert.That(actualCheckpoint.SnapshotFormatVersion).IsEqualTo(expectedCheckpoint.SnapshotFormatVersion);
        await Assert.That(actualCheckpoint.ClientState.Payload.ToArray().SequenceEqual(
            expectedCheckpoint.ClientState.Payload.ToArray())).IsTrue();
        await Assert.That(actual.OperationDispositions).Count().IsEqualTo(expected.OperationDispositions.Count);
        await Assert.That(actual.OperationDispositions[0].OperationId).IsEqualTo(expected.OperationDispositions[0].OperationId);
        await Assert.That(actual.OperationDispositions[0].Kind).IsEqualTo(expected.OperationDispositions[0].Kind);
        await Assert.That(actual.OperationDispositions[0].Result).IsEqualTo(expected.OperationDispositions[0].Result);
    }

    /// <summary>Captures operation cancellation from a response task.</summary>
    /// <param name="responseTask">The response task expected to be canceled.</param>
    /// <returns>The observed cancellation exception.</returns>
    /// <exception cref="InvalidOperationException">The response task completed without cancellation.</exception>
    private static async Task<OperationCanceledException> CaptureOperationCanceledExceptionAsync(Task<HttpResponseMessage> responseTask)
    {
        try
        {
            using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected snapshot recovery request cancellation.");
    }

    /// <summary>Creates a snapshot recovery hub that blocks after recording entry.</summary>
    /// <param name="hubEntered">The hub entry signal.</param>
    /// <returns>The snapshot recovery hub.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RecordingSnapshotRecoveryHub CreateBlockingSnapshotRecoveryHub(TaskCompletionSource<object?> hubEntered) =>
        new()
        {
            Handler = async (request, client, cancellationToken) =>
            {
                _ = hubEntered.TrySetResult(null);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return CreateEndpointSnapshotRecoveryResult(request);
            },
        };

    /// <summary>Records server snapshot recovery calls.</summary>
    private sealed class RecordingSnapshotRecoveryHub : IServerSnapshotRecoveryHub
    {
        /// <summary>Gets or sets the result returned by recovery.</summary>
        internal RemoteSnapshotRecoveryResult? Result { get; init; }

        /// <summary>Gets or sets the failure thrown by recovery.</summary>
        internal Exception? Failure { get; init; }

        /// <summary>Gets or sets the recovery handler.</summary>
        internal Func<RemoteSnapshotRecoveryRequest, ServerAuthenticatedClient, CancellationToken, ValueTask<RemoteSnapshotRecoveryResult>>? Handler { get; init; }

        /// <summary>Gets the last trusted recovery client.</summary>
        internal ServerAuthenticatedClient? RecoveryClient { get; private set; }

        /// <summary>Gets the last recovery request.</summary>
        internal RemoteSnapshotRecoveryRequest? RecoveryRequest { get; private set; }

        /// <summary>Gets the number of recovery calls.</summary>
        internal int RecoveryCalls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RecoveryCalls++;
            RecoveryClient = client;
            RecoveryRequest = request;
            if (Failure is not null)
            {
                throw Failure;
            }

            return Handler is { } handler
                ? handler(request, client, cancellationToken)
                : ValueTask.FromResult(Result ?? CreateEndpointSnapshotRecoveryResult(request));
        }
    }

    /// <summary>Authorizes only one expected snapshot recovery subscription.</summary>
    /// <param name="subscriptionId">The allowed subscription identifier.</param>
    private sealed class SubscriptionBindingReplayAuthorizer(SubscriptionId subscriptionId) : IHttpReplayAuthorizer
    {
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
            return ValueTask.FromResult(context.SubscriptionId is null || context.SubscriptionId == subscriptionId);
        }
    }
}
