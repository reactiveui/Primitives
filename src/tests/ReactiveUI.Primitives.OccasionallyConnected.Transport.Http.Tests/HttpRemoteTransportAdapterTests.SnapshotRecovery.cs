// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests HTTP snapshot recovery on the client adapter.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The configured snapshot recovery route used by replay endpoint integration tests.</summary>
    private const string ReplayEndpointSnapshotRecoveryPath = "sync/recover";

    /// <summary>The first snapshot recovery cursor fixture.</summary>
    private const string SnapshotRecoveryCursor = "cursor-expired";

    /// <summary>The first snapshot recovery server version fixture.</summary>
    private const string SnapshotRecoveryServerVersion = "server-snapshot-1";

    /// <summary>The snapshot recovery response byte limit fixture.</summary>
    private const int SnapshotRecoveryMaximumResponseBytes = 2048;

    /// <summary>The snapshot recovery logical byte limit fixture.</summary>
    private const int SnapshotRecoveryLogicalByteLimit = 4096;

    /// <summary>The expected request count after connect and one recovery call.</summary>
    private const int SnapshotRecoveryExpectedHttpRequestCount = 2;

    /// <summary>The expected disposition count for pending and replay recovery responses.</summary>
    private const int SnapshotRecoveryExpectedDispositionCount = 2;

    /// <summary>The replay operation sequence fixture.</summary>
    private const int SnapshotRecoveryReplayOperationSequence = 2;

    /// <summary>The configured connect URI used by snapshot recovery adapter tests.</summary>
    private static readonly Uri SnapshotRecoveryConnectUri = new("https://example.invalid/api/sync/connect");

    /// <summary>Verifies connect can negotiate snapshot recovery when the endpoint has a recovery hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectAsyncNegotiatesSnapshotRecoveryCapability()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new SnapshotRecoveryEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, timeProvider);

        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        await Assert.That((session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.SnapshotRecovery) == RemoteTransportCapabilities.SnapshotRecovery).IsTrue();
        await Assert.That(session as IRemoteSnapshotRecoverySession).IsNotNull();
    }

    /// <summary>Verifies recovery fails closed before HTTP I/O when the capability was not negotiated.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsWhenCapabilityWasNotNegotiated()
    {
        var handler = new RecordingHttpHandler(static request => CreateReplayConnectResponse());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, new ReplayTimeProvider(ReplayObservedUtc));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var recovery = (IRemoteSnapshotRecoverySession)session;

        var exception = await CaptureHttpExceptionAsync(
            async () => _ = await recovery.GetSnapshotAsync(CreateSnapshotRecoveryRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Configuration);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies snapshot recovery is signed with the configured route and exact body bytes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSnapshotAsyncSignsSnapshotRecoveryRouteWithRequestBody()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new SnapshotRecoveryEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, timeProvider);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        var connectReplaySessionId = handler.ConnectReplaySessionId;
        await Assert.That(connectReplaySessionId).IsNotNull();
        await Assert.That(connectReplaySessionId).IsNotEqualTo(string.Empty);
        var request = CreateSnapshotRecoveryRequest();

        var result = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(request, CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(hub.RecoveryClient).IsEqualTo(new(ReplayEndpointTenantId, ReplayEndpointClientId));
        await AssertSnapshotRecoveryRequestMatchesAsync(hub.RecoveryRequest, request);
        await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
        await Assert.That(handler.Requests[1].RequestUri).IsEqualTo(new("https://example.invalid/api/sync/recover"));
        await Assert.That(GetSingleHeader(handler.Requests[1].Headers, ReplaySessionIdHeader)).IsEqualTo(connectReplaySessionId);
        await Assert.That(GetSingleHeader(handler.Requests[1].Headers, ReplayMacHeader)).IsNotNull();
        await Assert.That(handler.Requests[1].Body).Contains(SnapshotRecoveryCursor);
    }

    /// <summary>Verifies replay operations are included in the signed snapshot recovery request and response binding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSnapshotAsyncSendsReplayOperationsInSignedSnapshotRecoveryRequest()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var hub = new SnapshotRecoveryEndpointHub();
        await using var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryEndpointOptions(hub, timeProvider));
        using var handler = new ReplayEndpointHandler(endpoint);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, timeProvider);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        var request = CreateSnapshotRecoveryRequestWithReplay();

        var result = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(request, CancellationToken.None);

        await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
        await Assert.That(GetSingleHeader(handler.Requests[1].Headers, ReplayMacHeader)).IsNotNull();
        await AssertSnapshotRecoveryRequestMatchesAsync(hub.RecoveryRequest, request);
        await Assert.That(result.OperationDispositions).Count().IsEqualTo(SnapshotRecoveryExpectedDispositionCount);
        await Assert.That(result.OperationDispositions[1].OperationId).IsEqualTo(request.ReplayOperations[0].OperationId);
    }

    /// <summary>Verifies unsafe snapshot recovery routes cannot enter replay canonical paths.</summary>
    /// <param name="snapshotRecoveryPath">The configured unsafe snapshot recovery route.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("sync%2frecover")]
    [Arguments("sync%5crecover")]
    [Arguments("sync//recover")]
    public async Task GetSnapshotAsyncRejectsUnsafeSnapshotRecoveryRouteBeforeHttpRequest(string snapshotRecoveryPath)
    {
        var handler = new RecordingHttpHandler(static _ => CreateReplayConnectResponseWithSnapshotRecovery());
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, new ReplayTimeProvider(ReplayObservedUtc), snapshotRecoveryPath);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(
            async () => _ = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(CreateSnapshotRecoveryRequest(), CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(handler.Requests.Count).IsEqualTo(1);
    }

    /// <summary>Verifies response binding validation rejects a recovered checkpoint for another subscription.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSnapshotAsyncValidatesRecoveredResponseBinding()
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var request = CreateSnapshotRecoveryRequest();
        HttpStatusCode? recoveryStatusCode = null;
        var handler = new RecordingHttpHandler(httpRequest =>
        {
            if (httpRequest.RequestUri == SnapshotRecoveryConnectUri)
            {
                return CreateReplayConnectResponseWithSnapshotRecovery();
            }

            recoveryStatusCode = HttpStatusCode.OK;
            return CreateMalformedSnapshotRecoveryProtocolResponse(request);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, timeProvider);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(
            async () => _ = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(request, CancellationToken.None));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(recoveryStatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
    }

    /// <summary>Verifies client snapshot recovery exposes bounded cancellation without materializing a response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GetSnapshotAsyncHonorsCancellationDuringResponseRead()
    {
        var readStarted = CreateCompletionSource();
        var handler = new RecordingHttpHandler(request =>
            request.RequestUri == SnapshotRecoveryConnectUri
                ? CreateReplayConnectResponseWithSnapshotRecovery()
                : CreateBlockingSnapshotRecoveryResponse(readStarted));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, new ReplayTimeProvider(ReplayObservedUtc));
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        var cancellationToken = cancellation.Token;
        Task<RemoteSnapshotRecoveryResult>? recovery = null;

        try
        {
            recovery = ((IRemoteSnapshotRecoverySession)session)
                .GetSnapshotAsync(CreateSnapshotRecoveryRequest(), cancellationToken)
                .AsTask();
            await AwaitWithTimeoutAsync(readStarted.Task);
            await cancellation.CancelAsync();

            var exception = await CaptureOperationCanceledExceptionAsync(() => AwaitWithTimeoutAsync(recovery));

            await Assert.That(exception.CancellationToken == cancellationToken).IsTrue();
            await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
        }
        finally
        {
            await cancellation.CancelAsync();
            if (recovery is not null)
            {
                try
                {
                    await AwaitWithTimeoutAsync(recovery);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }

    /// <summary>Verifies session disposal cancels a blocked snapshot response read without canceling the caller token.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionDisposeAsyncCancelsActiveSnapshotRecoveryResponseRead()
    {
        var readStarted = CreateCompletionSource();
        var handler = new RecordingHttpHandler(request =>
            request.RequestUri == SnapshotRecoveryConnectUri
                ? CreateReplayConnectResponseWithSnapshotRecovery()
                : CreateBlockingSnapshotRecoveryResponse(readStarted));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, new ReplayTimeProvider(ReplayObservedUtc));
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        using var callerCancellation = new CancellationTokenSource();
        var recovery = ((IRemoteSnapshotRecoverySession)session)
            .GetSnapshotAsync(CreateSnapshotRecoveryRequest(), callerCancellation.Token)
            .AsTask();

        try
        {
            await AwaitWithTimeoutAsync(readStarted.Task);
            await AwaitWithTimeoutAsync(session.DisposeAsync().AsTask());
            _ = await CaptureOperationCanceledExceptionAsync(() => AwaitWithTimeoutAsync(recovery));

            await Assert.That(callerCancellation.IsCancellationRequested).IsFalse();
            await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
        }
        finally
        {
            await callerCancellation.CancelAsync();
            try
            {
                await AwaitWithTimeoutAsync(recovery);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    /// <summary>Verifies adapter disposal cancels a blocked snapshot response read without canceling the caller token.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AdapterDisposeAsyncCancelsActiveSnapshotRecoveryResponseRead()
    {
        var readStarted = CreateCompletionSource();
        var handler = new RecordingHttpHandler(request =>
            request.RequestUri == SnapshotRecoveryConnectUri
                ? CreateReplayConnectResponseWithSnapshotRecovery()
                : CreateBlockingSnapshotRecoveryResponse(readStarted));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, new ReplayTimeProvider(ReplayObservedUtc));
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        using var callerCancellation = new CancellationTokenSource();
        var recovery = ((IRemoteSnapshotRecoverySession)session)
            .GetSnapshotAsync(CreateSnapshotRecoveryRequest(), callerCancellation.Token)
            .AsTask();

        try
        {
            await AwaitWithTimeoutAsync(readStarted.Task);
            await AwaitWithTimeoutAsync(adapter.DisposeAsync().AsTask());
            _ = await CaptureOperationCanceledExceptionAsync(() => AwaitWithTimeoutAsync(recovery));

            await Assert.That(callerCancellation.IsCancellationRequested).IsFalse();
            await Assert.That(handler.Requests.Count).IsEqualTo(SnapshotRecoveryExpectedHttpRequestCount);
        }
        finally
        {
            await callerCancellation.CancelAsync();
            try
            {
                await AwaitWithTimeoutAsync(recovery);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    /// <summary>Creates a snapshot-recovery-capable connect request.</summary>
    /// <returns>The connect request.</returns>
    private static TransportConnectRequest CreateSnapshotRecoveryConnectRequest() => new(
        new VersionRange(new Version(1, 0), new Version(1, 0)),
        new ClientIdentity("client-1", "tenant-1"),
        [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Creates a valid snapshot recovery request fixture.</summary>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateSnapshotRecoveryRequest() => new()
    {
        StreamId = CreateStreamId(),
        SubscriptionId = new(Guid.Parse("00000000-0000-0000-0000-000000000501")),
        ExpiredCursor = SnapshotRecoveryCursor,
        ClientStateContractId = "client-state",
        ClientStateSchemaVersion = 1,
        SnapshotFormatVersion = 1,
        PendingOperations = [CreateOperation(1)],
        MaximumResponseBytes = SnapshotRecoveryMaximumResponseBytes,
    };

    /// <summary>Creates a valid snapshot recovery request fixture with a replay operation.</summary>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateSnapshotRecoveryRequestWithReplay() => CreateSnapshotRecoveryRequest() with
    {
        ReplayOperations = [CreateOperation(SnapshotRecoveryReplayOperationSequence)],
    };

    /// <summary>Creates a valid snapshot recovery result fixture.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The recovery result.</returns>
    private static RemoteSnapshotRecoveryResult CreateSnapshotRecoveryResult(RemoteSnapshotRecoveryRequest request) => new()
    {
        Status = RemoteSnapshotRecoveryStatus.Recovered,
        Checkpoint = CreateSnapshotCheckpoint(request),
        OperationDispositions =
        [
            new()
            {
                OperationId = request.PendingOperations[0].OperationId,
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(request.PendingOperations[0].OperationId, OperationResultKind.Accepted, null, SnapshotRecoveryServerVersion),
            },
            .. request.ReplayOperations.Select(static operation => new SnapshotOperationDisposition
            {
                OperationId = operation.OperationId,
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(operation.OperationId, OperationResultKind.Accepted, null, SnapshotRecoveryServerVersion),
            }),
        ],
    };

    /// <summary>Creates a valid snapshot recovery checkpoint fixture.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The checkpoint.</returns>
    private static RemoteSnapshotCheckpoint CreateSnapshotCheckpoint(RemoteSnapshotRecoveryRequest request) => new()
    {
        StreamId = request.StreamId,
        SubscriptionId = request.SubscriptionId,
        FrontierCursor = ReplayCursorOne,
        ServerVersion = SnapshotRecoveryServerVersion,
        SnapshotFormatVersion = request.SnapshotFormatVersion,
        ClientState = new("client-state", 1, "application/json", "{}"u8.ToArray(), "sha256-snapshot"),
        ObservedAtUtc = ReplayObservedUtc,
    };

    /// <summary>Creates a connect response that advertises snapshot recovery.</summary>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateReplayConnectResponseWithSnapshotRecovery()
    {
        const string SnapshotConnectJson =
            "{\"protocolVersion\":\"1.0\",\"features\":79,\"maximumBatchOperations\":10,\"maximumBatchBytes\":1024,"
            + "\"serverIdempotencyRetentionMilliseconds\":60000,\"clientInboxRetentionRequiredMilliseconds\":120000}";
        var response = CreateJsonResponse(HttpStatusCode.OK, SnapshotConnectJson, ProtocolMediaType);
        AddReplaySessionHeaders(response);
        return response;
    }

    /// <summary>Creates a raw malformed snapshot recovery protocol response that bypasses encoder-side validation.</summary>
    /// <param name="request">The recovery request used to shape the response.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateMalformedSnapshotRecoveryProtocolResponse(RemoteSnapshotRecoveryRequest request)
    {
        var operationId = request.PendingOperations[0].OperationId.Value;
        var observedAt = ReplayObservedUtc.ToString("O", CultureInfo.InvariantCulture);
        var json = "{\"status\":0,\"checkpoint\":{\"streamId\":\"" + request.StreamId.Value
            + "\",\"subscriptionId\":\"00000000-0000-0000-0000-000000000999\""
            + ",\"frontierCursor\":\"" + ReplayCursorOne
            + "\",\"serverVersion\":\"" + SnapshotRecoveryServerVersion
            + "\",\"snapshotFormatVersion\":" + request.SnapshotFormatVersion.ToString(CultureInfo.InvariantCulture)
            + ",\"clientState\":{\"contractId\":\"client-state\",\"schemaVersion\":1"
            + ",\"contentType\":\"application/json\",\"payload\":\"e30=\",\"payloadHash\":\"sha256-snapshot\"}"
            + ",\"observedAtUtc\":\"" + observedAt
            + "\"},\"operationDispositions\":[{\"operationId\":\"" + operationId.ToString("D")
            + "\",\"kind\":0,\"result\":{\"operationId\":\"" + operationId.ToString("D")
            + "\",\"kind\":0,\"reasonCode\":null,\"serverVersion\":\"" + SnapshotRecoveryServerVersion
            + "\"}}]}";
        return CreateJsonResponse(HttpStatusCode.OK, json, ProtocolMediaType);
    }

    /// <summary>Captures an operation-canceled exception from a bounded asynchronous action.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static async Task<OperationCanceledException> CaptureOperationCanceledExceptionAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected an operation-canceled exception.");
    }

    /// <summary>Asserts a recovered request preserves field values and owned payload bytes.</summary>
    /// <param name="actual">The actual request.</param>
    /// <param name="expected">The expected request.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    /// <exception cref="InvalidOperationException">The expected request is absent.</exception>
    private static async Task AssertSnapshotRecoveryRequestMatchesAsync(
        RemoteSnapshotRecoveryRequest? actual,
        RemoteSnapshotRecoveryRequest expected)
    {
        await Assert.That(actual).IsNotNull();
        var actualRequest = actual ?? throw new InvalidOperationException("Expected snapshot recovery request.");
        await Assert.That(actualRequest.StreamId).IsEqualTo(expected.StreamId);
        await Assert.That(actualRequest.SubscriptionId).IsEqualTo(expected.SubscriptionId);
        await Assert.That(actualRequest.ExpiredCursor).IsEqualTo(expected.ExpiredCursor);
        await Assert.That(actualRequest.ClientStateContractId).IsEqualTo(expected.ClientStateContractId);
        await Assert.That(actualRequest.ClientStateSchemaVersion).IsEqualTo(expected.ClientStateSchemaVersion);
        await Assert.That(actualRequest.SnapshotFormatVersion).IsEqualTo(expected.SnapshotFormatVersion);
        await Assert.That(actualRequest.MaximumResponseBytes).IsEqualTo(expected.MaximumResponseBytes);
        await Assert.That(actualRequest.PendingOperations).Count().IsEqualTo(expected.PendingOperations.Count);
        await Assert.That(actualRequest.PendingOperations[0].OperationId).IsEqualTo(expected.PendingOperations[0].OperationId);
        await Assert.That(actualRequest.PendingOperations[0].ClientSequence).IsEqualTo(expected.PendingOperations[0].ClientSequence);
        await Assert.That(actualRequest.PendingOperations[0].Payload.Payload.ToArray().SequenceEqual(
            expected.PendingOperations[0].Payload.Payload.ToArray())).IsTrue();
        await Assert.That(actualRequest.ReplayOperations).Count().IsEqualTo(expected.ReplayOperations.Count);
        if (expected.ReplayOperations.Count == 0)
        {
            return;
        }

        await Assert.That(actualRequest.ReplayOperations[0].OperationId).IsEqualTo(expected.ReplayOperations[0].OperationId);
        await Assert.That(actualRequest.ReplayOperations[0].ClientSequence).IsEqualTo(expected.ReplayOperations[0].ClientSequence);
    }

    /// <summary>Creates a blocking snapshot recovery response.</summary>
    /// <param name="readStarted">The read-start signal.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateBlockingSnapshotRecoveryResponse(TaskCompletionSource<object?> readStarted)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new BlockingSnapshotRecoveryReadStream(readStarted)) };
        response.Content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(ProtocolMediaType);
        return response;
    }

    /// <summary>Creates a snapshot recovery adapter with custom route and limits.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="timeProvider">The deterministic replay clock.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateSnapshotRecoveryAdapter(HttpClient httpClient, TimeProvider timeProvider) =>
        CreateSnapshotRecoveryAdapter(httpClient, timeProvider, ReplayEndpointSnapshotRecoveryPath);

    /// <summary>Creates a snapshot recovery adapter with a supplied recovery route.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="timeProvider">The deterministic replay clock.</param>
    /// <param name="snapshotRecoveryPath">The snapshot recovery route.</param>
    /// <returns>The adapter.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportAdapter CreateSnapshotRecoveryAdapter(
        HttpClient httpClient,
        TimeProvider timeProvider,
        string snapshotRecoveryPath) =>
        CreateAdapter(
            httpClient,
            new(ReplayEndpointBaseAddressText),
            options => options with
            {
                ConnectPath = ReplayEndpointConnectPath,
                PushPath = ReplayEndpointPushPath,
                SubscribePath = ReplayEndpointSubscribePath,
                AcknowledgePath = ReplayEndpointAcknowledgePath,
                SnapshotRecoveryPath = snapshotRecoveryPath,
                SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryLogicalByteLimit },
                ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = timeProvider },
            });

    /// <summary>Creates replay-enabled endpoint options with snapshot recovery.</summary>
    /// <param name="hub">The borrowed hub.</param>
    /// <param name="timeProvider">The replay clock.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateSnapshotRecoveryEndpointOptions(SnapshotRecoveryEndpointHub hub, TimeProvider timeProvider)
    {
        var options = CreateReplayEndpointOptions(hub, timeProvider);
        return options with
        {
            DeclaredCapabilities = options.DeclaredCapabilities with
            {
                Features = options.DeclaredCapabilities.Features | RemoteTransportCapabilities.SnapshotRecovery,
            },
            SnapshotRecoveryHub = hub,
            SnapshotRecoveryPath = ReplayEndpointSnapshotRecoveryPath,
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryLogicalByteLimit },
        };
    }

    /// <summary>Records snapshot recovery endpoint calls.</summary>
    private sealed class SnapshotRecoveryEndpointHub : IServerStreamHub, IServerSnapshotRecoveryHub
    {
        /// <summary>Gets or sets the recovery result.</summary>
        internal RemoteSnapshotRecoveryResult? Result { get; init; }

        /// <summary>Gets the last trusted recovery principal.</summary>
        internal ServerAuthenticatedClient? RecoveryClient { get; private set; }

        /// <summary>Gets the last recovery request.</summary>
        internal RemoteSnapshotRecoveryRequest? RecoveryRequest { get; private set; }

        /// <summary>Gets the recovery call count.</summary>
        internal int RecoveryCalls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
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
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.CompletedTask.ConfigureAwait(false);
            yield return new(
                Guid.Parse("00000000-0000-0000-0000-000000000401"),
                request.StreamId,
                request.Cursor,
                ReplayCursorOne,
                []);
        }

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
            return ValueTask.FromResult(Result ?? CreateSnapshotRecoveryResult(request));
        }
    }

    /// <summary>Provides a response stream that remains pending until canceled.</summary>
    /// <param name="readStarted">The signal completed when a read is attempted.</param>
    private sealed class BlockingSnapshotRecoveryReadStream(TaskCompletionSource<object?> readStarted) : Stream
    {
        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException("Synchronous response reads are not used by snapshot recovery tests.");

        /// <inheritdoc/>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _ = readStarted.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        /// <inheritdoc/>
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _ = readStarted.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
