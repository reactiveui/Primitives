// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationServerExample"/>.</summary>
public sealed class CollaborationServerExampleTests
{
    /// <summary>The bearer token accepted by the development credential store.</summary>
    private const string Token = "token-a";

    /// <summary>The second bearer token accepted by the development credential store.</summary>
    private const string TokenB = "token-b";

    /// <summary>The tenant identifier configured on the trusted development credential.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The second tenant identifier configured on the trusted development credential.</summary>
    private const string TenantB = "tenant-b";

    /// <summary>The forged tenant hint sent by a portable client during negotiation.</summary>
    private const string TenantHint = "forged-tenant";

    /// <summary>The client identifier used by the socket test.</summary>
    private const string Client = "client-a";

    /// <summary>The second client identifier used by tenant isolation tests.</summary>
    private const string ClientB = "client-b";

    /// <summary>The first client operation sequence used by socket tests.</summary>
    private const long FirstClientSequence = 1;

    /// <summary>The second client operation sequence used by socket tests.</summary>
    private const long SecondClientSequence = 2;

    /// <summary>The expected event count when replaying from the beginning after two persisted writes.</summary>
    private const int ReplayEventCount = 2;

    /// <summary>The receive timeout in seconds for public socket tests.</summary>
    private const int ReceiveTimeoutSeconds = 10;

    /// <summary>The empty poll delay in milliseconds for socket tests.</summary>
    private const int EmptyPollDelayMilliseconds = 10;

    /// <summary>The protocol major version used by the example server.</summary>
    private const int ProtocolMajorVersion = 1;

    /// <summary>The protocol minor version used by the example server.</summary>
    private const int ProtocolMinorVersion = 0;

    /// <summary>The activity status property name.</summary>
    private const string StatusPropertyName = "status";

    /// <summary>The stable rejection reason for invalid activity status field types.</summary>
    private const string InvalidStatusReasonCode = "activity-status-type";

    /// <summary>The unknown bearer token used by authentication failure tests.</summary>
    private const string UnknownToken = "unknown-token";

    /// <summary>The relative ASP.NET path base used by mounted route socket tests.</summary>
    private const string MountedPathBase = "mounted";

    /// <summary>The deterministic client operation timestamp used by socket tests.</summary>
    private static readonly DateTimeOffset OperationTimestampUtc = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies the runnable ASP.NET host accepts a real loopback HTTP transport connection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationRunsPortableEndpointOnLoopbackSocket()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path);

        await using var app = CollaborationServerExample.CreateWebApplication(options);
        await app.StartAsync().ConfigureAwait(false);
        var address = GetBoundAddress(app.Services);
        using var httpClient = CreateHttpClient(address, Token);
        await using var adapter = CreateAdapter(httpClient, address);

        await using var session = await ConnectAsync(adapter, Client, DeliveryGuarantee.ExactlyOnce).ConfigureAwait(false);

        var features = session.NegotiatedCapabilities.Features;
        await Assert.That((features & RemoteTransportCapabilities.BatchPush) == RemoteTransportCapabilities.BatchPush).IsTrue();
        await Assert.That((features & RemoteTransportCapabilities.ReceiveAcknowledgements) == RemoteTransportCapabilities.ReceiveAcknowledgements).IsTrue();
        await Assert.That(
            (features & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) == RemoteTransportCapabilities.AtomicApplyAndAcknowledge).IsTrue();
        await app.StopAsync().ConfigureAwait(false);
        await Assert.That(File.Exists(lease.Path)).IsTrue();
    }

    /// <summary>Verifies the runnable ASP.NET host serves the portable endpoint under a mounted route.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationRunsPortableEndpointUnderMountedPathBaseOnLoopbackSocket()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path) with { PathBase = MountedPathBase };

        await using var app = CollaborationServerExample.CreateWebApplication(options);
        await app.StartAsync().ConfigureAwait(false);
        var mountedAddress = CreateMountedAddress(GetBoundAddress(app.Services), MountedPathBase);
        using var httpClient = CreateHttpClient(mountedAddress, Token);
        await using var adapter = CreateAdapter(httpClient, mountedAddress);

        await using var session = await ConnectAsync(adapter, Client).ConfigureAwait(false);

        await Assert.That(session.NegotiatedCapabilities.MaximumBatchOperations).IsEqualTo(options.MaximumBatchOperations);
        await app.StopAsync().ConfigureAwait(false);
        await Assert.That(File.Exists(lease.Path)).IsTrue();
    }

    /// <summary>Verifies the runnable server pushes, receives, ACKs and resumes from the SQLite journal after restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationPushesReceivesAcknowledgesAndRestartsFromSqliteJournal()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path);
        var subscriptionId = SubscriptionId.New();
        var first = CreateActivityOperation(FirstClientSequence, "first");

        string firstCursor;
        await using (var app = CollaborationServerExample.CreateWebApplication(options))
        {
            await app.StartAsync().ConfigureAwait(false);
            var firstBatch = await PushReceiveAndAcknowledgeAsync(app, Token, Client, first, subscriptionId, null).ConfigureAwait(false);
            await AssertSingleEventFromOperationAsync(firstBatch, first, Client).ConfigureAwait(false);
            firstCursor = firstBatch.NextCursor;
            await app.StopAsync().ConfigureAwait(false);
        }

        var second = CreateActivityOperation(SecondClientSequence, "second");
        await using var restartedApp = CollaborationServerExample.CreateWebApplication(options);
        await restartedApp.StartAsync().ConfigureAwait(false);
        var address = GetBoundAddress(restartedApp.Services);
        using var httpClient = CreateHttpClient(address, Token);
        await using var adapter = CreateAdapter(httpClient, address);
        await using var session = await ConnectAsync(adapter, Client).ConfigureAwait(false);
        var push = await session.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None).ConfigureAwait(false);
        await AssertPushAcceptedAsync(push, second).ConfigureAwait(false);

        var replayBatch = await ReceiveOneBatchAsync(session, subscriptionId, null).ConfigureAwait(false);
        await Assert.That(replayBatch.Events).Count().IsEqualTo(ReplayEventCount);
        var secondBatch = await ReceiveOneBatchAsync(session, subscriptionId, firstCursor).ConfigureAwait(false);
        await session.AcknowledgeAsync(
            new(subscriptionId, CollaborationStreamRegistrations.ActivityStream, secondBatch.NextCursor),
            CancellationToken.None).ConfigureAwait(false);

        await Assert.That(secondBatch.Events).Count().IsEqualTo(1);
        await AssertSingleEventFromOperationAsync(secondBatch, second, Client).ConfigureAwait(false);
        await restartedApp.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies forged tenant hints do not let one token read another tenant's stream events.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationUsesAuthenticatedTenantInsteadOfTenantHint()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path) with { Credentials = [new(Token, Tenant, Client), new(TokenB, TenantB, ClientB)] };
        var first = CreateActivityOperation(FirstClientSequence, "tenant-a");
        var second = CreateActivityOperation(FirstClientSequence, "tenant-b");

        await using var app = CollaborationServerExample.CreateWebApplication(options);
        await app.StartAsync().ConfigureAwait(false);
        var firstPush = await PushOnlyAsync(app, Token, Client, first).ConfigureAwait(false);
        await Assert.That(firstPush.Operations[0].Kind).IsNotEqualTo(OperationResultKind.Rejected);
        var secondBatch = await PushReceiveAndAcknowledgeAsync(app, TokenB, ClientB, second, SubscriptionId.New(), null).ConfigureAwait(false);

        await Assert.That(secondBatch.Events).Count().IsEqualTo(1);
        await AssertSingleEventFromOperationAsync(secondBatch, second, ClientB).ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies the runnable ASP.NET host rejects unknown development tokens before protocol handling.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the expected transport exception is not captured.</exception>
    [Test]
    public async Task CreateWebApplicationRejectsUnknownDevelopmentTokenOnLoopbackSocket()
    {
        using var lease = new DatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateOptions(lease.Path));
        await app.StartAsync().ConfigureAwait(false);
        var address = GetBoundAddress(app.Services);
        using var httpClient = CreateHttpClient(address, UnknownToken);
        await using var adapter = CreateAdapter(httpClient, address);

        var exception = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(() =>
            ConnectAsync(adapter, Client).AsTask())
            ?? throw new InvalidOperationException("Unknown development tokens should fail with a transport exception.");

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Authentication);
        await Assert.That(exception.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies invalid activity payloads are rejected through the real ASP.NET and SQLite-backed server path.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationRejectsInvalidActivityPayloadOnLoopbackSocket()
    {
        using var lease = new DatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateOptions(lease.Path));
        await app.StartAsync().ConfigureAwait(false);
        var operation = CreateActivityOperation(
            FirstClientSequence,
            ActivityPayloadTestFactory.CreateEnvelope("""{"status":42}"""));

        var push = await PushOnlyAsync(app, Token, Client, operation).ConfigureAwait(false);

        await Assert.That(push.Operations).Count().IsEqualTo(1);
        await Assert.That(push.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(push.Operations[0].ReasonCode).IsEqualTo(InvalidStatusReasonCode);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies the runnable ASP.NET host enforces negotiated batch operation limits.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the expected transport exception is not captured.</exception>
    [Test]
    public async Task CreateWebApplicationRejectsBatchesBeyondConfiguredLimitOnLoopbackSocket()
    {
        using var lease = new DatabaseLease();
        var options = CreateOptions(lease.Path) with { MaximumBatchOperations = 1 };
        await using var app = CollaborationServerExample.CreateWebApplication(options);
        await app.StartAsync().ConfigureAwait(false);
        var address = GetBoundAddress(app.Services);
        using var httpClient = CreateHttpClient(address, Token);
        await using var adapter = CreateAdapter(httpClient, address);
        await using var session = await ConnectAsync(adapter, Client).ConfigureAwait(false);
        var first = CreateActivityOperation(FirstClientSequence, "first");
        var second = CreateActivityOperation(SecondClientSequence, "second");

        var exception = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(() =>
            session.PushAsync(new(Guid.NewGuid(), [first, second]), CancellationToken.None).AsTask())
            ?? throw new InvalidOperationException("Oversized batches should fail with a transport exception.");

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies the health endpoint is served by the ASP.NET host without protocol authentication.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationServesHealthCheckWithoutToken()
    {
        using var lease = new DatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateOptions(lease.Path));
        await app.StartAsync().ConfigureAwait(false);
        await using var clientServices = new ServiceCollection()
            .AddHttpClient(nameof(CreateWebApplicationServesHealthCheckWithoutToken), static (_, _) => { })
            .Services
            .BuildServiceProvider();
        using var httpClient = clientServices.GetRequiredService<IHttpClientFactory>()
            .CreateClient(nameof(CreateWebApplicationServesHealthCheckWithoutToken));
        httpClient.BaseAddress = new(GetBoundAddress(app.Services));

        using var response = await httpClient.GetAsync(new Uri("/healthz", UriKind.Relative), CancellationToken.None).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(CancellationToken.None).ConfigureAwait(false);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(body).IsEqualTo("ok");
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies protocol routes require host authentication even when the ASP.NET endpoint is reachable.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateWebApplicationRejectsProtocolRequestWithoutToken()
    {
        using var lease = new DatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateOptions(lease.Path));
        await app.StartAsync().ConfigureAwait(false);
        await using var clientServices = new ServiceCollection()
            .AddHttpClient(nameof(CreateWebApplicationRejectsProtocolRequestWithoutToken), static (_, _) => { })
            .Services
            .BuildServiceProvider();
        using var httpClient = clientServices.GetRequiredService<IHttpClientFactory>()
            .CreateClient(nameof(CreateWebApplicationRejectsProtocolRequestWithoutToken));
        httpClient.BaseAddress = new(GetBoundAddress(app.Services));

        using var response = await httpClient.PostAsync(new Uri("/oc/connect", UriKind.Relative), null, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Creates bounded loopback options for a test database.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The configured server options.</returns>
    private static CollaborationServerOptions CreateOptions(string databasePath) =>
        new()
        {
            ListenUri = new("http://127.0.0.1:0"),
            DatabasePath = databasePath,
            Credentials = [new(Token, Tenant, Client)],
            LongPollTimeout = TimeSpan.FromSeconds(1),
            EmptyPollDelay = TimeSpan.FromMilliseconds(EmptyPollDelayMilliseconds),
        };

    /// <summary>Pushes one operation, receives one batch and acknowledges its next cursor.</summary>
    /// <param name="app">The running web application.</param>
    /// <param name="token">The development token used for authentication.</param>
    /// <param name="clientId">The expected client identifier.</param>
    /// <param name="operation">The operation to push.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="cursor">The optional client-owned resume cursor.</param>
    /// <returns>The received event batch.</returns>
    private static async ValueTask<RemoteEventBatch> PushReceiveAndAcknowledgeAsync(
        WebApplication app,
        string token,
        string clientId,
        SyncOperation operation,
        SubscriptionId subscriptionId,
        string? cursor)
    {
        var address = GetBoundAddress(app.Services);
        using var httpClient = CreateHttpClient(address, token);
        await using var adapter = CreateAdapter(httpClient, address);
        await using var session = await ConnectAsync(adapter, clientId).ConfigureAwait(false);
        var push = await session.PushAsync(new(Guid.NewGuid(), [operation]), CancellationToken.None).ConfigureAwait(false);
        await AssertPushAcceptedAsync(push, operation).ConfigureAwait(false);
        var batch = await ReceiveOneBatchAsync(session, subscriptionId, cursor).ConfigureAwait(false);
        await session.AcknowledgeAsync(
            new(subscriptionId, CollaborationStreamRegistrations.ActivityStream, batch.NextCursor),
            CancellationToken.None).ConfigureAwait(false);
        return batch;
    }

    /// <summary>Pushes one operation without receiving from the server.</summary>
    /// <param name="app">The running web application.</param>
    /// <param name="token">The development token used for authentication.</param>
    /// <param name="clientId">The expected client identifier.</param>
    /// <param name="operation">The operation to push.</param>
    /// <returns>The synchronization result.</returns>
    private static async ValueTask<RemoteSyncResult> PushOnlyAsync(
        WebApplication app,
        string token,
        string clientId,
        SyncOperation operation)
    {
        var address = GetBoundAddress(app.Services);
        using var httpClient = CreateHttpClient(address, token);
        await using var adapter = CreateAdapter(httpClient, address);
        await using var session = await ConnectAsync(adapter, clientId).ConfigureAwait(false);
        return await session.PushAsync(new(Guid.NewGuid(), [operation]), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Asserts a push result accepted one expected operation and advanced a server cursor.</summary>
    /// <param name="push">The push result.</param>
    /// <param name="operation">The pushed operation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPushAcceptedAsync(RemoteSyncResult push, SyncOperation operation)
    {
        await Assert.That(push.Operations).Count().IsEqualTo(1);
        await Assert.That(push.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(push.Operations[0].Kind).IsNotEqualTo(OperationResultKind.Rejected);
        await Assert.That(push.ServerCursor).IsNotNull();
    }

    /// <summary>Receives one event batch from the activity stream.</summary>
    /// <param name="session">The active remote transport session.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="cursor">The optional client-owned resume cursor.</param>
    /// <returns>The first event batch.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no batch is returned before the receive timeout.</exception>
    private static async ValueTask<RemoteEventBatch> ReceiveOneBatchAsync(
        IRemoteTransportSession session,
        SubscriptionId subscriptionId,
        string? cursor)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(ReceiveTimeoutSeconds));
        var request = new RemoteSubscribeRequest(
            CollaborationStreamRegistrations.ActivityStream,
            subscriptionId,
            cursor,
            StartPosition.FromSequence(0));
        await using var enumerator = session.SubscribeAsync(request, cancellation.Token).GetAsyncEnumerator(CancellationToken.None);
        if (await enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            return enumerator.Current;
        }

        throw new InvalidOperationException("The server did not return an activity event batch.");
    }

    /// <summary>Asserts a received batch contains exactly the expected operation event.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="operation">The expected operation.</param>
    /// <param name="expectedClientId">The authenticated client expected in event origin.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the server event did not include origin metadata.</exception>
    private static async Task AssertSingleEventFromOperationAsync(RemoteEventBatch batch, SyncOperation operation, string expectedClientId)
    {
        await Assert.That(batch.Events).Count().IsEqualTo(1);
        var remoteEvent = batch.Events[0];
        var origin = remoteEvent.Origin ?? throw new InvalidOperationException("The server event did not include origin metadata.");
        await Assert.That(remoteEvent.CausedByOperationId).IsEqualTo(operation.OperationId);
        await Assert.That(origin.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(origin.ClientId).IsEqualTo(expectedClientId);
        using var document = JsonDocument.Parse(remoteEvent.Payload.Payload.ToArray());
        await Assert.That(document.RootElement.GetProperty(StatusPropertyName).GetString()).IsEqualTo(ReadStatus(operation));
    }

    /// <summary>Creates an authenticated HTTP transport session.</summary>
    /// <param name="adapter">The HTTP transport adapter.</param>
    /// <param name="clientId">The client identifier placed in the portable connect request.</param>
    /// <returns>The connected session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<IRemoteTransportSession> ConnectAsync(HttpRemoteTransportAdapter adapter, string clientId) =>
        ConnectAsync(adapter, clientId, DeliveryGuarantee.AtLeastOnce);

    /// <summary>Creates an authenticated HTTP transport session for one delivery guarantee.</summary>
    /// <param name="adapter">The HTTP transport adapter.</param>
    /// <param name="clientId">The client identifier placed in the portable connect request.</param>
    /// <param name="guarantee">The delivery guarantee requested by the portable client.</param>
    /// <returns>The connected session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<IRemoteTransportSession> ConnectAsync(
        HttpRemoteTransportAdapter adapter,
        string clientId,
        DeliveryGuarantee guarantee) =>
        adapter.ConnectAsync(
            new(
                new(new Version(ProtocolMajorVersion, ProtocolMinorVersion), new Version(ProtocolMajorVersion, ProtocolMinorVersion)),
                new(clientId, TenantHint),
                [guarantee]),
            CancellationToken.None);

    /// <summary>Creates an HTTP client with the configured development token.</summary>
    /// <param name="address">The bound loopback address.</param>
    /// <param name="token">The development token.</param>
    /// <returns>The configured HTTP client.</returns>
    private static HttpClient CreateHttpClient(string address, string token)
    {
        var httpClient = new HttpClient { BaseAddress = new(address) };
        _ = httpClient.DefaultRequestHeaders.TryAddWithoutValidation(DevelopmentCredentialStore.TokenHeaderName, token);
        return httpClient;
    }

    /// <summary>Creates an HTTP transport adapter for a loopback address.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="address">The bound loopback address.</param>
    /// <returns>The configured transport adapter.</returns>
    private static HttpRemoteTransportAdapter CreateAdapter(HttpClient httpClient, string address) =>
        new(new() { HttpClient = httpClient, BaseAddress = new(address), AllowInsecureLoopbackHttp = true });

    /// <summary>Creates a loopback address that targets a mounted path base.</summary>
    /// <param name="address">The bound loopback address.</param>
    /// <param name="pathBase">The relative path base.</param>
    /// <returns>The mounted loopback address.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateMountedAddress(string address, string pathBase) =>
        new Uri(new Uri(EnsureTrailingSlash(address)), $"{pathBase}/").ToString();

    /// <summary>Ensures a base address ends with a slash before relative URI composition.</summary>
    /// <param name="address">The address to inspect.</param>
    /// <returns>The slash-terminated address.</returns>
    private static string EnsureTrailingSlash(string address) =>
        address.EndsWith('/') ? address : $"{address}/";

    /// <summary>Creates a custom activity operation for the public socket tests.</summary>
    /// <param name="sequence">The per-client sequence.</param>
    /// <param name="status">The activity status.</param>
    /// <returns>The custom activity operation.</returns>
    private static SyncOperation CreateActivityOperation(long sequence, string status) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = sequence,
            TimestampUtc = OperationTimestampUtc,
            Type = SyncOperationType.Custom,
            Payload = ActivityPayloads.Create($$"""{"status":"{{status}}"}"""),
            Policy = SyncOperation.DefaultPolicy with { ConflictPolicy = ConflictPolicy.Custom },
        };

    /// <summary>Creates a custom activity operation with a supplied payload envelope.</summary>
    /// <param name="sequence">The per-client sequence.</param>
    /// <param name="payload">The operation payload.</param>
    /// <returns>The custom activity operation.</returns>
    private static SyncOperation CreateActivityOperation(long sequence, PayloadEnvelope payload) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = sequence,
            TimestampUtc = OperationTimestampUtc,
            Type = SyncOperationType.Custom,
            Payload = payload,
            Policy = SyncOperation.DefaultPolicy with { ConflictPolicy = ConflictPolicy.Custom },
        };

    /// <summary>Reads the status value from an operation payload.</summary>
    /// <param name="operation">The operation to inspect.</param>
    /// <returns>The status value.</returns>
    private static string? ReadStatus(SyncOperation operation)
    {
        using var document = JsonDocument.Parse(operation.Payload.Payload.ToArray());
        return document.RootElement.GetProperty(StatusPropertyName).GetString();
    }

    /// <summary>Reads the first address reported by the ASP.NET server.</summary>
    /// <param name="services">The host service provider.</param>
    /// <returns>The bound HTTP address.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the server does not report a usable address.</exception>
    private static string GetBoundAddress(IServiceProvider services)
    {
        var server = services.GetRequiredService<IServer>();
        var feature = server.Features.Get<IServerAddressesFeature>()
            ?? throw new InvalidOperationException("The server address feature was not available.");
        return feature.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("The server did not report a bound address.");
    }

    /// <summary>Owns a temporary SQLite database path for a single test.</summary>
    private sealed class DatabaseLease : IDisposable
    {
        /// <summary>The temporary directory containing the database file.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="DatabaseLease"/> class.</summary>
        internal DatabaseLease()
        {
            _directory = OwnedTempDirectory.Create("rxui-oc-server-example-");
            Path = System.IO.Path.Combine(_directory, "journal.db");
        }

        /// <summary>Gets the leased database file path.</summary>
        internal string Path { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);
    }
}
