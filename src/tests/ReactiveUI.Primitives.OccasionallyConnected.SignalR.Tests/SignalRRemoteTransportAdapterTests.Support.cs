// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Real loopback server support for the SignalR client tests.</summary>
public sealed partial class SignalRRemoteTransportAdapterTests
{
    /// <summary>The trusted client name.</summary>
    private const string ClientName = "client";

    /// <summary>The body and batch byte bound.</summary>
    private const int MaximumBodyBytes = 1024 * 1024;

    /// <summary>The default connection capacity.</summary>
    private const int SessionCapacity = 8;

    /// <summary>The test stream.</summary>
    private static readonly StreamId Stream = new("signalr-test");

    /// <summary>Builds a protocol handshake with an untrusted tenant hint.</summary>
    /// <param name="client">The requested client identity.</param>
    /// <returns>The handshake request.</returns>
    private static TransportConnectRequest ConnectRequest(string client = ClientName) =>
        new(new(new(1, 0), new(1, 0)), new(client, "forged-tenant"), [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Builds one valid operation.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch Batch(long sequence = 1) => new(Guid.NewGuid(), [new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = TimeProvider.System.GetUtcNow(),
        Type = SyncOperationType.Append,
        Payload = Payload(),
    }]);

    /// <summary>Builds a valid small payload.</summary>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope Payload() => new("test", 1, "application/json", "{}"u8.ToArray(), "test-hash");

    /// <summary>Reads one page through the real SignalR connection.</summary>
    /// <param name="session">The connection session.</param>
    /// <param name="subscription">The logical subscription.</param>
    /// <param name="cursor">The resume cursor.</param>
    /// <returns>The page.</returns>
    private static async Task<RemoteEventBatch> ReadAsync(
        IRemoteTransportSession session,
        SubscriptionId subscription,
        string? cursor = null)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(DeadlineSeconds));
        await using var enumerator = session.SubscribeAsync(
            new(Stream, subscription, cursor, StartPosition.FromSequence(0)),
            deadline.Token).GetAsyncEnumerator(deadline.Token);
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        return enumerator.Current;
    }

    /// <summary>Owns a real Kestrel server and core journal.</summary>
    internal sealed class TestServer : IAsyncDisposable
    {
        /// <summary>The running host.</summary>
        private readonly WebApplication _app;

        /// <summary>The core server hub.</summary>
        private readonly ServerStreamHub _hub;

        /// <summary>Initializes a new instance of the <see cref="TestServer"/> class.</summary>
        /// <param name="app">The host.</param>
        /// <param name="hub">The core server.</param>
        /// <param name="domain">The domain recorder.</param>
        /// <param name="endpoint">The bound address.</param>
        private TestServer(WebApplication app, ServerStreamHub hub, DomainHandler domain, Uri endpoint)
        {
            _app = app;
            _hub = hub;
            Domain = domain;
            Endpoint = endpoint;
        }

        /// <summary>Gets the domain recorder.</summary>
        internal DomainHandler Domain { get; }

        /// <summary>Gets the hub address.</summary>
        internal Uri Endpoint { get; }

        /// <summary>Gets the shared production endpoint.</summary>
        internal SignalRServerEndpoint ProtocolEndpoint => _app.Services.GetRequiredService<SignalRServerEndpoint>();

        /// <summary>Gets the borrowed real core server hub.</summary>
        internal IServerStreamHub CoreHub => _hub;

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await ProtocolEndpoint.DisposeAsync();
            await _app.DisposeAsync();
            await _hub.DisposeAsync();
        }

        /// <summary>Starts a real server.</summary>
        /// <param name="maximumOperations">The server batch limit.</param>
        /// <param name="dropPushResponse">Whether one committed push response is lost.</param>
        /// <param name="peerFailure">The malicious peer response to inject.</param>
        /// <returns>The server.</returns>
#if NET8_0 && !NET9_0_OR_GREATER
        [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
        internal static async Task<TestServer> StartAsync(
            int maximumOperations = 100,
            bool dropPushResponse = false,
            SignalRPeerFailure peerFailure = SignalRPeerFailure.None)
        {
            var domain = new DomainHandler();
            var policy = new AuthorizationPolicy();
            var hub = CreateHub(policy, domain);
            var builder = WebApplication.CreateBuilder();
            _ = builder.Logging.ClearProviders();
            _ = builder.WebHost.ConfigureKestrel(static options => options.Listen(IPAddress.Loopback, 0));
            _ = builder.Services.AddOccasionallyConnectedSignalR();
            _ = builder.Services.AddSingleton(CreateEndpoint(hub, policy, maximumOperations))
                .AddSingleton(new LostResponseState { DropPushResponse = dropPushResponse, PeerFailure = peerFailure });
            var app = builder.Build();
            _ = app.Use(AuthenticateTestRequest);
            if (dropPushResponse || peerFailure != SignalRPeerFailure.None)
            {
                _ = app.MapHub<LostResponseHub>("/sync");
            }
            else
            {
                _ = app.MapOccasionallyConnectedSignalR("/sync");
            }

            await app.StartAsync();
            var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
            return new(app, hub, domain, new Uri($"{address}/sync"));
        }

        /// <summary>Creates an authenticated adapter.</summary>
        /// <param name="authenticate">Whether credentials are supplied.</param>
        /// <param name="maximumPayloadBytes">The client payload limit.</param>
        /// <param name="maximumSessions">The physical session limit.</param>
        /// <returns>The owned adapter.</returns>
        internal SignalRRemoteTransportAdapter Adapter(
            bool authenticate = true,
            int maximumPayloadBytes = MaximumBodyBytes,
            int maximumSessions = SessionCapacity) => new(new()
            {
                Endpoint = Endpoint,
                AllowInsecureLoopbackHttp = true,
                MaximumSessions = maximumSessions,
                ConfigureConnection = options =>
                {
                    if (authenticate)
                    {
                        options.Headers.Add("Authorization", "Bearer test");
                    }
                },
                ConfigureProtocol = client => new()
                { HttpClient = client, BaseAddress = new("https://signalr.invalid/"), MaximumPayloadBytes = maximumPayloadBytes },
            });

        /// <summary>Stops all physical connections without initiating reconnect.</summary>
        /// <returns>The stop task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task StopAsync() => _app.StopAsync();

        /// <summary>Authenticates test credentials or injects a negotiation status.</summary>
        /// <param name="context">The request.</param>
        /// <param name="next">The following middleware.</param>
        /// <returns>The request task.</returns>
        private static Task AuthenticateTestRequest(HttpContext context, RequestDelegate next)
        {
            if (int.TryParse(context.Request.Headers["X-Negotiation-Failure"], out var status))
            {
                context.Response.StatusCode = status;
                return Task.CompletedTask;
            }

            if (context.Request.Headers.Authorization == "Bearer test")
            {
                context.User = new(new ClaimsIdentity([new(ClaimTypes.NameIdentifier, ClientName)], "test"));
            }

            return next(context);
        }

        /// <summary>Composes the real core journal and domain.</summary>
        /// <param name="policy">The authorization policy.</param>
        /// <param name="domain">The domain recorder.</param>
        /// <returns>The hub.</returns>
        private static ServerStreamHub CreateHub(AuthorizationPolicy policy, DomainHandler domain)
        {
            var resolver = new LastWriterWinsResolver(new() { VersionFactory = new VersionFactory() });
            return ServerStreamHub.CreateInMemory(new()
            {
                AuthorizationPolicy = policy,
                SnapshotRecoveryAuthorizationPolicy = policy,
                SnapshotRecoveryMaterializer = new Materializer(),
                ConflictHandler = new()
                {
                    Streams = [new()
                    {
                        StreamId = Stream,
                        InitialStateFactory = new InitialState(),
                        LastWriterWinsResolver = resolver,
                        MergeResolver = resolver,
                        CustomResolver = resolver,
                        DomainHandler = domain,
                    }],
                },
            });
        }

        /// <summary>Composes the real shared security and protocol endpoint.</summary>
        /// <param name="hub">The core hub.</param>
        /// <param name="policy">The authorization policy.</param>
        /// <param name="maximumOperations">The batch bound.</param>
        /// <returns>The endpoint.</returns>
        private static SignalRServerEndpoint CreateEndpoint(ServerStreamHub hub, AuthorizationPolicy policy, int maximumOperations) =>
            new(
                new()
                {
                    Hub = hub,
                    SnapshotRecoveryHub = hub,
                    DeclaredCapabilities = new(
                        new(1, 0),
                        RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.CursorResume
                        | RemoteTransportCapabilities.ReceiveAcknowledgements | RemoteTransportCapabilities.ServerIdempotency
                        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge | RemoteTransportCapabilities.SnapshotRecovery,
                        maximumOperations,
                        MaximumBodyBytes,
                        TimeSpan.FromHours(1),
                        TimeSpan.FromHours(1)),
                    MaximumBatchOperations = maximumOperations,
                    ReplayAuthorizer = policy,
                    LongPollTimeout = TimeSpan.FromSeconds(1),
                },
                static principal => principal.Identity?.IsAuthenticated == true ? new("tenant", ClientName) : null);
    }

    /// <summary>Records actual committed domain effects.</summary>
    internal sealed class DomainHandler : IServerDomainHandler
    {
        /// <summary>The domain invocation count.</summary>
        private int _calls;

        /// <summary>Gets the domain invocation count.</summary>
        internal int Calls => Volatile.Read(ref _calls);

        /// <inheritdoc/>
        public ValueTask<ServerDomainApplyResult> ApplyAsync(ServerDomainApplyContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _calls);
            return ValueTask.FromResult(new ServerDomainApplyResult
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }],
            });
        }
    }

    /// <summary>Authorizes only the trusted test stream and tenant.</summary>
    private sealed class AuthorizationPolicy :
        IServerStreamAuthorizationPolicy,
        IServerSnapshotRecoveryAuthorizationPolicy,
        IHttpReplayAuthorizer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            Scope(client, operation.StreamId);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            Scope(client, request.StreamId);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            Scope(client, acknowledgement.StreamId);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(context.Client.TenantId == "tenant" && context.StreamIds.All(static stream => stream == Stream));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
            ServerAuthenticatedClient client,
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken) => Scope(client, request.StreamId);

        /// <summary>Checks authorization before journal access.</summary>
        /// <param name="client">The trusted client.</param>
        /// <param name="stream">The requested stream.</param>
        /// <returns>The trusted scope.</returns>
        /// <exception cref="UnauthorizedAccessException">The stream is not authorized.</exception>
        private static ValueTask<ServerStreamAuthorizationScope> Scope(ServerAuthenticatedClient client, StreamId? stream = null) =>
            stream is not null && stream != Stream
                ? throw new UnauthorizedAccessException()
                : ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }

    /// <summary>Creates initial state.</summary>
    private sealed class InitialState : IServerInitialStateFactory
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(StreamId streamId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, "v0", Payload()));
    }

    /// <summary>Allocates a unique server version.</summary>
    private sealed class VersionFactory : IServerConflictVersionFactory
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string CreateNextVersion(ConflictContext context, SyncOperation operation) => Guid.NewGuid().ToString("N");
    }

    /// <summary>Materializes the allowlisted test contract from captured server state.</summary>
    private sealed class Materializer : IServerSnapshotMaterializer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerSnapshotMaterializationResult
            { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = context.CapturedServerState.State });
    }
}
