// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Builds a public client context over SQLite and a real hub for delivery-guarantee tests.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The loopback transport selector.</summary>
    private const int LoopbackTransport = 0;

    /// <summary>The HTTP transport selector.</summary>
    private const int HttpTransport = 1;

    /// <summary>The trusted tenant.</summary>
    private const string Tenant = "tenant-delivery";

    /// <summary>The trusted client.</summary>
    private const string Client = "client-delivery";

    /// <summary>The client store identity.</summary>
    private const string StoreIdentity = "delivery-guarantees";

    /// <summary>The input contract.</summary>
    private const string InputContract = "counter-input";

    /// <summary>The state contract.</summary>
    private const string StateContract = "counter-state";

    /// <summary>The payload content type.</summary>
    private const string TextContentType = "text/plain";

    /// <summary>The initial server version.</summary>
    private const string InitialVersion = "v0";

    /// <summary>The in-process HTTP base address.</summary>
    private const string HttpBaseAddress = "https://example.invalid/";

    /// <summary>The negotiated batch operation bound.</summary>
    private const int BatchOperations = 4;

    /// <summary>The negotiated batch byte bound.</summary>
    private const long BatchBytes = 64 * 1024;

    /// <summary>The retained bytes declared for one typed input.</summary>
    private const long TypedInputBytes = 128;

    /// <summary>The retry attempt and circuit-breaker threshold that no test reaches.</summary>
    private const int UnreachedLimit = 10_000;

    /// <summary>The fixed retry jitter sample.</summary>
    private const double RetryJitter = 0.5D;

    /// <summary>The real-time pause between pump steps, in milliseconds.</summary>
    private const int PumpPauseMilliseconds = 5;

    /// <summary>The features advertised by both peers.</summary>
    private const RemoteTransportCapabilities PeerFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge;

    /// <summary>The deterministic start instant.</summary>
    private static readonly DateTimeOffset StartUtc = new(2026, 9, 27, 8, 0, 0, TimeSpan.Zero);

    /// <summary>The real-time bound for each awaited condition.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The fake-time step used while pumping the client.</summary>
    private static readonly TimeSpan PumpStep = TimeSpan.FromMilliseconds(100);

    /// <summary>The retry delay used by every client.</summary>
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>The long retention window used when a test must not expire its guarantee.</summary>
    private static readonly TimeSpan LongRetention = TimeSpan.FromHours(1);

    /// <summary>The shared stream.</summary>
    private static readonly StreamId Stream = new("delivery/counter");

    /// <summary>The push route suffix used to recognise push requests.</summary>
    private static readonly string PushRouteSuffix = $"/{HttpRemoteTransportOptions.DefaultPushPath}";

    /// <summary>Gets the display name of a transport selector.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The display name.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string TransportName(int transport) => transport == LoopbackTransport ? "Loopback" : "Http";

    /// <summary>Creates client options with deterministic retry and the selected expiry behaviour.</summary>
    /// <param name="behavior">The exactly-once expiry behaviour.</param>
    /// <returns>The options.</returns>
    private static OccasionallyConnectedOptions CreateOptions(ExactlyOnceExpiryBehavior behavior) =>
        OccasionallyConnectedOptions.Default with
        {
            AutoStart = false,
            MaxConcurrentStreams = 1,
            ExactlyOnceExpiryBehavior = behavior,
            Batching = new() { MaximumOperations = 1, MaximumBytes = BatchBytes, MaximumDwellTime = RetryDelay, MaxInFlightBatchesPerStream = 1 },
            Retry = new() { MinimumDelay = RetryDelay, MaximumDelay = RetryDelay, MaximumRetryAttempts = UnreachedLimit, MaximumRetryAge = LongRetention },
            CircuitBreaker = new() { FailureThreshold = UnreachedLimit, OpenDuration = RetryDelay },
        };

    /// <summary>Creates the stream definition.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition() => new()
    {
        StreamId = Stream,
        Projection = new CounterProjection(),
        InputContractId = InputContract,
        StateContractId = StateContract,
        TypedInput = new() { MaximumRetainedInputBytes = TypedInputBytes },
    };

    /// <summary>Creates publish options for one delivery guarantee.</summary>
    /// <param name="guarantee">The delivery guarantee.</param>
    /// <returns>The publish options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemotePublishOptions CreatePublishOptions(DeliveryGuarantee guarantee) =>
        new() { StreamId = Stream, Durable = true, DeliveryGuarantee = guarantee };

    /// <summary>Advances fake time in small steps until a condition holds.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="description">The condition description used on timeout.</param>
    /// <returns>The pump task.</returns>
    /// <exception cref="TimeoutException">The condition did not hold before the real-time guard expired.</exception>
    private static async Task PumpUntilAsync(FakeTimeProvider clock, Func<ValueTask<bool>> condition, string description)
    {
        var started = Stopwatch.GetTimestamp();
        using var pause = new PeriodicTimer(TimeSpan.FromMilliseconds(PumpPauseMilliseconds));
        while (!await condition())
        {
            if (Stopwatch.GetElapsedTime(started) > GuardTimeout)
            {
                throw new TimeoutException($"Timed out waiting for {description}.");
            }

            clock.Advance(PumpStep);
            _ = await pause.WaitForNextTickAsync();
        }
    }

    /// <summary>Releases the first batching dwell, then waits for a lost response with the fake clock held steady.</summary>
    /// <param name="stack">The client and server stack.</param>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">No push response was lost before the real-time guard expired.</exception>
    private static async Task WaitForFirstLostAcknowledgementAsync(DeliveryStack stack, int transport)
    {
        // PublishAsync schedules the upload before returning. An overdue head runs even if the pump starts waiting later.
        stack.Clock.Advance(RetryDelay);
        var started = Stopwatch.GetTimestamp();
        using var pause = new PeriodicTimer(TimeSpan.FromMilliseconds(PumpPauseMilliseconds));
        while (stack.DroppedResponses == 0)
        {
            if (Stopwatch.GetElapsedTime(started) > GuardTimeout)
            {
                throw new TimeoutException($"Timed out waiting for {TransportName(transport)} first lost acknowledgement.");
            }

            _ = await pause.WaitForNextTickAsync();
        }
    }

    /// <summary>Advances fake time for a fixed number of pump steps so any pending retry would run.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="steps">The number of steps.</param>
    /// <returns>The pump task.</returns>
    private static async Task PumpStepsAsync(FakeTimeProvider clock, int steps)
    {
        using var pause = new PeriodicTimer(TimeSpan.FromMilliseconds(PumpPauseMilliseconds));
        for (var step = 0; step < steps; step++)
        {
            clock.Advance(PumpStep);
            _ = await pause.WaitForNextTickAsync();
        }
    }

    /// <summary>Owns one client context, its SQLite store, its transport and the SQLite hub behind it.</summary>
    private sealed class DeliveryStack : IAsyncDisposable
    {
        /// <summary>The server-side and transport parts.</summary>
        private readonly StackParts _parts;

        /// <summary>Whether the client side has been disposed.</summary>
        private int _clientDisposed;

        /// <summary>Whether another stack took ownership of the server side.</summary>
        private int _serverTransferred;

        /// <summary>Initializes a new instance of the <see cref="DeliveryStack"/> class.</summary>
        /// <param name="parts">The created parts.</param>
        /// <param name="context">The public client context.</param>
        private DeliveryStack(StackParts parts, OccasionallyConnectedContext context)
        {
            _parts = parts;
            Context = context;
            ClientStream = context.GetOrCreateStream(CreateDefinition());
            _ = context.SyncEngine.Faults.Subscribe(Faults);
            _ = context.SyncEngine.OperationStates.Subscribe(States);
        }

        /// <summary>Gets the fake clock shared by client and server.</summary>
        internal FakeTimeProvider Clock => _parts.Clock;

        /// <summary>Gets the counting, response-dropping hub wrapper.</summary>
        internal AckDroppingServerStreamHub Peer => _parts.Peer;

        /// <summary>Gets the domain handler that counts server effects.</summary>
        internal EffectCountingDomainHandler Domain => _parts.Domain;

        /// <summary>Gets the public client context.</summary>
        internal OccasionallyConnectedContext Context { get; }

        /// <summary>Gets the client stream.</summary>
        internal IOccasionallyConnectedStream<CounterState, CounterInput> ClientStream { get; }

        /// <summary>Gets the recorded faults.</summary>
        internal RecordingObserver<OccasionallyConnectedFault> Faults { get; } = new();

        /// <summary>Gets the recorded operation states.</summary>
        internal RecordingObserver<SyncOperationStatus> States { get; } = new();

        /// <summary>Gets the client database path.</summary>
        internal string ClientDatabasePath => _parts.ClientDatabasePath;

        /// <summary>Gets the number of push responses dropped after the server committed.</summary>
        internal int DroppedResponses => _parts.Handler?.DroppedResponses ?? _parts.Peer.DroppedResponses;

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await DisposeClientAsync();
            if (Volatile.Read(ref _serverTransferred) != 0)
            {
                return;
            }

            DropPushResponses(false);
            _parts.HttpClient?.Dispose();
            if (_parts.Endpoint is not null)
            {
                await _parts.Endpoint.DisposeAsync();
            }

            await _parts.Hub.DisposeAsync();
            if (_parts.Directory.Exists)
            {
                _parts.Directory.Delete(recursive: true);
            }
        }

        /// <summary>Creates a started stack.</summary>
        /// <param name="transport">The transport selector.</param>
        /// <param name="options">The client options.</param>
        /// <param name="serverRetention">The server idempotency retention advertised to the client.</param>
        /// <returns>The started stack.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Task<DeliveryStack> StartAsync(int transport, OccasionallyConnectedOptions options, TimeSpan serverRetention) =>
            StartAsync(CreateParts(transport, serverRetention), options);

        /// <summary>Disposes this client and starts a new client over the same database, transport kind and server.</summary>
        /// <param name="options">The client options.</param>
        /// <returns>The restarted stack, which now owns the server side.</returns>
        internal async Task<DeliveryStack> RestartClientAsync(OccasionallyConnectedOptions options)
        {
            await DisposeClientAsync();
            _ = Interlocked.Exchange(ref _serverTransferred, 1);
            IRemoteTransportAdapter transport = _parts.HttpClient is { } httpClient
                ? CreateHttpAdapter(httpClient, _parts.Clock)
                : CreateLoopbackAdapter(_parts.Peer, _parts.Capabilities);
            var store = new SqliteLocalStoreAdapter(_parts.ClientDatabasePath, new() { TimeProvider = _parts.Clock });
            return await StartAsync(_parts with { Transport = transport, Store = store }, options);
        }

        /// <summary>Drops every push response after the server commits until disabled.</summary>
        /// <param name="drop">Whether responses are dropped.</param>
        internal void DropPushResponses(bool drop)
        {
            if (_parts.Handler is { } handler)
            {
                handler.DropPushResponses = drop;
                return;
            }

            _parts.Peer.DropPushResponses = drop;
        }

        /// <summary>Reads the durable client status.</summary>
        /// <param name="operationId">The operation.</param>
        /// <returns>The durable status.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<SyncOperationStatus?> GetStatusAsync(OperationId operationId) =>
            _parts.Store.GetOperationStatusAsync(operationId, CancellationToken.None);

        /// <summary>Checks whether a fault with a code has been recorded.</summary>
        /// <param name="code">The fault code.</param>
        /// <returns><see langword="true"/> when the fault was recorded.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HasFault(string code) => Faults.Values.Any(fault => fault.Code == code);

        /// <summary>Stops and disposes the client context, transport and store, keeping the server and files.</summary>
        /// <returns>The disposal task.</returns>
        internal async Task DisposeClientAsync()
        {
            if (Interlocked.Exchange(ref _clientDisposed, 1) != 0)
            {
                return;
            }

            await Context.DisposeAsync();
            await _parts.Transport.DisposeAsync();
            await _parts.Store.DisposeAsync();
        }

        /// <summary>Starts a client context over created parts.</summary>
        /// <param name="parts">The parts.</param>
        /// <param name="options">The client options.</param>
        /// <returns>The started stack.</returns>
        private static async Task<DeliveryStack> StartAsync(StackParts parts, OccasionallyConnectedOptions options)
        {
            var context = new OccasionallyConnectedBuilder()
                .UseClient(new(Client, Tenant))
                .UseBorrowedStore(parts.Store)
                .UseBorrowedTransport(parts.Transport)
                .UseSerializer(new TextPayloadSerializer())
                .UseStoreIdentity(StoreIdentity)
                .UseTimeProvider(parts.Clock)
                .UseRetryRandomSource(new FixedRetryRandomSource(RetryJitter))
                .UseOptions(options)
                .Build();
            var stack = new DeliveryStack(parts, context);
            await context.StartAsync(CancellationToken.None);
            return stack;
        }

        /// <summary>Creates the hub, transport and store parts.</summary>
        /// <param name="transport">The transport selector.</param>
        /// <param name="serverRetention">The server idempotency retention advertised to the client.</param>
        /// <returns>The parts.</returns>
        private static StackParts CreateParts(int transport, TimeSpan serverRetention)
        {
            var clock = new FakeTimeProvider(StartUtc);
            var directory = PhysicalTempDirectory.Create("rxui-oc-delivery-");
            var domain = new EffectCountingDomainHandler();
            var hub = ServerStreamHub.CreateSqlite(Path.Combine(directory.FullName, "server.db"), CreateHubOptions(domain, clock));
            var peer = new AckDroppingServerStreamHub(hub);
            var capabilities = new NegotiatedCapabilities(new(1, 0), PeerFeatures, BatchOperations, BatchBytes, serverRetention, ClientInboxRetentionRequired: null);
            var clientDatabasePath = Path.Combine(directory.FullName, "client.db");
            var store = new SqliteLocalStoreAdapter(clientDatabasePath, new() { TimeProvider = clock });
            if (transport == LoopbackTransport)
            {
                return new(clock, directory, clientDatabasePath, capabilities, hub, peer, domain, null, null, null, CreateLoopbackAdapter(peer, capabilities), store);
            }

            var endpoint = new HttpServerEndpoint(new()
            {
                Hub = peer,
                DeclaredCapabilities = capabilities,
                ReplayAuthorizer = AllowReplayAuthorizer.Instance,
                ReplayProtection = new() { TimeProvider = clock },
                TimeProvider = clock,
            });
            var handler = new AckDroppingEndpointHandler(endpoint);
            var httpClient = new HttpClient(handler);
            return new(clock, directory, clientDatabasePath, capabilities, hub, peer, domain, endpoint, httpClient, handler, CreateHttpAdapter(httpClient, clock), store);
        }

        /// <summary>Creates a loopback adapter over the hub wrapper.</summary>
        /// <param name="peer">The hub wrapper.</param>
        /// <param name="capabilities">The peer capabilities.</param>
        /// <returns>The adapter.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static LoopbackTransportAdapter CreateLoopbackAdapter(AckDroppingServerStreamHub peer, NegotiatedCapabilities capabilities) =>
            new(new() { Hub = peer, AuthenticatedClient = new(Tenant, Client), PeerCapabilities = capabilities });

        /// <summary>Creates an HTTP adapter over the in-process host.</summary>
        /// <param name="httpClient">The HTTP client bound to the host handler.</param>
        /// <param name="clock">The clock.</param>
        /// <returns>The adapter.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static HttpRemoteTransportAdapter CreateHttpAdapter(HttpClient httpClient, TimeProvider clock) =>
            new(new() { HttpClient = httpClient, BaseAddress = new(HttpBaseAddress), ReplayProtection = new() { TimeProvider = clock }, TimeProvider = clock });

        /// <summary>Creates hub options that register the shared stream.</summary>
        /// <param name="domain">The domain handler.</param>
        /// <param name="clock">The clock.</param>
        /// <returns>The hub options.</returns>
        private static ServerStreamHubOptions CreateHubOptions(EffectCountingDomainHandler domain, TimeProvider clock)
        {
            var resolver = new LastWriterWinsResolver(new() { VersionFactory = NumberedVersionFactory.Instance });
            return new()
            {
                AuthorizationPolicy = TrustedClientAuthorizationPolicy.Instance,
                ConflictHandler = new()
                {
                    Streams =
                    [
                        new()
                        {
                            StreamId = Stream,
                            InitialStateFactory = EmptyInitialStateFactory.Instance,
                            LastWriterWinsResolver = resolver,
                            MergeResolver = resolver,
                            CustomResolver = resolver,
                            DomainHandler = domain,
                        },
                    ],
                },
                TimeProvider = clock,
            };
        }
    }

    /// <summary>Delegates to a real hub, records pushed operations and loses committed push results on request.</summary>
    /// <param name="inner">The real hub.</param>
    private sealed class AckDroppingServerStreamHub(ServerStreamHub inner) : IServerStreamHub
    {
        /// <summary>The pushed operation identifiers in arrival order.</summary>
        private readonly List<OperationId> _pushed = [];

        /// <summary>The number of dropped responses.</summary>
        private int _dropped;

        /// <summary>Whether committed push results are lost.</summary>
        private volatile bool _dropPushResponses;

        /// <summary>Gets or sets a value indicating whether committed push results are lost.</summary>
        internal bool DropPushResponses
        {
            get => _dropPushResponses;
            set => _dropPushResponses = value;
        }

        /// <summary>Gets the number of dropped responses.</summary>
        internal int DroppedResponses => Volatile.Read(ref _dropped);

        /// <summary>Gets a snapshot of the pushed operation identifiers.</summary>
        internal IReadOnlyList<OperationId> Pushed
        {
            get
            {
                lock (_pushed)
                {
                    return [.. _pushed];
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            lock (_pushed)
            {
                _pushed.AddRange(batch.Operations.Select(static operation => operation.OperationId));
            }

            var applied = await inner.ApplyOperationsAsync(batch, client, cancellationToken);
            if (!_dropPushResponses)
            {
                return applied;
            }

            _ = Interlocked.Increment(ref _dropped);
            throw new IOException("The push response was lost after the server committed.");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken) =>
            inner.AcknowledgeAsync(acknowledgement, client, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken) =>
            inner.SubscribeStreamAsync(request, client, cancellationToken);
    }

    /// <summary>Hosts the endpoint in process and loses committed push responses on request, like a dropped connection.</summary>
    /// <param name="endpoint">The endpoint.</param>
    private sealed class AckDroppingEndpointHandler(HttpServerEndpoint endpoint) : HttpMessageHandler
    {
        /// <summary>The number of dropped responses.</summary>
        private int _dropped;

        /// <summary>Whether committed push responses are lost.</summary>
        private volatile bool _dropPushResponses;

        /// <summary>Gets or sets a value indicating whether committed push responses are lost.</summary>
        internal bool DropPushResponses
        {
            get => _dropPushResponses;
            set => _dropPushResponses = value;
        }

        /// <summary>Gets the number of dropped responses.</summary>
        internal int DroppedResponses => Volatile.Read(ref _dropped);

        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await endpoint.HandleAsync(request, new(Tenant, Client), cancellationToken);
            if (cancellationToken.IsCancellationRequested)
            {
                response.Dispose();
                throw new OperationCanceledException(cancellationToken);
            }

            var isPush = request.Method == HttpMethod.Post
                && request.RequestUri?.AbsolutePath.EndsWith(PushRouteSuffix, StringComparison.Ordinal) == true;
            if (!isPush || !_dropPushResponses || !response.IsSuccessStatusCode)
            {
                return response;
            }

            response.Dispose();
            _ = Interlocked.Increment(ref _dropped);
            throw new HttpRequestException("The push response was lost after the server committed.");
        }
    }

    /// <summary>Counts domain effects per operation and emits one event per accepted operation.</summary>
    private sealed class EffectCountingDomainHandler : IServerDomainHandler
    {
        /// <summary>The effect count per operation.</summary>
        private readonly Dictionary<OperationId, int> _effects = [];

        /// <inheritdoc/>
        public ValueTask<ServerDomainApplyResult> ApplyAsync(ServerDomainApplyContext context, CancellationToken cancellationToken)
        {
            lock (_effects)
            {
                _effects[context.Operation.OperationId] = EffectsFor(context.Operation.OperationId) + 1;
            }

            var state = new ServerState(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload);
            ServerProducedEvent[] events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }];
            return ValueTask.FromResult(new ServerDomainApplyResult { NewState = state, Events = events });
        }

        /// <summary>Gets the number of domain effects for an operation.</summary>
        /// <param name="operationId">The operation.</param>
        /// <returns>The effect count.</returns>
        internal int EffectsFor(OperationId operationId)
        {
            lock (_effects)
            {
                return _effects.TryGetValue(operationId, out var count) ? count : 0;
            }
        }
    }

    /// <summary>Authorizes the trusted principal supplied by the host transport.</summary>
    private sealed class TrustedClientAuthorizationPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>Gets the shared policy.</summary>
        internal static TrustedClientAuthorizationPolicy Instance { get; } = new();

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
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) => Scope(client);

        /// <summary>Creates the scope for the trusted principal.</summary>
        /// <param name="client">The trusted principal.</param>
        /// <returns>The scope.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ValueTask<ServerStreamAuthorizationScope> Scope(ServerAuthenticatedClient client) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }

    /// <summary>Creates the initial state for the shared stream.</summary>
    private sealed class EmptyInitialStateFactory : IServerInitialStateFactory
    {
        /// <summary>Gets the shared factory.</summary>
        internal static EmptyInitialStateFactory Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(StreamId streamId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, InitialVersion, new(StateContract, 1, TextContentType, "0"u8.ToArray(), "hash-0")));
    }

    /// <summary>Creates the next numbered server version.</summary>
    private sealed class NumberedVersionFactory : IServerConflictVersionFactory
    {
        /// <summary>Gets the shared factory.</summary>
        internal static NumberedVersionFactory Instance { get; } = new();

        /// <inheritdoc/>
        public string CreateNextVersion(ConflictContext context, SyncOperation operation)
        {
            var current = int.Parse(context.Current.Version.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture);
            return string.Create(CultureInfo.InvariantCulture, $"v{current + 1}");
        }
    }

    /// <summary>Allows replay admission for the trusted principal.</summary>
    private sealed class AllowReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets the shared authorizer.</summary>
        internal static AllowReplayAuthorizer Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);
    }

    /// <summary>Applies counter deltas locally.</summary>
    private sealed class CounterProjection : ILocalProjection<CounterState, CounterInput>
    {
        /// <inheritdoc/>
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc/>
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) => new(state.Sum + input.Delta);

        /// <inheritdoc/>
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) => new(state.Sum + input.Delta);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Serializes counter values as invariant text.</summary>
    private sealed class TextPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc/>
        public string ContentType => TextContentType;

        /// <inheritdoc/>
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(string contractId, int schemaVersion, T value, CancellationToken cancellationToken)
        {
            var text = value switch
            {
                CounterInput input => input.Delta.ToString(CultureInfo.InvariantCulture),
                CounterState state => state.Sum.ToString(CultureInfo.InvariantCulture),
                _ => throw new InvalidOperationException("Unexpected payload type."),
            };
            return ValueTask.FromResult(new PayloadEnvelope(contractId, schemaVersion, ContentType, Encoding.UTF8.GetBytes(text), $"hash-{text}"));
        }

        /// <inheritdoc/>
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            var value = int.Parse(Encoding.UTF8.GetString(envelope.Payload.Span), CultureInfo.InvariantCulture);
            return targetType == typeof(CounterInput)
                ? ValueTask.FromResult<object>(new CounterInput(value))
                : ValueTask.FromResult<object>(new CounterState(value));
        }
    }

    /// <summary>Returns a fixed retry jitter sample.</summary>
    /// <param name="value">The sample.</param>
    private sealed class FixedRetryRandomSource(double value) : IRetryRandomSource
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double NextDouble() => value;
    }

    /// <summary>Records observed values.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>The observed values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Gets a snapshot of the observed values.</summary>
        internal IReadOnlyList<T> Values
        {
            get
            {
                lock (_values)
                {
                    return [.. _values];
                }
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_values)
            {
                _values.Add(value);
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Groups the parts used to build a <see cref="DeliveryStack"/>.</summary>
    /// <param name="Clock">The fake clock.</param>
    /// <param name="Directory">The temporary directory.</param>
    /// <param name="ClientDatabasePath">The client SQLite database path.</param>
    /// <param name="Capabilities">The peer capabilities.</param>
    /// <param name="Hub">The real hub.</param>
    /// <param name="Peer">The hub wrapper.</param>
    /// <param name="Domain">The domain handler.</param>
    /// <param name="Endpoint">The optional HTTP endpoint.</param>
    /// <param name="HttpClient">The optional HTTP client.</param>
    /// <param name="Handler">The optional HTTP host handler.</param>
    /// <param name="Transport">The client transport.</param>
    /// <param name="Store">The client store.</param>
    private sealed record StackParts(
        FakeTimeProvider Clock,
        DirectoryInfo Directory,
        string ClientDatabasePath,
        NegotiatedCapabilities Capabilities,
        ServerStreamHub Hub,
        AckDroppingServerStreamHub Peer,
        EffectCountingDomainHandler Domain,
        HttpServerEndpoint? Endpoint,
        HttpClient? HttpClient,
        AckDroppingEndpointHandler? Handler,
        IRemoteTransportAdapter Transport,
        SqliteLocalStoreAdapter Store);

    /// <summary>Test input value.</summary>
    /// <param name="Delta">The delta.</param>
    private sealed record CounterInput(int Delta);

    /// <summary>Test state value.</summary>
    /// <param name="Sum">The sum.</param>
    private sealed record CounterState(int Sum);
}
