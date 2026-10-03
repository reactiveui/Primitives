// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Builds real transport stacks over a real <see cref="ServerStreamHub"/> for the shared transport suite.</summary>
public sealed partial class IRemoteTransportAdapterTests
{
    /// <summary>The loopback adapter selector.</summary>
    private const int LoopbackTransport = 0;

    /// <summary>The HTTP adapter selector.</summary>
    private const int HttpTransport = 1;

    /// <summary>The in-memory hub journal selector.</summary>
    private const int InMemoryHub = 0;

    /// <summary>The SQLite hub journal selector.</summary>
    private const int SqliteHub = 1;

    /// <summary>The trusted tenant used by every transport stack.</summary>
    private const string Tenant = "tenant-1";

    /// <summary>The trusted client used by every transport stack.</summary>
    private const string Client = "client-1";

    /// <summary>The shared stream name.</summary>
    private const string StreamName = "conformance-stream";

    /// <summary>The payload contract used by operations and snapshots.</summary>
    private const string Contract = "contract";

    /// <summary>The payload content type.</summary>
    private const string ContentType = "application/json";

    /// <summary>The initial server version for the stream.</summary>
    private const string InitialVersion = "v0";

    /// <summary>The HTTP base address used by the in-process endpoint.</summary>
    private const string HttpBaseAddress = "https://example.invalid/";

    /// <summary>The operation bound negotiated by both peers.</summary>
    private const int NegotiatedBatchOperations = 4;

    /// <summary>The byte bound negotiated by both peers.</summary>
    private const long NegotiatedBatchBytes = 64 * 1024;

    /// <summary>The idempotency retention advertised by both peers, in minutes.</summary>
    private const int IdempotencyRetentionMinutes = 5;

    /// <summary>The client inbox retention advertised by both peers, in minutes.</summary>
    private const int InboxRetentionMinutes = 2;

    /// <summary>The default adapter-side receive event bound.</summary>
    private const int DefaultAdapterReceiveEvents = 16;

    /// <summary>The features the loopback peer advertises.</summary>
    private const RemoteTransportCapabilities LoopbackFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive
        | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The features the HTTP endpoint declares.</summary>
    private const RemoteTransportCapabilities HttpFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The deterministic start instant of every clock.</summary>
    private static readonly DateTimeOffset StartUtc = new(2026, 9, 17, 22, 0, 0, TimeSpan.Zero);

    /// <summary>The upper bound for every awaited transport step.</summary>
    private static readonly TimeSpan AwaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The shared stream identifier.</summary>
    private static readonly StreamId Stream = new(StreamName);

    /// <summary>Gets the display name of a transport selector.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The display name.</returns>
    private static string TransportName(int transport) => transport == LoopbackTransport ? "Loopback" : "Http";

    /// <summary>Creates the negotiated capabilities declared by a peer.</summary>
    /// <param name="features">The declared features.</param>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities(RemoteTransportCapabilities features) =>
        new(
            new Version(1, 0),
            features,
            NegotiatedBatchOperations,
            NegotiatedBatchBytes,
            TimeSpan.FromMinutes(IdempotencyRetentionMinutes),
            TimeSpan.FromMinutes(InboxRetentionMinutes));

    /// <summary>Creates one deterministic operation.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long sequence) => new()
    {
        OperationId = new(Guid.Parse(string.Create(CultureInfo.InvariantCulture, $"00000000-0000-0000-0000-{sequence:000000000000}"))),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = StartUtc.AddSeconds(sequence),
        Type = SyncOperationType.Append,
        Payload = new(Contract, 1, ContentType, "{}"u8.ToArray(), "sha256-conformance"),
    };

    /// <summary>Owns one adapter, its in-process peer and the real hub behind it.</summary>
    private sealed class TransportHarness : IAsyncDisposable
    {
        /// <summary>The server-side parts of the stack.</summary>
        private readonly HubParts _server;

        /// <summary>The HTTP endpoint, when the HTTP adapter is under test.</summary>
        private readonly HttpServerEndpoint? _endpoint;

        /// <summary>The HTTP client, when the HTTP adapter is under test.</summary>
        private readonly HttpClient? _httpClient;

        /// <summary>Initializes a new instance of the <see cref="TransportHarness"/> class.</summary>
        /// <param name="transport">The transport selector.</param>
        /// <param name="server">The server-side parts of the stack.</param>
        /// <param name="adapter">The adapter under test.</param>
        /// <param name="endpoint">The HTTP endpoint, when used.</param>
        /// <param name="httpClient">The HTTP client, when used.</param>
        private TransportHarness(
            int transport,
            HubParts server,
            IRemoteTransportAdapter adapter,
            HttpServerEndpoint? endpoint,
            HttpClient? httpClient)
        {
            Transport = transport;
            _server = server;
            Adapter = adapter;
            _endpoint = endpoint;
            _httpClient = httpClient;
        }

        /// <summary>Gets the transport selector.</summary>
        internal int Transport { get; }

        /// <summary>Gets the fault-injecting peer in front of the hub.</summary>
        internal FaultInjectingServerStreamHub Peer => _server.Peer;

        /// <summary>Gets the domain handler that counts server effects.</summary>
        internal HubDomainHandler Domain => _server.Domain;

        /// <summary>Gets the adapter under test.</summary>
        internal IRemoteTransportAdapter Adapter { get; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await Adapter.DisposeAsync();
            _httpClient?.Dispose();
            if (_endpoint is not null)
            {
                await _endpoint.DisposeAsync();
            }

            await _server.Hub.DisposeAsync();
            if (_server.Directory is { } directory && Directory.Exists(directory.FullName))
            {
                Directory.Delete(directory.FullName, recursive: true);
            }
        }

        /// <summary>Creates a transport stack.</summary>
        /// <param name="transport">The transport selector.</param>
        /// <param name="hubKind">The hub journal selector.</param>
        /// <param name="options">The optional stack options.</param>
        /// <returns>The harness.</returns>
        internal static TransportHarness Create(int transport, int hubKind, HarnessOptions? options = null)
        {
            options ??= new();
            var clock = new FakeTimeProvider(StartUtc);
            var server = CreateServer(hubKind, options, clock);
            if (transport == LoopbackTransport)
            {
                var loopback = new LoopbackTransportAdapter(new()
                {
                    Hub = server.Peer,
                    SnapshotRecoveryHub = server.Peer,
                    AuthenticatedClient = new(Tenant, Client),
                    PeerCapabilities = CreateCapabilities(options.PeerFeatures ?? LoopbackFeatures),
                    MaximumReceiveEvents = options.AdapterMaximumReceiveEvents,
                });
                return new(transport, server, loopback, null, null);
            }

            var endpoint = new HttpServerEndpoint(new()
            {
                Hub = server.Peer,
                SnapshotRecoveryHub = server.Peer,
                DeclaredCapabilities = CreateCapabilities(options.PeerFeatures ?? HttpFeatures),
                ReplayAuthorizer = AllowReplayAuthorizer.Instance,
                ReplayProtection = new() { TimeProvider = clock },
                TimeProvider = clock,
            });
            var httpClient = new HttpClient(new EndpointHandler(endpoint));
            var adapter = new HttpRemoteTransportAdapter(new()
            {
                HttpClient = httpClient,
                BaseAddress = new(HttpBaseAddress),
                ReplayProtection = new() { TimeProvider = clock },
                TimeProvider = clock,
                MaximumEventsPerBatch = options.AdapterMaximumReceiveEvents,
            });
            return new(transport, server, adapter, endpoint, httpClient);
        }

        /// <summary>Connects a session for the trusted client.</summary>
        /// <param name="guarantees">The required delivery guarantees.</param>
        /// <returns>The connected session.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<IRemoteTransportSession> ConnectAsync(params DeliveryGuarantee[] guarantees) =>
            Adapter.ConnectAsync(
                new(new(new(1, 0), new(1, 0)), new(Client, Tenant), guarantees.Length == 0 ? [DeliveryGuarantee.AtLeastOnce] : guarantees),
                CancellationToken.None);

        /// <summary>Returns whether both the adapter and the session advertise a capability.</summary>
        /// <param name="session">The connected session.</param>
        /// <param name="capability">The capability.</param>
        /// <returns><see langword="true"/> when the capability is advertised.</returns>
        internal bool Advertises(IRemoteTransportSession session, RemoteTransportCapabilities capability) =>
            (Adapter.Capabilities & capability) == capability
            && (session.NegotiatedCapabilities.Features & capability) == capability;

        /// <summary>Skips the calling test with a clear reason when a capability is not advertised.</summary>
        /// <param name="session">The connected session.</param>
        /// <param name="capability">The capability the suite requires.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RequireCapability(IRemoteTransportSession session, RemoteTransportCapabilities capability) =>
            Skip.Unless(
                Advertises(session, capability),
                $"{TransportName(Transport)} does not advertise {capability}; its positive conformance suite does not apply.");

        /// <summary>Creates the real hub, its fault-injecting peer and the domain handler.</summary>
        /// <param name="hubKind">The hub journal selector.</param>
        /// <param name="options">The stack options.</param>
        /// <param name="clock">The deterministic clock.</param>
        /// <returns>The server-side parts.</returns>
        private static HubParts CreateServer(int hubKind, HarnessOptions options, TimeProvider clock)
        {
            var domain = new HubDomainHandler(options.EventsPerOperation);
            var hubOptions = CreateHubOptions(domain, clock, options.HubMaximumReceiveGroups);
            if (hubKind != SqliteHub)
            {
                var memoryHub = ServerStreamHub.CreateInMemory(hubOptions);
                return new(memoryHub, new(memoryHub), domain, null);
            }

            var directory = PhysicalTempDirectory.Create("rxui-oc-transport-");
            var sqliteHub = ServerStreamHub.CreateSqlite(Path.Combine(directory.FullName, "journal.db"), hubOptions);
            return new(sqliteHub, new(sqliteHub), domain, directory);
        }

        /// <summary>Creates hub options that register the shared stream.</summary>
        /// <param name="domain">The domain handler.</param>
        /// <param name="clock">The deterministic clock.</param>
        /// <param name="maximumReceiveGroups">The maximum operation groups per page.</param>
        /// <returns>The hub options.</returns>
        private static ServerStreamHubOptions CreateHubOptions(HubDomainHandler domain, TimeProvider clock, int maximumReceiveGroups)
        {
            var resolver = new LastWriterWinsResolver(new() { VersionFactory = HubVersionFactory.Instance });
            return new()
            {
                AuthorizationPolicy = HubAuthorizationPolicy.Instance,
                SnapshotRecoveryAuthorizationPolicy = HubAuthorizationPolicy.Instance,
                SnapshotRecoveryMaterializer = FixedSnapshotMaterializer.Instance,
                ConflictHandler = new()
                {
                    Streams =
                    [
                        new()
                        {
                            StreamId = Stream,
                            InitialStateFactory = HubInitialStateFactory.Instance,
                            LastWriterWinsResolver = resolver,
                            MergeResolver = resolver,
                            CustomResolver = resolver,
                            DomainHandler = domain,
                        },
                    ],
                },
                TimeProvider = clock,
                MaximumReceiveGroups = maximumReceiveGroups,
            };
        }
    }

    /// <summary>Configures one transport stack.</summary>
    private sealed record HarnessOptions
    {
        /// <summary>Gets the peer features, or <see langword="null"/> for the adapter defaults.</summary>
        internal RemoteTransportCapabilities? PeerFeatures { get; init; }

        /// <summary>Gets the maximum complete operation groups the hub returns per page.</summary>
        internal int HubMaximumReceiveGroups { get; init; } = 1;

        /// <summary>Gets the number of events the domain emits per accepted operation.</summary>
        internal int EventsPerOperation { get; init; } = 1;

        /// <summary>Gets the maximum events the adapter accepts in one received batch.</summary>
        internal int AdapterMaximumReceiveEvents { get; init; } = DefaultAdapterReceiveEvents;
    }

    /// <summary>Groups the server-side parts of one transport stack.</summary>
    /// <param name="Hub">The real hub.</param>
    /// <param name="Peer">The fault-injecting peer in front of the hub.</param>
    /// <param name="Domain">The domain handler.</param>
    /// <param name="Directory">The SQLite directory owned by the stack, when a SQLite hub is used.</param>
    private sealed record HubParts(ServerStreamHub Hub, FaultInjectingServerStreamHub Peer, HubDomainHandler Domain, DirectoryInfo? Directory);
}
