// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Hosting;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// One device: a public <see cref="OccasionallyConnectedContext"/> over a <see cref="SqliteLocalStoreAdapter"/>
/// and an <see cref="HttpRemoteTransportAdapter"/>, with a G-counter stream (spec section 16.1).
/// </summary>
internal sealed class SampleClient : IAsyncDisposable
{
    private readonly HttpClient _httpClient;
    private readonly IDisposable _localSubscription;
    private readonly IDisposable _syncSubscription;
    private readonly IDisposable _faultSubscription;
    private readonly IDisposable _engineFaultSubscription;

    private SampleClient(string clientId, SampleServer server, string databasePath, SubscriptionId subscriptionId, bool refuseWhenDown)
    {
        ClientId = clientId;
        _httpClient = server.CreateHttpClient(clientId, refuseWhenDown);
        var store = new SqliteLocalStoreAdapter(databasePath);
        var transport = new HttpRemoteTransportAdapter(new HttpRemoteTransportOptions
        {
            HttpClient = _httpClient,
            BaseAddress = SampleServer.BaseAddress,
            TimeProvider = TimeProvider.System,
        });
        Context = new OccasionallyConnectedBuilder()
            .UseClient(new ClientIdentity(clientId, SampleServer.Tenant))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new CrdtPayloadSerializer())
            .UseStoreInitialization(new LocalStoreInitialization(clientId, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = clientId })
            .UseTimeProvider(TimeProvider.System)
            .UseOptions(OccasionallyConnectedOptions.Default with
            {
                AutoStart = false,
                MaxConcurrentStreams = 1,
                Batching = new BatchingOptions { MaximumOperations = 8, MaximumBytes = 64 * 1024, MaximumDwellTime = TimeSpan.FromMilliseconds(50), MaxInFlightBatchesPerStream = 1 },
                Retry = new RetryOptions { MinimumDelay = TimeSpan.FromMilliseconds(100), MaximumDelay = TimeSpan.FromMilliseconds(250), MaximumRetryAttempts = 10_000, MaximumRetryAge = TimeSpan.FromHours(1) },
                CircuitBreaker = new CircuitBreakerOptions { FailureThreshold = 10_000, OpenDuration = TimeSpan.FromMilliseconds(250) },
            })
            .Build();
        Stream = Context.GetOrCreateStream(CreateDefinition(clientId, subscriptionId));
        _localSubscription = Stream.Local.Subscribe(Local);
        _syncSubscription = Context.SyncStates.Subscribe(Sync);
        _faultSubscription = Stream.Faults.Subscribe(Faults);
        _engineFaultSubscription = Context.SyncEngine.Faults.Subscribe(Faults);
        Health = new OccasionallyConnectedHealthMonitor(Context, TimeProvider.System);
    }

    /// <summary>Gets the client identifier.</summary>
    internal string ClientId { get; }

    /// <summary>Gets the public context.</summary>
    internal OccasionallyConnectedContext Context { get; }

    /// <summary>Gets the G-counter stream.</summary>
    internal IOccasionallyConnectedStream<CrdtState, CrdtInput> Stream { get; }

    /// <summary>Gets the latest local (optimistic) state.</summary>
    internal LatestValueObserver<CrdtState> Local { get; } = new();

    /// <summary>Gets the latest sync state.</summary>
    internal LatestValueObserver<SyncState> Sync { get; } = new();

    /// <summary>Gets the stream and engine faults.</summary>
    internal FaultLog Faults { get; } = new();

    /// <summary>Gets the hosting health monitor for this context.</summary>
    internal OccasionallyConnectedHealthMonitor Health { get; }

    /// <summary>Opens (or reopens) a client over its SQLite database and starts it.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="server">The in-process server.</param>
    /// <param name="databasePath">The client SQLite database path.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="refuseWhenDown">Whether a stopped server refuses the connection instead of answering 503.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The started client.</returns>
    internal static async Task<SampleClient> StartAsync(string clientId, SampleServer server, string databasePath, SubscriptionId subscriptionId, bool refuseWhenDown, CancellationToken cancellationToken)
    {
        var client = new SampleClient(clientId, server, databasePath, subscriptionId, refuseWhenDown);
        try
        {
            await client.Context.StartAsync(cancellationToken).ConfigureAwait(false);
            return client;
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Creates the input that sets this client's G-counter component.</summary>
    /// <param name="value">The new component value.</param>
    /// <returns>The CRDT input.</returns>
    internal CrdtInput SetCounter(long value) => CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, value));

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        Health.Dispose();
        _localSubscription.Dispose();
        _syncSubscription.Dispose();
        _faultSubscription.Dispose();
        _engineFaultSubscription.Dispose();
        await Context.DisposeAsync().ConfigureAwait(false);
        _httpClient.Dispose();
    }

    private static StreamDefinition<CrdtState, CrdtInput> CreateDefinition(string clientId, SubscriptionId subscriptionId) => new()
    {
        StreamId = SampleServer.Stream,
        SubscriptionId = subscriptionId,
        Projection = new CrdtLocalProjection(clientId, CrdtKind.GCounter),
        InputContractId = CrdtContracts.InputContractId,
        StateContractId = CrdtContracts.StateContractId,
        InputSchemaVersion = CrdtContracts.SchemaVersion,
        StateSchemaVersion = CrdtContracts.SchemaVersion,
        Subscription = new RemoteSubscriptionOptions
        {
            StreamId = SampleServer.Stream,
            SubscriptionId = subscriptionId,
            StartPosition = StartPosition.FromSequence(0),
            DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
            BufferCapacity = 256,
            BufferCapacityBytes = 1_048_576,
        },
        Publish = new RemotePublishOptions
        {
            StreamId = SampleServer.Stream,
            Durable = true,
            DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
            ConflictPolicy = ConflictPolicy.Merge,
        },
        Input = new ObserverInputOptions { BufferCapacity = 64, BufferCapacityBytes = 64 * 1024 },
        InputCapture = CrdtInputCapture.Instance,
        // The stream work lane holds min(BufferCapacity, BufferCapacityBytes / MaximumRetainedInputBytes) items, so keep
        // the per-input bound small; a lane of one item rejects concurrent receive and upload work.
        TypedInput = new TypedInputOptions { BufferCapacity = 64, BufferCapacityBytes = 1024 * 1024, MaximumRetainedInputBytes = 4 * 1024 },
    };
}
