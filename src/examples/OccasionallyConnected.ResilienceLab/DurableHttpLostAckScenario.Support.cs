// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>The lab credential header name.</summary>
    private const string LabCredentialHeader = "X-Resilience-Lab-Credential";

    /// <summary>Runs session cleanup while preserving the first failure.</summary>
    /// <param name="existing">The existing.</param>
    /// <param name="cleanup">The cleanup.</param>
    /// <returns>The result.</returns>
    internal static async ValueTask<Exception?> CaptureSessionCleanupFailureAsync(Exception? existing, Func<Task> cleanup)
    {
        try
        {
            await cleanup().ConfigureAwait(false);
            return existing;
        }
        catch (Exception exception)
        {
            return existing is null ? exception : new AggregateException(existing, exception);
        }
    }

    /// <summary>Disposes the HTTP client while preserving an earlier cleanup failure.</summary>
    /// <param name="existing">The existing cleanup failure.</param>
    /// <param name="httpClient">The partially created HTTP client.</param>
    /// <returns>The first cleanup failure, or null.</returns>
    internal static Exception? CaptureHttpClientCleanupFailure(Exception? existing, HttpClient? httpClient)
    {
        if (httpClient is null)
        {
            return existing;
        }

        try
        {
            httpClient.Dispose();
            return existing;
        }
        catch (Exception exception)
        {
            return existing is null ? exception : new AggregateException(existing, exception);
        }
    }

    /// <summary>Disposes resources after failed session construction.</summary>
    /// <param name="context">The partially created context.</param>
    /// <param name="transport">The partially created transport.</param>
    /// <param name="store">The partially created local store.</param>
    /// <returns>The asynchronous cleanup operation.</returns>
    /// <exception cref="InvalidOperationException">Client session cleanup fails.</exception>
    internal static async ValueTask DisposeFailedSessionAsync(
        IAsyncDisposable? context,
        IAsyncDisposable? transport,
        IAsyncDisposable? store)
    {
        Exception? failure = null;
        if (context is not null)
        {
            failure = await CaptureSessionCleanupFailureAsync(failure, () => context.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
        else
        {
            if (transport is not null)
            {
                failure = await CaptureSessionCleanupFailureAsync(failure, () => transport.DisposeAsync().AsTask()).ConfigureAwait(false);
            }

            if (store is not null)
            {
                failure = await CaptureSessionCleanupFailureAsync(failure, () => store.DisposeAsync().AsTask()).ConfigureAwait(false);
            }
        }

        if (failure is not null)
        {
            throw new InvalidOperationException("The durable HTTP lost-ACK client session cleanup failed.", failure);
        }
    }

    /// <summary>Determines whether a fault is terminal for the lab proof.</summary>
    /// <param name="fault">The fault.</param>
    /// <returns>The result.</returns>
    internal static bool IsTerminalFault(OccasionallyConnectedFault fault) =>
        !fault.IsTransient && fault.Severity is FaultSeverity.Error or FaultSeverity.Critical;

    /// <summary>Creates one public context and stream session.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="baseAddress">The server base address.</param>
    /// <param name="clock">The deterministic public clock.</param>
    /// <param name="settings">The client identity, store, subscription, and operation settings.</param>
    /// <param name="factory">The concrete owned resource constructors.</param>
    /// <returns>The created client session.</returns>
    /// <exception cref="InvalidOperationException">Client session cleanup fails after construction fails.</exception>
    internal static async ValueTask<ClientSession> CreateClientSessionAsync(
        string databasePath,
        Uri baseAddress,
        MutableTimeProvider clock,
        ClientSessionSettings settings,
        ClientSessionResourceFactory factory)
    {
        HttpClient? httpClient = null;
        SqliteLocalStoreAdapter? store = null;
        HttpRemoteTransportAdapter? transport = null;
        OccasionallyConnectedContext? context = null;
        StreamTelemetry? telemetry = null;
        try
        {
            httpClient = factory.CreateHttpClient();
            _ = httpClient.DefaultRequestHeaders.TryAddWithoutValidation(LabCredentialHeader, settings.LabCredential);
            store = factory.CreateStore(databasePath, clock);
            transport = factory.CreateTransport(baseAddress, clock, httpClient);
            context = factory.CreateContext(settings, clock, store, transport);
            var stream = factory.CreateStream(context, settings);
            telemetry = new(stream);
            var session = new ClientSession(context, stream, store, httpClient, telemetry);
            context = null;
            store = null;
            transport = null;
            httpClient = null;
            telemetry = null;
            return session;
        }
        catch (Exception creationFailure)
        {
            telemetry?.Dispose();
            var cleanupFailure = await CaptureSessionCleanupFailureAsync(
                null,
                () => DisposeFailedSessionAsync(context, transport, store).AsTask()).ConfigureAwait(false);
            cleanupFailure = CaptureHttpClientCleanupFailure(cleanupFailure, httpClient);
            if (cleanupFailure is not null)
            {
                throw new InvalidOperationException(
                    "The durable HTTP lost-ACK client session failed to initialize and cleanup failed.",
                    new AggregateException(creationFailure, cleanupFailure));
            }

            throw;
        }
    }

    /// <summary>Creates the production client session from concrete resource constructors.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="baseAddress">The host base address.</param>
    /// <param name="clock">The shared clock.</param>
    /// <param name="settings">The durable client identity.</param>
    /// <returns>The created client session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<ClientSession> CreateClientSessionAsync(
        string databasePath,
        Uri baseAddress,
        MutableTimeProvider clock,
        ClientSessionSettings settings) =>
        CreateClientSessionAsync(databasePath, baseAddress, clock, settings, ClientSessionResourceFactory.Default);

    /// <summary>Creates a public occasionally connected context.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="operationIdSource">The operation id source.</param>
    /// <param name="store">The store.</param>
    /// <param name="transport">The transport.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedContext CreateContext(
        string clientId,
        string storeIdentity,
        MutableTimeProvider clock,
        IOperationIdSource operationIdSource,
        SqliteLocalStoreAdapter store,
        HttpRemoteTransportAdapter transport) =>
        new OccasionallyConnectedBuilder()
            .UseClient(new(clientId, TenantId))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new CrdtPayloadSerializer(CreateBounds()))
            .UseStoreInitialization(CreateStoreInitialization(clientId, storeIdentity))
            .UseTimeProvider(clock)
            .UseOperationIdSource(operationIdSource)
            .UseRetryRandomSource(FixedRetryRandomSource.Instance)
            .UseOptions(CreateOptions())
            .Build();

    /// <summary>Creates a public stream definition.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <returns>The result.</returns>
    private static StreamDefinition<CrdtState, CrdtInput> CreateStreamDefinition(string clientId, SubscriptionId subscriptionId) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            Projection = new CrdtLocalProjection(clientId, new CrdtState { Kind = CrdtKind.GCounter }, CreateBounds()),
            InputContractId = CrdtContracts.InputContractId,
            StateContractId = CrdtContracts.StateContractId,
            InputSchemaVersion = CrdtContracts.SchemaVersion,
            StateSchemaVersion = CrdtContracts.SchemaVersion,
            Subscription = CreateSubscriptionOptions(subscriptionId),
            Publish = CreatePublishOptions(),
            TypedInput = new() { BufferCapacity = SmallCapacity, BufferCapacityBytes = PayloadBytes, MaximumRetainedInputBytes = TypedInputBytes },
        };

    /// <summary>Creates remote publish options.</summary>
    /// <returns>The result.</returns>
    private static RemotePublishOptions CreatePublishOptions() =>
        new() { StreamId = Stream, Durable = true, DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce, AdmissionStrategy = BufferStrategy.Block, ConflictPolicy = ConflictPolicy.Merge, };

    /// <summary>Creates remote subscription options.</summary>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <returns>The result.</returns>
    private static RemoteSubscriptionOptions CreateSubscriptionOptions(SubscriptionId subscriptionId) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            StartPosition = StartPosition.FromSequence(0),
            DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
            BufferStrategy = BufferStrategy.Block,
            BufferCapacity = SmallCapacity,
            BufferCapacityBytes = PayloadBytes,
        };

    /// <summary>Creates public context options.</summary>
    /// <returns>The result.</returns>
    private static OccasionallyConnectedOptions CreateOptions() =>
        OccasionallyConnectedOptions.Default with
        {
            AutoStart = false,
            MaxConcurrentStreams = 1,
            Batching = new() { MaximumOperations = 1, MaximumBytes = PayloadBytes, MaximumDwellTime = RetryDelay, MaxInFlightBatchesPerStream = 1 },
            Retry = new() { MinimumDelay = RetryDelay, MaximumDelay = RetryDelay, MaximumRetryAttempts = RetryAttempts, MaximumRetryAge = TimeSpan.FromMinutes(RetentionMinutes) },
        };

    /// <summary>Creates local store initialization.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The result.</returns>
    private static LocalStoreInitialization CreateStoreInitialization(string clientId, string storeIdentity) =>
        new(storeIdentity, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = clientId };

    /// <summary>Creates a SQLite store adapter.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="clock">The clock.</param>
    /// <returns>The result.</returns>
    private static SqliteLocalStoreAdapter CreateSqliteStore(string databasePath, MutableTimeProvider clock) =>
        new(databasePath, new() { TimeProvider = clock });

    /// <summary>Creates the HTTP transport adapter.</summary>
    /// <param name="baseAddress">The base address.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="httpClient">The http client.</param>
    /// <returns>The result.</returns>
    private static HttpRemoteTransportAdapter CreateTransport(Uri baseAddress, MutableTimeProvider clock, HttpClient httpClient) =>
        new(new()
        {
            HttpClient = httpClient,
            BaseAddress = baseAddress,
            AllowInsecureLoopbackHttp = true,
            MaximumBatchOperations = SmallCapacity,
            MaximumEventsPerBatch = SmallCapacity,
            MaximumCompletedOperationsPerBatch = SmallCapacity,
            MaximumPayloadBytes = PayloadBytes,
            MaximumRequestBytes = PayloadBytes,
            MaximumResponseBytes = PayloadBytes,
            MaximumConcurrentRequests = SmallCapacity,
            MaximumConcurrentSubscriptions = 1,
            TimeProvider = clock,
            ReplayProtection = new() { TimeProvider = clock },
        });

    /// <summary>Creates a temporary root directory.</summary>
    /// <returns>The result.</returns>
    private static string CreateTemporaryRoot()
    {
        var path = Path.Combine(Path.GetTempPath(), $"reactiveui-oc-lost-ack-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>Deletes the temporary lab directory after all hosted resources are disposed.</summary>
    /// <param name="path">The path.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DeleteDirectory(string path) => Directory.Delete(path, recursive: true);

    /// <summary>Creates a SQLite connection string.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="mode">The mode.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSqliteConnectionString(string databasePath, SqliteOpenMode mode) =>
        new SqliteConnectionStringBuilder { DataSource = databasePath, Mode = mode, Pooling = false }.ToString();

    /// <summary>Waits for a sampled asynchronous proof to satisfy a predicate.</summary>
    /// <typeparam name="T">The sampled proof type.</typeparam>
    /// <param name="read">The read.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<T> WaitForAsync<T>(
        Func<ValueTask<T>> read,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            var value = await read().ConfigureAwait(false);
            if (predicate(value))
            {
                return value;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(ProofPollMilliseconds), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Mutable deterministic time provider with manually fired timers.</summary>
    /// <param name="utcNow">The utc now.</param>
    internal sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The gate.</summary>
        private readonly object _gate = new();

        /// <summary>The timers.</summary>
        private readonly List<ManualTimer> _timers = [];

        /// <summary>The utc now.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
            {
                return _utcNow;
            }
        }

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state);
            Register(timer);
            _ = timer.Change(dueTime, period);
            return timer;
        }

        /// <summary>Advances the current time and fires due timers.</summary>
        /// <param name="duration">The duration.</param>
        internal void Advance(TimeSpan duration)
        {
            List<ManualTimer> due;
            lock (_gate)
            {
                _utcNow = _utcNow.Add(duration);
                due = CollectDueTimers();
            }

            for (var index = 0; index < due.Count; index++)
            {
                due[index].Fire();
            }
        }

        /// <summary>The register.</summary>
        /// <param name="timer">The timer.</param>
        private void Register(ManualTimer timer)
        {
            lock (_gate)
            {
                _timers.Add(timer);
            }
        }

        /// <summary>The remove.</summary>
        /// <param name="timer">The timer.</param>
        private void Remove(ManualTimer timer)
        {
            lock (_gate)
            {
                _ = _timers.Remove(timer);
            }
        }

        /// <summary>The collect due timers.</summary>
        /// <returns>The result.</returns>
        private List<ManualTimer> CollectDueTimers()
        {
            List<ManualTimer> due = [];
            for (var index = 0; index < _timers.Count; index++)
            {
                if (_timers[index].TryMarkDue(_utcNow))
                {
                    due.Add(_timers[index]);
                }
            }

            return due;
        }

        /// <summary>Fires deterministic time-provider callbacks when the lab advances time.</summary>
        private sealed class ManualTimer : ITimer
        {
            /// <summary>The owner.</summary>
            private readonly MutableTimeProvider _owner;

            /// <summary>The callback.</summary>
            private readonly TimerCallback _callback;

            /// <summary>The state.</summary>
            private readonly object? _state;

            /// <summary>The due utc.</summary>
            private DateTimeOffset? _dueUtc;

            /// <summary>The period.</summary>
            private TimeSpan _period = Timeout.InfiniteTimeSpan;

            /// <summary>The disposed.</summary>
            private bool _disposed;

            /// <summary>Initializes a new instance of the <see cref="ManualTimer"/> class.</summary>
            /// <param name="owner">The owner.</param>
            /// <param name="callback">The callback.</param>
            /// <param name="state">The state.</param>
            internal ManualTimer(MutableTimeProvider owner, TimerCallback callback, object? state)
            {
                _owner = owner;
                _callback = callback;
                _state = state;
            }

            /// <inheritdoc/>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                if (_disposed)
                {
                    return false;
                }

                _period = period;
                _dueUtc = dueTime == Timeout.InfiniteTimeSpan ? null : _owner.GetUtcNow().Add(dueTime < TimeSpan.Zero ? TimeSpan.Zero : dueTime);
                return true;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                _disposed = true;
                _owner.Remove(this);
            }

            /// <inheritdoc/>
            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }

            /// <summary>The try mark due.</summary>
            /// <param name="nowUtc">The now utc.</param>
            /// <returns>The result.</returns>
            internal bool TryMarkDue(DateTimeOffset nowUtc)
            {
                if (_disposed || _dueUtc is not { } dueUtc || dueUtc > nowUtc)
                {
                    return false;
                }

                _dueUtc = _period == Timeout.InfiniteTimeSpan ? null : nowUtc.Add(_period);
                return true;
            }

            /// <summary>The fire.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Fire() => _callback(_state);
        }
    }

    /// <summary>Operation source that fails if a client tries to create unexpected new work.</summary>
    internal sealed class ThrowingOperationIdSource : IOperationIdSource
    {
        /// <summary>Gets the operation source that rejects unexpected new work.</summary>
        internal static ThrowingOperationIdSource Instance { get; } = new();

        /// <inheritdoc/>
        public OperationId New() => throw new InvalidOperationException("This client must not publish a new operation in the lost-ACK scenario.");
    }

    /// <summary>Records observable values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    internal sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>The gate.</summary>
        private readonly object _gate = new();

        /// <summary>The values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Gets the number of recorded observer values.</summary>
        internal int Count
        {
            get
            {
                lock (_gate)
                {
                    return _values.Count;
                }
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                _values.Add(value);
            }
        }

        /// <summary>The count where.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <returns>The result.</returns>
        internal int CountWhere(Func<T, bool> predicate)
        {
            lock (_gate)
            {
                var count = 0;
                for (var index = 0; index < _values.Count; index++)
                {
                    count += predicate(_values[index]) ? 1 : 0;
                }

                return count;
            }
        }

        /// <summary>Formats recorded values for a bounded diagnostic.</summary>
        /// <param name="format">The formatter.</param>
        /// <returns>The recorded values.</returns>
        internal string Describe(Func<T, string> format)
        {
            lock (_gate)
            {
                var formatted = new string[_values.Count];
                for (var index = 0; index < _values.Count; index++)
                {
                    formatted[index] = format(_values[index]);
                }

                return string.Join(",", formatted);
            }
        }

        /// <summary>The wait for async.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask<T> WaitForAsync(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            while (true)
            {
                if (TryFind(predicate, out var value))
                {
                    return value;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(ProofPollMilliseconds), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>The try find.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <param name="value">The matching value when one is present.</param>
        /// <returns>The result.</returns>
        private bool TryFind(Func<T, bool> predicate, [MaybeNullWhen(false)] out T value)
        {
            lock (_gate)
            {
                var index = 0;
                while (index < _values.Count && !predicate(_values[index]))
                {
                    index++;
                }

                if (index == _values.Count)
                {
                    value = default;
                    return false;
                }

                value = _values[index];
                return true;
            }
        }
    }

    /// <summary>Public context session resources.</summary>
    internal sealed class ClientSession : IAsyncDisposable
    {
        /// <summary>The http client.</summary>
        private readonly HttpClient _httpClient;

        /// <summary>Initializes a new instance of the <see cref="ClientSession"/> class.</summary>
        /// <param name="context">The context.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="store">The initialized durable local store.</param>
        /// <param name="httpClient">The http client.</param>
        /// <param name="telemetry">The telemetry.</param>
        internal ClientSession(
            OccasionallyConnectedContext context,
            IOccasionallyConnectedStream<CrdtState, CrdtInput> stream,
            SqliteLocalStoreAdapter store,
            HttpClient httpClient,
            StreamTelemetry telemetry)
        {
            Context = context;
            ConnectedStream = stream;
            Store = store;
            _httpClient = httpClient;
            Telemetry = telemetry;
        }

        /// <summary>Gets the context.</summary>
        internal OccasionallyConnectedContext Context { get; }

        /// <summary>Gets the stream.</summary>
        internal IOccasionallyConnectedStream<CrdtState, CrdtInput> ConnectedStream { get; }

        /// <summary>Gets the initialized durable local store owned by the context.</summary>
        internal SqliteLocalStoreAdapter Store { get; }

        /// <summary>Gets the telemetry.</summary>
        internal StreamTelemetry Telemetry { get; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            Telemetry.Dispose();
            try
            {
                await Context.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _httpClient.Dispose();
            }
        }
    }

    /// <summary>Collects public stream observations.</summary>
    internal sealed class StreamTelemetry : IDisposable
    {
        /// <summary>The remote.</summary>
        private readonly RecordingObserver<RemoteMessage<CrdtInput>> _remote = new();

        /// <summary>The operation states.</summary>
        private readonly RecordingObserver<SyncOperationStatus> _operationStates = new();

        /// <summary>The faults.</summary>
        private readonly RecordingObserver<OccasionallyConnectedFault> _faults = new();

        /// <summary>The local.</summary>
        private readonly RecordingObserver<CrdtState> _local = new();

        /// <summary>The subscriptions.</summary>
        private readonly List<IDisposable> _subscriptions;

        /// <summary>Initializes a new instance of the <see cref="StreamTelemetry"/> class.</summary>
        /// <param name="stream">The stream.</param>
        internal StreamTelemetry(IOccasionallyConnectedStream<CrdtState, CrdtInput> stream) =>
            _subscriptions =
            [
                stream.Local.Subscribe(_local),
                stream.Remote.Subscribe(_remote),
                stream.OperationStates.Subscribe(_operationStates),
                stream.Faults.Subscribe(_faults),
            ];

        /// <summary>Gets the remote count.</summary>
        internal int RemoteCount => _remote.Count;

        /// <summary>Gets the terminal fault count.</summary>
        internal int TerminalFaultCount => _faults.CountWhere(IsTerminalFault);

        /// <summary>Gets the public fault codes observed during this session.</summary>
        internal string FaultCodes => _faults.Describe(static fault => fault.Code);

        /// <inheritdoc/>
        public void Dispose()
        {
            for (var index = 0; index < _subscriptions.Count; index++)
            {
                _subscriptions[index].Dispose();
            }
        }

        /// <summary>The wait for operation state async.</summary>
        /// <param name="operationId">The operation id.</param>
        /// <param name="state">The state.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask<SyncOperationStatus> WaitForOperationStateAsync(
            OperationId operationId,
            SyncOperationState state,
            CancellationToken cancellationToken) =>
            await _operationStates.WaitForAsync(
                status => status.OperationId == operationId && status.State == state,
                cancellationToken).ConfigureAwait(false);

        /// <summary>The wait for remote count async.</summary>
        /// <param name="count">The count.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask WaitForRemoteCountAsync(int count, CancellationToken cancellationToken) =>
            await _remote.WaitForAsync(_ => _remote.Count >= count, cancellationToken).ConfigureAwait(false);

        /// <summary>The wait for local counter async.</summary>
        /// <param name="counter">The counter.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        internal async ValueTask<long> WaitForLocalCounterAsync(long counter, CancellationToken cancellationToken)
        {
            var state = await _local.WaitForAsync(item => item.Value.Counter == counter, cancellationToken).ConfigureAwait(false);
            return state.Value.Counter;
        }
    }

    /// <summary>Fixed operation id source.</summary>
    /// <param name="operationId">The operation id.</param>
    private sealed class FixedOperationIdSource(OperationId operationId) : IOperationIdSource
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationId New() => operationId;
    }

    /// <summary>Deterministic retry jitter source.</summary>
    private sealed class FixedRetryRandomSource : IRetryRandomSource
    {
        /// <summary>Gets the deterministic retry random source.</summary>
        internal static FixedRetryRandomSource Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double NextDouble() => 0;
    }

    /// <summary>Authorizes all lab operations for one trusted tenant.</summary>
    /// <param name="tenantId">The tenant id.</param>
    private sealed class LabAuthorizationPolicy(string tenantId) : IServerStreamAuthorizationPolicy
    {
        /// <summary>The tenant id.</summary>
        private readonly string _tenantId = tenantId;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            Authorize(client, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            Authorize(client, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            Authorize(client, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            Authorize(client, cancellationToken);

        /// <summary>The authorize.</summary>
        /// <param name="client">The client.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        private ValueTask<ServerStreamAuthorizationScope> Authorize(
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new ServerStreamAuthorizationScope(_tenantId, client.ClientId));
        }
    }
}
