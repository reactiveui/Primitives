// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Composes an occasionally connected public context from concrete store, transport, serialization, and scheduling dependencies.</summary>
[DebuggerDisplay("Client={_client,nq}; Store={_storeIdentity,nq}; Built={_built,nq}")]
public sealed class OccasionallyConnectedBuilder
{
    /// <summary>Stores the supported transport protocol range.</summary>
    private readonly VersionRange _supportedProtocolVersions = new(new(1, 0), new(1, 0));

    /// <summary>Stores the finite active subscription cap.</summary>
    private readonly int _activeSubscriptionCapacity = SyncEngineOptions.DefaultMaxActiveSubscriptions;

    /// <summary>Stores the finite diagnostic subscription cap.</summary>
    private readonly int _diagnosticSubscriptionCapacity = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions;

    /// <summary>Stores the retained byte budget for scheduler descriptors.</summary>
    private readonly long _schedulerDescriptorBytes = SyncEngineOptions.DefaultMaxSchedulerDescriptorBytes;

    /// <summary>Stores the configured client identity.</summary>
    private ClientIdentity? _client;

    /// <summary>Stores the configured local store dependency.</summary>
    private ILocalStoreAdapter? _store;

    /// <summary>Stores the configured remote transport dependency.</summary>
    private IRemoteTransportAdapter? _transport;

    /// <summary>Stores the configured payload serializer dependency.</summary>
    private IPayloadSerializer? _serializer;

    /// <summary>Stores the optional JSON registry snapshot paired with the serializer.</summary>
    private SchemaRegistry? _schemaRegistry;

    /// <summary>Stores the configured occasionally connected options.</summary>
    private OccasionallyConnectedOptions _options = OccasionallyConnectedOptions.Default;

    /// <summary>Stores the explicit store identity used to build default store initialization.</summary>
    private string? _storeIdentity;

    /// <summary>Stores the explicit store initialization request.</summary>
    private LocalStoreInitialization? _storeInitialization;

    /// <summary>Stores the configured clock.</summary>
    private TimeProvider _timeProvider = TimeProvider.System;

    /// <summary>Stores the optional public sequencer.</summary>
    private ISequencer? _sequencer;

    /// <summary>Stores the configured operation identifier source.</summary>
    private IOperationIdSource _operationIdSource = GuidOperationIdSource.Instance;

    /// <summary>Stores the configured retry jitter source.</summary>
    private IRetryRandomSource _retryRandomSource = SyncEngine.EngineRetryRandomSource.Instance;

    /// <summary>Stores the finite stream registry capacity.</summary>
    private int _registryCapacity = SyncEngineOptions.DefaultMaxRegisteredStreams;

    /// <summary>Stores the configured store ownership.</summary>
    private SyncEngineDependencyOwnership _storeOwnership;

    /// <summary>Stores the configured transport ownership.</summary>
    private SyncEngineDependencyOwnership _transportOwnership;

    /// <summary>Tracks whether a successful build transferred dependencies.</summary>
    private bool _built;

    /// <summary>Configures the client identity for the context.</summary>
    /// <param name="client">The client identity.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseClient(ClientIdentity client)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(client);
        _client = client;
        return this;
    }

    /// <summary>Configures an owned local store dependency.</summary>
    /// <param name="store">The local store dependency.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseStore(ILocalStoreAdapter store)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(store);
        _store = store;
        _storeOwnership = SyncEngineDependencyOwnership.Owned;
        return this;
    }

    /// <summary>Configures a borrowed local store dependency.</summary>
    /// <param name="store">The local store dependency.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseBorrowedStore(ILocalStoreAdapter store)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(store);
        _store = store;
        _storeOwnership = SyncEngineDependencyOwnership.Borrowed;
        return this;
    }

    /// <summary>Configures an owned remote transport dependency.</summary>
    /// <param name="transport">The remote transport dependency.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseTransport(IRemoteTransportAdapter transport)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(transport);
        _transport = transport;
        _transportOwnership = SyncEngineDependencyOwnership.Owned;
        return this;
    }

    /// <summary>Configures a borrowed remote transport dependency.</summary>
    /// <param name="transport">The remote transport dependency.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseBorrowedTransport(IRemoteTransportAdapter transport)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(transport);
        _transport = transport;
        _transportOwnership = SyncEngineDependencyOwnership.Borrowed;
        return this;
    }

    /// <summary>Configures a payload serializer dependency.</summary>
    /// <param name="serializer">The payload serializer dependency.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseSerializer(IPayloadSerializer serializer)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(serializer);
        _serializer = serializer;
        _schemaRegistry = null;
        return this;
    }

    /// <summary>Configures a JSON serializer from an allowlisted schema registry.</summary>
    /// <param name="schemaRegistry">The schema registry.</param>
    /// <returns>The current builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OccasionallyConnectedBuilder UseJsonSerializer(SchemaRegistry schemaRegistry) => ConfigureJsonSerializer(schemaRegistry, maximumPayloadBytes: null);

    /// <summary>Configures a JSON serializer from an allowlisted schema registry and maximum payload size.</summary>
    /// <param name="schemaRegistry">The schema registry.</param>
    /// <param name="maximumPayloadBytes">The maximum serialized payload size.</param>
    /// <returns>The current builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OccasionallyConnectedBuilder UseJsonSerializer(SchemaRegistry schemaRegistry, int maximumPayloadBytes) =>
        ConfigureJsonSerializer(schemaRegistry, maximumPayloadBytes: maximumPayloadBytes);

    /// <summary>Configures occasionally connected behavior options.</summary>
    /// <param name="options">The behavior options.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseOptions(OccasionallyConnectedOptions options)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(options);
        _options = options;
        return this;
    }

    /// <summary>Configures occasionally connected behavior options with a transform of the current options.</summary>
    /// <param name="configure">The option transform.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="InvalidOperationException">The transform returned <see langword="null"/>.</exception>
    public OccasionallyConnectedBuilder ConfigureOptions(Func<OccasionallyConnectedOptions, OccasionallyConnectedOptions> configure)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(configure);
        _options = configure(_options) ?? throw new InvalidOperationException("Options transform returned null.");
        return this;
    }

    /// <summary>Configures the store partition identity used by default store initialization.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The current builder.</returns>
    /// <exception cref="ArgumentException"><paramref name="storeIdentity"/> is null, empty, or whitespace.</exception>
    public OccasionallyConnectedBuilder UseStoreIdentity(string storeIdentity)
    {
        EnsureMutable();
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(storeIdentity);
#else
        ArgumentExceptionHelper.ThrowIfNull(storeIdentity);
        if (string.IsNullOrWhiteSpace(storeIdentity))
        {
            throw new ArgumentException("Store identity must be supplied.", nameof(storeIdentity));
        }
#endif

        _storeIdentity = storeIdentity;
        return this;
    }

    /// <summary>Configures explicit local store initialization requirements.</summary>
    /// <param name="initialization">The store initialization requirements.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseStoreInitialization(LocalStoreInitialization initialization)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        _storeInitialization = initialization;
        return this;
    }

    /// <summary>Configures the context clock.</summary>
    /// <param name="timeProvider">The clock.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseTimeProvider(TimeProvider timeProvider)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        return this;
    }

    /// <summary>Configures observer notifications to use a public sequencer.</summary>
    /// <param name="sequencer">The public sequencer.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseSequencer(ISequencer sequencer)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(sequencer);
        _sequencer = sequencer;
        return this;
    }

    /// <summary>Configures the operation identifier source used by typed local commits.</summary>
    /// <param name="operationIdSource">The operation identifier source.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseOperationIdSource(IOperationIdSource operationIdSource)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(operationIdSource);
        _operationIdSource = operationIdSource;
        return this;
    }

    /// <summary>Configures the retry random source used by engine retry policies.</summary>
    /// <param name="retryRandomSource">The retry random source.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder UseRetryRandomSource(IRetryRandomSource retryRandomSource)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(retryRandomSource);
        _retryRandomSource = retryRandomSource;
        return this;
    }

    /// <summary>Configures the finite stream registry capacity.</summary>
    /// <param name="capacity">The maximum registered stream count.</param>
    /// <returns>The current builder.</returns>
    public OccasionallyConnectedBuilder WithRegistryCapacity(int capacity)
    {
        EnsureMutable();
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(capacity);
        _registryCapacity = capacity;
        return this;
    }

    /// <summary>Builds the context and transfers ownership of configured owned dependencies.</summary>
    /// <returns>The composed context.</returns>
    /// <exception cref="InvalidOperationException">The builder is incomplete, invalid, or already consumed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured option contains an out-of-range value.</exception>
    public OccasionallyConnectedContext Build()
    {
        EnsureMutable();
        var client = _client ?? throw new InvalidOperationException($"{nameof(ClientIdentity)} must be supplied.");
        var store = _store ?? throw new InvalidOperationException($"{nameof(ILocalStoreAdapter)} must be supplied.");
        var transport = _transport ?? throw new InvalidOperationException($"{nameof(IRemoteTransportAdapter)} must be supplied.");
        var serializer = _serializer ?? throw new InvalidOperationException($"{nameof(IPayloadSerializer)} must be supplied.");
        var initialization = ResolveStoreInitialization(client);
        _options.Validate();

        IObserverNotificationScheduler notificationScheduler = _sequencer is null
            ? ThreadPoolObserverNotificationScheduler.Instance
            : new SequencerObserverNotificationScheduler(_sequencer);
        var engineOptions = new SyncEngineOptions
        {
            Store = store,
            Transport = transport,
            StoreOwnership = _storeOwnership,
            TransportOwnership = _transportOwnership,
            TimeProvider = _timeProvider,
            NotificationScheduler = notificationScheduler,
            Options = _options,
            StoreInitialization = initialization,
            Client = client,
            SupportedProtocolVersions = _supportedProtocolVersions,
            MaxRegisteredStreams = _registryCapacity,
            MaxActiveSubscriptions = _activeSubscriptionCapacity,
            MaxDiagnosticSubscriptions = _diagnosticSubscriptionCapacity,
            MaxSchedulerDescriptorBytes = _schedulerDescriptorBytes,
            RetryRandomSource = _retryRandomSource,
        };
        var engine = new SyncEngine(engineOptions);
        var context = new OccasionallyConnectedContext(new()
        {
            Engine = engine,
            Store = store,
            Serializer = serializer,
            TimeProvider = _timeProvider,
            OperationIdSource = _operationIdSource,
            NotificationScheduler = notificationScheduler,
            Options = _options,
            Client = client,
            RegistryCapacity = _registryCapacity,
            AutoStart = _options.AutoStart,
            SchemaRegistry = _schemaRegistry,
        });
        _built = true;
        return context;
    }

    /// <summary>Configures a JSON serializer from an allowlisted schema registry.</summary>
    /// <param name="schemaRegistry">The schema registry.</param>
    /// <param name="maximumPayloadBytes">The optional maximum serialized payload size.</param>
    /// <returns>The current builder.</returns>
    private OccasionallyConnectedBuilder ConfigureJsonSerializer(SchemaRegistry schemaRegistry, int? maximumPayloadBytes)
    {
        EnsureMutable();
        ArgumentExceptionHelper.ThrowIfNull(schemaRegistry);
        var snapshot = schemaRegistry.Snapshot();
        _schemaRegistry = snapshot;
        _serializer = maximumPayloadBytes is { } limit ? new JsonPayloadSerializer(snapshot, limit) : new JsonPayloadSerializer(snapshot);
        return this;
    }

    /// <summary>Resolves and validates local store initialization.</summary>
    /// <param name="client">The configured client.</param>
    /// <returns>The validated store initialization.</returns>
    /// <exception cref="InvalidOperationException">Explicit outbox limits conflict with the context limits.</exception>
    private LocalStoreInitialization ResolveStoreInitialization(ClientIdentity client)
    {
        var initialization = _storeInitialization ?? CreateDefaultStoreInitialization(client);
        if (initialization.Outbox is { } outbox && outbox != _options.Outbox)
        {
            throw new InvalidOperationException("Store initialization outbox limits must match the configured context limits.");
        }

        initialization = initialization with { Outbox = _options.Outbox };
        if (initialization.ClientId is null)
        {
            initialization = initialization with { ClientId = client.ClientId };
        }

        ValidateStoreInitialization(initialization, client);
        return initialization;
    }

    /// <summary>Creates default local store initialization from client, store identity, and security options.</summary>
    /// <param name="client">The configured client.</param>
    /// <returns>The default initialization.</returns>
    /// <exception cref="InvalidOperationException">The store identity is missing.</exception>
    private LocalStoreInitialization CreateDefaultStoreInitialization(ClientIdentity client)
    {
        var storeIdentity = _storeIdentity ?? throw new InvalidOperationException("Store identity must be supplied.");
        return new(storeIdentity, 1, _options.Security.RequireAuthenticatedEncryptionAtRest) { ClientId = client.ClientId };
    }

    /// <summary>Validates local store initialization compatibility with the configured client and security options.</summary>
    /// <param name="initialization">The initialization to validate.</param>
    /// <param name="client">The configured client.</param>
    /// <exception cref="InvalidOperationException">The initialization is malformed or incompatible.</exception>
    private void ValidateStoreInitialization(LocalStoreInitialization initialization, ClientIdentity client)
    {
        if (string.IsNullOrWhiteSpace(initialization.StoreIdentity))
        {
            throw new InvalidOperationException("StoreIdentity must be supplied.");
        }

        if (initialization.RequiredSchemaVersion <= 0)
        {
            throw new InvalidOperationException("RequiredSchemaVersion must be positive.");
        }

        if (!string.Equals(initialization.ClientId, client.ClientId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Store initialization ClientId must match the configured client.");
        }

        if (!_options.Security.RequireAuthenticatedEncryptionAtRest || initialization.RequireAuthenticatedEncryptionAtRest)
        {
            return;
        }

        throw new InvalidOperationException("Store initialization cannot weaken authenticated encryption requirements.");
    }

    /// <summary>Ensures the builder has not already transferred dependencies.</summary>
    /// <exception cref="InvalidOperationException">The builder already transferred dependencies.</exception>
    private void EnsureMutable()
    {
        if (!_built)
        {
            return;
        }

        throw new InvalidOperationException("The builder has already built a context.");
    }
}
