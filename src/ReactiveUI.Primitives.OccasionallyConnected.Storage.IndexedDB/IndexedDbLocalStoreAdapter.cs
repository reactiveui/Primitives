// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.JSInterop;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

/// <summary>Stores occasionally connected stream state in IndexedDB through JS interop.</summary>
/// <remarks>
/// The adapter imports a JavaScript module from this package's Razor class library static asset path and stores one
/// durable JSON document per initialized store identity. Lease ownership is durable across tabs in the same browser
/// profile through IndexedDB transactions and expiry timestamps, but it is not an operating-system process lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplayAttribute("IndexedDbLocalStoreAdapter: {Capabilities}")]
public sealed partial class IndexedDbLocalStoreAdapter : ILocalStoreAdapter
{
    /// <summary>The current IndexedDB document schema version.</summary>
    private const int CurrentFormatVersion = 1;

    /// <summary>The default IndexedDB database name.</summary>
    private const string DefaultDatabaseName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The default IndexedDB object store name.</summary>
    private const string DefaultObjectStoreName = "LocalStores";

    /// <summary>The default static web asset path for the IndexedDB JS module.</summary>
    private const string DefaultModulePath =
        "./_content/ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB/indexedDbInterop.js";

    /// <summary>The JS function used for optimistic compare-exchange persistence.</summary>
    private const string CompareExchangeIdentifier = "compareExchangeStore";

    /// <summary>The JS function used to load one persisted store document.</summary>
    private const string LoadStoreIdentifier = "loadStore";

    /// <summary>The exception message used when initialization has not completed.</summary>
    private const string StoreNotInitializedMessage = "The IndexedDB store must be initialized before use.";

    /// <summary>The serializer options used by the generated JSON context.</summary>
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General)
    {
        Converters = { new StreamIdConverter(), new PayloadEnvelopeConverter(), new LocalSnapshotConverter() },
    };

    /// <summary>The source-generated JSON metadata for IndexedDB state documents.</summary>
    private static readonly IndexedDbJsonContext JsonContext = new(JsonOptions);

    /// <summary>The configured IndexedDB database name.</summary>
    private readonly string _databaseName;

    /// <summary>The configured JS module import path.</summary>
    private readonly string _modulePath;

    /// <summary>The configured IndexedDB object store name.</summary>
    private readonly string _objectStoreName;

    /// <summary>The JS runtime used to import and call the IndexedDB module.</summary>
    private readonly IJSRuntime _jsRuntime;

    /// <summary>Serializes access to initialization, mutation, and disposal.</summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Completes when asynchronous disposal has finished.</summary>
    private readonly TaskCompletionSource<bool> _disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Provides the current time for durable timestamps and lease expiry.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The imported JS module once initialization has used it.</summary>
    private IJSObjectReference? _module;

    /// <summary>Whether asynchronous disposal has started.</summary>
    private int _disposed;

    /// <summary>Whether initialization has completed successfully.</summary>
    private int _initialized;

    /// <summary>The currently initialized durable store identity.</summary>
    private string? _storeIdentity;

    /// <summary>Initializes a new instance of the <see cref="IndexedDbLocalStoreAdapter"/> class.</summary>
    /// <param name="jsRuntime">The JS runtime used to reach IndexedDB.</param>
    public IndexedDbLocalStoreAdapter(IJSRuntime jsRuntime)
        : this(jsRuntime, TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="IndexedDbLocalStoreAdapter"/> class with a time provider.</summary>
    /// <param name="jsRuntime">The JS runtime used to reach IndexedDB.</param>
    /// <param name="timeProvider">The time provider used for durable timestamps and lease expiry.</param>
    public IndexedDbLocalStoreAdapter(IJSRuntime jsRuntime, TimeProvider timeProvider)
        : this(jsRuntime, timeProvider, DefaultModulePath, DefaultDatabaseName, DefaultObjectStoreName)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="IndexedDbLocalStoreAdapter"/> class with an internal module override.</summary>
    /// <param name="jsRuntime">The JS runtime used to reach IndexedDB.</param>
    /// <param name="timeProvider">The time provider used for durable timestamps and lease expiry.</param>
    /// <param name="modulePath">The module path used for import.</param>
    /// <param name="databaseName">The IndexedDB database name.</param>
    /// <param name="objectStoreName">The IndexedDB object store name.</param>
    /// <exception cref="ArgumentNullException"><paramref name="jsRuntime"/> or <paramref name="timeProvider"/> is null.</exception>
    /// <exception cref="ArgumentException">A string argument is null, empty, or whitespace.</exception>
    internal IndexedDbLocalStoreAdapter(
        IJSRuntime jsRuntime,
        TimeProvider timeProvider,
        string modulePath,
        string databaseName,
        string objectStoreName)
    {
        ArgumentExceptionHelper.ThrowIfNull(jsRuntime);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
#if NET5_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(modulePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(objectStoreName);
#else
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(modulePath);
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(objectStoreName);
#endif

        _databaseName = databaseName;
        _jsRuntime = jsRuntime;
        _modulePath = modulePath;
        _objectStoreName = objectStoreName;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc/>
    public LocalStoreCapabilities Capabilities =>
        LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply
        | LocalStoreCapabilities.DurableInbox
        | LocalStoreCapabilities.LeasedOutbox
        | LocalStoreCapabilities.DurableLocalCommit
        | LocalStoreCapabilities.ClientIdentityBinding;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        if (initialization.RequireAuthenticatedEncryptionAtRest)
        {
            throw new NotSupportedException("Authenticated encryption at rest is not implemented by the IndexedDB adapter.");
        }

        if (initialization.RequiredSchemaVersion > CurrentFormatVersion)
        {
            throw new InvalidOperationException(
                $"The IndexedDB store supports schema version {CurrentFormatVersion}, not {initialization.RequiredSchemaVersion}.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (Volatile.Read(ref _initialized) != 0)
            {
                ValidateStoreIdentity(await LoadStateAsync(cancellationToken).ConfigureAwait(false), initialization);
                return;
            }

            await InitializeStoreUnderLockAsync(initialization, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompletion.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_module is not null)
                {
                    try
                    {
                        await _module.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (JSDisconnectedException)
                    {
                    }
                }

                _module = null;
            }
            finally
            {
                _ = _gate.Release();
                _gate.Dispose();
            }

            _ = _disposeCompletion.TrySetResult(true);
        }
        catch (Exception error)
        {
            _ = _disposeCompletion.TrySetException(error);
            throw;
        }
    }

    /// <summary>Serializes a store state to the JSON document kept in IndexedDB.</summary>
    /// <param name="state">The state to serialize.</param>
    /// <returns>The serialized JSON document.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SerializeState(StoreState state) => JsonSerializer.Serialize(state, JsonContext.StoreState);

    /// <summary>Throws when a loaded state is bound to another store or client identity.</summary>
    /// <param name="state">The loaded durable state.</param>
    /// <param name="initialization">The requested initialization values.</param>
    /// <exception cref="InvalidOperationException">The store identity or client identity does not match.</exception>
    private static void ValidateStoreIdentity(StoreState state, LocalStoreInitialization initialization)
    {
        if (state.StoreIdentity is not null
            && !string.Equals(state.StoreIdentity, initialization.StoreIdentity, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The IndexedDB store is bound to another store identity.");
        }

        if (state.ClientId is not null
            && !string.Equals(state.ClientId, initialization.ClientId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The IndexedDB store is bound to another client identity.");
        }
    }

    /// <summary>Compares the stable fields used during initialization.</summary>
    /// <param name="left">The currently loaded state.</param>
    /// <param name="right">The next state.</param>
    /// <returns>True when initialization would persist a change.</returns>
    private static bool StateChanged(StoreState left, StoreState right) =>
        !string.Equals(left.StoreIdentity, right.StoreIdentity, StringComparison.Ordinal)
        || !string.Equals(left.ClientId, right.ClientId, StringComparison.Ordinal)
        || !EqualityComparer<OutboxOptions?>.Default.Equals(left.Outbox, right.Outbox);

    /// <summary>Computes the UTF-8 byte count for one JSON document.</summary>
    /// <param name="json">The JSON text.</param>
    /// <returns>The UTF-8 byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetUtf8ByteCount(string json) => System.Text.Encoding.UTF8.GetByteCount(json);

    /// <summary>Creates the durable snapshot represented by a mutation.</summary>
    /// <param name="mutation">The snapshot mutation.</param>
    /// <param name="cursor">The server cursor to associate with the snapshot.</param>
    /// <param name="revision">The committed revision.</param>
    /// <param name="authoritativeState">The authoritative state to store with the snapshot.</param>
    /// <returns>The resulting snapshot.</returns>
    private LocalSnapshot CreateSnapshot(
        SnapshotMutation mutation,
        string? cursor,
        long revision,
        PayloadEnvelope? authoritativeState) =>
        new(mutation.StreamId, mutation.FormatVersion, cursor, mutation.State, revision, _timeProvider.GetUtcNow()) { AuthoritativeState = authoritativeState ?? mutation.AuthoritativeState };

    /// <summary>Imports the IndexedDB module through JS interop.</summary>
    /// <param name="cancellationToken">The token used to cancel import.</param>
    /// <returns>The imported module reference.</returns>
    /// <exception cref="JSException">The JS runtime cannot import the module.</exception>
    private async ValueTask<IJSObjectReference> ImportModuleAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _jsRuntime.InvokeAsync<IJSObjectReference>("import", cancellationToken, _modulePath).ConfigureAwait(false);
        }
        catch (JSException error)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
            throw;
        }
    }

    /// <summary>Ensures the current adapter has imported its JS module.</summary>
    /// <param name="cancellationToken">The token used to cancel import.</param>
    /// <returns>The imported JS module reference.</returns>
    /// <exception cref="JSException">The JS runtime cannot import the module.</exception>
    private async ValueTask<IJSObjectReference> GetModuleAsync(CancellationToken cancellationToken)
    {
        if (_module is not null)
        {
            return _module;
        }

        _module = await ImportModuleAsync(cancellationToken).ConfigureAwait(false);
        return _module;
    }

    /// <summary>Checks that this adapter is open and initialized.</summary>
    /// <exception cref="InvalidOperationException">The adapter has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">The adapter has been disposed.</exception>
    private void EnsureInitialized()
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _initialized) == 0 || string.IsNullOrWhiteSpace(_storeIdentity))
        {
            throw new InvalidOperationException(StoreNotInitializedMessage);
        }
    }

    /// <summary>Loads the current store state for a store identity.</summary>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="cancellationToken">The token used to cancel the JS interop call.</param>
    /// <returns>The loaded state, or a new empty state when nothing is stored.</returns>
    /// <exception cref="JSException">The JS module cannot load the persisted state.</exception>
    /// <exception cref="InvalidDataException">The persisted document has no valid generation or store identity.</exception>
    private async ValueTask<StoreState> LoadStateAsync(string storeIdentity, CancellationToken cancellationToken)
    {
        var module = await GetModuleAsync(cancellationToken).ConfigureAwait(false);
        string? json;
        try
        {
            json = await module.InvokeAsync<string?>(
                LoadStoreIdentifier,
                cancellationToken,
                _databaseName,
                _objectStoreName,
                storeIdentity).ConfigureAwait(false);
        }
        catch (JSException error)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
            throw;
        }

        if (json is null)
        {
            return new();
        }

        var state = JsonSerializer.Deserialize(json, JsonContext.StoreState)
            ?? throw new InvalidDataException("The IndexedDB store document is invalid.");
        if (state.Generation <= 0 || !string.Equals(state.StoreIdentity, storeIdentity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The IndexedDB store document has an invalid generation or store identity.");
        }

        return state;
    }

    /// <summary>Loads the current state for the initialized store identity.</summary>
    /// <param name="cancellationToken">The token used to cancel the JS interop call.</param>
    /// <returns>The loaded state.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not been initialized.</exception>
    /// <exception cref="JSException">The JS module cannot load the persisted state.</exception>
    private ValueTask<StoreState> LoadStateAsync(CancellationToken cancellationToken)
    {
        var storeIdentity = _storeIdentity
            ?? throw new InvalidOperationException(StoreNotInitializedMessage);
        return LoadStateAsync(storeIdentity, cancellationToken);
    }

    /// <summary>Attempts to write the next generation of store state.</summary>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="expectedGeneration">The generation observed before mutation.</param>
    /// <param name="next">The next state to persist.</param>
    /// <param name="cancellationToken">The token used to cancel the JS interop call.</param>
    /// <returns>True when the write succeeded; otherwise false.</returns>
    /// <exception cref="JSException">The JS module cannot persist the updated state.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<bool> TryWriteStateAsync(
        string storeIdentity,
        long expectedGeneration,
        StoreState next,
        CancellationToken cancellationToken)
    {
        var module = await GetModuleAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await module.InvokeAsync<bool>(
                CompareExchangeIdentifier,
                cancellationToken,
                _databaseName,
                _objectStoreName,
                storeIdentity,
                expectedGeneration,
                SerializeState(next)).ConfigureAwait(false);
        }
        catch (JSException error)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
            throw;
        }
    }

    /// <summary>Persists the initialized store record with optimistic compare-exchange.</summary>
    /// <param name="initialization">The requested initialization values.</param>
    /// <param name="cancellationToken">The token used to cancel initialization.</param>
    /// <returns>A task that completes when initialization is durable.</returns>
    /// <exception cref="InvalidOperationException">The store is already bound to a different identity.</exception>
    /// <exception cref="JSException">The JS module cannot load or persist the store state.</exception>
    private async ValueTask InitializeStoreUnderLockAsync(
        LocalStoreInitialization initialization,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var current = await LoadStateAsync(initialization.StoreIdentity, cancellationToken).ConfigureAwait(false);
            ValidateStoreIdentity(current, initialization);

            var next = new StoreState(current) { ClientId = initialization.ClientId, Outbox = initialization.Outbox ?? current.Outbox, StoreIdentity = initialization.StoreIdentity };

            _storeIdentity = initialization.StoreIdentity;
            if (!StateChanged(current, next))
            {
                _ = Interlocked.Exchange(ref _initialized, 1);
                return;
            }

            next.Generation = current.Generation + 1;
            cancellationToken.ThrowIfCancellationRequested();
            if (!await TryWriteStateAsync(initialization.StoreIdentity, current.Generation, next, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            _ = Interlocked.Exchange(ref _initialized, 1);
            return;
        }
    }

    /// <summary>Loads the current state, applies a mutation, and retries on generation conflicts.</summary>
    /// <typeparam name="TResult">The mutation result type.</typeparam>
    /// <param name="mutation">The mutation to apply to a copied state.</param>
    /// <param name="cancellationToken">The token used to cancel the mutation.</param>
    /// <returns>The committed mutation result.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not been initialized.</exception>
    /// <exception cref="JSException">The JS module cannot load or persist the store state.</exception>
    private async ValueTask<TResult> ExecuteMutationAsync<TResult>(
        Func<StoreState, MutationOutcome<TResult>> mutation,
        CancellationToken cancellationToken)
    {
        var storeIdentity = _storeIdentity
            ?? throw new InvalidOperationException(StoreNotInitializedMessage);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var current = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
            var working = new StoreState(current);
            var outcome = mutation(working);
            if (!outcome.HasChanges)
            {
                return outcome.Result;
            }

            outcome.NextState.Generation = current.Generation + 1;
            cancellationToken.ThrowIfCancellationRequested();
            if (!await TryWriteStateAsync(storeIdentity, current.Generation, outcome.NextState, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            return outcome.Result;
        }
    }

    /// <summary>Gets an active lease or throws when it is missing or expired.</summary>
    /// <param name="state">The loaded durable state.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <returns>The active lease state.</returns>
    /// <exception cref="InvalidOperationException">The lease is missing or expired.</exception>
    private LeaseState GetLease(StoreState state, Guid leaseId)
    {
        if (!state.Leases.TryGetValue(leaseId, out var lease) || lease.ExpiresAtUtc <= _timeProvider.GetUtcNow())
        {
            throw new InvalidOperationException("The lease is missing or expired.");
        }

        return lease;
    }

    /// <summary>Throws when this adapter has been disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Contains the complete durable state represented by one IndexedDB document.</summary>
    internal sealed class StoreState
    {
        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class.</summary>
        public StoreState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class by copying another durable store state.</summary>
        /// <param name="other">The state to copy.</param>
        public StoreState(StoreState other)
        {
            ClientId = other.ClientId;
            Generation = other.Generation;
            Outbox = other.Outbox;
            StoreIdentity = other.StoreIdentity;
            foreach (var pair in other.Streams)
            {
                Streams.Add(pair.Key, new(pair.Value));
            }

            foreach (var pair in other.Leases)
            {
                var lease = new LeaseState { ExpiresAtUtc = pair.Value.ExpiresAtUtc, LeaseId = pair.Value.LeaseId };
                lease.OperationIds.AddRange(pair.Value.OperationIds);
                Leases.Add(pair.Key, lease);
            }

            Inbox = new(other.Inbox);
            IncludedOperations = new(other.IncludedOperations);
        }

        /// <summary>Gets or sets the client identity bound to this durable store.</summary>
        public string? ClientId { get; set; }

        /// <summary>Gets or sets the optimistic compare-exchange generation.</summary>
        public long Generation { get; set; }

        /// <summary>Gets the durable inbox keys for applied remote events.</summary>
        public HashSet<string> Inbox { get; init; } = [];

        /// <summary>Gets the locally originated operations already reflected by authoritative remote state.</summary>
        public HashSet<Guid> IncludedOperations { get; init; } = [];

        /// <summary>Gets the active durable upload leases keyed by lease identifier.</summary>
        public Dictionary<Guid, LeaseState> Leases { get; init; } = [];

        /// <summary>Gets or sets the configured durable outbox options.</summary>
        public OutboxOptions? Outbox { get; set; }

        /// <summary>Gets or sets the durable store identity.</summary>
        public string? StoreIdentity { get; set; }

        /// <summary>Gets the durable per-stream state keyed by stream identifier.</summary>
        public Dictionary<string, StreamState> Streams { get; init; } = [];
    }

    /// <summary>Contains durable state associated with one logical stream.</summary>
    internal sealed class StreamState
    {
        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class.</summary>
        public StreamState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class by copying another stream state.</summary>
        /// <param name="other">The stream state to copy.</param>
        public StreamState(StreamState other)
        {
            Cursor = other.Cursor;
            DeadLetters = new(other.DeadLetters);
            NextSequence = other.NextSequence;
            Snapshot = other.Snapshot;
            SubscriptionId = other.SubscriptionId;
            foreach (var pair in other.Operations)
            {
                Operations.Add(pair.Key, new(pair.Value));
            }
        }

        /// <summary>Gets or sets the last known remote cursor for the stream.</summary>
        public string? Cursor { get; set; }

        /// <summary>Gets the durable dead-letter records for the stream.</summary>
        public List<DeadLetterRecord> DeadLetters { get; init; } = [];

        /// <summary>Gets or sets the next expected client sequence for local commits.</summary>
        public long NextSequence { get; set; }

        /// <summary>Gets the durable operations keyed by operation identifier.</summary>
        public Dictionary<Guid, OperationState> Operations { get; init; } = [];

        /// <summary>Gets or sets the current durable snapshot.</summary>
        public LocalSnapshot? Snapshot { get; set; }

        /// <summary>Gets or sets the durable subscription identity.</summary>
        public SubscriptionId? SubscriptionId { get; set; }
    }

    /// <summary>Tracks local status and lease metadata for one operation.</summary>
    internal sealed class OperationState
    {
        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class.</summary>
        public OperationState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class by copying another operation state.</summary>
        /// <param name="other">The operation state to copy.</param>
        public OperationState(OperationState other)
        {
            LeaseExpiry = other.LeaseExpiry;
            LeaseId = other.LeaseId;
            Operation = other.Operation;
            RetryState = other.RetryState;
            Status = other.Status;
            Terminal = other.Terminal;
        }

        /// <summary>Gets or sets the current lease expiry for the operation.</summary>
        public DateTimeOffset? LeaseExpiry { get; set; }

        /// <summary>Gets or sets the current lease identifier for the operation.</summary>
        public Guid? LeaseId { get; set; }

        /// <summary>Gets or sets the durable operation payload and metadata.</summary>
        public SyncOperation Operation { get; set; } = null!;

        /// <summary>Gets or sets the persisted retry state for the operation.</summary>
        public RetryState? RetryState { get; set; }

        /// <summary>Gets or sets the current durable operation status.</summary>
        public SyncOperationStatus Status { get; set; } = null!;

        /// <summary>Gets or sets a value indicating whether the operation has reached a terminal state.</summary>
        public bool Terminal { get; set; }
    }

    /// <summary>Records durable ownership and expiry for a leased operation batch.</summary>
    internal sealed class LeaseState
    {
        /// <summary>Gets or sets the lease expiry time.</summary>
        public DateTimeOffset ExpiresAtUtc { get; set; }

        /// <summary>Gets or sets the lease identifier.</summary>
        public Guid LeaseId { get; set; }

        /// <summary>Gets the leased operation identifiers.</summary>
        [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
        public List<Guid> OperationIds { get; } = [];
    }

    /// <summary>Serializes and deserializes <see cref="PayloadEnvelope"/> values.</summary>
    private sealed class PayloadEnvelopeConverter : JsonConverter<PayloadEnvelope>
    {
        /// <inheritdoc/>
        public override PayloadEnvelope Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            return new(
                root.GetProperty("ContractId").GetString()!,
                root.GetProperty("SchemaVersion").GetInt32(),
                root.GetProperty("ContentType").GetString()!,
                Convert.FromBase64String(root.GetProperty("Payload").GetString()!),
                root.GetProperty("PayloadHash").GetString()!);
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, PayloadEnvelope value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("ContractId", value.ContractId);
            writer.WriteNumber("SchemaVersion", value.SchemaVersion);
            writer.WriteString("ContentType", value.ContentType);
            writer.WriteString("Payload", Convert.ToBase64String(value.Payload.ToArray()));
            writer.WriteString("PayloadHash", value.PayloadHash);
            writer.WriteEndObject();
        }
    }

    /// <summary>Serializes and deserializes <see cref="StreamId"/> values.</summary>
    private sealed class StreamIdConverter : JsonConverter<StreamId>
    {
        /// <inheritdoc/>
        public override StreamId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            return new(document.RootElement.GetProperty(nameof(StreamId.Value)).GetString()!);
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, StreamId value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(StreamId.Value), value.Value);
            writer.WriteEndObject();
        }
    }

    /// <summary>Serializes and deserializes <see cref="LocalSnapshot"/> values.</summary>
    private sealed class LocalSnapshotConverter : JsonConverter<LocalSnapshot>
    {
        /// <inheritdoc/>
        public override LocalSnapshot Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            var snapshot = new LocalSnapshot(
                new(root.GetProperty(nameof(StreamId)).GetProperty("Value").GetString()!),
                root.GetProperty("FormatVersion").GetInt32(),
                root.GetProperty("ServerCursor").GetString(),
                JsonSerializer.Deserialize(root.GetProperty("State"), JsonContext.PayloadEnvelope)!,
                root.GetProperty("Revision").GetInt64(),
                root.GetProperty("SavedAtUtc").GetDateTimeOffset());
            if (root.TryGetProperty("AuthoritativeState", out var authoritative) && authoritative.ValueKind != JsonValueKind.Null)
            {
                snapshot = snapshot with
                {
                    AuthoritativeState = JsonSerializer.Deserialize(authoritative, JsonContext.PayloadEnvelope),
                };
            }

            return snapshot;
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, LocalSnapshot value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(StreamId));
            writer.WriteStartObject();
            writer.WriteString("Value", value.StreamId.Value);
            writer.WriteEndObject();
            writer.WriteNumber("FormatVersion", value.FormatVersion);
            writer.WriteString("ServerCursor", value.ServerCursor);
            writer.WritePropertyName("State");
            JsonSerializer.Serialize(writer, value.State, JsonContext.PayloadEnvelope);
            writer.WriteNumber("Revision", value.Revision);
            writer.WriteString("SavedAtUtc", value.SavedAtUtc);
            writer.WritePropertyName("AuthoritativeState");
            if (value.AuthoritativeState is null)
            {
                writer.WriteNullValue();
            }
            else
            {
                JsonSerializer.Serialize(writer, value.AuthoritativeState, JsonContext.PayloadEnvelope);
            }

            writer.WriteEndObject();
        }
    }

    /// <summary>Contains the result of a mutation applied to a copied durable state.</summary>
    /// <typeparam name="TResult">The mutation result type.</typeparam>
    /// <param name="NextState">The next durable state to persist.</param>
    /// <param name="Result">The mutation result returned to the caller.</param>
    /// <param name="HasChanges">Whether the mutation changed durable state.</param>
    internal sealed record MutationOutcome<TResult>(StoreState NextState, TResult Result, bool HasChanges);
}
