// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using LiteDB;
using ReactiveUI.Primitives.OccasionallyConnected;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb;

/// <summary>Stores local commits in a transactional LiteDB document.</summary>
[System.Diagnostics.DebuggerDisplay("LiteDbLocalStoreAdapter: {Capabilities}")]
public sealed partial class LiteDbLocalStoreAdapter : ILocalStoreAdapter
{
    /// <summary>The current durable store schema version.</summary>
    private const int CurrentFormatVersion = 1;

    /// <summary>The collection holding the durable store document.</summary>
    private const string StoreCollectionName = "rxui_store";

    /// <summary>The single durable store document identifier.</summary>
    private const string StoreDocumentId = "store";

    /// <summary>The serializer options used to create the generated JSON context.</summary>
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General)
    {
        Converters = { new StreamIdConverter(), new PayloadEnvelopeConverter(), new LocalSnapshotConverter() },
    };

    /// <summary>The source-generated metadata used for store serialization.</summary>
    private static readonly LiteDbJsonContext JsonContext = new(JsonOptions);

    /// <summary>The configured LiteDB database path.</summary>
    private readonly string _databasePath;

    /// <summary>Provides the current time for durable operation metadata and lease checks.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Serializes access to the database and in-memory state.</summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Completes when the adapter has finished disposing.</summary>
    private readonly TaskCompletionSource<bool> _disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The open LiteDB database.</summary>
    private LiteDatabase? _database;

    /// <summary>The latest durable state loaded from or persisted to the database.</summary>
    private StoreState _state = new();

    /// <summary>Whether initialization has completed successfully.</summary>
    private int _initialized;

    /// <summary>Whether this adapter has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="LiteDbLocalStoreAdapter"/> class.</summary>
    /// <param name="databasePath">The configured store database path.</param>
    public LiteDbLocalStoreAdapter(string databasePath)
        : this(databasePath, TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LiteDbLocalStoreAdapter"/> class with a time provider.</summary>
    /// <param name="databasePath">The configured store database path.</param>
    /// <param name="timeProvider">The time provider used for operation timestamps and lease expiry.</param>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is null, empty, or whitespace.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is null.</exception>
    public LiteDbLocalStoreAdapter(string databasePath, TimeProvider timeProvider)
    {
#if NET5_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
#else
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(databasePath);
#endif
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);

        _databasePath = Path.GetFullPath(databasePath);
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
            throw new NotSupportedException("Authenticated encryption at rest is not implemented by the LiteDB adapter.");
        }

        if (initialization.RequiredSchemaVersion > CurrentFormatVersion)
        {
            throw new InvalidOperationException(
                $"The LiteDB store supports schema version {CurrentFormatVersion}, not {initialization.RequiredSchemaVersion}.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (Volatile.Read(ref _initialized) != 0)
            {
                return;
            }

            InitializeStoreUnderLock(initialization, cancellationToken);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>Loads the durable store state from LiteDB.</summary>
    /// <param name="database">The open database.</param>
    /// <returns>The recovered state, or an empty state when no record exists.</returns>
    /// <exception cref="InvalidDataException">The persisted store document is invalid.</exception>
    /// <exception cref="InvalidOperationException">The persisted schema version is newer than this adapter supports.</exception>
    private static StoreState LoadState(LiteDatabase database)
    {
        var collection = database.GetCollection<BsonDocument>(StoreCollectionName);
        var persisted = collection.FindById(StoreDocumentId);
        if (persisted is null)
        {
            return new();
        }

        if (!persisted.TryGetValue("StateJson", out var stateJsonValue) || !stateJsonValue.IsString)
        {
            throw new InvalidDataException("The LiteDB store document is missing its serialized state.");
        }

        var document = JsonSerializer.Deserialize(stateJsonValue.AsString, JsonContext.StoreDocument)
            ?? throw new InvalidDataException("The LiteDB store document is invalid.");
        if (document.SchemaVersion > CurrentFormatVersion)
        {
            throw new InvalidOperationException(
                $"The LiteDB store supports schema version {CurrentFormatVersion}, not {document.SchemaVersion}.");
        }

        return document.State;
    }

    /// <summary>Opens, recovers, binds, and initializes the store while the gate is held.</summary>
    /// <param name="initialization">The requested store and client identities.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private void InitializeStoreUnderLock(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(Path.GetDirectoryName(_databasePath) ?? ".");
        try
        {
            _database = OpenDatabase();
            _state = LoadState(_database);
            ValidateStoreIdentity(initialization);
            _state.StoreIdentity = initialization.StoreIdentity;
            _state.ClientId = initialization.ClientId;
            if (initialization.Outbox is not null)
            {
                _state.Outbox = initialization.Outbox;
            }

            PersistState(_state, cancellationToken);
            _ = Interlocked.Exchange(ref _initialized, 1);
        }
        catch
        {
            _database?.Dispose();
            _database = null;
            throw;
        }
    }

    /// <summary>Ensures recovered identities match the requested initialization.</summary>
    /// <param name="initialization">The requested store and client identities.</param>
    /// <exception cref="InvalidOperationException">The store is already bound to different identities.</exception>
    private void ValidateStoreIdentity(LocalStoreInitialization initialization)
    {
        if (_state.StoreIdentity is not null
            && !string.Equals(_state.StoreIdentity, initialization.StoreIdentity, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The LiteDB store is bound to another store identity.");
        }

        if (_state.ClientId is not null
            && !string.Equals(_state.ClientId, initialization.ClientId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The LiteDB store is bound to another client identity.");
        }
    }

    /// <summary>Opens the store database using a single direct connection.</summary>
    /// <returns>The open LiteDB database.</returns>
    private LiteDatabase OpenDatabase() => new(new ConnectionString { Filename = _databasePath, Connection = ConnectionType.Direct, });

    /// <summary>Persists a complete durable state document transactionally.</summary>
    /// <param name="next">The state to persist.</param>
    /// <param name="cancellationToken">The cancellation token observed before the transaction starts.</param>
    private void PersistState(StoreState next, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = _database!;
        var collection = database.GetCollection<BsonDocument>(StoreCollectionName);
        var json = JsonSerializer.Serialize(new(CurrentFormatVersion, next), JsonContext.StoreDocument);
        var document = new BsonDocument { ["_id"] = StoreDocumentId, ["StateJson"] = json, };

        _ = database.BeginTrans();
        try
        {
            _ = collection.Upsert(document);
            _ = database.Commit();
            _state = next;
        }
        catch
        {
            _ = database.Rollback();
            throw;
        }
    }

    /// <summary>Gets an active lease or throws when it is missing or expired.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <returns>The matching lease state.</returns>
    /// <exception cref="InvalidOperationException">The lease is missing or expired.</exception>
    private LeaseState GetLease(StoreState state, Guid leaseId)
    {
        if (!state.Leases.TryGetValue(leaseId, out var lease) || lease.ExpiresAtUtc <= _timeProvider.GetUtcNow())
        {
            throw new InvalidOperationException("The lease is missing or expired.");
        }

        return lease;
    }

    /// <summary>Creates the durable snapshot represented by a mutation.</summary>
    /// <param name="mutation">The snapshot mutation.</param>
    /// <param name="cursor">The server cursor to associate with the snapshot.</param>
    /// <param name="revision">The committed revision.</param>
    /// <returns>The resulting snapshot.</returns>
    private LocalSnapshot CreateSnapshot(SnapshotMutation mutation, string? cursor, long revision) => new(
        mutation.StreamId,
        mutation.FormatVersion,
        cursor,
        mutation.State,
        revision,
        _timeProvider.GetUtcNow())
    { AuthoritativeState = mutation.AuthoritativeState, };

    /// <summary>Checks that this adapter is open and initialized.</summary>
    /// <exception cref="InvalidOperationException">The adapter has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">The adapter has been disposed.</exception>
    private void EnsureInitialized()
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _initialized) == 0 || _database is null)
        {
            throw new InvalidOperationException("The LiteDB store must be initialized before use.");
        }
    }

    /// <summary>Throws when this adapter has been disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    /// <summary>Contains the complete durable state represented by the LiteDB store document.</summary>
    internal sealed class StoreState
    {
        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class.</summary>
        public StoreState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StoreState"/> class by copying an existing state.</summary>
        /// <param name="other">The source state.</param>
        public StoreState(StoreState other)
        {
            StoreIdentity = other.StoreIdentity;
            ClientId = other.ClientId;
            Outbox = other.Outbox;
            foreach (var pair in other.Streams)
            {
                Streams.Add(pair.Key, new(pair.Value));
            }

            foreach (var pair in other.Leases)
            {
                Leases.Add(pair.Key, new LeaseState { LeaseId = pair.Value.LeaseId, ExpiresAtUtc = pair.Value.ExpiresAtUtc, OperationIds = new(pair.Value.OperationIds), });
            }

            Inbox = new(other.Inbox);
            IncludedOperations = new(other.IncludedOperations);
        }

        /// <summary>Gets or sets the identity of the store instance.</summary>
        public string? StoreIdentity { get; set; }

        /// <summary>Gets or sets the bound client identity.</summary>
        public string? ClientId { get; set; }

        /// <summary>Gets or sets the configured outbox limits.</summary>
        public OutboxOptions? Outbox { get; set; }

        /// <summary>Gets durable state indexed by stream identifier.</summary>
        public Dictionary<string, StreamState> Streams { get; init; } = [];

        /// <summary>Gets active outbox leases indexed by lease identifier.</summary>
        public Dictionary<Guid, LeaseState> Leases { get; init; } = [];

        /// <summary>Gets durably applied remote event identities.</summary>
        public HashSet<string> Inbox { get; init; } = [];

        /// <summary>Gets local operations already included in authoritative snapshots.</summary>
        public HashSet<Guid> IncludedOperations { get; init; } = [];
    }

    /// <summary>Contains durable state associated with one logical stream.</summary>
    internal sealed class StreamState
    {
        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class.</summary>
        public StreamState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="StreamState"/> class by copying an existing state.</summary>
        /// <param name="other">The source stream state.</param>
        public StreamState(StreamState other)
        {
            NextSequence = other.NextSequence;
            Snapshot = other.Snapshot;
            Cursor = other.Cursor;
            SubscriptionId = other.SubscriptionId;
            foreach (var pair in other.Operations)
            {
                Operations.Add(pair.Key, new(pair.Value));
            }

            DeadLetters = new(other.DeadLetters);
        }

        /// <summary>Gets or sets the persisted logical subscription identity.</summary>
        public SubscriptionId? SubscriptionId { get; set; }

        /// <summary>Gets or sets the next client sequence assigned to this stream.</summary>
        public long NextSequence { get; set; }

        /// <summary>Gets or sets the latest locally committed snapshot.</summary>
        public LocalSnapshot? Snapshot { get; set; }

        /// <summary>Gets or sets the latest durably applied remote cursor.</summary>
        public string? Cursor { get; set; }

        /// <summary>Gets operations retained in the durable outbox.</summary>
        public Dictionary<Guid, OperationState> Operations { get; init; } = [];

        /// <summary>Gets terminal operations retained in the dead-letter store.</summary>
        public List<DeadLetterRecord> DeadLetters { get; init; } = [];
    }

    /// <summary>Tracks local status and lease metadata for one operation.</summary>
    internal sealed class OperationState
    {
        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class.</summary>
        public OperationState()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="OperationState"/> class by copying an existing state.</summary>
        /// <param name="other">The source operation state.</param>
        public OperationState(OperationState other)
        {
            Operation = other.Operation;
            Status = other.Status;
            RetryState = other.RetryState;
            LeaseId = other.LeaseId;
            LeaseExpiry = other.LeaseExpiry;
            Terminal = other.Terminal;
        }

        /// <summary>Gets or sets the immutable operation payload and metadata.</summary>
        public SyncOperation Operation { get; set; } = null!;

        /// <summary>Gets or sets the latest local operation status.</summary>
        public SyncOperationStatus Status { get; set; } = null!;

        /// <summary>Gets or sets the persisted retry schedule, when one exists.</summary>
        public RetryState? RetryState { get; set; }

        /// <summary>Gets or sets the active lease identifier.</summary>
        public Guid? LeaseId { get; set; }

        /// <summary>Gets or sets the active lease expiry time.</summary>
        public DateTimeOffset? LeaseExpiry { get; set; }

        /// <summary>Gets or sets whether this operation reached a terminal state.</summary>
        public bool Terminal { get; set; }
    }

    /// <summary>Records durable ownership and expiry for a leased operation batch.</summary>
    internal sealed class LeaseState
    {
        /// <summary>Gets or sets the lease identifier.</summary>
        public Guid LeaseId { get; set; }

        /// <summary>Gets or sets the lease expiry time.</summary>
        public DateTimeOffset ExpiresAtUtc { get; set; }

        /// <summary>Gets or sets the operation identifiers owned by the lease.</summary>
        public List<Guid> OperationIds { get; init; } = [];
    }

    /// <summary>Serializes payload envelopes using a stable persisted shape.</summary>
    private sealed class PayloadEnvelopeConverter : JsonConverter<PayloadEnvelope>
    {
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

    /// <summary>Preserves the object-shaped stream identifier stored by the database document.</summary>
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

    /// <summary>Serializes snapshots while preserving their public value shape.</summary>
    private sealed class LocalSnapshotConverter : JsonConverter<LocalSnapshot>
    {
        public override LocalSnapshot Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            var snapshot = new LocalSnapshot(
                new StreamId(root.GetProperty(nameof(StreamId)).GetProperty("Value").GetString()!),
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

    /// <summary>Represents the one serialized store document kept in LiteDB.</summary>
    /// <param name="SchemaVersion">The durable schema version.</param>
    /// <param name="State">The persisted store state.</param>
    internal sealed record StoreDocument(int SchemaVersion, StoreState State);
}
