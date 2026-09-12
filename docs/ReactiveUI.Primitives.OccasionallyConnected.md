# ReactiveUI.Primitives.OccasionallyConnected Design Specification

> Status: implementation-ready proposal  
> Package family: `ReactiveUI.Primitives.OccasionallyConnected`  
> Language: C#  
> Primary API model: BCL `IObservable<T>` plus explicit asynchronous commands  
> Normative terms: **MUST**, **SHOULD**, and **MAY** have their RFC 2119 meanings.

## 1. Executive summary

`ReactiveUI.Primitives.OccasionallyConnected` provides local-first reactive streams whose logical subscriptions and outbound writes survive intermittent connectivity, process termination, application crashes, and power loss. It preserves the familiar `IObservable<T>`/`IObserver<T>` consumption model while making durability, replay, synchronization, conflict handling, and bounded resource use explicit.

The library is transport-agnostic. Storage and transport adapters are composed behind stable contracts, so HTTP, WebSocket, MQTT, SignalR, custom TCP, SQLite, IndexedDB, and file-backed implementations can be supplied independently.

An in-memory `IDisposable` subscription cannot literally survive process death. The library instead persists a durable `SubscriptionId`, remote cursor, inbox deduplication records, local snapshot, and pending operation log. When the application recreates and starts the same stream, the engine automatically restores and resumes that logical subscription without allocating a new remote subscription identity.

The default delivery guarantee is **at least once** with idempotent operation IDs and durable deduplication. The existing `ExactlyOnce` term is retained, but it is capability-gated and precisely means “exactly-once effect within the configured deduplication-retention window.” Configuration MUST fail if the selected store, transport, or server cannot provide the required transactional and idempotency capabilities.

## 2. Goals

- Extend the ReactiveUI.Primitives package family with resilient subscriptions that recover from power loss, application crash, process restart, and transient connection loss.
- Preserve `IObservable<T>`/`IObserver<T>` semantics at application boundaries while using awaitable APIs wherever persistence or acknowledgement can fail.
- Make offline and occasionally connected operation a first-class concern rather than a bolt-on cache.
- Keep the core independent of HTTP, WebSocket, MQTT, SignalR, database, UI framework, and hosting-container implementations.
- Support local-first reads and optimistic writes across IoT, mobile, desktop, server, and web environments.
- Provide deterministic, pluggable conflict resolution including last-writer-wins, CRDT-compatible resolvers, and domain-specific merge logic.
- Bound memory, disk, concurrency, retry, and batch behavior under sustained disconnection or slow consumers.
- Provide observable lifecycle, synchronization, diagnostics, and health state without leaking payloads or high-cardinality identifiers by default.
- Align with ReactiveUI.Primitives naming and sequencing conventions: lean packages use `RxVoid` and `ISequencer`; System.Reactive-facing behavior belongs in an explicit `.Reactive` variant.

## 3. Non-goals

- Providing a distributed database, general event store, message broker, or consensus protocol.
- Claiming unconditional exactly-once delivery across arbitrary transports or user side effects.
- Defining domain conflict semantics for consumers.
- Persisting arbitrary object graphs without an explicitly registered serialization contract.
- Hiding permanent authorization, schema, validation, quota, or data-corruption failures behind infinite retries.
- Guaranteeing global ordering across streams or across independent clients.
- Keeping a live CLR observer or `IDisposable` instance across process termination.
- Shipping all platform and transport adapters in the first release.

## 4. Terminology

| Term | Definition |
| --- | --- |
| `StreamId` | Stable, tenant-scoped logical stream name such as `sensor/temperature`. |
| `SubscriptionId` | Stable logical subscription identity persisted across reconnects and process restarts. |
| `ClientIdentity` | Authenticated client/device identity. It is not a credential. |
| `SyncOperation` | Immutable client-originated append, update, delete, or custom mutation stored in the outbox. |
| `RemoteEvent` | Immutable server-originated event with a server-assigned stream cursor. |
| Operation log / outbox | Durable ordered set of local operations awaiting a terminal server result. |
| Inbox | Durable set of received event IDs used for deduplication. |
| Snapshot | Materialized local stream state plus the cursor through which it is valid. |
| Cursor | Opaque server-issued resume token. Clients compare cursors only for equality unless the adapter declares them numeric. |
| ACK | Durable acknowledgement that identifies accepted, rejected, or conflicted operations. |
| Conflict | A server decision that an operation cannot be applied against the current base version without resolution. |
| Quarantine | Durable holding area for corrupt, unrecognized, or non-upcastable data. |
| Dead letter | Durable terminal record for an outbound operation that cannot be retried automatically. |

## 5. Required invariants and guarantees

### 5.1 Local durability

1. With `RemotePublishOptions.Durable = true`, `PublishAsync` MUST return success only after the operation, its per-stream client sequence, and its optimistic local projection have committed atomically.
2. A crash before that commit MAY lose the attempted write; a crash after it MUST recover the operation exactly once in the local outbox.
3. Snapshot replacement, remote cursor advancement, and inbox deduplication MUST commit atomically.
4. A store adapter MUST provide read-your-writes consistency within one `IOccasionallyConnectedContext`.

### 5.2 Ordering

- Local publishes are ordered by a monotonically increasing `ClientSequence` scoped to `(ClientId, StreamId)`.
- Server events are ordered by an opaque `ServerCursor` scoped to a stream.
- Observer notifications for one stream MUST be serialized and MUST preserve the committed local order.
- No global ordering is promised across streams.
- With multiple writers, the server sequence is authoritative; client timestamps are metadata only.

### 5.3 Delivery guarantees

| Mode | Contract |
| --- | --- |
| `AtMostOnce` | The engine attempts one send. It does not retry after an ambiguous transport outcome. Durable local logging is optional. Loss is possible; duplication is not intentionally introduced. |
| `AtLeastOnce` | The operation is retained and retried until a terminal ACK. Duplicate transport delivery is possible. `OperationId`-based server idempotency is REQUIRED. This is the default. |
| `ExactlyOnce` | Exactly-once *effect* for `(tenant, client, stream, operationId)` within a declared deduplication window. Requires an atomic local outbox, server idempotency ledger, atomic application plus ACK, and durable inbox. Unsupported combinations MUST be rejected during validation. If a previously attempted operation outlives the negotiated window, the engine stops it as `GuaranteeExpired` unless explicit policy permits at-least-once fallback. |

`ExactlyOnce` MUST never be documented as an unconditional network-delivery guarantee.

### 5.4 Observable grammar

- A returned observable MUST serialize `OnNext`, `OnError`, and `OnCompleted` calls per subscription.
- No notification may occur after a terminal notification or disposal.
- Operational disconnects, retries, and authentication refreshes MUST be exposed through state/fault streams and MUST NOT terminate `Local`.
- `Local` is long-lived and completes only when its owning stream is disposed or permanently faulted by unrecoverable local corruption.
- `Remote` represents committed, deduplicated server events. A transient disconnect does not complete it.
- Observer exceptions MUST be isolated to the failing subscription and MUST NOT corrupt engine state.

## 6. Architecture

### 6.1 Component view

```text
Application
  |
  +-- IOccasionallyConnectedStream<T>
        |-- Local projection / StateSignal<T>
        |-- Remote event signal
        |-- PublishAsync + convenience Input observer
        |-- SyncStates, OperationStates, Faults
        |
        +-- StreamCoordinator (one serialized lane per StreamId)
              |-- Projection reducer
              |-- Subscription restorer
              |-- Outbox dispatcher
              |-- Inbox processor
              |-- Conflict coordinator
              |
              +-- ILocalStoreAdapter
              +-- IRemoteTransportAdapter
              +-- IPayloadSerializer / ISchemaRegistry
              +-- IRetryPolicy / IConflictResolver
              +-- IConnectivityMonitor (hint only)

Server
  +-- IServerStreamHub
        |-- authorization + validation
        |-- idempotency ledger
        |-- operation applier / conflict resolver
        |-- canonical event log + cursor allocation
        +-- transport-specific endpoint
```

### 6.2 Ownership and composition

- `OccasionallyConnectedContext` owns storage initialization, transport sessions, engine lifetime, shared diagnostics, and per-stream coordinators.
- A stream owns its local projection and public signals but does not own shared storage or transport instances.
- The sync engine owns reconnect and retry policy. Transport adapters MUST NOT implement independent unbounded retry loops.
- The engine composes stores, transports, serializers, reducers, policies, and diagnostics. Consumer extension points are interfaces or delegates; no base-class inheritance is required.
- A context MAY multiplex streams over one transport session. Fair scheduling prevents a busy stream from starving another.

### 6.3 Data flow: local publish

1. Validate stream ID, payload contract, payload size, and context lifecycle.
2. Serialize the payload outside the per-stream commit lock where safe.
3. Enter the stream's sequenced commit lane.
4. Allocate `OperationId` and the next `ClientSequence`.
5. Atomically append the operation and update the optimistic projection/snapshot.
6. Emit the new local state and `SavedLocally`/`QueuedForUpload` status after commit.
7. Signal the bounded outbox dispatcher.
8. Batch, send, and await a protocol ACK.
9. Atomically apply the ACK: mark synchronized, record conflict/rejection, or dead-letter.
10. Emit status and metrics after the store commit.

### 6.4 Data flow: remote receive

1. Connect or resume with the persisted `SubscriptionId` and cursor.
2. Validate envelope, tenant/stream scope, size, integrity, schema, and cursor continuity.
3. Deserialize and upcast outside the commit lock where safe.
4. Enter the stream's sequenced commit lane.
5. If `EventId` exists in the inbox, acknowledge it without notifying observers again.
6. Atomically apply the event to the snapshot, add the inbox record, and advance the cursor.
7. Emit `Remote` and then the updated `Local` projection in documented order.
8. Send or piggyback the receive ACK after the durable commit.

## 7. Public API

The snippets define the intended shape. Final names and signatures MUST be frozen with public API baselines before the first release candidate.

### 7.1 Identifiers and envelopes

```csharp
namespace ReactiveUI.Primitives.OccasionallyConnected;

public readonly record struct StreamId
{
    public StreamId(string value);
    public string Value { get; }
    public override string ToString();
}

public readonly record struct SubscriptionId(Guid Value)
{
    public static SubscriptionId New();
}

public readonly record struct OperationId(Guid Value)
{
    public static OperationId New();
}

public sealed record ClientIdentity(string ClientId, string? TenantHint = null);

public sealed record PayloadEnvelope(
    string ContractId,
    int SchemaVersion,
    string ContentType,
    ReadOnlyMemory<byte> Payload,
    string PayloadHash);

public sealed record SyncOperation
{
    public required OperationId OperationId { get; init; }
    public required StreamId StreamId { get; init; }
    public required long ClientSequence { get; init; }
    public required DateTimeOffset TimestampUtc { get; init; }
    public string? BaseVersion { get; init; }
    public required SyncOperationType Type { get; init; }
    public required PayloadEnvelope Payload { get; init; }
    public OperationPolicy Policy { get; init; } = OperationPolicy.Default;
    public IReadOnlyDictionary<string, string> Metadata { get; init; }
}

public sealed record RemoteEvent(
    Guid EventId,
    StreamId StreamId,
    string ServerCursor,
    DateTimeOffset CommittedAtUtc,
    OperationId? CausedByOperationId,
    PayloadEnvelope Payload,
    IReadOnlyDictionary<string, string> Metadata);

public enum SyncOperationType
{
    Append,
    Update,
    Delete,
    Custom
}
```

The `SyncOperation` snippet shows the approved required-init API shape. The implementation defaults metadata to an empty owned dictionary and copies supplied metadata; `Policy` is persisted with the operation. Projection receives the decoded, validated `TInput` after schema upcasting and inbox filtering.

`StreamId` MUST be non-empty, normalized to Unicode NFC, at most 256 UTF-8 bytes, and restricted by default to letters, digits, `/`, `.`, `_`, and `-`. It MUST NOT contain `..`, empty path segments, control characters, a leading slash, or a trailing slash. Adapters MUST treat it as data, never as a file path or SQL fragment.

### 7.2 Options

```csharp
public enum StartPositionKind { Latest, FromTimestamp, FromSequence, FromCursor }
public enum DeliveryGuarantee { AtMostOnce, AtLeastOnce, ExactlyOnce }
public enum BufferStrategy { DropOldest, DropNewest, Block, Reject, Custom }
public enum ConflictPolicy { LastWriterWins, Merge, Custom }
public enum ExactlyOnceExpiryBehavior { StopAndReport, FallbackToAtLeastOnce }

public sealed record StartPosition
{
    private StartPosition(
        StartPositionKind kind,
        DateTimeOffset? timestamp = null,
        long? sequence = null,
        string? cursor = null)
    {
        Kind = kind;
        Timestamp = timestamp;
        Sequence = sequence;
        Cursor = cursor;
    }

    public StartPositionKind Kind { get; }
    public DateTimeOffset? Timestamp { get; }
    public long? Sequence { get; }
    public string? Cursor { get; }

    public static StartPosition Latest { get; } = new(StartPositionKind.Latest);
    public static StartPosition FromTimestamp(DateTimeOffset timestamp) =>
        new(StartPositionKind.FromTimestamp, timestamp: timestamp);
    public static StartPosition FromSequence(long sequence) =>
        new(StartPositionKind.FromSequence, sequence: sequence);
    public static StartPosition FromCursor(string cursor) =>
        new(StartPositionKind.FromCursor, cursor: cursor);
}

public sealed record RemoteSubscriptionOptions
{
    public required StreamId StreamId { get; init; }
    public SubscriptionId? SubscriptionId { get; init; }
    public StartPosition StartPosition { get; init; } = StartPosition.Latest;
    public DeliveryGuarantee DeliveryGuarantee { get; init; } = DeliveryGuarantee.AtLeastOnce;
    public BufferStrategy BufferStrategy { get; init; } = BufferStrategy.Block;
    public int BufferCapacity { get; init; } = 1_024;
    public long BufferCapacityBytes { get; init; } = 16 * 1024 * 1024;
}

public sealed record RemotePublishOptions
{
    public required StreamId StreamId { get; init; }
    public bool Durable { get; init; } = true;
    public int Priority { get; init; }
    public ConflictPolicy ConflictPolicy { get; init; } = ConflictPolicy.Merge;
    public DeliveryGuarantee DeliveryGuarantee { get; init; } = DeliveryGuarantee.AtLeastOnce;
    public BufferStrategy AdmissionStrategy { get; init; } = BufferStrategy.Block;
    public string? BaseVersion { get; init; }
}

public sealed record ObserverInputOptions
{
    public BufferStrategy BufferStrategy { get; init; } = BufferStrategy.Reject;
    public int BufferCapacity { get; init; } = 256;
    public long BufferCapacityBytes { get; init; } = 4 * 1024 * 1024;
}
```

Validation rules:

- `BufferCapacity` and `BufferCapacityBytes` MUST both be positive and enforced.
- A start position MUST contain exactly the value required by its kind: timestamp for `FromTimestamp`, non-negative sequence for `FromSequence`, non-empty bounded cursor for `FromCursor`, and no value for `Latest`. A recovered durable cursor takes precedence over the initial start position.
- `Block` is valid only on awaitable producer paths. `AsObserver` and the stream `Input` bridge MUST reject `ObserverInputOptions.BufferStrategy = Block` during construction because `IObserver<T>.OnNext` cannot safely perform asynchronous backpressure.
- `PublishAsync` applies `RemotePublishOptions.AdmissionStrategy` to the context's bounded outbox limits. Durable publishing permits only `Block`, `Reject`, or a custom policy that does not drop durable work. Observer bridges use `ObserverInputOptions` for their separate admission queue.
- `AtMostOnce` with `Durable = true` is allowed for audit/recovery, but an ambiguous send is terminal and is not retried.
- `ExactlyOnce` requires `Durable = true` and capability negotiation at context start.
- The effective exactly-once window is exposed through `NegotiatedCapabilities`. If an ambiguous operation reaches that age, `StopAndReport` transitions it to `GuaranteeExpired`. `FallbackToAtLeastOnce` emits an `OC.GuaranteeDowngraded` fault before retrying; this fallback is never implicit.
- Priorities are bounded to a configured range; the default range is `-10..10`.

### 7.3 Remote primitives

```csharp
public interface IRemoteObservable<T>
{
    IObservable<RemoteMessage<T>> SubscribeRemote(
        RemoteSubscriptionOptions options);
}

public interface IRemoteObserver<T>
{
    ValueTask<PublishReceipt> PublishAsync(
        T value,
        RemotePublishOptions options,
        CancellationToken cancellationToken);

    IObserver<T> AsObserver(
        RemotePublishOptions options,
        ObserverInputOptions? inputOptions);
}

public sealed record RemoteMessage<T>(
    Guid EventId,
    StreamId StreamId,
    string ServerCursor,
    DateTimeOffset CommittedAtUtc,
    T Value);

public sealed record PublishReceipt(
    OperationId OperationId,
    long ClientSequence,
    SyncOperationState State,
    DateTimeOffset SavedAtUtc);
```

Core extension overloads provide `PublishAsync(value, options)` with `CancellationToken.None` and `AsObserver(options)` with default observer input options. The interface itself keeps cancellation and observer input explicit; it declares no optional parameters.

`IRemoteObservable<T>` emits only decoded messages that have committed locally after deduplication. `PublishAsync` is the authoritative write API. `AsObserver` is a convenience bridge: `OnNext` performs bounded in-memory admission, `OnError` reports a producer fault, and `OnCompleted` closes only that producer. Persistence or overflow failures are emitted on `Faults`; therefore callers that require a durable receipt MUST use `PublishAsync`.

### 7.4 Local-first stream facade

```csharp
public interface IOccasionallyConnectedStream<TState, TInput> : IAsyncDisposable
{
    StreamId StreamId { get; }
    SubscriptionId SubscriptionId { get; }

    IObservable<TState> Local { get; }
    IObservable<RemoteMessage<TInput>> Remote { get; }
    IObservable<SyncState> SyncStates { get; }
    IObservable<SyncOperationStatus> OperationStates { get; }
    IObservable<OccasionallyConnectedFault> Faults { get; }

    IObserver<TInput> Input { get; }

    ValueTask<PublishReceipt> PublishAsync(
        TInput value,
        RemotePublishOptions? options,
        CancellationToken cancellationToken);

    ValueTask StartAsync(CancellationToken cancellationToken);
    ValueTask StopAsync(CancellationToken cancellationToken);
}

public interface IOccasionallyConnectedStream<T>
    : IOccasionallyConnectedStream<T, T>
{
}
```

`Local` replays the latest committed local state to new subscribers. `Remote` exposes decoded, deduplicated server event payloads of `TInput` after durable inbox application; the non-generic `RemoteEvent` envelope remains an engine/storage boundary type. `Remote` does not replay by default. A replaying remote view is opt-in through a normal Primitives replay operator.

Core extension overloads provide `PublishAsync(value)`, `PublishAsync(value, options)`, and `PublishAsync(value, cancellationToken)`, forwarding omitted options as `null` and omitted cancellation as `CancellationToken.None`. Parameterless `StartAsync()` and `StopAsync()` likewise forward `CancellationToken.None`; the interface declares no optional parameters.

### 7.5 Projection and conflict contracts

```csharp
public interface ILocalProjection<TState, in TInput>
{
    TState InitialState { get; }
    TState ApplyLocal(TState state, TInput input, SyncOperation operation);
    TState ApplyRemote(TState state, TInput input, RemoteEvent remoteEvent);
    TState Reconcile(TState state, ConflictResolutionResult result);
}

public interface IConflictResolver
{
    ValueTask<ConflictResolutionResult> ResolveAsync(
        ConflictContext context,
        CancellationToken cancellationToken = default);
}

public sealed record ConflictContext(
    ServerState Current,
    IReadOnlyList<SyncOperation> Incoming,
    ClientIdentity Client);

public sealed record ConflictResolutionResult(
    IReadOnlyList<OperationId> AcceptedOperations,
    IReadOnlyList<RejectedOperation> RejectedOperations,
    IReadOnlyList<ResolvedConflict> Conflicts,
    IReadOnlyList<RemoteEvent> ProducedEvents,
    string ServerVersion);
```

Resolvers MUST be deterministic for the same ordered input and configuration. They MUST NOT perform network I/O or mutate external state inside the server transaction. Side effects are emitted as committed events and handled afterward.

### 7.6 Sync engine

```csharp
public interface ISyncEngine : IAsyncDisposable
{
    IObservable<SyncState> SyncStates { get; }
    IObservable<SyncOperationStatus> OperationStates { get; }
    IObservable<OccasionallyConnectedFault> Faults { get; }

    ValueTask<PublishReceipt> EnqueueOperationAsync(
        SyncOperation operation,
        CancellationToken cancellationToken = default);

    ValueTask StartAsync(CancellationToken cancellationToken = default);
    ValueTask StopAsync(CancellationToken cancellationToken = default);
    ValueTask TriggerSyncAsync(CancellationToken cancellationToken = default);
}

public enum SyncLifecycleStatus
{
    Created,
    Initializing,
    Offline,
    Connecting,
    Synchronizing,
    Online,
    Degraded,
    Stopping,
    Stopped,
    Faulted
}

public enum SyncOperationState
{
    SavedLocally,
    QueuedForUpload,
    Uploading,
    Conflict,
    Synchronized,
    Rejected,
    DeadLettered,
    Ambiguous,
    GuaranteeExpired
}

public sealed record SyncState(
    SyncLifecycleStatus Status,
    bool NetworkAvailable,
    int PendingOperations,
    long PendingBytes,
    DateTimeOffset ChangedAtUtc,
    DateTimeOffset? LastSuccessfulSyncUtc,
    TimeSpan? RetryAfter,
    string? ReasonCode);

public sealed record SyncOperationStatus(
    OperationId OperationId,
    StreamId StreamId,
    SyncOperationState State,
    int Attempt,
    DateTimeOffset ChangedAtUtc,
    string? ReasonCode);
```

All lifecycle operations are idempotent. Concurrent calls to `StartAsync` share one start transition. `StopAsync` waits for in-flight store commits, stops admitting new work, cancels transport I/O, and persists retry/checkpoint state. It does not require the remote peer to be available.

### 7.7 Storage contract

```csharp
public interface ILocalStoreAdapter : IAsyncDisposable
{
    LocalStoreCapabilities Capabilities { get; }

    ValueTask InitializeAsync(
        LocalStoreInitialization initialization,
        CancellationToken cancellationToken = default);

    ValueTask<RecoveredStream> RecoverStreamAsync(
        StreamId streamId,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken = default);

    ValueTask<LocalCommitResult> CommitLocalOperationAsync(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
        OutboxLeaseRequest request,
        CancellationToken cancellationToken = default);

    ValueTask ApplySyncResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        CancellationToken cancellationToken = default);

    ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken = default);

    ValueTask RenewLeaseAsync(
        Guid leaseId,
        TimeSpan extension,
        CancellationToken cancellationToken = default);

    ValueTask ReleaseLeaseAsync(
        Guid leaseId,
        CancellationToken cancellationToken = default);

    ValueTask<CompactionResult> CompactAsync(
        CompactionRequest request,
        CancellationToken cancellationToken = default);
}
```

The adapter contract is intentionally transactional rather than CRUD-shaped. In particular:

- `CommitLocalOperationAsync` atomically stores the operation, increments the client sequence, and commits the optimistic snapshot mutation.
- `ApplyRemoteBatchAsync` atomically deduplicates event IDs, updates the snapshot, persists the subscription cursor, and records any reconciliation changes.
- Leases prevent two engine instances from uploading the same local record concurrently. Expired leases become available again.
- `InitializeAsync` MUST acquire a process/store ownership lock or provide safe multi-process coordination. The default is one writer per store identity.
- Store implementations MUST be crash-consistent and document durability settings such as SQLite synchronous mode or file `Flush(true)` behavior.

The default SQLite store does not advertise `MultiProcessCoordination`. During `InitializeAsync`, it acquires an exclusive sidecar file handle at `<database>.rxui-owner` on the same single SQLite worker before opening or mutating SQLite. The handle is tied to the adapter lifetime and is released only after disposal has closed admission, active captures have drained, and queued SQLite work has stopped. If initialization fails after acquiring a new handle, the adapter releases that handle before reporting the failure; a failed reinitialization retains an existing owner. A process crash or kill releases the operating-system handle.

SQLite ownership is based on the database path captured by `SqliteLocalStoreAdapter` construction after normal `Path.GetFullPath` lexical resolution. It coordinates adapters that use the same resolved local path string and does not lock byte ranges in the SQLite database file. Known unsafe path forms are rejected before acquisition: UNC paths, Windows network drives reported by the runtime, and existing reparse-point database files, ownership sidecars, or parent directories. Hard links, 8.3 short-name aliases, bind mounts, network-drive remappings that are not visible to the runtime, and filesystem clients that do not enforce the same exclusive sharing semantics remain unsupported SQLite storage deployments while the database is open. The default SQLite adapter keeps `MultiProcessCoordination` absent and enforces second-writer rejection for supported local database paths that resolve to the same sidecar path.

### 7.8 Transport contract

```csharp
public interface IRemoteTransportAdapter : IAsyncDisposable
{
    RemoteTransportCapabilities Capabilities { get; }

    ValueTask<IRemoteTransportSession> ConnectAsync(
        TransportConnectRequest request,
        CancellationToken cancellationToken = default);
}

public interface IRemoteTransportSession : IAsyncDisposable
{
    ValueTask<RemoteSyncResult> PushAsync(
        SyncBatch batch,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
        RemoteSubscribeRequest request,
        CancellationToken cancellationToken = default);

    ValueTask AcknowledgeAsync(
        ReceiveAcknowledgement acknowledgement,
        CancellationToken cancellationToken = default);
}
```

Transport implementations are thin protocol adapters. They MUST expose failure classification and server retry hints, but reconnect, backoff, circuit breaking, batching policy, and permanent-failure decisions belong to the sync engine.

#### 7.8.1 Supporting adapter and protocol models

The following records close the storage/transport contract. Implementations MAY add internal fields but MUST preserve these meanings.

```csharp
[Flags]
public enum LocalStoreCapabilities
{
    None = 0,
    AtomicLocalCommit = 1 << 0,
    AtomicRemoteApply = 1 << 1,
    DurableInbox = 1 << 2,
    LeasedOutbox = 1 << 3,
    MultiProcessCoordination = 1 << 4,
    AuthenticatedEncryptionAtRest = 1 << 5
}

[Flags]
public enum RemoteTransportCapabilities
{
    None = 0,
    BatchPush = 1 << 0,
    CursorResume = 1 << 1,
    ReceiveAcknowledgements = 1 << 2,
    ServerIdempotency = 1 << 3,
    AtomicApplyAndAcknowledge = 1 << 4,
    StreamingReceive = 1 << 5
}

public sealed record NegotiatedCapabilities(
    Version ProtocolVersion,
    RemoteTransportCapabilities Features,
    int MaximumBatchOperations,
    long MaximumBatchBytes,
    TimeSpan? ServerIdempotencyRetention,
    TimeSpan? ClientInboxRetentionRequired);

public sealed record LocalStoreInitialization(
    string StoreIdentity,
    int RequiredSchemaVersion,
    bool RequireAuthenticatedEncryptionAtRest);

public sealed record RecoveredStream(
    SubscriptionId SubscriptionId,
    string? ServerCursor,
    LocalSnapshot? Snapshot,
    IReadOnlyList<SyncOperation> PendingOperations,
    IReadOnlyList<DeadLetterRecord> DeadLetters,
    long NextClientSequence);

public sealed record LocalSnapshot(
    StreamId StreamId,
    int FormatVersion,
    string? ServerCursor,
    PayloadEnvelope State,
    DateTimeOffset SavedAtUtc);

public sealed record SnapshotMutation(
    StreamId StreamId,
    PayloadEnvelope State,
    int FormatVersion);

public sealed record OutboxLeaseRequest(
    StreamId? StreamId,
    int MaximumOperations,
    long MaximumBytes,
    TimeSpan LeaseDuration);

public sealed record LeasedOperationBatch(
    Guid LeaseId,
    DateTimeOffset ExpiresAtUtc,
    IReadOnlyList<SyncOperation> Operations);

public sealed record SyncBatch(
    Guid BatchId,
    IReadOnlyList<SyncOperation> Operations);

public sealed record OperationSyncResult(
    OperationId OperationId,
    OperationResultKind Kind,
    string? ReasonCode,
    string? ServerVersion);

public enum OperationResultKind
{
    Accepted,
    Conflict,
    Rejected,
    Retryable
}

public sealed record RemoteSyncResult(
    Guid BatchId,
    IReadOnlyList<OperationSyncResult> Operations,
    string? ServerCursor,
    TimeSpan? RetryAfter);

public sealed record RemoteEventBatch(
    Guid BatchId,
    StreamId StreamId,
    string? PreviousCursor,
    string NextCursor,
    IReadOnlyList<RemoteEvent> Events);

public sealed record RemoteApplyResult(
    string NextCursor,
    int AppliedCount,
    int DuplicateCount);

public sealed record TransportConnectRequest(
    VersionRange SupportedProtocolVersions,
    ClientIdentity Client,
    IReadOnlyCollection<DeliveryGuarantee> RequiredGuarantees);

public sealed record RemoteSubscribeRequest(
    StreamId StreamId,
    SubscriptionId SubscriptionId,
    string? Cursor,
    StartPosition InitialPosition);

public sealed record ReceiveAcknowledgement(
    SubscriptionId SubscriptionId,
    StreamId StreamId,
    string Cursor);

public sealed record CompactionRequest(
    StreamId? StreamId,
    DateTimeOffset RetainTerminalRecordsAfter,
    long TargetBytes);

public sealed record CompactionResult(
    long RecordsRemoved,
    long BytesReclaimed);

public sealed record ClientIdentity(string ClientId);

public sealed record VersionRange(Version Minimum, Version Maximum);

public sealed record LocalCommitResult(
    OperationId OperationId,
    long ClientSequence,
    DateTimeOffset CommittedAtUtc);

public sealed record DeadLetterRecord(
    SyncOperation Operation,
    string ReasonCode,
    int Attempts,
    DateTimeOffset DeadLetteredAtUtc);

public sealed record ServerState(
    StreamId StreamId,
    string Version,
    PayloadEnvelope State);

public sealed record RejectedOperation(
    OperationId OperationId,
    string ReasonCode,
    bool MayResubmit);

public sealed record ResolvedConflict(
    OperationId OperationId,
    string ResolutionCode,
    PayloadEnvelope? ResolvedPayload);

public sealed record ServerSyncResult(
    RemoteSyncResult Result,
    IReadOnlyList<RemoteEvent> ProducedEvents);

public sealed record PendingSyncSummary(
    int OperationCount,
    long Bytes,
    DateTimeOffset? OldestOperationUtc);
```

These records are immutable value models in `.Core`. Production implementations MAY keep additional internal storage metadata, but it MUST NOT alter their public meaning. No capability flag may be advertised unless its associated conformance tests pass.

### 7.9 Server hub

```csharp
public interface IServerStreamHub
{
    ValueTask<ServerSyncResult> ApplyOperationsAsync(
        SyncBatch batch,
        ClientIdentity client,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
        RemoteSubscribeRequest request,
        ClientIdentity client,
        CancellationToken cancellationToken = default);
}
```

The server MUST authorize each stream and operation, enforce size/rate limits, deduplicate before invoking domain logic, allocate canonical cursors, and atomically persist accepted effects plus idempotency results. A duplicate `OperationId` MUST return the original terminal result.

### 7.10 Context and factory

```csharp
public interface IOccasionallyConnectedContext : IAsyncDisposable
{
    ISyncEngine SyncEngine { get; }
    IObservable<SyncState> SyncStates { get; }

    IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition);

    ValueTask StartAsync(CancellationToken cancellationToken);
    ValueTask StopAsync(CancellationToken cancellationToken);
}

public sealed record StreamDefinition<TState, TInput>
{
    public required StreamId StreamId { get; init; }
    public SubscriptionId? SubscriptionId { get; init; }
    public required ILocalProjection<TState, TInput> Projection { get; init; }
    public required string InputContractId { get; init; }
    public required string StateContractId { get; init; }
    public RemoteSubscriptionOptions? Subscription { get; init; }
    public RemotePublishOptions? Publish { get; init; }
    public ObserverInputOptions? Input { get; init; }
}
```

Calling `GetOrCreateStream` repeatedly with the same stream ID and compatible definition returns the same stream instance. An incompatible definition MUST throw a configuration exception before any network work begins.

Parameterless `StartAsync()` and `StopAsync()` extension overloads forward `CancellationToken.None` and preserve the underlying asynchronous operation.

### 7.11 Extension methods

```csharp
public static class OccasionallyConnectedExtensions
{
    public static IOccasionallyConnectedStream<T, T> ToOccasionallyConnected<T>(
        this IObservable<T> localSource,
        IOccasionallyConnectedContext context,
        StreamDefinition<T, T> definition);

    public static IRemoteObserver<T> ToRemoteObserver<T>(
        this IObserver<T> observer,
        IOccasionallyConnectedContext context,
        RemotePublishOptions options);

    public static IObservable<TState> WhereSynchronized<TState, TInput>(
        this IOccasionallyConnectedStream<TState, TInput> stream);

    public static IObservable<PendingSyncSummary> ObservePending<TState, TInput>(
        this IOccasionallyConnectedStream<TState, TInput> stream);

    public static ValueTask AwaitSynchronizedAsync(
        this ISyncEngine engine,
        OperationId operationId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);
}
```

`ToOccasionallyConnected` does not subscribe to `localSource` until the returned stream starts. It owns and disposes that subscription. Extension implementations MUST not introduce hidden global contexts or unbounded replay.

## 8. Serialization and versioning

### 8.1 Payload contracts

```csharp
public interface IPayloadSerializer
{
    string ContentType { get; }
    ValueTask<PayloadEnvelope> SerializeAsync<T>(
        string contractId,
        int schemaVersion,
        T value,
        CancellationToken cancellationToken = default);

    ValueTask<object> DeserializeAsync(
        PayloadEnvelope envelope,
        Type targetType,
        CancellationToken cancellationToken = default);
}

public interface IPayloadUpcaster
{
    string ContractId { get; }
    int FromVersion { get; }
    int ToVersion { get; }
    ValueTask<PayloadEnvelope> UpcastAsync(
        PayloadEnvelope source,
        CancellationToken cancellationToken = default);
}
```

- Every payload MUST carry `ContractId`, positive `SchemaVersion`, `ContentType`, payload bytes, and a cryptographic payload hash.
- Type names and assembly-qualified names MUST NOT be used as wire contract IDs.
- Polymorphic deserialization is deny-by-default. Only registered contract IDs and target types may be instantiated.
- Upcasters form an acyclic, contiguous chain. Missing, ambiguous, or failing chains quarantine the record and emit a permanent schema fault.
- Downcasting is not supported.
- The default JSON adapter uses `System.Text.Json`, source-generated serialization contexts, UTF-8, invariant formats, ISO-8601 UTC timestamps, and explicit enum representation. Reference preservation and arbitrary type metadata are disabled.
- Payload hashes are computed over the stored canonical bytes, not over a reserialized object.

### 8.2 Version domains

The following versions are independent:

| Version | Purpose | Compatibility rule |
| --- | --- | --- |
| NuGet SemVer | Public .NET API/package behavior | Semantic Versioning; breaking API changes require a major version. |
| `ProtocolVersion` | Envelope, handshake, ACK, and cursor protocol | Major mismatch fails negotiation; peers may negotiate the highest shared minor version. |
| `SchemaVersion` | One payload contract | Managed by registered upcasters; changes do not require a package major version. |
| Store schema version | Local database/log layout | Transactional forward migrations; rollback only when explicitly supported. |
| Snapshot format version | Serialized local projection | Rebuild from retained operations/events when possible; otherwise use a registered snapshot migrator. |

Store migrations MUST be restartable, checksummed, and backed up or journalled before destructive transformation. A newer unsupported store version fails closed and MUST NOT be overwritten.

### 8.3 Protocol envelope

```json
{
  "protocolVersion": "1.0",
  "messageType": "syncBatch",
  "messageId": "5c672ad0-2d35-4d80-a8c0-a724812e52da",
  "nonce": "96-bits-or-more-of-cryptographic-randomness",
  "correlationId": "d20be81a-5a09-4c82-aa91-f5127dffb505",
  "sentAtUtc": "2026-09-10T22:00:00Z",
  "body": {
    "streamId": "sensor/temperature",
    "operations": [
      {
        "operationId": "375f4a08-3b8a-44af-ad0d-e9e3904beb43",
        "clientSequence": 42,
        "type": "Append",
        "baseVersion": "stream-seq-99",
        "timestampUtc": "2026-09-10T22:00:00Z",
        "payload": {
          "contractId": "temperature-reading",
          "schemaVersion": 1,
          "contentType": "application/json",
          "data": { "value": 21.3, "unit": "C" },
          "payloadHash": "sha256-base64"
        }
      }
    ]
  }
}
```

The corresponding result identifies every submitted operation exactly once as accepted, conflicted, rejected, or retryable and includes a server cursor/version. Missing operation results invalidate the batch response and cause a retry according to its guarantee.

Client identity is bound to the authenticated transport/session, not trusted from the message body. If an adapter carries tenant or client routing hints in headers, the server treats them as untrusted hints and replaces them with the authenticated principal before authorization, idempotency lookup, conflict resolution, or persistence.

`messageId` identifies protocol correlation and safe duplicate responses; `nonce` provides freshness for authenticated requests. For non-streaming requests, the server accepts at most five minutes of clock skew by default and stores `(authenticated tenant, authenticated client, nonce)` until the freshness window closes. Reuse with different bytes is rejected as tampering; byte-identical reuse returns the prior protocol result when available. Long-lived authenticated sessions derive per-session replay state during handshake and use monotonically increasing frame sequence numbers. Adapter documentation MUST define which scheme it implements and its replay-cache retention.

### 8.4 Capability handshake

At connection start, peers negotiate protocol versions, maximum message/batch size, compression, delivery guarantees, resume support, acknowledgement mode, idempotency retention, and authentication scheme. Required unsupported capabilities fail before synchronization begins. Compression MUST occur before transport encryption, and decompressed size limits MUST be enforced to prevent decompression bombs.

## 9. Lifecycle and recovery semantics

### 9.1 State machine

```text
Created -> Initializing -> Offline <-> Connecting -> Synchronizing -> Online
                         ^              |               |             |
                         +--------------+---------------+-------------+
                                        transient failure

Any active state -> Degraded -> Connecting/Offline
Any state -> Stopping -> Stopped
Initializing/active state -> Faulted (permanent local/configuration fault)
Stopped -> Initializing (restart is supported)
```

- Connectivity monitor changes are hints; only a successful authenticated handshake establishes `Online`.
- `Synchronizing` drains/resumes both inbound and outbound work. Implementations SHOULD pull remote events before pushing stale local operations when conflict frequency would otherwise increase; the policy is configurable.
- `Degraded` means useful local operation continues while one or more remote capabilities are impaired.
- Permanent configuration, incompatible protocol, unsupported schema, unrecoverable store corruption, or ownership-lock failure enters `Faulted`.

### 9.2 Durable subscription restoration

On `StartAsync`, the engine:

1. Opens and migrates the store under an exclusive migration lock.
2. Loads registered stream definitions and validates their contracts.
3. Recovers snapshot, outbox, inbox, cursor, dead letters, and the durable `SubscriptionId`.
4. Replays only records committed after the last valid snapshot.
5. Reclaims expired upload leases.
6. Emits recovered local state before attempting network I/O.
7. Connects using the persisted subscription identity and cursor.
8. If the server accepts the cursor, resumes from the next event.
9. If the cursor expired or a gap is reported, requests a snapshot plus new cursor and reconciles transactionally.

### 9.3 Retry and circuit breaking

- Default transient retry uses exponential backoff with decorrelated jitter: 500 ms minimum, 30 s maximum, and server `Retry-After` as a lower bound.
- Authentication refresh receives one immediate retry after token renewal. Repeated authentication failure is permanent until credentials change.
- Validation, authorization, schema incompatibility, payload-too-large, and deterministic conflict rejection are not transient.
- A per-endpoint circuit breaker opens after five consecutive transient failures, probes after 30 s, and resets after a successful handshake. Defaults are configurable.
- Retry state MUST persist for durable operations so a restart does not create a tight retry loop.
- Retry policies MUST accept a time provider and deterministic random source for tests.

### 9.4 Partial and ambiguous failures

- Batch results are per operation; accepted items are committed even if siblings conflict or fail.
- A lost ACK is ambiguous. At-least-once and exactly-once modes retry with identical operation IDs. At-most-once mode transitions the operation to terminal `Ambiguous`, emits a typed fault, and does not retry.
- Cancellation before local commit cancels publication. Cancellation after local durable commit cancels only the caller's wait; it does not remove the queued operation.
- Disposal drains committed storage work but may cancel network I/O. Unacknowledged durable operations remain pending.
- Poison records are retried only up to the configured attempt/age policy, then dead-lettered with sanitized reason metadata.

### 9.5 Retention and compaction

- Outbox terminal records, inbox deduplication entries, snapshots, dead letters, and server idempotency records have separate retention policies.
- The exactly-once-effect window is the minimum of client inbox and server idempotency retention.
- Compaction MUST be transactional, cancellable, and safe to restart. It MUST NOT remove operations needed to rebuild the current snapshot or resolve pending conflicts.
- Disk-pressure thresholds trigger diagnostics before hard capacity is reached. The default high-water mark is 80%; at the critical threshold, durable writes reject rather than silently drop.

## 10. Concurrency and backpressure

### 10.1 Sequencing model

- A keyed `ISequencer` serializes state mutations per `StreamId`.
- Different streams may synchronize concurrently up to `MaxConcurrentStreams`.
- Serializer, compression, encryption, and transport I/O may execute concurrently, but commits re-enter the stream sequencer in source order.
- Consumer callbacks are never invoked while a store transaction or internal lock is held.
- Engine state is published through replaying Primitives signals with serialized notifications.

### 10.2 Bounded queues

All queues MUST be bounded by both item count and bytes:

- producer admission queue;
- per-stream outbound queue;
- transport batch;
- inbound decode queue;
- observer notification queue;
- diagnostics queue.

`BufferStrategy` semantics:

| Strategy | Behavior |
| --- | --- |
| `Block` | Await capacity. Available only to awaitable methods. Cancellation does not delete already committed work. |
| `Reject` | Fail before persistence with `QueueCapacityExceededException`. |
| `DropOldest` | Drop the oldest non-durable, non-control item and emit an overflow fault/metric. Durable records MUST NOT be dropped. |
| `DropNewest` | Reject/drop the incoming non-durable item and emit an overflow fault/metric. Durable records MUST NOT be dropped. |
| `Custom` | Invoke a registered deterministic `IBufferOverflowPolicy`; it may block, reject, or select only eligible non-durable items. |

Batching is limited by operation count, encoded bytes, and maximum dwell time. Defaults are 100 operations, 1 MiB, and 50 ms. The server-negotiated maximum always wins when smaller.

### 10.3 Fairness and priority

- Priority affects outbound selection, never per-stream sequence order.
- Weighted fair scheduling plus aging prevents starvation.
- Control messages and ACKs have a reserved capacity and cannot be starved by payload traffic.
- One stream may have at most `MaxInFlightBatchesPerStream` batches; the v1 default is one to simplify ordering.

### 10.4 Slow observers

The storage/transport commit path MUST NOT wait on application observers. Each subscription has a bounded notification queue. Overflow follows the subscription's configured strategy; default local-state behavior coalesces to the newest state, while remote event behavior rejects/disconnects that observer with a typed overflow error. The durable inbox remains correct regardless of observer speed.

## 11. Conflict resolution

### 11.1 Conflict detection

Operations carry `BaseVersion`. The server compares it with the current aggregate/stream version. Absence of a base version means append-only or unconditional behavior as defined by the stream contract.

### 11.2 Built-in strategies

- `LastWriterWinsResolver`: uses the server commit time and a deterministic tie-breaker `(serverTime, clientId, operationId)`. Client clocks MUST NOT decide the winner.
- `CrdtResolver`: supports explicitly registered CRDT types such as grow-only counters, PN-counters, observed-remove sets, and last-writer registers. CRDT metadata is versioned and compacted safely.
- `CustomDomainResolver`: application-supplied deterministic pure logic registered per contract/stream pattern.

### 11.3 Client reconciliation

The server returns accepted operations, rejected operations, conflicts, replacement events/snapshot, and the authoritative server version. The client atomically:

1. marks terminal outbox results;
2. applies authoritative events/snapshot;
3. replays still-pending local operations in client sequence order;
4. emits the reconciled `Local` state;
5. exposes unresolved conflicts through `OperationStates` and `Faults`.

Automatic resolution is bounded by `MaxConflictResolutionRounds` (default 3). Beyond that, the operation is marked `Conflict` for user/domain review; the engine MUST NOT loop indefinitely.

## 12. Security and privacy

### 12.1 Trust boundaries

Storage contents, transport input, cursors, metadata, and serialized payloads are untrusted. Server authorization is authoritative; client-side checks are usability aids only.

### 12.2 Required controls

- Transport adapters MUST use authenticated encryption in transit appropriate to the protocol (normally TLS 1.2+; platform policy may require newer).
- Credentials are supplied by `IAccessTokenProvider` or transport-specific credential providers and MUST NOT be stored in operation metadata, snapshots, logs, or the default local store.
- Every server operation is authorized against authenticated tenant, client, stream, and operation type.
- Tenant identity comes from authenticated server context, not a client-trusted payload field.
- Message, metadata, collection, nesting, decompressed, and batch sizes are bounded before allocation where possible.
- Deserialization uses an allowlisted schema registry; arbitrary CLR type activation and insecure type-name handling are forbidden.
- `OperationId`, `EventId`, nonce, timestamp window, and idempotency ledger provide replay protection. Authorization is re-evaluated for replays.
- Payload hashes detect corruption; authenticity comes from the protected transport or an optional message-signing adapter, not from an unkeyed hash.
- An unkeyed payload hash detects accidental corruption only. When the configured threat model includes local-store modification, the selected store MUST advertise `AuthenticatedEncryptionAtRest` and protect every outbox, inbox, snapshot, cursor, quarantine, and dead-letter record with AEAD or a keyed MAC/signature. Authentication failure quarantines the record, emits a security fault, and fails the affected stream closed; the engine never applies or uploads it.
- File adapters canonicalize and validate paths beneath a configured root. SQL adapters use parameters exclusively.
- Encryption at rest is an adapter capability. Keys come from platform secure storage or `IEncryptionKeyProvider`, carry key IDs, support rotation, and are never logged.
- Local erase supports tenant/stream-scoped cryptographic or physical deletion subject to platform limits.

### 12.3 Logging and telemetry

- Payload bodies, access tokens, encryption keys, personally identifiable data, and raw tenant/client/stream identifiers MUST NOT be logged by default.
- High-cardinality values are represented by opt-in hashed tags with per-installation salt.
- Exception messages crossing trust boundaries are mapped to stable reason codes; raw server/store details remain in protected local diagnostics.
- Security-sensitive actions—authentication failure, authorization denial, key rotation, schema rejection, quarantine, and destructive store migration—produce audit events.

### 12.4 Threats to test

The test plan MUST include forged tenant IDs, unauthorized streams, duplicate/replayed batches, cursor tampering, path traversal, SQL metacharacters, oversized and deeply nested JSON, zip/decompression bombs, malicious polymorphic payloads, hash mismatch, stale keys, metadata cardinality attacks, and observer-triggered denial of service.

## 13. Configuration and dependency injection

### 13.1 Core builder

The core package has no dependency on `Microsoft.Extensions.DependencyInjection`.

```csharp
var context = new OccasionallyConnectedBuilder()
    .UseClient(new ClientIdentity("device-123"))
    .UseStore(store)
    .UseTransport(transport)
    .UseSerializer(serializer)
    .UseSequencer(Sequencer.CurrentThread)
    .Configure(options =>
    {
        options.MaxConcurrentStreams = 4;
        options.Outbox.MaxOperations = 10_000;
        options.Outbox.MaxBytes = 64 * 1024 * 1024;
        options.Retry.MaximumDelay = TimeSpan.FromSeconds(30);
    })
    .Build();
```

`Build()` performs structural validation. `StartAsync()` performs adapter initialization and negotiated-capability validation.

### 13.2 Options

```csharp
public sealed record OccasionallyConnectedOptions
{
    public bool AutoStart { get; init; }
    public int MaxConcurrentStreams { get; init; } = 4;
    public ExactlyOnceExpiryBehavior ExactlyOnceExpiryBehavior { get; init; } =
        ExactlyOnceExpiryBehavior.StopAndReport;
    public required OutboxOptions Outbox { get; init; }
    public required InboxOptions Inbox { get; init; }
    public required BatchingOptions Batching { get; init; }
    public required RetryOptions Retry { get; init; }
    public required CircuitBreakerOptions CircuitBreaker { get; init; }
    public required RetentionOptions Retention { get; init; }
    public required SecurityOptions Security { get; init; }
    public required DiagnosticsOptions Diagnostics { get; init; }
}
```

Defaults MUST be finite. Options are immutable after context start. Retry delays, concurrency, batching, and diagnostic sampling MAY be updated through an explicit validated reconfiguration API. Client/store identity, encryption settings, protocol requirements, serializer registry, and stream contracts require a stopped context and normally a restart.

### 13.3 Microsoft.Extensions integration

`ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection` provides:

```csharp
services.AddOccasionallyConnected(builder =>
{
    builder.UseStore<SqliteLocalStoreAdapter>();
    builder.UseTransport<HttpRemoteTransportAdapter>();
    builder.AddJsonContract<TemperatureReading>(
        contractId: "temperature-reading",
        schemaVersion: 1,
        TemperatureJsonContext.Default.TemperatureReading);
});
```

- The context is singleton by default.
- Stream definitions are named singleton registrations.
- Stores, transports, token providers, serializers, and policies declare their supported lifetime. Singleton context dependencies MUST be singleton-safe.
- Options use `IOptions`/`IValidateOptions`; invalid configuration fails at startup.
- The optional `.Hosting` package supplies `IHostedService`, health checks, and graceful shutdown integration.
- DI packages adapt `ILogger` to the core diagnostics sink without making logging a core dependency.

## 14. Diagnostics, metrics, and health

### 14.1 Diagnostic surfaces

The core uses `ActivitySource`, `Meter`, and a typed `IObservable<OccasionallyConnectedFault>`. Optional logging adapters consume typed events.

Activity source and meter name: `ReactiveUI.Primitives.OccasionallyConnected`.

Required activities:

- `oc.context.start`
- `oc.transport.connect`
- `oc.sync.push`
- `oc.sync.receive`
- `oc.store.commit`
- `oc.conflict.resolve`
- `oc.store.compact`

Trace context is propagated through supported transports. Payloads are never attached to spans.

### 14.2 Metrics

| Metric | Type | Unit |
| --- | --- | --- |
| `oc.operations.published` | Counter | operations |
| `oc.operations.synchronized` | Counter | operations |
| `oc.operations.rejected` | Counter | operations |
| `oc.conflicts` | Counter | conflicts |
| `oc.retries` | Counter | retries |
| `oc.duplicates` | Counter | events |
| `oc.queue.pending` | UpDownCounter/observable gauge | operations |
| `oc.queue.bytes` | UpDownCounter/observable gauge | bytes |
| `oc.sync.batch.size` | Histogram | operations |
| `oc.sync.duration` | Histogram | milliseconds |
| `oc.store.commit.duration` | Histogram | milliseconds |
| `oc.connection.state_changes` | Counter | transitions |
| `oc.dead_letters` | Counter | operations |

Default tags are low-cardinality: adapter name, transport kind, operation type, lifecycle state, delivery guarantee, and stable reason code. Raw IDs are opt-in.

### 14.3 Fault model

```csharp
public sealed record OccasionallyConnectedFault(
    string Code,
    FaultCategory Category,
    FaultSeverity Severity,
    bool IsTransient,
    StreamId? StreamId,
    OperationId? OperationId,
    DateTimeOffset OccurredAtUtc,
    Exception? Exception);
```

Stable fault categories include configuration, storage, serialization, transport, authentication, authorization, protocol, capacity, conflict, observer, and internal invariant. Normal offline state is not a fault.

### 14.4 Health

Health reports distinguish:

- `Healthy`: local store usable and remote synchronized or no remote work pending.
- `Degraded`: local operation available but remote is unavailable, retrying, lagging, or circuit-open.
- `Unhealthy`: local store unavailable/corrupt, configuration invalid, ownership lost, or an invariant failed.

Health details expose counts, ages, and reason codes—not payloads or raw identifiers.

## 15. Package, project, and file structure

### 15.1 Package topology

| Package | Phase | Responsibility |
| --- | --- | --- |
| `ReactiveUI.Primitives.OccasionallyConnected.Core` | v1 | Models, options, contracts, fault types, protocol DTOs, serializer/store/transport interfaces. Depends on `ReactiveUI.Primitives.Core`. |
| `ReactiveUI.Primitives.OccasionallyConnected` | v1 | Engine, local-first facade, signals, sequencing, retry, batching, extensions, in-memory reference adapters. Depends on the Core package and `ReactiveUI.Primitives`. |
| `ReactiveUI.Primitives.OccasionallyConnected.Reactive` | v1.x | System.Reactive-facing variant using `Unit`/`IScheduler` and `.Reactive` namespaces; shares implementation source with the lean package. |
| `ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection` | v1 | Microsoft.Extensions DI/options integration. |
| `ReactiveUI.Primitives.OccasionallyConnected.Hosting` | v1.x | Hosted service, health checks, lifecycle integration. |
| `ReactiveUI.Primitives.OccasionallyConnected.Server` | v1 | Server hub contracts, idempotency and resolver pipeline, in-memory conformance host. |
| `ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite` | v1 | Reference durable relational store for desktop/mobile/server. |
| `ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem` | v1.x | Append-only log/snapshot adapter for constrained IoT/desktop. |
| `ReactiveUI.Primitives.OccasionallyConnected.Transport.Http` | v1 | Batched sync and long-poll/SSE reference transport. |
| `ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets` | v1.x | Bidirectional streaming adapter. |
| `ReactiveUI.Primitives.OccasionallyConnected.SignalR` | later | SignalR client/server adapter. |
| `ReactiveUI.Primitives.OccasionallyConnected.Mqtt` | later | MQTT topic/QoS adapter. MQTT QoS is mapped explicitly and does not replace end-to-end idempotency. |
| `ReactiveUI.Primitives.OccasionallyConnected.Web` | later | IndexedDB storage and browser connectivity/lifecycle adapters. |
| `ReactiveUI.Primitives.OccasionallyConnected.Mobile` | later | Mobile lifecycle, secure storage, and SQLite convenience integration. |
| `ReactiveUI.Primitives.OccasionallyConnected.IoT` | later | File/embedded-store defaults and MQTT convenience integration. |

Storage and transport package names describe mechanisms; Web, Mobile, and IoT are convenience compositions and MUST NOT duplicate core logic.

### 15.2 Target frameworks

Core library projects follow the parent family and target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481` where their dependencies permit. Compatibility assets use centrally managed `Microsoft.Bcl.AsyncInterfaces`, `Microsoft.Bcl.TimeProvider`, `System.Threading.Channels`, and `System.Text.Json` packages where required. Adapter projects may target a narrower platform-specific set and MUST document it in package metadata.

All targets enable nullable reference types. Modern targets enable trimming and NativeAOT compatibility analysis. Reflection-free serializers are required for AOT scenarios.

### 15.3 Repository layout

```text
src/
  ReactiveUI.Primitives.slnx
  Directory.Build.props
  Directory.Packages.props
  ReactiveUI.Primitives.OccasionallyConnected.Core/
    ReactiveUI.Primitives.OccasionallyConnected.Core.csproj
    Contracts/
      IOccasionallyConnectedContext.cs
      IRemoteObservable.cs
      IRemoteObserver.cs
      ISyncEngine.cs
      ILocalStoreAdapter.cs
      IRemoteTransportAdapter.cs
      IConflictResolver.cs
      IPayloadSerializer.cs
    Models/
      Identifiers.cs
      SyncOperation.cs
      RemoteEvent.cs
      SyncState.cs
      Results.cs
      Capabilities.cs
    Options/
      OccasionallyConnectedOptions.cs
      RemoteSubscriptionOptions.cs
      RemotePublishOptions.cs
    Protocol/
      ProtocolEnvelope.cs
      SyncBatch.cs
      Acknowledgements.cs
      ProtocolVersions.cs
    Diagnostics/
      OccasionallyConnectedFault.cs
      DiagnosticNames.cs
    PublicAPI/<tfm>/
      PublicAPI.Shipped.txt
      PublicAPI.Unshipped.txt
  ReactiveUI.Primitives.OccasionallyConnected/
    ReactiveUI.Primitives.OccasionallyConnected.csproj
    Context/
      OccasionallyConnectedBuilder.cs
      OccasionallyConnectedContext.cs
    Engine/
      SyncEngine.cs
      StreamCoordinator.cs
      OutboxDispatcher.cs
      InboxProcessor.cs
      SubscriptionRestorer.cs
      ConflictCoordinator.cs
    Concurrency/
      KeyedSequencer.cs
      BoundedAdmissionQueue.cs
      FairStreamScheduler.cs
    Resilience/
      RetryPolicy.cs
      CircuitBreaker.cs
      FailureClassifier.cs
    Serialization/
      SchemaRegistry.cs
      PayloadUpcasterPipeline.cs
    Signals/
      OccasionallyConnectedStream.cs
    Extensions/
      OccasionallyConnectedExtensions.cs
    ReferenceAdapters/
      InMemoryLocalStoreAdapter.cs
      LoopbackTransportAdapter.cs
    PublicAPI/<tfm>/...
  ReactiveUI.Primitives.OccasionallyConnected.Reactive/
  ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection/
  ReactiveUI.Primitives.OccasionallyConnected.Hosting/
  ReactiveUI.Primitives.OccasionallyConnected.Server/
  ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/
  ReactiveUI.Primitives.OccasionallyConnected.Transport.Http/
tests/
  ReactiveUI.Primitives.OccasionallyConnected.Tests/
  ReactiveUI.Primitives.OccasionallyConnected.ContractTests/
  ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/
  ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests/
  ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/
  ReactiveUI.Primitives.OccasionallyConnected.CrashTests/
benchmarks/
  ReactiveUI.Primitives.OccasionallyConnected.Benchmarks/
samples/
  OccasionallyConnected.ConsoleSample/
  OccasionallyConnected.AspNetCoreSample/
```

Shared lean/Reactive implementation files use neutral `RxVoid` and `ISequencer` aliases and conditional namespaces, following the parent package pattern. Logic is linked/shared, not copied.

## 16. API usage examples

### 16.1 Local-first temperature stream

```csharp
await using var context = new OccasionallyConnectedBuilder()
    .UseClient(new ClientIdentity("device-123"))
    .UseStore(new SqliteLocalStoreAdapter("readings.db"))
    .UseTransport(new HttpRemoteTransportAdapter(
        new Uri("https://api.example.test"), tokenProvider))
    .UseJsonSerializer(TemperatureJsonContext.Default)
    .Build();

var definition = new StreamDefinition<TemperatureState, TemperatureReading>
{
    StreamId = new StreamId("sensor/temperature"),
    InputContractId = "temperature-reading",
    StateContractId = "temperature-state",
    Projection = new TemperatureProjection(),
    Subscription = new RemoteSubscriptionOptions
    {
        StreamId = new StreamId("sensor/temperature"),
        StartPosition = StartPosition.Latest,
        DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
        BufferCapacity = 256,
        BufferCapacityBytes = 1_048_576
    },
    Publish = new RemotePublishOptions
    {
        StreamId = new StreamId("sensor/temperature"),
        Durable = true,
        ConflictPolicy = ConflictPolicy.Merge
    }
};

var stream = context.GetOrCreateStream(definition);

using var localSubscription = stream.Local.Subscribe(state =>
    Console.WriteLine($"Latest: {state.Value} {state.Unit}"));

using var syncSubscription = stream.SyncStates.Subscribe(state =>
    Console.WriteLine($"Sync: {state.Status}; pending={state.PendingOperations}"));

await context.StartAsync(cancellationToken);

PublishReceipt receipt = await stream.PublishAsync(
    new TemperatureReading(21.3, "C"),
    cancellationToken: cancellationToken);

await context.SyncEngine.AwaitSynchronizedAsync(
    receipt.OperationId,
    timeout: TimeSpan.FromSeconds(20),
    cancellationToken: cancellationToken);
```

The local subscription receives the optimistic state after the durable local commit, whether or not the remote endpoint is online.

### 16.2 Awaiting a specific operation

```csharp
PublishReceipt receipt = await stream.PublishAsync(reading, cancellationToken: ct);

await context.SyncEngine.AwaitSynchronizedAsync(
    receipt.OperationId,
    timeout: TimeSpan.FromSeconds(20),
    cancellationToken: ct);
```

Timeout or caller cancellation stops the wait, not the durable synchronization operation.

### 16.3 Convenience observer

```csharp
IObserver<TemperatureReading> input = stream.Input;
input.OnNext(new TemperatureReading(21.8, "C"));
```

This form is appropriate only when the caller does not require a persistence receipt. Capacity and persistence faults must be observed through `stream.Faults`.

### 16.4 Custom deterministic conflict resolver

```csharp
public sealed class HighestQualityReadingResolver : IConflictResolver
{
    public ValueTask<ConflictResolutionResult> ResolveAsync(
        ConflictContext context,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        SyncOperation winner = context.Incoming
            .OrderByDescending(GetQuality)
            .ThenBy(operation => operation.OperationId.Value)
            .First();

        return ValueTask.FromResult(CreateResult(context, winner));
    }
}
```

## 17. Testing strategy

All automated .NET tests use Microsoft.Testing.Platform with TUnit and only TUnit assertions.

### 17.0 Testability and conformance contract

The runtime MUST accept an `ISequencer`, `TimeProvider`, operation/message ID source, and deterministic jitter/random source. The test support assembly provides:

- `VirtualClock`/virtual sequencer control with no wall-clock sleeps;
- a scripted store that can pause, fail, corrupt, or terminate at every transaction boundary;
- a controlled transport peer that can duplicate, reorder, delay, truncate, reject, or drop messages and ACKs;
- a deterministic race runner for `StartAsync`, `StopAsync`, `DisposeAsync`, publish, subscribe, lease renewal, and compaction interleavings;
- an observer recorder that detects concurrent callbacks, notifications after disposal/termination, ordering violations, and bounded-queue overflow;
- protocol fixture readers for every supported version;
- compile-tested samples that reference only the packed public API.

Conformance is capability-driven:

| Capability | Mandatory positive tests | Mandatory negative tests |
| --- | --- | --- |
| `AtomicLocalCommit` | operation, sequence, and snapshot survive or roll back together at every crash point | engine rejects durable publishing when absent |
| `AtomicRemoteApply` | inbox insert, snapshot update, and cursor advancement survive or roll back together | cursor-resume stream fails startup when absent |
| `DurableInbox` | duplicates before/after restart produce one effect/notification | `ExactlyOnce` negotiation fails when absent |
| `LeasedOutbox` | lease exclusion, expiry, renewal, reclaim, and owner crash | concurrent drain is disabled/rejected when absent |
| `MultiProcessCoordination` | two processes preserve ownership and ordering | second writer fails clearly when absent |
| `AuthenticatedEncryptionAtRest` | confidentiality, integrity, rotation, stale key, and tamper failure | `RequireAuthenticatedEncryptionAtRest` fails startup when absent |
| `BatchPush` | count/byte/dwell bounds and per-item result completeness | batching is disabled or startup fails when required |
| `CursorResume` | reconnect resumes at exactly the next durable event | persisted-resume configuration fails when absent |
| `ReceiveAcknowledgements` | ACK only follows durable apply; duplicate ACK is harmless | engine uses declared non-ACK mode and does not claim exactly once |
| `ServerIdempotency` | duplicate operation returns the original terminal result | at-least-once/exactly-once configuration fails when absent |
| `AtomicApplyAndAcknowledge` | server effect, canonical event, ledger, and ACK are one transaction | `ExactlyOnce` negotiation fails when absent |
| `StreamingReceive` | cancellation, reconnect, frame sequencing, and slow-receiver bounds | engine uses negotiated polling without claiming streaming |

Every advertised flag runs its positive suite. Every required-but-missing flag runs the startup validation suite. Transport conformance also proves that adapters perform no hidden unbounded retries and map failures/retry hints to the standard classification.

CI scans test project package references and source imports. It fails on xUnit, NUnit, MSTest, FluentAssertions, or another assertion framework, and requires TUnit plus Microsoft.Testing.Platform. Documentation samples are compiled from clean projects against freshly packed packages.

### 17.1 Test layers

1. **Model and validation tests**: identifiers, option combinations, capability negotiation, fault classification, hash validation, and state invariants.
2. **Deterministic engine tests**: virtual time, retry, circuit breaker, cancellation races, ordering, fairness, backpressure, batch limits, and observer grammar.
3. **Store contract suite**: the same reusable tests run against every `ILocalStoreAdapter`.
4. **Transport contract suite**: the same reusable tests run against every transport adapter with a controllable protocol peer.
5. **Protocol golden tests**: canonical JSON/binary fixtures, version negotiation, unknown fields, corrupted envelopes, upcast chains, and cross-version compatibility.
6. **Crash-consistency tests**: terminate a child process at injected commit checkpoints, reopen the store, and verify invariants.
7. **Server conformance tests**: authorization, idempotent duplicate results, atomic apply plus ACK, conflict determinism, cursor gaps, and retention.
8. **End-to-end fault-injection tests**: disconnect, reconnect, latency, duplication, reordering, dropped ACK, partial batch result, token expiry, disk-full, and clock skew.
9. **Security tests**: the threats listed in section 12.4.
10. **Performance tests**: throughput, allocation, recovery time, large-outbox scan, compaction, and slow-observer isolation.
11. **Trimming/AOT tests**: publish trimmed/AOT samples and execute serializer registration paths.
12. **Public API sample tests**: compile and execute every C# snippet represented as a sample, including all start-position factories and observer input options.

### 17.2 Mandatory scenario matrix

Each supported delivery guarantee is tested at these crash/failure points:

- before serialization;
- after serialization but before local commit;
- during local commit;
- after local commit before enqueue signal;
- during upload;
- after server apply before client receives ACK;
- after ACK before local ACK commit;
- during remote event apply;
- after inbox insert before observer notification;
- during compaction and store migration.

Each producer path—`PublishAsync`, `IRemoteObserver<T>.AsObserver`, and `stream.Input`—is also tested for every applicable `BufferStrategy`, count limit, byte limit, cancellation point, concurrent producer count, and durable/non-durable combination. Tests assert that `Block` is rejected on synchronous observer bridges and that durable operations are never selected for dropping.

For every case, assert operation count, client sequence, server effect count, inbox count, cursor, reconstructed snapshot, notification sequence, and terminal operation state.

### 17.3 Example TUnit style

```csharp
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

public sealed class RecoveryTests
{
    [Test]
    public async Task Lost_ack_retries_with_the_same_operation_id()
    {
        var fixture = await EngineFixture.CreateAsync();
        fixture.Transport.DropNextAcknowledgement();

        PublishReceipt receipt = await fixture.Stream.PublishAsync(new Reading(42));
        await fixture.Clock.AdvanceUntilIdleAsync();

        await Assert.That(fixture.Server.EffectCount(receipt.OperationId)).IsEqualTo(1);
        await Assert.That(fixture.Transport.Attempts(receipt.OperationId)).IsGreaterThanOrEqualTo(2);
    }
}
```

No xUnit, NUnit, MSTest, FluentAssertions, or mixed assertion style is permitted in these projects.

### 17.4 Quality gates

- Zero test failures and zero build/analyzer warnings.
- 100% transition and invariant coverage for lifecycle, outbox, inbox, ACK, and conflict state machines.
- At least 95% line and 90% branch coverage for core runtime projects; generated code and platform interop exclusions require documented approval.
- Adapter contract suites pass for every advertised capability/TFM.
- Mutation testing demonstrates meaningful assertions on durability, ordering, idempotency, and retry decisions.
- Crash tests run on each supported storage engine in CI-capable environments.
- Performance budgets are recorded before release and regressions above 10% require review.

## 18. Build, compatibility, and packaging

### 18.1 Build settings

- Use `src/ReactiveUI.Primitives.slnx` as the solution entry point and build/test from `src`.
- Enable deterministic builds, continuous integration build metadata, nullable reference types, XML documentation, analyzers, package validation, and warnings as errors.
- Centralize package versions and common properties.
- Produce Source Link-enabled deterministic PDBs and `.snupkg` symbol packages.
- Run API compatibility against the previous stable package and maintain `PublicAPI.Shipped.txt`/`PublicAPI.Unshipped.txt` per package and TFM.
- Validate trimming and NativeAOT annotations on applicable targets.
- Generate an SBOM/provenance record and scan dependencies and packages before publishing.

### 18.2 NuGet metadata

Every package includes license, repository URL/commit, icon, README, release notes, tags, description, and symbol package. Package descriptions clearly state whether the package is core, lean, `.Reactive`, an adapter, or a convenience composition.

The leaf package MAY pack this design/usage skill at package root and `.agents/skills/reactiveui-primitives-occasionally-connected/SKILL.md` if adopted by the repository. The source filename and packed paths must follow the repository's established singular `Skill.md` convention.

### 18.3 Compatibility policy

- Public API follows SemVer.
- Wire and store compatibility are tested independently of assembly API compatibility.
- Minor releases may add optional fields/capabilities but MUST remain readable by older peers within the supported protocol minor range.
- Removing a protocol version, schema upcast path, or store migration path requires a major release and an announced support window.
- Serialization golden fixtures from every supported release are retained in source control.
- Preview packages use prerelease versions and make no stable wire compatibility promise until RC1; RC1 freezes protocol v1 and store schema v1.

### 18.4 CI matrix

CI MUST include:

- build and TUnit tests across supported desktop TFMs;
- Linux, Windows, and macOS for platform-neutral projects;
- adapter-specific integration services/containers where appropriate;
- API compatibility and public API baseline checks;
- package pack/install smoke tests from a clean sample;
- deterministic package comparison;
- trimming/AOT smoke tests on supported modern TFMs;
- security/dependency/license scanning;
- separate scheduled crash, soak, and performance jobs.

## 19. Phased implementation plan

### Phase 0 — Architecture and contract freeze

Deliverables:

- approve this specification and record ADRs for delivery guarantees, storage atomics, sequencing, cursor opacity, serializer allowlisting, and package boundaries;
- prototype the local publish and remote receive transactions against an in-memory model;
- define public API baselines, protocol v1 schema, store schema v1, and adapter capability flags.

Exit criteria:

- no unresolved semantic questions in crash points, ACK handling, ordering, or ownership;
- representative consumer code compiles against API stubs;
- threat model and test matrix reviewed.

### Phase 1 — Core contracts and deterministic runtime

Deliverables:

- `.Core` models/contracts/options;
- builder, context, per-stream coordinator, lifecycle state machine;
- schema registry, JSON serializer, retry/circuit breaker, bounded queues;
- in-memory store and loopback transport;
- Primitives signals and public extension methods.

Exit criteria:

- deterministic TUnit suites pass with virtual time;
- observable grammar, ordering, backpressure, and cancellation invariants meet quality gates;
- at-most-once and at-least-once behavior passes all in-memory fault points.

### Phase 2 — Durable storage and crash recovery

Deliverables:

- SQLite store with migrations, leases, inbox, outbox, snapshots, quarantine, dead letters, and compaction;
- reusable store conformance kit;
- child-process crash harness.

Exit criteria:

- every crash point recovers without missing durable operations or duplicating local application;
- disk-full, corruption detection, migration interruption, and ownership locking are verified;
- recovery and compaction performance budgets are met.

### Phase 3 — Protocol, server, and HTTP reference transport

Deliverables:

- capability handshake and protocol v1 codecs;
- server hub, authorization hooks, idempotency ledger, canonical cursors, conflict pipeline;
- HTTP batching plus streaming/long-poll receive path;
- transport/server conformance suites.

Exit criteria:

- dropped ACK and duplicate/reordered delivery tests prove at-least-once plus idempotent effect;
- cursor expiry/gap snapshot recovery works;
- security and protocol fuzz tests pass.

### Phase 4 — Exactly-once-effect and DI/hosting

Deliverables:

- capability-gated `ExactlyOnce` validation and retention reporting;
- Microsoft.Extensions DI/options package;
- hosting lifecycle, health checks, logging adapters, metrics, and trace propagation.

Exit criteria:

- end-to-end transactional/idempotency tests pass across SQLite, server ledger, and HTTP adapter;
- unsupported adapter combinations fail at startup with actionable diagnostics;
- graceful shutdown and restart tests pass.

### Phase 5 — Preview release and hardening

Deliverables:

- preview packages, samples, API documentation, migration/operations guidance;
- soak tests under long disconnection, large queues, slow observers, and reconnect storms;
- performance baselines, SBOM, package validation, trim/AOT results.

Exit criteria:

- no critical/high security issues;
- no data-loss or invariant defects in soak/crash suites;
- documented upgrade and rollback procedure;
- protocol/store formats ready to freeze.

### Phase 6 — RC and stable v1

Deliverables:

- RC1 freezes public API, protocol v1, store schema v1, and core metric names;
- resolve RC feedback without expanding scope;
- stable Core, leaf, Server, SQLite, HTTP, and DI packages.

Exit criteria:

- all quality and compatibility gates pass on the release commit;
- clean-project package install samples pass;
- support, deprecation, and security-reporting policies are published.

### Phase 7 — Optional adapters

Add WebSocket, file-system, `.Reactive`, Hosting, SignalR, MQTT, Web/IndexedDB, Mobile, and IoT packages in that order based on demand. Every adapter MUST pass the shared contract suite, publish precise capability claims, and avoid weakening core guarantees.

## 20. Definition of done for v1

Version 1 is complete when:

- a durable local publish acknowledged by `PublishAsync` survives every defined crash point;
- the same logical subscription resumes with its persisted `SubscriptionId` and cursor after restart;
- remote duplicates never update the local projection or notify a subscriber twice within retention;
- local and remote notifications obey the serialized observable grammar;
- all queues, batches, retries, and retention stores are bounded and observable;
- at-least-once and capability-gated exactly-once-effect semantics are proven by conformance and crash tests;
- schema, protocol, package, store, and snapshot version policies are implemented and covered by golden fixtures;
- server authorization, idempotency, cursor, and conflict rules are enforced transactionally;
- diagnostics contain stable reason codes and no payloads/credentials by default;
- core, SQLite, HTTP, Server, and DI packages pass TUnit, API compatibility, packaging, security, and platform gates;
- samples demonstrate offline startup, optimistic local writes, restart recovery, conflict reconciliation, and eventual synchronization.

## 21. Fixed design decisions

The following are resolved by this specification and are not implementation-time options:

1. The durable entity is a logical subscription identity and checkpoint, not a live CLR subscription object.
2. The canonical write API is awaitable; `IObserver<T>` is a convenience bridge only.
3. At-least-once is the default. `ExactlyOnce` is capability-gated exactly-once effect within a declared retention window.
4. Storage APIs express atomic workflow operations rather than independent CRUD calls.
5. The server cursor is opaque and server-assigned.
6. Client timestamps never determine canonical order or last-writer-wins alone.
7. Core reconnect/retry policy is centralized in the engine, not duplicated in adapters.
8. All queues are bounded by item count and bytes; durable work is never silently dropped.
9. Payload types are allowlisted by stable contract ID and schema version; CLR type names are not wire contracts.
10. Lean core packages use ReactiveUI.Primitives `ISequencer`; System.Reactive support is an explicit `.Reactive` variant.
11. Platform bundles compose storage/transport adapters and do not fork the synchronization engine.
12. TUnit with Microsoft.Testing.Platform and TUnit assertions is the sole test stack.
