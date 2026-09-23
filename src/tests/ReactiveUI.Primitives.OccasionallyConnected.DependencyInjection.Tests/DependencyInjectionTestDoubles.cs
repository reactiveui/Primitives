// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Shared test doubles for dependency-injection tests.</summary>
public static class DependencyInjectionTestDoubles
{
    /// <summary>The client identifier used by tests.</summary>
    public static readonly string ClientId = "client-a";

    /// <summary>The store identity used by tests.</summary>
    public static readonly string StoreIdentity = "di-tests";

    /// <summary>The input contract used by tests.</summary>
    public static readonly string InputContract = "counter-input";

    /// <summary>The state contract used by tests.</summary>
    public static readonly string StateContract = "counter-state";

    /// <summary>The bounded wait timeout used by asynchronous test coordination.</summary>
    public static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The maximum retained input payload size used by test stream definitions.</summary>
    private const int MaximumRetainedInputBytes = 128;

    /// <summary>Creates a valid counter stream definition.</summary>
    /// <param name="services">The service provider.</param>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static StreamDefinition<CounterState, CounterInput> CreateDefinition(IServiceProvider services) =>
        CreateDefinition(services, "counter/main");

    /// <summary>Creates a valid counter stream definition.</summary>
    /// <param name="services">The service provider.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The stream definition.</returns>
    public static StreamDefinition<CounterState, CounterInput> CreateDefinition(
        IServiceProvider services,
        string streamId) => new()
        {
            StreamId = new(streamId),
            Projection = services.GetRequiredService<CounterProjection>(),
            InputContractId = InputContract,
            StateContractId = StateContract,
            Publish = CreateVolatilePublishOptions(streamId),
            TypedInput = new() { MaximumRetainedInputBytes = MaximumRetainedInputBytes },
        };

    /// <summary>Creates a valid counter stream definition with another state type.</summary>
    /// <param name="services">The service provider.</param>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static StreamDefinition<OtherState, CounterInput> CreateOtherDefinition(IServiceProvider services) =>
        CreateOtherDefinition();

    /// <summary>Creates a valid counter stream definition with another state type.</summary>
    /// <returns>The stream definition.</returns>
    public static StreamDefinition<OtherState, CounterInput> CreateOtherDefinition() => new()
    {
        StreamId = new("counter/other"),
        Projection = new OtherProjection(),
        InputContractId = InputContract,
        StateContractId = StateContract,
        Publish = CreateVolatilePublishOptions("counter/other"),
        TypedInput = new() { MaximumRetainedInputBytes = MaximumRetainedInputBytes },
    };

    /// <summary>Creates local store initialization for volatile in-memory test storage.</summary>
    /// <returns>The local store initialization.</returns>
    public static LocalStoreInitialization CreateStoreInitialization() =>
        new(StoreIdentity, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = ClientId };

    /// <summary>Deserializes a stored counter snapshot.</summary>
    /// <param name="snapshot">The recovered snapshot.</param>
    /// <returns>The counter state.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot payload is not a counter state.</exception>
    public static CounterState DeserializeCounterState(LocalSnapshot snapshot) =>
        JsonSerializer.Deserialize(snapshot.State.Payload.Span, DependencyInjectionJsonContext.Default.CounterState)
        ?? throw new InvalidOperationException("The counter state snapshot was null.");

    /// <summary>Creates the generated metadata registry used by serializer tests.</summary>
    /// <returns>The schema registry.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SchemaRegistry CreateSchemaRegistry() =>
        new SchemaRegistry()
            .Register(InputContract, 1, DependencyInjectionJsonContext.Default.CounterInput)
            .Register(StateContract, 1, DependencyInjectionJsonContext.Default.CounterState);

    /// <summary>Creates the generated metadata serializer used by serializer tests.</summary>
    /// <returns>The payload serializer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static JsonPayloadSerializer CreateJsonPayloadSerializer() => new(CreateSchemaRegistry());

    /// <summary>Creates volatile publish options supported by the in-memory store.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreateVolatilePublishOptions(string streamId) =>
        new() { StreamId = new(streamId), Durable = false };

    /// <summary>Records serializer calls while delegating to the generated JSON serializer.</summary>
    public sealed class PrimaryRecordingPayloadSerializer : IPayloadSerializer
    {
        /// <summary>Stores the shared recorder.</summary>
        private readonly RecordingPayloadSerializerCore _core = new();

        /// <inheritdoc />
        public string ContentType => _core.ContentType;

        /// <summary>Gets the number of serialize calls.</summary>
        public int SerializeCalls => _core.SerializeCalls;

        /// <summary>Gets the number of deserialize calls.</summary>
        public int DeserializeCalls => _core.DeserializeCalls;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken) =>
            _core.SerializeAsync(contractId, schemaVersion, value, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken) =>
            _core.DeserializeAsync(envelope, targetType, cancellationToken);
    }

    /// <summary>Records secondary serializer calls while delegating to the generated JSON serializer.</summary>
    public sealed class SecondaryRecordingPayloadSerializer : IPayloadSerializer
    {
        /// <summary>Stores the shared recorder.</summary>
        private readonly RecordingPayloadSerializerCore _core = new();

        /// <inheritdoc />
        public string ContentType => _core.ContentType;

        /// <summary>Gets the number of serialize calls.</summary>
        public int SerializeCalls => _core.SerializeCalls;

        /// <summary>Gets the number of deserialize calls.</summary>
        public int DeserializeCalls => _core.DeserializeCalls;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken) =>
            _core.SerializeAsync(contractId, schemaVersion, value, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken) =>
            _core.DeserializeAsync(envelope, targetType, cancellationToken);
    }

    /// <summary>Records store initialization and disposal while delegating behavior to the in-memory store.</summary>
    public sealed class RecordingStoreAdapter : ILocalStoreAdapter
    {
        /// <summary>Stores the inner in-memory adapter.</summary>
        private readonly InMemoryLocalStoreAdapter _inner = new();

        /// <summary>Gets the number of dispose calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets the last initialization request.</summary>
        public LocalStoreInitialization? Initialization { get; private set; }

        /// <summary>Gets the last committed local operation.</summary>
        public SyncOperation? LastCommittedOperation { get; private set; }

        /// <summary>Gets the last committed snapshot mutation.</summary>
        public SnapshotMutation? LastSnapshotMutation { get; private set; }

        /// <summary>Gets the last registered stream identifier.</summary>
        public StreamId? LastStreamId { get; private set; }

        /// <summary>Gets the last registered subscription identifier.</summary>
        public SubscriptionId? LastSubscriptionId { get; private set; }

        /// <inheritdoc />
        public LocalStoreCapabilities Capabilities => _inner.Capabilities;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            Initialization = initialization;
            return _inner.InitializeAsync(initialization, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken)
        {
            var subscriptionId = await _inner
                .GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken)
                .ConfigureAwait(false);
            LastStreamId = streamId;
            LastSubscriptionId = subscriptionId;
            return subscriptionId;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            _inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            LastCommittedOperation = operation;
            LastSnapshotMutation = snapshotMutation;
            return _inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken) =>
            _inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            CancellationToken cancellationToken) =>
            _inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            _inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            _inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            _inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            _inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
            OperationId operationId,
            CancellationToken cancellationToken) =>
            _inner.GetOperationStatusAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(
            OperationId operationId,
            CancellationToken cancellationToken) =>
            _inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            _inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(
            OperationId operationId,
            RetryState retryState,
            CancellationToken cancellationToken) =>
            _inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            _inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            _inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(
            CompactionRequest request,
            CancellationToken cancellationToken) =>
            _inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            DisposeCalls++;
            await _inner.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>Recovers the last registered stream snapshot from the inner store.</summary>
        /// <returns>The recovered local snapshot.</returns>
        /// <exception cref="InvalidOperationException">No stream was recorded or the stream has no snapshot.</exception>
        public async ValueTask<LocalSnapshot> RecoverLastSnapshotAsync()
        {
            if (LastStreamId is not { } streamId || LastSubscriptionId is not { } subscriptionId)
            {
                throw new InvalidOperationException("The store did not record a stream subscription.");
            }

            var recovered = await _inner
                .RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None)
                .ConfigureAwait(false);
            return recovered.Snapshot
                ?? throw new InvalidOperationException("The recovered stream did not contain a snapshot.");
        }
    }

    /// <summary>Records transport connection and disposal.</summary>
    public sealed class RecordingTransportAdapter : IRemoteTransportAdapter
    {
        /// <summary>Gets or sets the exception thrown from connect.</summary>
        public Exception? ConnectException { get; init; }

        /// <summary>Gets the number of dispose calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc />
        public RemoteTransportCapabilities Capabilities => RemoteTransportCapabilities.BatchPush;

        /// <inheritdoc />
        public ValueTask<IRemoteTransportSession> ConnectAsync(
            TransportConnectRequest request,
            CancellationToken cancellationToken)
        {
            if (ConnectException is not null)
            {
                throw ConnectException;
            }

            return new(new RecordingTransportSession());
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Provides an inert remote session.</summary>
    public sealed class RecordingTransportSession : IRemoteTransportSession
    {
        /// <summary>The negotiated batch count used by the inert transport session.</summary>
        private const int NegotiatedBatchCount = 100;

        /// <summary>The negotiated payload byte count used by the inert transport session.</summary>
        private const int NegotiatedPayloadBytes = 1_048_576;

        /// <inheritdoc />
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.BatchPush,
            NegotiatedBatchCount,
            NegotiatedPayloadBytes,
            null,
            null);

        /// <inheritdoc />
        public ValueTask<RemoteSyncResult> PushAsync(
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            new(new RemoteSyncResult(batch.BatchId, [], null, null));

        /// <inheritdoc />
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Throws when a logger is requested.</summary>
    public sealed class ThrowingLoggerFactory : ILoggerFactory
    {
        /// <summary>Initializes a new instance of the <see cref="ThrowingLoggerFactory"/> class.</summary>
        /// <param name="failure">The failure to throw.</param>
        public ThrowingLoggerFactory(Exception failure) => Failure = failure;

        /// <summary>Gets the failure thrown by <see cref="CreateLogger"/>.</summary>
        public Exception Failure { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddProvider(ILoggerProvider provider)
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ILogger CreateLogger(string categoryName) => throw Failure;

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }

    /// <summary>Records log entries.</summary>
    public sealed class RecordingLoggerProvider : ILoggerProvider
    {
        /// <summary>Stores the latest log completion.</summary>
        private readonly TaskCompletionSource<LogEntry> _entry =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a value indicating whether a log entry was recorded.</summary>
        public bool HasEntry => _entry.Task.IsCompleted;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ILogger CreateLogger(string categoryName) => new RecordingLogger(_entry);

        /// <summary>Waits for one log entry.</summary>
        /// <param name="timeout">The timeout.</param>
        /// <returns>The log entry.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<LogEntry> WaitForEntryAsync(TimeSpan timeout) => _entry.Task.WaitAsync(timeout);

        /// <inheritdoc />
        public void Dispose()
        {
        }

        /// <summary>Records one log entry.</summary>
        /// <param name="entry">The log entry completion source.</param>
        public sealed class RecordingLogger(TaskCompletionSource<LogEntry> entry) : ILogger
        {
            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IDisposable BeginScope<TState>(TState state)
                where TState : notnull =>
                NullScope.Instance;

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool IsEnabled(LogLevel logLevel) => true;

            /// <inheritdoc />
            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                _ = entry.TrySetResult(new(formatter(state, exception), exception));
        }

        /// <summary>A no-op log scope.</summary>
        public sealed class NullScope : IDisposable
        {
            /// <summary>Gets the singleton scope.</summary>
            public static NullScope Instance { get; } = new();

            /// <inheritdoc />
            public void Dispose()
            {
            }
        }

        /// <summary>A captured log entry.</summary>
        /// <param name="Message">The formatted message.</param>
        /// <param name="Exception">The logged exception.</param>
        public sealed record LogEntry(string Message, Exception? Exception);
    }

    /// <summary>Throws from logger callbacks after recording invocation.</summary>
    public sealed class ThrowingLoggerProvider : ILoggerProvider
    {
        /// <summary>Stores the log invocation signal.</summary>
        private readonly TaskCompletionSource _logged = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ILogger CreateLogger(string categoryName) => new ThrowingLogger(_logged);

        /// <summary>Waits for one log invocation.</summary>
        /// <param name="timeout">The timeout.</param>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task WaitForLogAsync(TimeSpan timeout) => _logged.Task.WaitAsync(timeout);

        /// <inheritdoc />
        public void Dispose()
        {
        }

        /// <summary>A no-op log scope.</summary>
        public sealed class NullScope : IDisposable
        {
            /// <summary>Gets the singleton scope.</summary>
            public static NullScope Instance { get; } = new();

            /// <inheritdoc />
            public void Dispose()
            {
            }
        }

        /// <summary>Throws after recording one log invocation.</summary>
        /// <param name="logged">The log invocation completion source.</param>
        public sealed class ThrowingLogger(TaskCompletionSource logged) : ILogger
        {
            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IDisposable BeginScope<TState>(TState state)
                where TState : notnull =>
                NullScope.Instance;

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool IsEnabled(LogLevel logLevel) => true;

            /// <inheritdoc />
            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                _ = logged.TrySetResult();
                throw new InvalidOperationException("logger failed");
            }
        }
    }

    /// <summary>Projects counter state.</summary>
    public sealed class CounterProjection : ILocalProjection<CounterState, CounterInput>
    {
        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Projects the alternate state type.</summary>
    public sealed class OtherProjection : ILocalProjection<OtherState, CounterInput>
    {
        /// <inheritdoc />
        public OtherState InitialState { get; } = new(0);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState ApplyLocal(OtherState state, CounterInput input, SyncOperation operation) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState ApplyRemote(OtherState state, CounterInput input, RemoteEvent remoteEvent) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState Reconcile(OtherState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Records observed values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    public sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Synchronizes observer state.</summary>
#if NET9_0_OR_GREATER
        private readonly Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>Stores observed values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Stores the first observed value that matches the active predicate.</summary>
        private TaskCompletionSource<T>? _matchingValue;

        /// <summary>Stores the active wait predicate.</summary>
        private Func<T, bool>? _predicate;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error) => throw error;

        /// <inheritdoc />
        public void OnNext(T value)
        {
            TaskCompletionSource<T>? matchingValue = null;
            lock (_gate)
            {
                _values.Add(value);
                if (_matchingValue is not null && _predicate?.Invoke(value) == true)
                {
                    matchingValue = _matchingValue;
                    _matchingValue = null;
                    _predicate = null;
                }
            }

            _ = matchingValue?.TrySetResult(value);
        }

        /// <summary>Waits for the first observed value that matches the predicate.</summary>
        /// <param name="predicate">The value predicate.</param>
        /// <param name="timeout">The maximum wait time.</param>
        /// <returns>The first matching observed value.</returns>
        public async Task<T> WaitForValueAsync(Func<T, bool> predicate, TimeSpan timeout)
        {
            TaskCompletionSource<T> matchingValue = new(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                foreach (var value in _values)
                {
                    if (!predicate(value))
                    {
                        continue;
                    }

                    _ = matchingValue.TrySetResult(value);
                    break;
                }

                if (!matchingValue.Task.IsCompleted)
                {
                    _predicate = predicate;
                    _matchingValue = matchingValue;
                }
            }

            return await matchingValue.Task.WaitAsync(timeout).ConfigureAwait(false);
        }
    }

    /// <summary>Shares serializer call recording and generated JSON delegation.</summary>
    private sealed class RecordingPayloadSerializerCore : IPayloadSerializer
    {
        /// <summary>Stores the delegated JSON serializer.</summary>
        private readonly JsonPayloadSerializer _inner = CreateJsonPayloadSerializer();

        /// <summary>Stores the number of serialize calls.</summary>
        private int _serializeCalls;

        /// <summary>Stores the number of deserialize calls.</summary>
        private int _deserializeCalls;

        /// <inheritdoc />
        public string ContentType => _inner.ContentType;

        /// <summary>Gets the number of serialize calls.</summary>
        public int SerializeCalls => Volatile.Read(ref _serializeCalls);

        /// <summary>Gets the number of deserialize calls.</summary>
        public int DeserializeCalls => Volatile.Read(ref _deserializeCalls);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _serializeCalls);
            return _inner.SerializeAsync(contractId, schemaVersion, value, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _deserializeCalls);
            return _inner.DeserializeAsync(envelope, targetType, cancellationToken);
        }
    }

    /// <summary>Test input value.</summary>
    /// <param name="Delta">The delta.</param>
    public sealed record CounterInput(int Delta);

    /// <summary>Test state value.</summary>
    /// <param name="Sum">The sum.</param>
    public sealed record CounterState(int Sum);

    /// <summary>Alternate state value.</summary>
    /// <param name="Sum">The sum.</param>
    public sealed record OtherState(int Sum);
}
