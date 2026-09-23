// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Helper members for <see cref="OccasionallyConnectedStreamTests"/>.</summary>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Creates an initialized SQLite local store for a facade test.</summary>
    /// <param name="databasePath">The optional database path.</param>
    /// <returns>The initialized local store.</returns>
    private static async ValueTask<SqliteLocalStoreAdapter> CreateInitializedStoreAsync(string? databasePath = null)
    {
        var path = databasePath ?? Path.Combine(SqliteTestDirectory.Create("oc-stream-store-").FullName, LocalDatabaseFileName);
        var store = new SqliteLocalStoreAdapter(path);
        await store.InitializeAsync(new(StoreIdentity, 1, false) { ClientId = ClientId }, CancellationToken.None);
        return store;
    }

    /// <summary>Creates a counter stream with default definition and optional test seams.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The optional coordinator.</param>
    /// <param name="inputProducer">The optional input producer.</param>
    /// <param name="scheduler">The optional scheduler.</param>
    /// <param name="serializer">The optional serializer.</param>
    /// <param name="workCapacity">The stream work-lane capacity.</param>
    /// <returns>The constructed stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStream<CounterState, CounterInput> CreateStream(
        ILocalStoreAdapter store,
        RecordingCoordinator? coordinator = null,
        RecordingInputProducer<CounterInput>? inputProducer = null,
        IObserverNotificationScheduler? scheduler = null,
        IPayloadSerializer? serializer = null,
        int workCapacity = WorkCapacity) =>
        CreateStream(store, CreateDefinition(), coordinator, inputProducer, scheduler, serializer, workCapacity);

    /// <summary>Creates a counter stream with an explicit definition and optional test seams.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="coordinator">The optional coordinator.</param>
    /// <param name="inputProducer">The optional input producer.</param>
    /// <param name="scheduler">The optional scheduler.</param>
    /// <param name="serializer">The optional serializer.</param>
    /// <param name="workCapacity">The stream work-lane capacity.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<CounterState, CounterInput> CreateStream(
        ILocalStoreAdapter store,
        StreamDefinition<CounterState, CounterInput> definition,
        RecordingCoordinator? coordinator = null,
        RecordingInputProducer<CounterInput>? inputProducer = null,
        IObserverNotificationScheduler? scheduler = null,
        IPayloadSerializer? serializer = null,
        int workCapacity = WorkCapacity)
    {
        var payloadSerializer = serializer ?? new ScriptedPayloadSerializer();
        return new(
            new OccasionallyConnectedStreamOptions<CounterState, CounterInput>
            {
                Definition = definition,
                Store = store,
                Serializer = payloadSerializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = coordinator ?? new RecordingCoordinator(store),
                InputProducer = inputProducer,
                LocalStateSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterInputSnapshot(payload)),
                NotificationScheduler = scheduler ?? new ControlledObserverScheduler(),
                NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
                WorkCapacity = workCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
    }

    /// <summary>Creates a mutable counter stream with optional scheduler control.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="scheduler">The notification scheduler.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<MutableCounterState, CounterInput> CreateMutableStream(
        ILocalStoreAdapter store,
        IObserverNotificationScheduler scheduler)
    {
        var payloadSerializer = new MutableCounterPayloadSerializer();
        return new(
            new OccasionallyConnectedStreamOptions<MutableCounterState, CounterInput>
            {
                Definition = CreateMutableDefinition(),
                Store = store,
                Serializer = payloadSerializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = new RecordingCoordinator(store),
                InputProducer = new RecordingInputProducer<CounterInput>(),
                LocalStateSnapshotFactory = static (payload, _) => new(MutableCounterPayloadSerializer.CreateMutableCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (payload, _) => new(MutableCounterPayloadSerializer.CreateCounterInputSnapshot(payload)),
                NotificationScheduler = scheduler,
                NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
                WorkCapacity = WorkCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
    }

    /// <summary>Creates the default counter stream definition.</summary>
    /// <param name="subscriptionId">The optional explicit subscription identity.</param>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition(SubscriptionId? subscriptionId = null) =>
        new() { StreamId = Stream, SubscriptionId = subscriptionId, Projection = new CounterProjection(), InputContractId = InputContract, StateContractId = StateContract };

    /// <summary>Creates the mutable counter stream definition.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<MutableCounterState, CounterInput> CreateMutableDefinition() =>
        new() { StreamId = Stream, Projection = new MutableCounterProjection(), InputContractId = InputContract, StateContractId = StateContract };

    /// <summary>Creates a remote event containing a counter input payload.</summary>
    /// <param name="value">The remote input value.</param>
    /// <param name="cursor">The server cursor.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(int value, string cursor) =>
        new(
            Guid.NewGuid(),
            Stream,
            cursor,
            Now,
            null,
            CreatePayload(value),
            new Dictionary<string, string>());

    /// <summary>Creates a counter payload envelope.</summary>
    /// <param name="value">The numeric value to encode.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(int value)
    {
        var text = value.ToString(CultureInfo.InvariantCulture);
        return new(InputContract, 1, PayloadContentType, System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text}");
    }

    /// <summary>Creates a serialized sync operation with a counter payload.</summary>
    /// <param name="value">The counter delta.</param>
    /// <param name="sequence">The local client sequence.</param>
    /// <returns>The serialized operation.</returns>
    private static SyncOperation CreateSerializedOperation(int value, long sequence = FirstSequence) =>
        new() { OperationId = OperationId.New(), StreamId = Stream, ClientSequence = sequence, TimestampUtc = Now, Type = SyncOperationType.Update, Payload = CreatePayload(value) };

    /// <summary>Asserts that two sequences have matching values in order.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="actual">The actual values.</param>
    /// <param name="expected">The expected values.</param>
    /// <returns>A task that completes when assertions finish.</returns>
    private static async Task AssertSequenceAsync<T>(IReadOnlyList<T> actual, IReadOnlyList<T> expected)
    {
        await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            await Assert.That(actual[i]).IsEqualTo(expected[i]);
        }
    }

    /// <summary>Parses the integer payload value from a test envelope.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <returns>The parsed payload value.</returns>
    private static int ParsePayloadValue(PayloadEnvelope envelope)
    {
        var text = System.Text.Encoding.UTF8.GetString(envelope.Payload.Span);
        return int.Parse(text, CultureInfo.InvariantCulture);
    }

    /// <summary>Creates a runtime null value without suppressing nullable analysis.</summary>
    /// <typeparam name="T">The reference type to return.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T"/>.</returns>
    private static T MissingRequired<T>()
        where T : class
    {
        object? missing = null;
        return Unsafe.As<object?, T>(ref missing);
    }

    /// <summary>Creates a runtime null snapshot without suppressing nullable analysis.</summary>
    /// <typeparam name="T">The reference type to return through a value task.</typeparam>
    /// <returns>A value task completed with null.</returns>
    private static ValueTask<T> MissingSnapshotAsync<T>()
        where T : class
    {
        object? missing = null;
        return new(Unsafe.As<object?, T>(ref missing));
    }

    /// <summary>Projects immutable counter state for stream facade tests.</summary>
    private sealed class CounterProjection : ILocalProjection<CounterState, CounterInput>
    {
        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Projects mutable counter state for notification isolation tests.</summary>
    private sealed class MutableCounterProjection : ILocalProjection<MutableCounterState, CounterInput>
    {
        /// <inheritdoc />
        public MutableCounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        public MutableCounterState ApplyLocal(MutableCounterState state, CounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        public MutableCounterState ApplyRemote(MutableCounterState state, CounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public MutableCounterState Reconcile(MutableCounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Records coordinator interactions and delegates durable identity work to the real store.</summary>
    /// <param name="store">The backing local store.</param>
    private sealed class RecordingCoordinator(ILocalStoreAdapter store) : IOccasionallyConnectedStreamCoordinator
    {
        /// <summary>Gets the number of identity calls.</summary>
        public int IdentityCalls { get; private set; }

        /// <summary>Gets the number of start calls.</summary>
        public int StartCalls { get; private set; }

        /// <summary>Gets the number of stop calls.</summary>
        public int StopCalls { get; private set; }

        /// <summary>Gets the number of commit-ready calls.</summary>
        public int CommitReadyCalls { get; private set; }

        /// <summary>Gets the number of participant registration calls.</summary>
        public int RegisterCalls { get; private set; }

        /// <summary>Gets the number of local admission calls.</summary>
        public int EnterLocalCommitCalls { get; private set; }

        /// <summary>Gets the number of completed local admission calls.</summary>
        public int CompleteLocalCommitCalls { get; private set; }

        /// <summary>Gets the number of capacity-generation reads.</summary>
        public int CapacityGenerationReads { get; private set; }

        /// <summary>Gets the number of capacity-release waits.</summary>
        public int CapacityReleaseWaits { get; private set; }

        /// <summary>Gets the retained bytes charged by the most recent capacity wait.</summary>
        public long LastCapacityWaitRetainedBytes { get; private set; }

        /// <summary>Gets a value indicating whether commit-ready should throw.</summary>
        public bool ThrowOnCommitReady { get; init; }

        /// <summary>Gets or sets a value indicating whether start should throw.</summary>
        public bool ThrowOnStart { get; set; }

        /// <summary>Gets or sets a value indicating whether stop should throw.</summary>
        public bool ThrowOnStop { get; set; }

        /// <summary>Gets the optional signal set when identity resolution starts.</summary>
        public TaskCompletionSource? IdentityEntered { get; init; }

        /// <summary>Gets the optional signal that releases identity resolution.</summary>
        public TaskCompletionSource? ReleaseIdentity { get; init; }

        /// <summary>Gets the optional signal set when start begins.</summary>
        public TaskCompletionSource? StartEntered { get; init; }

        /// <summary>Gets the optional signal that releases start.</summary>
        public TaskCompletionSource? ReleaseStart { get; init; }

        /// <summary>Gets the optional signal set when capacity wait begins.</summary>
        public TaskCompletionSource? CapacityWaitEntered { get; init; }

        /// <summary>Gets the optional signal that releases capacity wait.</summary>
        public TaskCompletionSource? ReleaseCapacityWait { get; init; }

        /// <inheritdoc />
        public IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant)
        {
            ArgumentNullException.ThrowIfNull(participant);
            RegisterCalls++;
            return new Registration();
        }

        /// <inheritdoc />
        public async ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken)
        {
            IdentityCalls++;
            _ = IdentityEntered?.TrySetResult();
            if (ReleaseIdentity is not null)
            {
                await ReleaseIdentity.Task.ConfigureAwait(false);
            }

            return await store.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(StreamId streamId, long retainedBytes, CancellationToken cancellationToken)
        {
            EnterLocalCommitCalls++;
            cancellationToken.ThrowIfCancellationRequested();
            return new(new LocalCommitAdmission(streamId, retainedBytes, Guid.NewGuid()));
        }

        /// <inheritdoc />
        public void CompleteLocalCommit(LocalCommitAdmission admission)
        {
            _ = admission;
            CompleteLocalCommitCalls++;
        }

        /// <inheritdoc />
        public long GetCapacityReleaseGeneration(StreamId streamId)
        {
            _ = streamId;
            CapacityGenerationReads++;
            return CapacityGenerationReads;
        }

        /// <inheritdoc />
        public async ValueTask WaitForCapacityReleaseAsync(
            StreamId streamId,
            long observedGeneration,
            long retainedBytes,
            CancellationToken cancellationToken)
        {
            _ = streamId;
            _ = observedGeneration;
            LastCapacityWaitRetainedBytes = retainedBytes;
            CapacityReleaseWaits++;
            cancellationToken.ThrowIfCancellationRequested();
            _ = CapacityWaitEntered?.TrySetResult();
            if (ReleaseCapacityWait is not null)
            {
                await ReleaseCapacityWait.Task.ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken)
        {
            StartCalls++;
            _ = StartEntered?.TrySetResult();
            if (ReleaseStart is not null)
            {
                await ReleaseStart.Task.ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (ThrowOnStart)
            {
                throw new InvalidOperationException("start failed");
            }
        }

        /// <inheritdoc />
        public ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken)
        {
            StopCalls++;
            cancellationToken.ThrowIfCancellationRequested();
            if (ThrowOnStop)
            {
                throw new InvalidOperationException("stop failed");
            }

            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public void RecordSavedLocalCommit(
            StreamId streamId,
            SyncOperation operation,
            QueueDiagnosticSnapshot snapshot,
            PublishReceipt receipt)
        {
            _ = snapshot;
            _ = receipt;
            NotifyLocalCommitReady(streamId, operation);
        }

        /// <inheritdoc />
        public void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation)
        {
            CommitReadyCalls++;
            if (!ThrowOnCommitReady)
            {
                return;
            }

            throw new InvalidOperationException("nudge failed");
        }

        /// <inheritdoc />
        public void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot)
        {
            _ = streamId;
            _ = snapshot;
        }

        /// <inheritdoc />
        public void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null)
        {
            _ = streamId;
            _ = priority;
            _ = notBeforeUtc;
        }

        /// <inheritdoc />
        public void NotifyCapacityReleased(StreamId streamId) => _ = streamId;

        /// <summary>Represents one inert test registration.</summary>
        private sealed class Registration : IDisposable
        {
            /// <inheritdoc />
            public void Dispose()
            {
            }
        }
    }

    /// <summary>Records disposal of an owned input producer seam.</summary>
    /// <typeparam name="T">The input type.</typeparam>
    private sealed class RecordingInputProducer<T> : IOccasionallyConnectedInputProducer<T>
    {
        /// <summary>Gets the number of dispose calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets or sets a value indicating whether disposal should throw.</summary>
        public bool ThrowOnDispose { get; set; }

        /// <summary>Gets the callback invoked before returning the disposal awaitable.</summary>
        public Action BeforeDispose { get; init; } = static () => { };

        /// <inheritdoc />
        public IObserver<T> Observer { get; } = new RecordingInputObserver();

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            BeforeDispose();
            return ThrowOnDispose
                ? ValueTask.FromException(new InvalidOperationException("input dispose failed"))
                : ValueTask.CompletedTask;
        }

        /// <summary>Records input values without transport behavior.</summary>
        private sealed class RecordingInputObserver : IObserver<T>
        {
            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted()
            {
            }

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(T value)
            {
            }
        }
    }

    /// <summary>Records observer callbacks.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the number of completion callbacks.</summary>
        public int CompletedCount { get; private set; }

        /// <summary>Gets the last observed error.</summary>
        public Exception? Error { get; private set; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => CompletedCount++;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Error = error;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Invokes an action for each observer value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="onNext">The callback.</param>
    private sealed class ActionObserver<T>(Action<T> onNext) : IObserver<T>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => onNext(value);
    }

    /// <summary>Queues observer work until tests explicitly drain it.</summary>
    private sealed class ControlledObserverScheduler : IObserverNotificationScheduler
    {
        /// <summary>Stores queued work items.</summary>
        private readonly Queue<IWorkItem> _items = [];

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _items.Enqueue(item);

        /// <summary>Runs all queued work items.</summary>
        public void RunAll()
        {
            while (_items.Count > 0)
            {
                _items.Dequeue().Execute();
            }
        }
    }

    /// <summary>Generates deterministic operation identifiers.</summary>
    private sealed class SequenceOperationIdSource : IOperationIdSource
    {
        /// <summary>Stores the next operation number.</summary>
        private int _next = 1;

        /// <inheritdoc />
        public OperationId New()
        {
            var current = _next;
            _next++;
            return new(new Guid(current, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1]));
        }
    }

    /// <summary>Provides a fixed clock value.</summary>
    /// <param name="utcNow">The fixed UTC timestamp.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
