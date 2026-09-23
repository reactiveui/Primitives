// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Remote notification tests.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies mutable remote notifications materialize from committed payloads per observer.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task RemoteNotificationsUseCommittedSnapshotsForMutableInputs()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var serializer = new MutableInputPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new OccasionallyConnectedStreamOptions<CounterState, MutableCounterInput>
            {
                Definition = CreateMutableInputDefinition(),
                Store = store,
                Serializer = serializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = new RecordingCoordinator(store),
                InputProducer = new RecordingInputProducer<MutableCounterInput>(),
                LocalStateSnapshotFactory = static (payload, _) => new(MutableInputPayloadSerializer.CreateCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (payload, _) => new(MutableInputPayloadSerializer.CreateMutableInputSnapshot(payload)),
                NotificationScheduler = scheduler,
                NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
                WorkCapacity = WorkCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
        await stream.StartAsync(CancellationToken.None);
        var observedBySecondObserver = new List<int>();
        using var mutatingSubscription = stream.Remote.Subscribe(new ActionObserver<RemoteMessage<MutableCounterInput>>(
            static message => message.Value.Replace(CorruptedValue)));
        using var recordingSubscription = stream.Remote.Subscribe(new ActionObserver<RemoteMessage<MutableCounterInput>>(
            message => observedBySecondObserver.Add(message.Value.Delta)));

        var batch = new RemoteEventBatch(
            Guid.NewGuid(),
            Stream,
            null,
            FirstCursor,
            [CreateRemoteEvent(ThirdValue, FirstCursor)]);

        var result = await stream.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        scheduler.RunAll();

        await AssertSequenceAsync(observedBySecondObserver, [ThirdValue]);
        await Assert.That(result.State.State.Sum).IsEqualTo(ThirdValue);
    }

    /// <summary>Verifies remote notifications enforce the configured retained byte capacity.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task RemoteNotificationsChargeEnvelopeFieldsAgainstByteCapacity()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var payloadSerializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            new OccasionallyConnectedStreamOptions<CounterState, CounterInput>
            {
                Definition = CreateDefinition(),
                Store = store,
                Serializer = payloadSerializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = new RecordingCoordinator(store),
                InputProducer = new RecordingInputProducer<CounterInput>(),
                LocalStateSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterInputSnapshot(payload)),
                NotificationScheduler = scheduler,
                NotificationOptions = new(NotificationCapacity, TinyNotificationCapacityBytes, ObserverNotificationOverflowMode.Disconnect),
                WorkCapacity = WorkCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
        await stream.StartAsync(CancellationToken.None);
        var remote = new RecordingObserver<RemoteMessage<CounterInput>>();
        using var remoteSubscription = stream.Remote.Subscribe(remote);
        var batch = new RemoteEventBatch(
            Guid.NewGuid(),
            Stream,
            null,
            FirstCursor,
            [CreateRemoteEvent(ThirdValue, FirstCursor)]);

        _ = await stream.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(remote.Values).IsEmpty();
        await Assert.That(remote.Error).IsTypeOf<ObserverNotificationOverflowException>();
    }

    /// <summary>Verifies remote snapshot materializers must return an owned input value.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task RemoteSnapshotNullMaterializerReportsObserverFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var payloadSerializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            new OccasionallyConnectedStreamOptions<CounterState, CounterInput>
            {
                Definition = CreateDefinition(),
                Store = store,
                Serializer = payloadSerializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = new RecordingCoordinator(store),
                InputProducer = new RecordingInputProducer<CounterInput>(),
                LocalStateSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (_, _) => MissingSnapshotAsync<CounterInput>(),
                NotificationScheduler = scheduler,
                NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
                WorkCapacity = WorkCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
        await stream.StartAsync(CancellationToken.None);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var remote = new RecordingObserver<RemoteMessage<CounterInput>>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var remoteSubscription = stream.Remote.Subscribe(remote);
        var batch = new RemoteEventBatch(
            Guid.NewGuid(),
            Stream,
            null,
            FirstCursor,
            [CreateRemoteEvent(ThirdValue, FirstCursor)]);

        _ = await stream.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(remote.Values).IsEmpty();
        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Observer");
    }

    /// <summary>Creates the mutable-input stream definition.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, MutableCounterInput> CreateMutableInputDefinition() =>
        new() { StreamId = Stream, Projection = new MutableInputProjection(), InputContractId = InputContract, StateContractId = StateContract };

    /// <summary>Projects mutable counter inputs into immutable counter state.</summary>
    private sealed class MutableInputProjection : ILocalProjection<CounterState, MutableCounterInput>
    {
        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        public CounterState ApplyLocal(CounterState state, MutableCounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        public CounterState ApplyRemote(CounterState state, MutableCounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Serializes mutable counter inputs and immutable counter state as invariant text.</summary>
    private sealed class MutableInputPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => PayloadContentType;

        /// <summary>Creates an immutable counter state snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The state snapshot.</returns>
        public static CounterState CreateCounterStateSnapshot(PayloadEnvelope envelope) =>
            new(ParsePayloadValue(envelope));

        /// <summary>Creates a mutable counter input snapshot from a payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The input snapshot.</returns>
        public static MutableCounterInput CreateMutableInputSnapshot(PayloadEnvelope envelope) =>
            new(CreateCounterStateSnapshot(envelope).Sum);

        /// <inheritdoc />
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var numeric = value switch
            {
                MutableCounterInput input => input.Delta,
                CounterState state => state.Sum,
                _ => throw new InvalidOperationException(UnexpectedPayloadTypeMessage),
            };
            var text = numeric.ToString(CultureInfo.InvariantCulture);
            var payload = System.Text.Encoding.UTF8.GetBytes(text);
            return ValueTask.FromResult(new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, $"hash-{text}"));
        }

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var value = ParsePayloadValue(envelope);
            if (targetType == typeof(MutableCounterInput))
            {
                return ValueTask.FromResult<object>(new MutableCounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return ValueTask.FromResult<object>(new CounterState(value));
            }

            throw new InvalidOperationException(UnexpectedTargetTypeMessage);
        }
    }

    /// <summary>Stores mutable counter input.</summary>
    private sealed class MutableCounterInput
    {
        /// <summary>Initializes a new instance of the <see cref="MutableCounterInput"/> class.</summary>
        /// <param name="delta">The initial delta.</param>
        public MutableCounterInput(int delta) => Delta = delta;

        /// <summary>Gets the current delta.</summary>
        public int Delta { get; private set; }

        /// <summary>Replaces the current delta.</summary>
        /// <param name="value">The replacement value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Replace(int value) => Delta = value;
    }
}
