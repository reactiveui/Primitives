// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Lifecycle and admission tests.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The original diagnostic message length used to test trimming.</summary>
    private const int OriginalDiagnosticMessageLength = 1024;

    /// <summary>The maximum retained diagnostic exception message length expected by tests.</summary>
    private const int RetainedDiagnosticMessageLimit = 800;

    /// <summary>The notification byte capacity used when retaining a bounded diagnostic fault.</summary>
    private const int DiagnosticNotificationCapacityBytes = 2048;

    /// <summary>Verifies concurrent starts share the same pending lifecycle failure.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ConcurrentStartAsyncSharesPendingFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource startEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStart = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { StartEntered = startEntered, ReleaseStart = releaseStart, ThrowOnStart = true };
        await using var stream = CreateStream(store, coordinator);

        var first = stream.StartAsync(CancellationToken.None).AsTask();
        await startEntered.Task;
        var second = stream.StartAsync(CancellationToken.None).AsTask();

        await Assert.That(second.IsCompleted).IsFalse();
        releaseStart.SetResult();

        await Assert.That(first).ThrowsExactly<InvalidOperationException>();
        await Assert.That(second).ThrowsExactly<InvalidOperationException>();
        await Assert.That(coordinator.StartCalls).IsEqualTo(1);
    }

    /// <summary>Verifies stop converges to the latest requested lifecycle state when a pending start fails.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task StopAsyncAfterFailingStartReportsLifecycleFaultAndSettlesStopped()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource startEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStart = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var scheduler = new ControlledObserverScheduler();
        var coordinator = new RecordingCoordinator(store) { StartEntered = startEntered, ReleaseStart = releaseStart, ThrowOnStart = true };
        await using var stream = CreateStream(store, coordinator, scheduler: scheduler);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);

        var start = stream.StartAsync(CancellationToken.None).AsTask();
        await startEntered.Task;
        var stop = stream.StopAsync(CancellationToken.None).AsTask();
        releaseStart.SetResult();

        await Assert.That(start).ThrowsExactly<InvalidOperationException>();
        await stop;
        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Lifecycle");
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        await Assert.That(() => stream.SubscriptionId).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies canceled publish admission does not commit the input.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task CanceledPublishAsyncDoesNotCommitInput()
    {
        await using var store = await CreateInitializedStoreAsync();
        await using var stream = CreateStream(store);
        await stream.StartAsync(CancellationToken.None);
        using CancellationTokenSource source = new();
        await source.CancelAsync();

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => PublishCounterInputAsync(stream, new(FirstValue), null, source.Token));
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations).IsEmpty();
    }

    /// <summary>Verifies non-default publish options are used and mismatched stream options are rejected.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PublishAsyncUsesDefinitionPublishOptionsAndRejectsMismatchedOptions()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateDefinition() with { Publish = new RemotePublishOptions { StreamId = Stream, Durable = false } };
        await using var stream = CreateStream(store, definition);

        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PublishCounterInputAsync(stream, new(SecondValue), new RemotePublishOptions { StreamId = new("counter/other") }, CancellationToken.None));
    }

    /// <summary>Verifies disposal closes admission and keeps cleanup failures stable for repeated callers.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task DisposeAsyncClosesAdmissionAndPreservesFirstCleanupFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store) { ThrowOnStop = true };
        var inputProducer = new RecordingInputProducer<CounterInput> { ThrowOnDispose = true };
        var stream = CreateStream(store, coordinator, inputProducer);
        await stream.StartAsync(CancellationToken.None);

        var firstDispose = stream.DisposeAsync().AsTask();
        var secondDispose = stream.DisposeAsync().AsTask();

        await Assert.That(firstDispose).ThrowsExactly<InvalidOperationException>();
        await Assert.That(secondDispose).ThrowsExactly<InvalidOperationException>();
        await Assert.That(inputProducer.DisposeCalls).IsEqualTo(1);
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => PublishCounterInputAsync(stream, new(ThirdValue), null, CancellationToken.None));
        await Assert.That(() => stream.Local.Subscribe(new RecordingObserver<CounterState>())).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies a synchronous input disposal failure is reported while coordinator shutdown still runs.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task DisposeAsyncReportsInputDisposalFailureAndStillStopsCoordinator()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store) { ThrowOnStop = true };
        var inputProducer = new RecordingInputProducer<CounterInput> { BeforeDispose = static () => throw new InvalidOperationException("synchronous input disposal failed") };
        var stream = CreateStream(store, coordinator, inputProducer);
        await stream.StartAsync(CancellationToken.None);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => stream.DisposeAsync().AsTask());
        await Assert.That(exception?.Message).IsEqualTo("synchronous input disposal failed");
        await Assert.That(inputProducer.DisposeCalls).IsEqualTo(1);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        await Assert.That(() => stream.Local.Subscribe(new RecordingObserver<CounterState>())).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies stream observable properties expose subscribable dispatchers.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ObservablePropertiesExposeSubscribers()
    {
        await using var store = await CreateInitializedStoreAsync();
        await using var stream = CreateStream(store);

        using var syncSubscription = stream.SyncStates.Subscribe(new RecordingObserver<SyncState>());
        using var operationSubscription = stream.OperationStates.Subscribe(new RecordingObserver<SyncOperationStatus>());

        await Assert.That(stream.SyncStates).IsNotNull();
        await Assert.That(stream.OperationStates).IsNotNull();
    }

    /// <summary>Verifies operation status notifications enforce the configured retained byte capacity.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task OperationStatusNotificationsChargeEnvelopeFieldsAgainstByteCapacity()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var serializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            CreateOptions(store, serializer, scheduler) with
            {
                NotificationOptions = new(NotificationCapacity, TinyNotificationCapacityBytes, ObserverNotificationOverflowMode.Disconnect),
            });
        await stream.StartAsync(CancellationToken.None);
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        using var operationSubscription = stream.OperationStates.Subscribe(operationStates);

        _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(operationStates.Values).IsEmpty();
        await Assert.That(operationStates.Error).IsTypeOf<ObserverNotificationOverflowException>();
    }

    /// <summary>Verifies stop waits for a pending successful start and applies the latest desired state.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task StopAsyncAfterPendingSuccessfulStartCallsStop()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource startEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStart = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { StartEntered = startEntered, ReleaseStart = releaseStart };
        await using var stream = CreateStream(store, coordinator);

        var start = stream.StartAsync(CancellationToken.None).AsTask();
        await startEntered.Task;
        var stop = stream.StopAsync(CancellationToken.None).AsTask();
        releaseStart.SetResult();

        await start;
        await stop;

        await Assert.That(coordinator.StartCalls).IsEqualTo(1);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies stopping before first start parks remote work while local publication remains admitted.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task StopAsyncBeforeFirstStartParksRemoteWorkAndPreservesOfflinePublish()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        await using var stream = CreateStream(store, CreateDefinition(subscriptionId: ExplicitSubscription), coordinator);

        await stream.StopAsync(CancellationToken.None);

        await Assert.That(coordinator.RegisterCalls).IsEqualTo(1);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        await Assert.That(coordinator.StartCalls).IsEqualTo(0);
        await Assert.That(coordinator.EnterLocalCommitCalls).IsEqualTo(0);

        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(coordinator.EnterLocalCommitCalls).IsEqualTo(1);
        await Assert.That(coordinator.CompleteLocalCommitCalls).IsEqualTo(1);
        await Assert.That(coordinator.StartCalls).IsEqualTo(0);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);

        await stream.StartAsync(CancellationToken.None);

        await Assert.That(coordinator.StartCalls).IsEqualTo(1);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies local snapshot preparation failures are reported without failing startup.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task LocalSnapshotPreparationFailureReportsFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var serializer = new ThrowingStatePayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            CreateOptions(store, serializer, scheduler) with { LocalStateSnapshotFactory = static (payload, _) => new(ThrowingStatePayloadSerializer.CreateCounterStateSnapshot(payload)) });
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);

        await stream.StartAsync(CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.LocalSnapshot");
    }

    /// <summary>Verifies local snapshot materializers must return an owned value.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task LocalSnapshotNullMaterializerReportsObserverFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var serializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            CreateOptions(store, serializer, scheduler) with { LocalStateSnapshotFactory = static (_, _) => MissingSnapshotAsync<CounterState>() });
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var locals = new RecordingObserver<CounterState>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var localSubscription = stream.Local.Subscribe(locals);

        await stream.StartAsync(CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(locals.Values).IsEmpty();
        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Observer");
    }

    /// <summary>Verifies observer callback failures are routed to the stream fault dispatcher.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task LocalObserverFailureReportsFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var localSubscription = stream.Local.Subscribe(new ThrowingObserver<CounterState>());

        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Observer");
    }

    /// <summary>Verifies fault notifications retain a bounded diagnostic instead of the original exception graph.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    /// <exception cref="InvalidOperationException">The fault did not include a diagnostic exception.</exception>
    [Test]
    public async Task ObserverFaultRetainsBoundedDiagnosticException()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var serializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(
            CreateOptions(store, serializer, scheduler) with
            {
                NotificationOptions = new(NotificationCapacity, DiagnosticNotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
            });
        await stream.StartAsync(CancellationToken.None);
        var original = new InvalidOperationException(new string('x', OriginalDiagnosticMessageLength));
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var localSubscription = stream.Local.Subscribe(new CapturingThrowingObserver<CounterState>(original));

        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Exception).IsNotSameReferenceAs(original);
        await Assert.That(faults.Values[0].Exception).IsTypeOf<InvalidOperationException>();
        var diagnostic = faults.Values[0].Exception ?? throw new InvalidOperationException("The fault did not include a diagnostic exception.");

        await Assert.That(diagnostic.Message.Length).IsLessThan(RetainedDiagnosticMessageLimit);
    }

    /// <summary>Verifies operational faults do not disclose caller exception messages or retained data.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ObserverFaultDoesNotDiscloseExceptionMessageOrData()
    {
        const string secret = "private-payload-and-bearer-token";
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var original = new InvalidOperationException(secret, new IOException(secret));
        original.Data[secret] = secret;
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var localSubscription = stream.Local.Subscribe(new CapturingThrowingObserver<CounterState>(original));

        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        var fault = faults.Values[0];
        await Assert.That(fault.Message.Contains(secret, StringComparison.Ordinal)).IsFalse();
        await Assert.That(fault.Exception?.ToString().Contains(secret, StringComparison.Ordinal)).IsFalse();
        await Assert.That(fault.Exception?.Data.Count).IsEqualTo(0);
        await Assert.That(fault.Exception?.InnerException).IsNull();
    }

    /// <summary>Verifies malformed internal stream options are rejected.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidInternalOptions()
    {
        await using var store = await CreateInitializedStoreAsync();
        var serializer = new ScriptedPayloadSerializer();
        var scheduler = new ControlledObserverScheduler();
        var options = CreateOptions(store, serializer, scheduler);

        await Assert.That(() => new OccasionallyConnectedStream<CounterState, CounterInput>(options with { WorkCapacity = 0 }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new OccasionallyConnectedStream<CounterState, CounterInput>(options with { LocalAdmissionRetainedBytes = 0 }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new OccasionallyConnectedStream<CounterState, CounterInput>(options with { LocalAdmissionRetainedBytes = -1 }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new OccasionallyConnectedStream<CounterState, CounterInput>(options with { MinimumPriority = 1, MaximumPriority = 0 }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new OccasionallyConnectedStream<CounterState, CounterInput>(options with { Store = MissingRequired<ILocalStoreAdapter>() }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Awaits a counter publish for async throw assertions.</summary>
    /// <param name="stream">The stream under test.</param>
    /// <param name="input">The input to publish.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when publish finishes.</returns>
    private static async Task PublishCounterInputAsync(
        OccasionallyConnectedStream<CounterState, CounterInput> stream,
        CounterInput input,
        RemotePublishOptions? options,
        CancellationToken cancellationToken) =>
        _ = await stream.PublishAsync(input, options, cancellationToken).ConfigureAwait(false);

    /// <summary>Creates a complete counter stream option record.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="serializer">The payload serializer.</param>
    /// <param name="scheduler">The notification scheduler.</param>
    /// <returns>The option record.</returns>
    private static OccasionallyConnectedStreamOptions<CounterState, CounterInput> CreateOptions(
        ILocalStoreAdapter store,
        IPayloadSerializer serializer,
        IObserverNotificationScheduler scheduler) =>
        new()
        {
            Definition = CreateDefinition(),
            Store = store,
            Serializer = serializer,
            TimeProvider = new FixedTimeProvider(Now),
            OperationIdSource = new SequenceOperationIdSource(),
            Coordinator = new RecordingCoordinator(store),
            InputProducer = new RecordingInputProducer<CounterInput>(),
            LocalStateSnapshotFactory = (payload, _) => new(serializer is ScriptedPayloadSerializer ? ScriptedPayloadSerializer.CreateCounterStateSnapshot(payload) : new CounterState(0)),
            RemoteInputSnapshotFactory = (payload, _) => new(serializer is ScriptedPayloadSerializer ? ScriptedPayloadSerializer.CreateCounterInputSnapshot(payload) : new CounterInput(0)),
            NotificationScheduler = scheduler,
            NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
            WorkCapacity = WorkCapacity,
            LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
            ClientId = ClientId,
        };

    /// <summary>Throws when serializing state snapshots.</summary>
    private sealed class ThrowingStatePayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => PayloadContentType;

        /// <summary>Creates a counter state snapshot.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The counter state.</returns>
        public static CounterState CreateCounterStateSnapshot(PayloadEnvelope envelope) =>
            new(ParsePayloadValue(envelope));

        /// <summary>Creates a counter input snapshot.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The counter input.</returns>
        public static CounterInput CreateCounterInputSnapshot(PayloadEnvelope envelope) =>
            new(CreateCounterStateSnapshot(envelope).Sum);

        /// <inheritdoc />
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (value is CounterState)
            {
                return ValueTask.FromException<PayloadEnvelope>(new InvalidOperationException("state snapshot failed"));
            }

            return value is CounterInput input
                ? ValueTask.FromResult(CreatePayload(input.Delta))
                : ValueTask.FromException<PayloadEnvelope>(new InvalidOperationException("Unexpected payload type."));
        }

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult<object>(new CounterInput(FirstValue));
        }
    }

    /// <summary>Throws for every callback.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(T value) => throw new InvalidOperationException("observer failed");
    }

    /// <summary>Throws a supplied exception for every callback.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="exception">The exception to throw.</param>
    private sealed class CapturingThrowingObserver<T>(Exception exception) : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(T value) => throw exception;
    }
}
