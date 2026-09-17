// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The input contract used by counter fixtures.</summary>
    private const string InputContract = "counter-input";

    /// <summary>The state contract used by counter fixtures.</summary>
    private const string StateContract = "counter-state";

    /// <summary>The local store identity used by facade tests.</summary>
    private const string StoreIdentity = "facade-tests";

    /// <summary>The local client identity used by facade tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The first counter input value.</summary>
    private const int FirstValue = 3;

    /// <summary>The second counter input value.</summary>
    private const int SecondValue = 4;

    /// <summary>The third counter input value.</summary>
    private const int ThirdValue = 5;

    /// <summary>The first local operation sequence number.</summary>
    private const long FirstSequence = 1L;

    /// <summary>The second local operation sequence number.</summary>
    private const long SecondSequence = 2L;

    /// <summary>The stop calls made by explicit stop plus dispose after restart.</summary>
    private const int StopCallsAfterRestartAndDispose = 2;

    /// <summary>The mutation value used to detect leaked mutable state instances.</summary>
    private const int CorruptedValue = 99;

    /// <summary>The serializer payload type failure message used by test serializers.</summary>
    private const string UnexpectedPayloadTypeMessage = "Unexpected payload type.";

    /// <summary>The serializer target type failure message used by test serializers.</summary>
    private const string UnexpectedTargetTypeMessage = "Unexpected target type.";

    /// <summary>The explicit base version used by publish-option propagation tests.</summary>
    private const string ExplicitBaseVersion = "server-v1";

    /// <summary>The stream work lane capacity used by tests.</summary>
    private const int WorkCapacity = 2;

    /// <summary>The notification queue capacity used by facade tests.</summary>
    private const int NotificationCapacity = 8;

    /// <summary>The notification queue byte capacity used by facade tests.</summary>
    private const int NotificationCapacityBytes = 1024;

    /// <summary>A deliberately tiny notification queue byte capacity for retained-size tests.</summary>
    private const int TinyNotificationCapacityBytes = 1;

    /// <summary>The number of seconds allowed for test synchronization waits.</summary>
    private const int TestWaitTimeoutSeconds = 5;

    /// <summary>The local SQLite file name used by tests.</summary>
    private const string LocalDatabaseFileName = "local.db";

    /// <summary>The remote cursor used by tests.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The content type emitted by test serializers.</summary>
    private const string PayloadContentType = "test/plain";

    /// <summary>The stream identity used by facade tests.</summary>
    private static readonly StreamId Stream = new("counter/main");

    /// <summary>An explicit subscription identity used by tests.</summary>
    private static readonly SubscriptionId ExplicitSubscription = new(new Guid("18f62c8f-d3a1-4d8a-bddf-2734e01f30ec"));

    /// <summary>A conflicting subscription identity used by tests.</summary>
    private static readonly SubscriptionId OtherSubscription = new(new Guid("4d7e0613-9f38-4759-afbc-909966892584"));

    /// <summary>The fixed test timestamp.</summary>
    private static readonly DateTimeOffset Now = new(2026, 9, 13, 2, 30, 0, TimeSpan.Zero);

    /// <summary>Verifies construction does not perform identity, store, or producer lifetime work.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ConstructorDoesNotResolveImplicitSubscriptionOrStartInputProducer()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var inputProducer = new RecordingInputProducer<CounterInput>();

        await using var stream = CreateStream(store, coordinator, inputProducer);

        await Assert.That(coordinator.IdentityCalls).IsEqualTo(0);
        await Assert.That(coordinator.StartCalls).IsEqualTo(0);
        await Assert.That(inputProducer.DisposeCalls).IsEqualTo(0);
        await Assert.That(() => stream.SubscriptionId).ThrowsExactly<InvalidOperationException>();
        await Assert.That(stream.Input).IsSameReferenceAs(inputProducer.Observer);
    }

    /// <summary>Verifies start resolves an implicit subscription and replays recovered SQLite state after restart.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task StartAsyncResolvesImplicitSubscriptionRecoversSqliteStateAndReplaysLatestAcrossRestart()
    {
        var directory = SqliteTestDirectory.Create("oc-stream-facade-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, LocalDatabaseFileName);
            SubscriptionId firstIdentity;

            await using (var store = await CreateInitializedStoreAsync(databasePath))
            {
                var scheduler = new ControlledObserverScheduler();
                await using var stream = CreateStream(store, scheduler: scheduler);
                await stream.StartAsync(CancellationToken.None);
                firstIdentity = stream.SubscriptionId;
                var live = new RecordingObserver<CounterState>();
                using var liveSubscription = stream.Local.Subscribe(live);
                scheduler.RunAll();
                _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
                scheduler.RunAll();

                await AssertSequenceAsync(live.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
            }

            await using var secondStore = await CreateInitializedStoreAsync(databasePath);
            var secondScheduler = new ControlledObserverScheduler();
            await using var secondStream = CreateStream(secondStore, scheduler: secondScheduler);
            await secondStream.StartAsync(CancellationToken.None);
            var late = new RecordingObserver<CounterState>();
            using var subscription = secondStream.Local.Subscribe(late);
            secondScheduler.RunAll();

            await Assert.That(secondStream.SubscriptionId).IsEqualTo(firstIdentity);
            await AssertSequenceAsync(late.Values.Select(static state => state.Sum).ToArray(), [FirstValue]);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies explicit subscription identities are visible synchronously and validated against the store on start.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ExplicitSubscriptionIsAvailableImmediatelyAndValidatedDuringStart()
    {
        var directory = SqliteTestDirectory.Create("oc-stream-explicit-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, LocalDatabaseFileName);
            await using (var store = await CreateInitializedStoreAsync(databasePath))
            {
                await using var stream = CreateStream(store, CreateDefinition(subscriptionId: ExplicitSubscription));
                await Assert.That(stream.SubscriptionId).IsEqualTo(ExplicitSubscription);
                await stream.StartAsync(CancellationToken.None);
            }

            await using var secondStore = await CreateInitializedStoreAsync(databasePath);
            await using var secondStream = CreateStream(secondStore, CreateDefinition(subscriptionId: OtherSubscription));

            await Assert.That(() => secondStream.StartAsync(CancellationToken.None).AsTask())
                .ThrowsExactly<InvalidOperationException>();
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies caller cancellation while waiting for startup does not poison shared initialization.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task CanceledStartWaitDoesNotPoisonSharedInitialization()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { IdentityEntered = identityEntered, ReleaseIdentity = releaseIdentity };
        await using var stream = CreateStream(store, coordinator);
        using CancellationTokenSource source = new();

        var start = stream.StartAsync(source.Token).AsTask();
        await identityEntered.Task;
        await source.CancelAsync();
        await Assert.That(start).Throws<OperationCanceledException>();
        releaseIdentity.SetResult();
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(coordinator.IdentityCalls).IsEqualTo(1);
    }

    /// <summary>Verifies concurrent local publishes are serialized through the bounded stream lane.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PublishAsyncSerializesConcurrentLocalMutationsThroughBoundedLane()
    {
        await using var store = await CreateInitializedStoreAsync();
        var serializer = new ScriptedPayloadSerializer();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, serializer: serializer, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        scheduler.RunAll();

        var first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        var second = stream.PublishAsync(new(SecondValue), null, CancellationToken.None).AsTask();
        var receipts = await Task.WhenAll(first, second);
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await AssertSequenceAsync(receipts.Select(static receipt => receipt.ClientSequence).ToArray(), [FirstSequence, SecondSequence]);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue, FirstValue + SecondValue]);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => operation.ClientSequence).ToArray(), [FirstSequence, SecondSequence]);
    }

    /// <summary>Verifies facade disposal waits for accepted durable input before lane close.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForAcceptedDurablePublishBeforeClosingLane()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource inputSerializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInputSerialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var serializer = new ScriptedPayloadSerializer { InputSerializeEntered = inputSerializeEntered, ReleaseInputSerialize = releaseInputSerialize };
        await using var stream = CreateStream(store, serializer: serializer);
        await stream.StartAsync(CancellationToken.None);
        var subscriptionId = stream.SubscriptionId;

        var publish = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        await inputSerializeEntered.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        var dispose = stream.DisposeAsync().AsTask();

        await Assert.That(dispose.IsCompleted).IsFalse();
        await Assert.That(publish.IsCompleted).IsFalse();

        _ = releaseInputSerialize.TrySetResult();
        var receipt = await publish.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        await dispose.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(async () => await stream.PublishAsync(new(SecondValue), null, CancellationToken.None))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies typed publish admitted before first initialization drains during dispose.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PublishAsyncBeforeStartInitializesAndDrainsDuringDispose()
    {
        await using var store = await CreateInitializedStoreAsync();
        await using var stream = CreateStream(store, CreateDefinition(subscriptionId: ExplicitSubscription));

        var publish = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        var dispose = stream.DisposeAsync().AsTask();
        var receipt = await publish.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        await dispose.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(async () => await stream.PublishAsync(new(SecondValue), null, CancellationToken.None))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies a commit-ready nudge failure is reported without discarding the durable receipt.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PublishAsyncReturnsReceiptAndReportsFaultWhenCommitReadyNudgeFails()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var coordinator = new RecordingCoordinator(store) { ThrowOnCommitReady = true };
        await using var stream = CreateStream(store, coordinator, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);

        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(faults.Values.Count).IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.CommitReady");
        await Assert.That(faults.Values[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Verifies stop preserves subscribers and dispose owns the injected input producer lifetime.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task StopAsyncCallsCoordinatorWithoutCompletingSubscribersAndDisposeOwnsInputProducer()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var inputProducer = new RecordingInputProducer<CounterInput>();
        var scheduler = new ControlledObserverScheduler();
        var stream = CreateStream(store, coordinator, inputProducer, scheduler);
        await stream.StartAsync(CancellationToken.None);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        scheduler.RunAll();

        await stream.StopAsync(CancellationToken.None);
        await stream.StartAsync(CancellationToken.None);
        _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        await stream.DisposeAsync();

        await Assert.That(coordinator.StopCalls).IsEqualTo(StopCallsAfterRestartAndDispose);
        await Assert.That(local.CompletedCount).IsEqualTo(0);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
        await Assert.That(inputProducer.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies remote apply publishes remote messages before the reconciled local state notification.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncEmitsRemoteMessagesBeforeReconciledLocalState()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var notifications = new List<string>();
        using var remoteSubscription = stream.Remote.Subscribe(new ActionObserver<RemoteMessage<CounterInput>>(
            message => notifications.Add($"remote:{message.Value.Delta}")));
        using var localSubscription = stream.Local.Subscribe(new ActionObserver<CounterState>(
            state => notifications.Add($"local:{state.Sum}")));
        scheduler.RunAll();
        notifications.Clear();

        var batch = new RemoteEventBatch(
            Guid.NewGuid(),
            Stream,
            null,
            FirstCursor,
            [CreateRemoteEvent(ThirdValue, FirstCursor)]);

        var result = await stream.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(result.Receipt.NextCursor).IsEqualTo(FirstCursor);
        await AssertSequenceAsync(notifications.ToArray(), ["remote:5", "local:5"]);
    }

    /// <summary>Verifies mutable local notifications are isolated from durable commit state and latest replay.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task LocalNotificationsUseCommittedSnapshotsForMutableStates()
    {
        var directory = SqliteTestDirectory.Create("oc-stream-mutable-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, LocalDatabaseFileName);
            SubscriptionId subscriptionId;

            await using (var store = await CreateInitializedStoreAsync(databasePath))
            {
                var scheduler = new ControlledObserverScheduler();
                await using var stream = CreateMutableStream(store, scheduler);
                await stream.StartAsync(CancellationToken.None);
                subscriptionId = stream.SubscriptionId;
                using var mutatingSubscription = stream.Local.Subscribe(new ActionObserver<MutableCounterState>(
                    static state => state.Replace(CorruptedValue)));
                scheduler.RunAll();

                _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
                scheduler.RunAll();
                var firstReplay = new RecordingObserver<MutableCounterState>();
                using var firstReplaySubscription = stream.Local.Subscribe(firstReplay);
                scheduler.RunAll();

                _ = await stream.PublishAsync(new(SecondValue), null, CancellationToken.None);
                scheduler.RunAll();
                var secondReplay = new RecordingObserver<MutableCounterState>();
                using var secondReplaySubscription = stream.Local.Subscribe(secondReplay);
                scheduler.RunAll();

                await AssertSequenceAsync(firstReplay.Values.Select(static state => state.Sum).ToArray(), [FirstValue, FirstValue + SecondValue]);
                await AssertSequenceAsync(secondReplay.Values.Select(static state => state.Sum).ToArray(), [FirstValue + SecondValue]);
            }

            await using var secondStore = await CreateInitializedStoreAsync(databasePath);
            var secondScheduler = new ControlledObserverScheduler();
            await using var secondStream = CreateMutableStream(secondStore, secondScheduler);
            await secondStream.StartAsync(CancellationToken.None);
            var recovered = new RecordingObserver<MutableCounterState>();
            using var recoveredSubscription = secondStream.Local.Subscribe(recovered);
            secondScheduler.RunAll();

            await Assert.That(secondStream.SubscriptionId).IsEqualTo(subscriptionId);
            await AssertSequenceAsync(recovered.Values.Select(static state => state.Sum).ToArray(), [FirstValue + SecondValue]);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Creates an initialized SQLite local store for a facade test.</summary>
    /// <param name="databasePath">The optional database path.</param>
    /// <returns>The initialized store.</returns>
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
    /// <returns>The constructed stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStream<CounterState, CounterInput> CreateStream(
        ILocalStoreAdapter store,
        RecordingCoordinator? coordinator = null,
        RecordingInputProducer<CounterInput>? inputProducer = null,
        IObserverNotificationScheduler? scheduler = null,
        ScriptedPayloadSerializer? serializer = null) =>
        CreateStream(store, CreateDefinition(), coordinator, inputProducer, scheduler, serializer);

    /// <summary>Creates a counter stream with an explicit definition and optional test seams.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="coordinator">The optional coordinator.</param>
    /// <param name="inputProducer">The optional input producer.</param>
    /// <param name="scheduler">The optional scheduler.</param>
    /// <param name="serializer">The optional serializer.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<CounterState, CounterInput> CreateStream(
        ILocalStoreAdapter store,
        StreamDefinition<CounterState, CounterInput> definition,
        RecordingCoordinator? coordinator = null,
        RecordingInputProducer<CounterInput>? inputProducer = null,
        IObserverNotificationScheduler? scheduler = null,
        ScriptedPayloadSerializer? serializer = null)
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
                WorkCapacity = WorkCapacity,
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
