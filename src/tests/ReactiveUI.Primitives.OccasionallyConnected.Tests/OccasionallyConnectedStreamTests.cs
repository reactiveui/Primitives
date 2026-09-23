// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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

    /// <summary>The stream work lane capacity used to force one active work item to fill the lane.</summary>
    private const int SingleWorkCapacity = 1;

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

    /// <summary>The timeout used to catch blocked stream capacity regressions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

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

    /// <summary>Verifies raw participant commits use the stream committer and publish local state.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ParticipantCommitSerializedAsyncCommitsRawOperationAndPublishesLocalState()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var coordinator = new RecordingCoordinator(store);
        await using var stream = CreateStream(store, coordinator: coordinator, scheduler: scheduler);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        var operation = CreateSerializedOperation(FirstValue);

        var receipt = await ((IOccasionallyConnectedStreamParticipant)stream).CommitSerializedAsync(operation, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(receipt.ClientSequence).IsEqualTo(operation.ClientSequence);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
        await Assert.That(coordinator.CapacityGenerationReads).IsEqualTo(1);
        await Assert.That(coordinator.CommitReadyCalls).IsEqualTo(1);
    }

    /// <summary>Verifies typed publishes retry through the coordinator when stream lane capacity is released.</summary>
    /// <param name="explicitPublishOptions">Whether the caller explicitly selects blocking admission.</param>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PublishAsyncWaitsForCapacityReleaseBeforeRetryingFullLane(bool explicitPublishOptions)
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource capacityWaitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCapacityWait = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store)
        {
            IdentityEntered = identityEntered,
            ReleaseIdentity = releaseIdentity,
            CapacityWaitEntered = capacityWaitEntered,
            ReleaseCapacityWait = releaseCapacityWait,
        };
        await using var stream = CreateStream(store, coordinator: coordinator, workCapacity: SingleWorkCapacity);
        var options = explicitPublishOptions ? new RemotePublishOptions { StreamId = Stream, AdmissionStrategy = BufferStrategy.Block } : null;

        var first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        await identityEntered.Task.WaitAsync(GuardTimeout);
        var second = stream.PublishAsync(new(SecondValue), options, CancellationToken.None).AsTask();
        try
        {
            await capacityWaitEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(second.IsCompleted).IsFalse();
            releaseIdentity.SetResult();
            _ = await first.WaitAsync(GuardTimeout);
            releaseCapacityWait.SetResult();
            var receipt = await second.WaitAsync(GuardTimeout);

            await Assert.That(receipt.ClientSequence).IsEqualTo(SecondSequence);
            await Assert.That(coordinator.CapacityReleaseWaits).IsEqualTo(1);
        }
        finally
        {
            _ = releaseIdentity.TrySetResult();
            _ = releaseCapacityWait.TrySetResult();
            await Task.WhenAll(first, second).WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Verifies raw participant commits preserve empty commands while waiting for stream capacity.</summary>
    /// <param name="emptyPayload">Whether the pending command carries an empty zero-delta payload.</param>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ParticipantCommitSerializedAsyncWaitsForCapacityReleaseBeforeRetryingFullLane(bool emptyPayload)
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource capacityWaitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCapacityWait = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store)
        {
            IdentityEntered = identityEntered,
            ReleaseIdentity = releaseIdentity,
            CapacityWaitEntered = capacityWaitEntered,
            ReleaseCapacityWait = releaseCapacityWait,
        };
        var serializer = new EmptyCommandPayloadSerializer(new ScriptedPayloadSerializer());
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, coordinator: coordinator, scheduler: scheduler, serializer: serializer, workCapacity: SingleWorkCapacity);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        var operation = CreateSerializedOperation(SecondValue, SecondSequence);
        if (emptyPayload)
        {
            operation = operation with { Payload = new(InputContract, 1, PayloadContentType, ReadOnlyMemory<byte>.Empty, "empty-command") };
        }

        var first = participant.CommitSerializedAsync(CreateSerializedOperation(FirstValue), CancellationToken.None).AsTask();
        List<Task> commits = [first];
        try
        {
            await identityEntered.Task.WaitAsync(GuardTimeout);
            var second = participant.CommitSerializedAsync(operation, CancellationToken.None).AsTask();
            commits.Add(second);
            await capacityWaitEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(second.IsCompleted).IsFalse();
            await Assert.That(coordinator.LastCapacityWaitRetainedBytes).IsEqualTo(1);
            releaseIdentity.SetResult();
            _ = await first.WaitAsync(GuardTimeout);
            releaseCapacityWait.SetResult();
            var receipt = await second.WaitAsync(GuardTimeout);
            scheduler.RunAll();

            await Assert.That(receipt.ClientSequence).IsEqualTo(SecondSequence);
            await Assert.That(coordinator.CapacityReleaseWaits).IsEqualTo(1);
            await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue + (emptyPayload ? 0 : SecondValue));
        }
        finally
        {
            _ = releaseIdentity.TrySetResult();
            _ = releaseCapacityWait.TrySetResult();
            await Task.WhenAll(commits).WaitAsync(GuardTimeout);
        }
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
}
