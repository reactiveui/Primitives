// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Convenience extension tests.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The expected count for two notifications.</summary>
    private const int TwoNotifications = 2;

    /// <summary>The expected count for three notifications.</summary>
    private const int ThreeNotifications = 3;

    /// <summary>The first synthetic pending byte count.</summary>
    private const long FirstPendingBytes = 17;

    /// <summary>The second synthetic pending byte count.</summary>
    private const long SecondPendingBytes = 5;

    /// <summary>The timeout for deterministic test gates.</summary>
    private const int DisposalTimeoutSeconds = 10;

    /// <summary>Verifies observable source wrapping owns source subscription lifetime and retained input capture.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task ToOccasionallyConnectedSubscribesOnlyWhileStartedAndCapturesMutableInputBeforeMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, scheduler);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        await Assert.That(source.SubscribeCount).IsEqualTo(0);
        await wrapped.StartAsync(CancellationToken.None);
        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(1);

        var first = new MutableCounterInput(FirstValue);
        source.Publish(first);
        first.Replace(CorruptedValue);
        await wrapped.StopAsync(CancellationToken.None);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);

        source.Publish(new(CorruptedValue));
        await wrapped.StartAsync(CancellationToken.None);
        await Assert.That(source.SubscribeCount).IsEqualTo(TwoNotifications);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(1);

        source.CompleteActive();
        _ = await wrapped.PublishAsync(new(SecondValue), null, CancellationToken.None);
        await wrapped.DisposeAsync();
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).ToArray(), [FirstValue, SecondValue]);
        await Assert.That(source.DisposeCount).IsEqualTo(TwoNotifications);
    }

    /// <summary>Verifies a stop requested during a delayed start leaves no source subscription behind.</summary>
    /// <returns>A task that completes when the lifecycle settles.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopWaitsForDelayedStartThenUnsubscribes()
    {
        await using var store = await CreateInitializedStoreAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { StartEntered = entered, ReleaseStart = release };
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        var start = wrapped.StartAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var stop = wrapped.StopAsync(CancellationToken.None).AsTask();
        _ = release.TrySetResult();
        await Task.WhenAll(start, stop);

        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies failed source registration stops the started inner stream.</summary>
    /// <returns>A task that completes when failure cleanup finishes.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamSubscribeFailureStopsInnerStream()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ThrowingObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        await Assert.That(() => wrapped.StartAsync(CancellationToken.None).AsTask()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(coordinator.StartCalls).IsEqualTo(1);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies a source registration failure stays visible when stopping the inner stream also fails.</summary>
    /// <returns>A task that completes when both failures are observed.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamSubscribeAndStopFailuresPreserveBothErrors()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store) { ThrowOnStop = true };
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ThrowingObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        var exception = await Assert.That(() => wrapped.StartAsync(CancellationToken.None).AsTask())
            .ThrowsExactly<AggregateException>();
        await Assert.That(exception).IsNotNull();
        if (exception is { } observedException)
        {
            await Assert.That(observedException.InnerExceptions).Count().IsEqualTo(TwoNotifications);
            await Assert.That(observedException.InnerExceptions[0].Message).IsEqualTo("The source rejected subscription.");
        }

        coordinator.ThrowOnStop = false;
    }

    /// <summary>Verifies a nonblocking stop requested during source registration is applied after start.</summary>
    /// <returns>A task that completes after the reentrant stop settles.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamReentrantStopDuringSubscribeLeavesNoSubscription()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler());
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        Task? stop = null;
        source.OnSubscribe = () => stop = wrapped.StopAsync(CancellationToken.None).AsTask();

        await wrapped.StartAsync(CancellationToken.None);
        await Assert.That(stop).IsNotNull();
        if (stop is { } pendingStop)
        {
            await pendingStop;
        }

        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies remote observer adapters expose independent synchronous producer lifetimes and owned capture.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task ToRemoteObserverAsObserverCreatesIndependentProducersAndCapturesMutableInputBeforeMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, scheduler);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var sink = new RecordingObserver<MutableCounterInput>();
        await using var adapter = sink.ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = Stream });
        var firstProducer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, definition.Input);
        var secondProducer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, definition.Input);

        var first = new MutableCounterInput(FirstValue);
        firstProducer.OnNext(first);
        first.Replace(CorruptedValue);
        firstProducer.OnCompleted();
        firstProducer.OnNext(new(CorruptedValue));
        secondProducer.OnNext(new(SecondValue));

        await adapter.DisposeAsync();
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).Order().ToArray(), [FirstValue, SecondValue]);
    }

    /// <summary>Verifies the concrete remote observer adapter owns only its remote-sink subscription.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task ToRemoteObserverDisposesRemoteSinkSubscriptionWithoutDisposingStream()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var definition = CreateMutableInputDefinition() with
        {
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, scheduler);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        await stream.StartAsync(CancellationToken.None);
        var raw = new RecordingObserver<MutableCounterInput>();
        await using var adapter = raw.ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = Stream });

        _ = await stream.ApplyRemoteBatchAsync(CreateRemoteBatch(null, FirstCursor, ThirdValue), CancellationToken.None);
        scheduler.RunAll();
        await AssertSequenceAsync(raw.Values.Select(static input => input.Delta).ToArray(), [ThirdValue]);

        await adapter.DisposeAsync();
        _ = await stream.ApplyRemoteBatchAsync(CreateRemoteBatch(FirstCursor, "cursor-2", CorruptedValue), CancellationToken.None);
        scheduler.RunAll();
        await AssertSequenceAsync(raw.Values.Select(static input => input.Delta).ToArray(), [ThirdValue]);

        var direct = new RecordingObserver<RemoteMessage<MutableCounterInput>>();
        using var directSubscription = stream.Remote.Subscribe(direct);
        _ = await stream.ApplyRemoteBatchAsync(CreateRemoteBatch("cursor-2", "cursor-3", FirstValue), CancellationToken.None);
        scheduler.RunAll();

        await AssertSequenceAsync(direct.Values.Select(static message => message.Value.Delta).ToArray(), [FirstValue]);
    }

    /// <summary>Verifies concurrent disposal shares one completion and drains producers after remote unsubscription fails.</summary>
    /// <returns>A task that completes when all owned resources have settled.</returns>
    [Test]
    public async Task RemoteObserverAdapterDisposeAsyncSharesCompletionAndDrainsProducerAfterSubscriptionFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var inputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes };
        var inputCapture = new MutableCounterInputCapture();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = inputOptions,
            InputCapture = inputCapture,
        };
        await using var stream = CreateMutableInputStream(store, definition, scheduler);
        var remote = new BlockingDisposeObservable<RemoteMessage<MutableCounterInput>>();
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = inputOptions,
            Capture = inputCapture,
            Publisher = (IOccasionallyConnectedSerializedInputPublisher)stream,
            PublishAsync = stream.PublishAsync,
            CreateProducer = static options => new OccasionallyConnectedInputProducer<MutableCounterInput>(options),
        });
        var producer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, definition.Input);
        producer.OnNext(new(FirstValue));

        var firstDispose = Task.Run(async () => await adapter.DisposeAsync());
        await remote.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var secondDispose = adapter.DisposeAsync().AsTask();
        await Assert.That(secondDispose.IsCompleted).IsFalse();
        remote.ReleaseDispose();

        await Assert.That(() => firstDispose).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => secondDispose).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).ToArray(), [FirstValue]);
    }

    /// <summary>Verifies a failed completed-producer diagnostic cannot fault detached owned cleanup.</summary>
    /// <returns>A task that completes after both cleanup paths settle.</returns>
    [Test]
    public async Task RemoteObserverAdapterCompletedProducerReporterFailureSettlesDisposal()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var publisher = new ThrowingFaultPublisher();
        var producer = new FailingDisposeProducer<MutableCounterInput>();
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = publisher,
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException("No typed publication is expected.")),
            CreateProducer = _ => producer,
        });

        var observer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);
        observer.OnCompleted();
        await adapter.DisposeAsync();

        await Assert.That(producer.DisposeCalls).IsEqualTo(1);
        await Assert.That(publisher.FaultCalls).IsEqualTo(1);
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies synchronized filtering uses paired state and queue snapshots rather than independently delayed sources.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task WhereSynchronizedUsesPairedSnapshotAndDoesNotCombineDelayedLocalWithStalePendingZero()
    {
        await using var stream = new PairedSnapshotStream();
        var synchronized = new RecordingObserver<CounterState>();
        using var subscription = stream.WhereSynchronized().Subscribe(synchronized);

        stream.PublishLocal(new(SecondValue));
        stream.PublishSyncState(new(SyncLifecycleStatus.Online, true, 0, 0, Now, Now, null, "stale-zero"));
        await Assert.That(synchronized.Values).IsEmpty();

        stream.PublishCommittedQueueSnapshot(new(FirstValue), new(0, 0, null));
        stream.PublishCommittedQueueSnapshot(new(SecondValue), new(1, NotificationCapacityBytes, Now));
        await AssertSequenceAsync(synchronized.Values.Select(static state => state.Sum).ToArray(), [FirstValue]);

        stream.PublishCommittedQueueSnapshot(new(SecondValue), new(0, 0, null));
        await AssertSequenceAsync(synchronized.Values.Select(static state => state.Sum).ToArray(), [FirstValue, SecondValue]);
    }

    /// <summary>Verifies pending observation surfaces paired per-stream queue counts and bytes.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task ObservePendingUsesPairedPerStreamPendingCountsAndAllowsUnknownOldestOperation()
    {
        await using var stream = new PairedSnapshotStream();
        var pending = new RecordingObserver<PendingSyncSummary>();
        using var subscription = stream.ObservePending().Subscribe(pending);
        var oldest = Now.AddMinutes(-1);

        stream.PublishCommittedQueueSnapshot(new(FirstValue), new(TwoNotifications, FirstPendingBytes, oldest));
        stream.PublishCommittedQueueSnapshot(new(SecondValue), new(1, SecondPendingBytes, null));
        stream.PublishCommittedQueueSnapshot(new(ThirdValue), new(0, 0, null));

        await AssertSequenceAsync(pending.Values.Select(static summary => summary.OperationCount).ToArray(), [TwoNotifications, 1, 0]);
        await AssertSequenceAsync(pending.Values.Select(static summary => summary.Bytes).ToArray(), [FirstPendingBytes, SecondPendingBytes, 0L]);
        await Assert.That(pending.Values[0].OldestOperationUtc).IsEqualTo(oldest);
        await Assert.That(pending.Values[1].OldestOperationUtc).IsNull();
        await Assert.That(pending.Values[2].OldestOperationUtc).IsNull();
    }

    /// <summary>Verifies the real stream facet publishes paired snapshots when local mutations commit.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task RealStreamPairedSnapshotFacetPublishesLocalMutationPendingCountsAtCommitBoundary()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var snapshots = new RecordingObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>>();
        using var subscription = ((IOccasionallyConnectedCommittedStateQueueSnapshots<CounterState>)stream).CommittedStateQueueSnapshots.Subscribe(snapshots);
        await stream.StartAsync(CancellationToken.None);

        _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(snapshots.Values).Count().IsEqualTo(TwoNotifications);
        await Assert.That(snapshots.Values[0].State.Sum).IsEqualTo(0);
        await Assert.That(snapshots.Values[0].Pending.OperationCount).IsEqualTo(0);
        await Assert.That(snapshots.Values[0].Pending.OldestOperationUtc).IsNull();
        await Assert.That(snapshots.Values[1].State.Sum).IsEqualTo(FirstValue);
        await Assert.That(snapshots.Values[1].Pending.OperationCount).IsEqualTo(1);
        await Assert.That(snapshots.Values[1].Pending.Bytes).IsGreaterThan(0);
        await Assert.That(snapshots.Values[1].Pending.OldestOperationUtc).IsNull();
    }

    /// <summary>Verifies synchronized observers receive separate mutable state instances.</summary>
    /// <returns>A task that completes after state and store checks.</returns>
    [Test]
    public async Task WhereSynchronizedIsolatesMutableStateAcrossObserversAndCommittedStore()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateMutableStream(store, scheduler);
        using var mutating = stream.WhereSynchronized().Subscribe(
            new ActionObserver<MutableCounterState>(static state => state.Replace(CorruptedValue)));
        var observing = new RecordingObserver<MutableCounterState>();
        using var observation = stream.WhereSynchronized().Subscribe(observing);

        await stream.StartAsync(CancellationToken.None);
        scheduler.RunAll();
        await AssertSequenceAsync(observing.Values.Select(static state => state.Sum).ToArray(), [0]);

        _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var local = new RecordingObserver<MutableCounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        scheduler.RunAll();
        await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue);
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(recovered.Snapshot).IsNotNull();
        if (recovered.Snapshot is { } snapshot)
        {
            await Assert.That(MutableCounterPayloadSerializer.CreateMutableCounterStateSnapshot(snapshot.State).Sum)
                .IsEqualTo(FirstValue);
        }
    }

    /// <summary>Verifies late subscribers replay the latest committed state and queue pair.</summary>
    /// <returns>A task that completes after late replay checks.</returns>
    [Test]
    public async Task ObservePendingAndWhereSynchronizedReplayLatestPairedSnapshotToLateSubscribers()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        scheduler.RunAll();

        var synchronized = new RecordingObserver<CounterState>();
        using var synchronizedSubscription = stream.WhereSynchronized().Subscribe(synchronized);
        var pending = new RecordingObserver<PendingSyncSummary>();
        using var pendingSubscription = stream.ObservePending().Subscribe(pending);
        scheduler.RunAll();
        await AssertSequenceAsync(synchronized.Values.Select(static state => state.Sum).ToArray(), [0]);
        await AssertSequenceAsync(pending.Values.Select(static summary => summary.OperationCount).ToArray(), [0]);

        _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var latePending = new RecordingObserver<PendingSyncSummary>();
        using var latePendingSubscription = stream.ObservePending().Subscribe(latePending);
        var lateSynchronized = new RecordingObserver<CounterState>();
        using var lateSynchronizedSubscription = stream.WhereSynchronized().Subscribe(lateSynchronized);
        scheduler.RunAll();
        await AssertSequenceAsync(latePending.Values.Select(static summary => summary.OperationCount).ToArray(), [1]);
        await Assert.That(lateSynchronized.Values).IsEmpty();
    }

    /// <summary>Verifies remote receive inclusion does not clear pending work before terminal upload acknowledgement.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task RealStreamPairedSnapshotFacetKeepsPendingUntilTerminalUploadAcknowledgement()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var snapshots = new RecordingObserver<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>>();
        using var subscription = ((IOccasionallyConnectedCommittedStateQueueSnapshots<CounterState>)stream).CommittedStateQueueSnapshots.Subscribe(snapshots);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();

        _ = await stream.ApplyRemoteBatchAsync(CreateCompletedRemoteBatch(null, FirstCursor, FirstValue, receipt.OperationId), CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(snapshots.Values).Count().IsEqualTo(ThreeNotifications);
        await Assert.That(snapshots.Values[0].State.Sum).IsEqualTo(0);
        await Assert.That(snapshots.Values[0].Pending.OperationCount).IsEqualTo(0);
        await Assert.That(snapshots.Values[1].State.Sum).IsEqualTo(FirstValue);
        await Assert.That(snapshots.Values[1].Pending.OperationCount).IsEqualTo(1);
        await Assert.That(snapshots.Values[2].State.Sum).IsEqualTo(FirstValue);
        await Assert.That(snapshots.Values[2].Pending.OperationCount).IsEqualTo(1);
        await Assert.That(snapshots.Values[2].Pending.Bytes).IsGreaterThan(0);
        await Assert.That(snapshots.Values[2].Pending.OldestOperationUtc).IsNull();
    }

    /// <summary>Verifies convenience helpers reject streams that do not expose the library internal support facets.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task ConvenienceHelpersRejectUnsupportedExternalStreamsWithExplicitErrors()
    {
        await using var stream = new UnsupportedExternalStream<CounterState, CounterInput>();
        await using var context = new ForwardingContext<CounterState, CounterInput>(stream);
        var sink = new RecordingObserver<CounterInput>();

        await Assert.That(() => stream.WhereSynchronized()).ThrowsExactly<NotSupportedException>();
        await Assert.That(() => stream.ObservePending()).ThrowsExactly<NotSupportedException>();
        var definition = CreateDefinition() with
        {
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new CounterInputCapture(),
        };
        await Assert.That(() => sink.ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = Stream })).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Creates a mutable-input stream using the real local stream implementation.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="scheduler">The notification scheduler.</param>
    /// <param name="coordinator">The optional controllable coordinator.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<CounterState, MutableCounterInput> CreateMutableInputStream(
        ILocalStoreAdapter store,
        StreamDefinition<CounterState, MutableCounterInput> definition,
        IObserverNotificationScheduler scheduler,
        RecordingCoordinator? coordinator = null)
    {
        var payloadSerializer = new MutableInputPayloadSerializer();
        return new(
            new OccasionallyConnectedStreamOptions<CounterState, MutableCounterInput>
            {
                Definition = definition,
                Store = store,
                Serializer = payloadSerializer,
                TimeProvider = new FixedTimeProvider(Now),
                OperationIdSource = new SequenceOperationIdSource(),
                Coordinator = coordinator ?? new RecordingCoordinator(store),
                InputProducer = new RecordingInputProducer<MutableCounterInput>(),
                LocalStateSnapshotFactory = static (payload, _) => new(MutableInputPayloadSerializer.CreateCounterStateSnapshot(payload)),
                RemoteInputSnapshotFactory = static (payload, _) => new(MutableInputPayloadSerializer.CreateMutableInputSnapshot(payload)),
                NotificationScheduler = scheduler,
                NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
                WorkCapacity = WorkCapacity,
                LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
                ClientId = ClientId,
            });
    }

    /// <summary>Creates a single-event remote batch for the counter stream.</summary>
    /// <param name="previousCursor">The previous server cursor.</param>
    /// <param name="nextCursor">The next server cursor.</param>
    /// <param name="value">The remote input value.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(string? previousCursor, string nextCursor, int value) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, [CreateRemoteEvent(value, nextCursor)]);

    /// <summary>Creates a remote batch that proves a pending local operation is complete at the next cursor.</summary>
    /// <param name="previousCursor">The previous server cursor.</param>
    /// <param name="nextCursor">The next server cursor.</param>
    /// <param name="value">The remote input value.</param>
    /// <param name="operationId">The completed local operation.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateCompletedRemoteBatch(string? previousCursor, string nextCursor, int value, OperationId operationId)
    {
        var remoteEvent = CreateRemoteEvent(value, nextCursor, operationId) with
        {
            Origin = new(ClientId, operationId),
        };
        return new(Guid.NewGuid(), Stream, previousCursor, nextCursor, [remoteEvent])
        { CompletedOperations = [new(new(ClientId, operationId), [remoteEvent.EventId])] };
    }

    /// <summary>Creates a remote event caused by a local operation.</summary>
    /// <param name="value">The remote input value.</param>
    /// <param name="cursor">The server cursor.</param>
    /// <param name="operationId">The causing operation identifier.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(int value, string cursor, OperationId operationId) =>
        new(Guid.NewGuid(), Stream, cursor, Now, operationId, CreatePayload(value), new Dictionary<string, string>());

    /// <summary>Captures mutable counter input into owned serialized payload envelopes.</summary>
    private sealed class MutableCounterInputCapture : IOccasionallyConnectedInputCapture<MutableCounterInput>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRetainedByteCount(MutableCounterInput value) => NotificationCapacityBytes;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadEnvelope Capture(MutableCounterInput value) => CreatePayload(value.Delta);
    }

    /// <summary>Forwards context stream lookups to one test stream.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="stream">The forwarded stream.</param>
    private sealed class ForwardingContext<TState, TInput>(IOccasionallyConnectedStream<TState, TInput> stream) : IOccasionallyConnectedContext
    {
        /// <summary>The sync engine used by this fixture.</summary>
        private readonly TestSyncEngine _syncEngine = new();

        /// <inheritdoc />
        public ISyncEngine SyncEngine => _syncEngine;

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates => _syncEngine.SyncStates;

        /// <inheritdoc />
        public IOccasionallyConnectedStream<TRequestedState, TRequestedInput> GetOrCreateStream<TRequestedState, TRequestedInput>(
            StreamDefinition<TRequestedState, TRequestedInput> definition)
        {
            ArgumentNullException.ThrowIfNull(definition);
            if (stream is IOccasionallyConnectedStream<TRequestedState, TRequestedInput> requested && definition.StreamId == stream.StreamId)
            {
                return requested;
            }

            throw new InvalidOperationException("The requested stream definition does not match the forwarded test stream.");
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return stream.StartAsync(cancellationToken);
        }

        /// <inheritdoc />
        public ValueTask StopAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return stream.StopAsync(cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => _syncEngine.DisposeAsync();
    }

    /// <summary>Minimal sync engine for forwarding context tests.</summary>
    private sealed class TestSyncEngine : ISyncEngine
    {
        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new ManualObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new ManualObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new ManualObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException("The forwarding test context does not enqueue operations through the context engine.");
        }

        /// <inheritdoc />
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult<SyncOperationStatus?>(null);
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask TriggerSyncAsync(CancellationToken cancellationToken) => StopAsync(cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Producer fixture whose owned cleanup fails synchronously.</summary>
    /// <typeparam name="T">The input type.</typeparam>
    private sealed class FailingDisposeProducer<T> : IOccasionallyConnectedInputProducer<T>
    {
        /// <summary>Gets the number of cleanup attempts.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc />
        public IObserver<T> Observer { get; } = new RecordingObserver<T>();

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.FromException(new InvalidOperationException("Producer cleanup failed."));
        }
    }

    /// <summary>Publisher fixture whose diagnostic callback fails.</summary>
    private sealed class ThrowingFaultPublisher : IOccasionallyConnectedSerializedInputPublisher
    {
        /// <summary>Gets the number of diagnostic attempts.</summary>
        public int FaultCalls { get; private set; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishSerializedInputAsync(PayloadEnvelope payload, RemotePublishOptions? options, CancellationToken cancellationToken) =>
            ValueTask.FromException<PublishReceipt>(new NotSupportedException("No serialized publication is expected."));

        /// <inheritdoc />
        public void PublishInputFault(string code, string message, OperationId? operationId, Exception exception)
        {
            FaultCalls++;
            FailFaultPublication();
        }

        /// <summary>Simulates a failed diagnostic sink.</summary>
        /// <exception cref="InvalidOperationException">The diagnostic callback failed.</exception>
        private static void FailFaultPublication() => throw new InvalidOperationException("Diagnostic callback failed.");
    }

    /// <summary>Observable whose subscription blocks during disposal and then fails.</summary>
    /// <typeparam name="T">The notification type.</typeparam>
    private sealed class BlockingDisposeObservable<T> : IObservable<T>
    {
        /// <summary>The signal that releases subscription disposal.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The active subscriber, when registration has succeeded.</summary>
        private IObserver<T>? _observer;

        /// <summary>Gets the signal sent when subscription disposal starts.</summary>
        public TaskCompletionSource DisposeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            _observer = observer;
            return new BlockingSubscription(this);
        }

        /// <summary>Publishes one input to the registered source producer.</summary>
        /// <param name="value">The source input.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Publish(T value) => _observer?.OnNext(value);

        /// <summary>Lets a blocked disposal finish.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseDispose() => _ = _release.TrySetResult();

        /// <summary>Subscription with externally controlled disposal.</summary>
        /// <param name="owner">The owning observable.</param>
        private sealed class BlockingSubscription(BlockingDisposeObservable<T> owner) : IDisposable
        {
            /// <inheritdoc />
            public void Dispose()
            {
                _ = owner.DisposeEntered.TrySetResult();
                if (!owner._release.Task.Wait(TimeSpan.FromSeconds(DisposalTimeoutSeconds)))
                {
                    FailDisposalTimeout();
                }

                FailDisposal();
            }

            /// <summary>Fails when the test gate was not released.</summary>
            /// <exception cref="TimeoutException">The gate did not release.</exception>
            private static void FailDisposalTimeout() => throw new TimeoutException("The test did not release subscription disposal.");

            /// <summary>Simulates remote subscription cleanup failure.</summary>
            /// <exception cref="InvalidOperationException">The simulated cleanup failed.</exception>
            private static void FailDisposal() => throw new InvalidOperationException("Subscription disposal failed after release.");
        }
    }

    /// <summary>Observable whose subscription fails after the inner stream starts.</summary>
    /// <typeparam name="T">The unused notification type.</typeparam>
    private sealed class ThrowingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the subscription attempt count.</summary>
        public int SubscribeCount { get; private set; }

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            SubscribeCount++;
            throw new InvalidOperationException("The source rejected subscription.");
        }
    }

    /// <summary>Manual observable used by convenience extension tests.</summary>
    /// <typeparam name="T">The notification type.</typeparam>
    private sealed class ManualObservable<T> : IObservable<T>
    {
        /// <summary>The subscriptions used by this fixture.</summary>
        private readonly List<Subscription> _subscriptions = [];

        /// <summary>Gets the number of subscriptions created.</summary>
        public int SubscribeCount { get; private set; }

        /// <summary>Gets the number of active subscriptions.</summary>
        public int ActiveSubscriptions { get; private set; }

        /// <summary>Gets the number of disposed subscriptions.</summary>
        public int DisposeCount { get; private set; }

        /// <summary>Gets or sets a callback invoked during subscription.</summary>
        public Action? OnSubscribe { get; set; }

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            SubscribeCount++;
            ActiveSubscriptions++;
            var subscription = new Subscription(this, observer);
            _subscriptions.Add(subscription);
            OnSubscribe?.Invoke();
            return subscription;
        }

        /// <summary>Publishes a value to active subscribers.</summary>
        /// <param name="value">The value.</param>
        public void Publish(T value)
        {
            foreach (var subscription in _subscriptions.ToArray())
            {
                subscription.Publish(value);
            }
        }

        /// <summary>Completes active subscribers.</summary>
        public void CompleteActive()
        {
            foreach (var subscription in _subscriptions.ToArray())
            {
                subscription.Complete();
            }
        }

        /// <summary>Reports an error to active subscribers.</summary>
        /// <param name="error">The source error.</param>
        public void ErrorActive(Exception error)
        {
            foreach (var subscription in _subscriptions.ToArray())
            {
                subscription.Error(error);
            }
        }

        /// <summary>Removes one disposed subscription.</summary>
        /// <param name="subscription">The disposed subscription.</param>
        private void Dispose(Subscription subscription)
        {
            if (!_subscriptions.Remove(subscription))
            {
                return;
            }

            DisposeCount++;
            ActiveSubscriptions--;
        }

        /// <summary>Represents one manual observable subscription.</summary>
        /// <param name="owner">The owner.</param>
        /// <param name="observer">The observer.</param>
        private sealed class Subscription(ManualObservable<T> owner, IObserver<T> observer) : IDisposable
        {
            /// <summary>The disposed used by this fixture.</summary>
            private int _disposed;

            /// <summary>Publishes a value if the subscription is active.</summary>
            /// <param name="value">The value.</param>
            public void Publish(T value)
            {
                if (Volatile.Read(ref _disposed) == 0)
                {
                    observer.OnNext(value);
                }
            }

            /// <summary>Completes the subscription if active.</summary>
            public void Complete()
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    return;
                }

                observer.OnCompleted();
            }

            /// <summary>Reports an error if the subscription is active.</summary>
            /// <param name="error">The source error.</param>
            public void Error(Exception error)
            {
                if (Volatile.Read(ref _disposed) == 0)
                {
                    observer.OnError(error);
                }
            }

            /// <inheritdoc />
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    return;
                }

                owner.Dispose(this);
            }
        }
    }

    /// <summary>Stream fixture that exposes the proposed paired-state internal facet for helper tests.</summary>
    private sealed class PairedSnapshotStream : IOccasionallyConnectedStream<CounterState, CounterInput>, IOccasionallyConnectedCommittedStateQueueSnapshots<CounterState>
    {
        /// <summary>The local used by this fixture.</summary>
        private readonly ManualObservable<CounterState> _local = new();

        /// <summary>The sync states used by this fixture.</summary>
        private readonly ManualObservable<SyncState> _syncStates = new();

        /// <summary>The snapshots used by this fixture.</summary>
        private readonly ManualObservable<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>> _snapshots = new();

        /// <inheritdoc />
        public StreamId StreamId => Stream;

        /// <inheritdoc />
        public SubscriptionId SubscriptionId => ExplicitSubscription;

        /// <inheritdoc />
        public IObservable<CounterState> Local => _local;

        /// <inheritdoc />
        public IObservable<RemoteMessage<CounterInput>> Remote { get; } = new ManualObservable<RemoteMessage<CounterInput>>();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates => _syncStates;

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new ManualObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new ManualObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public IObserver<CounterInput> Input => throw new NotSupportedException("The paired snapshot helper fixture does not accept input.");

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<CounterState>> CommittedStateQueueSnapshots => _snapshots;

        /// <summary>Publishes an independently delayed local state notification.</summary>
        /// <param name="state">The local state.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PublishLocal(CounterState state) => _local.Publish(state);

        /// <summary>Publishes an independently delayed sync state notification.</summary>
        /// <param name="state">The sync state.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PublishSyncState(SyncState state) => _syncStates.Publish(state);

        /// <summary>Publishes a paired committed state and queue snapshot captured at one mutation boundary.</summary>
        /// <param name="state">The committed state.</param>
        /// <param name="pending">The paired pending summary.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PublishCommittedQueueSnapshot(CounterState state, PendingSyncSummary pending) =>
            _snapshots.Publish(new(state, pending));

        /// <inheritdoc />
        public ValueTask<PublishReceipt> PublishAsync(CounterInput value, RemotePublishOptions? options, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException("The paired snapshot helper fixture does not publish inputs.");
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>External stream fixture deliberately missing library support facets.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    private sealed class UnsupportedExternalStream<TState, TInput> : IOccasionallyConnectedStream<TState, TInput>
    {
        /// <inheritdoc />
        public StreamId StreamId => Stream;

        /// <inheritdoc />
        public SubscriptionId SubscriptionId => ExplicitSubscription;

        /// <inheritdoc />
        public IObservable<TState> Local { get; } = new ManualObservable<TState>();

        /// <inheritdoc />
        public IObservable<RemoteMessage<TInput>> Remote { get; } = new ManualObservable<RemoteMessage<TInput>>();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new ManualObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new ManualObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new ManualObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public IObserver<TInput> Input => throw new NotSupportedException("The unsupported external stream does not accept observer input.");

        /// <inheritdoc />
        public ValueTask<PublishReceipt> PublishAsync(TInput value, RemotePublishOptions? options, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new NotSupportedException("The unsupported external stream does not publish inputs.");
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
