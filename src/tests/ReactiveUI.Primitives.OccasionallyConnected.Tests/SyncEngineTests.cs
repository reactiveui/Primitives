// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The first client sequence.</summary>
    private const long FirstSequence = 1;

    /// <summary>The small queue byte budget used by capacity tests.</summary>
    private const long TestQueueBytes = 4;

    /// <summary>The prepared upload byte budget that fits real operation metadata.</summary>
    private const long PreparedUploadBytes = 512;

    /// <summary>The bounded raw outbox byte budget used by admission tests.</summary>
    private const long BlockedAdmissionBytes = 4096;

    /// <summary>The expected commit attempts after one retry.</summary>
    private const int ExpectedCapacityCommitAttempts = 2;

    /// <summary>The expected sync states after start and stop.</summary>
    private const int ExpectedStartedAndStoppedStates = 2;

    /// <summary>The expected transport connections after restart.</summary>
    private const int ExpectedRestartConnectCalls = 2;

    /// <summary>The oversized encoded payload size used by upload tests.</summary>
    private const long OversizedPreparedBytes = PreparedUploadBytes + 1;

    /// <summary>The expected count for two-operation test batches.</summary>
    private const int ExpectedTwoOperations = 2;

    /// <summary>The expected count for one-operation test batches.</summary>
    private const int ExpectedSingleOperation = 1;

    /// <summary>The polling interval used by bounded asynchronous test waits.</summary>
    private const int PollMilliseconds = 10;

    /// <summary>The timeout used to catch blocked concurrency regressions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The tested stream identity.</summary>
    private static readonly StreamId Stream = new("sync/engine");

    /// <summary>The tested operation identity.</summary>
    private static readonly OperationId Operation = OperationId.New();

    /// <summary>The tested subscription identity.</summary>
    private static readonly SubscriptionId Subscription = new(Guid.Parse("f5b5acde-c57e-4f2f-b82b-9ee9e6e9f5f3"));

    /// <summary>The test payload bytes.</summary>
    private static readonly byte[] TestPayload = [1];

    /// <summary>The empty test payload bytes.</summary>
    private static readonly byte[] EmptyPayload = [];

    /// <summary>Verifies Created engines accept offline raw commits without opening transport.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreatedEngineEnqueuesRawOperationAfterInitializingStoreWithoutConnectingTransport()
    {
        var store = new RecordingStore();
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(store, transport);
        var participant = new RecordingParticipant();
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(receipt.ClientSequence).IsEqualTo(operation.ClientSequence);
        await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
        await Assert.That(store.InitializeCalls).IsEqualTo(1);
        await Assert.That(transport.ConnectCalls).IsEqualTo(0);
    }

    /// <summary>Verifies engine observables are finite and publish lifecycle and operation statuses.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineObservablesPublishAndEnforceFiniteSubscriptionCap()
    {
        var store = new RecordingStore();
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(store, transport, maxDiagnosticSubscriptions: 1);
        var syncObserver = new RecordingObserver<SyncState>();
        var operationObserver = new RecordingObserver<SyncOperationStatus>();
        var faultObserver = new RecordingObserver<OccasionallyConnectedFault>();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var syncSubscription = engine.SyncStates.Subscribe(syncObserver);

        await Assert.That(() => engine.SyncStates.Subscribe(new RecordingObserver<SyncState>()))
            .ThrowsExactly<InvalidOperationException>();
        var operationSubscription = engine.OperationStates.Subscribe(operationObserver);
        var faultSubscription = engine.Faults.Subscribe(faultObserver);

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => syncObserver.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        var operation = CreateOperation();
        _ = await engine.EnqueueOperationAsync(operation, CancellationToken.None);
        await WaitForConditionAsync(() => syncObserver.Values.Exists(static state => state.PendingOperations == 1));
        syncSubscription.Dispose();
        syncSubscription.Dispose();
        operationSubscription.Dispose();
        faultSubscription.Dispose();

        await Assert.That(syncObserver.Values.Count).IsEqualTo(ExpectedStartedAndStoppedStates);
        await Assert.That(syncObserver.Values[0].Status).IsEqualTo(SyncLifecycleStatus.Online);
        await Assert.That(syncObserver.Values[1].PendingOperations).IsEqualTo(1);
        await Assert.That(operationObserver.Values.Count).IsEqualTo(1);
        await Assert.That(operationObserver.Values[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(faultObserver.Values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies raw enqueue captures release generation before a capacity failure and retries after notification.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EnqueueOperationRetriesCapacityFailureAfterReleaseNotification()
    {
        await using var engine = CreateEngine();
        var participant = new RecordingParticipant { CapacityFailures = 1 };
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation(payload: EmptyPayload);

        var receiptTask = engine.EnqueueOperationAsync(operation, CancellationToken.None).AsTask();
        await participant.CapacityFailureObserved.Task.WaitAsync(GuardTimeout);
        await Assert.That(receiptTask.IsCompleted).IsFalse();

        engine.NotifyCapacityReleased(Stream);
        var receipt = await receiptTask.WaitAsync(GuardTimeout);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(participant.CommitAttempts).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies Stop from Created closes new admission until the engine is started.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncFromCreatedRejectsNewAdmissionUntilStartAsyncRuns()
    {
        var store = new RecordingStore();
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var operation = CreateOperation();

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(async () => await engine.EnqueueOperationAsync(operation, CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await engine.StartAsync(CancellationToken.None);
        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(store.InitializeCalls).IsEqualTo(1);
        await Assert.That(transport.ConnectCalls).IsEqualTo(1);
    }

    /// <summary>Verifies stopping a running engine closes global admission and publishes stopped state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncAfterStartDisposesSessionRejectsAdmissionAndCanRestart()
    {
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(transport: transport);
        var observer = new RecordingObserver<SyncState>();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using var subscription = engine.SyncStates.Subscribe(observer);

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => observer.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        await engine.StopAsync(CancellationToken.None);
        await WaitForConditionAsync(() => observer.Values.Exists(static state => state.Status == SyncLifecycleStatus.Stopped));

        await Assert.That(transport.Session?.DisposeCalls).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(ExpectedStartedAndStoppedStates);
        await Assert.That(observer.Values[1].Status).IsEqualTo(SyncLifecycleStatus.Stopped);
        await Assert.That(async () => await engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await engine.StartAsync(CancellationToken.None);
        var receipt = await engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(Operation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
    }

    /// <summary>Verifies default engine dependency ownership disposes store and transport after the active session drains.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncDefaultsToOwningStoreAndTransportAfterSessionDrain()
    {
        var store = new RecordingStore();
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        await engine.StartAsync(CancellationToken.None);
        await engine.DisposeAsync();

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies borrowed engine dependencies remain alive after stop and dispose while the active session still drains.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncLeavesBorrowedStoreAndTransportAliveAfterSessionDrain()
    {
        var store = new RecordingStore();
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngineWithOwnership(store, transport, SyncEngineDependencyOwnership.Borrowed, SyncEngineDependencyOwnership.Borrowed);
        var commitEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var participant = new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit };
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(operation, CancellationToken.None).AsTask();
        await commitEntered.Task.WaitAsync(GuardTimeout);
        var dispose = engine.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        releaseCommit.SetResult();
        var receipt = await publish.WaitAsync(GuardTimeout);
        await dispose.WaitAsync(GuardTimeout);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(0);
        await Assert.That(store.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies mixed ownership can borrow the store while still owning transport cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncCanBorrowStoreAndOwnTransport()
    {
        var store = new RecordingStore();
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngineWithOwnership(store, transport, SyncEngineDependencyOwnership.Borrowed, SyncEngineDependencyOwnership.Owned);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        await engine.StartAsync(CancellationToken.None);
        await engine.DisposeAsync();

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies mixed ownership can own the store while borrowing transport cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncCanOwnStoreAndBorrowTransport()
    {
        var store = new RecordingStore();
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngineWithOwnership(store, transport, SyncEngineDependencyOwnership.Owned, SyncEngineDependencyOwnership.Borrowed);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        await engine.StartAsync(CancellationToken.None);
        await engine.DisposeAsync();

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(0);
        await Assert.That(store.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies concurrent starts share store initialization and transport connection while caller cancellation stays isolated.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentStartAsyncSharesInitializationAndIsolatesCallerCancellation()
    {
        TaskCompletionSource initializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInitialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new RecordingStore { InitializeEntered = initializeEntered, ReleaseInitialize = releaseInitialize };
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using CancellationTokenSource canceledWait = new();

        var first = engine.StartAsync(CancellationToken.None).AsTask();
        await initializeEntered.Task;
        var second = engine.StartAsync(canceledWait.Token).AsTask();
        await canceledWait.CancelAsync();
        var secondCompleted = await Task.WhenAny(second, Task.Delay(GuardTimeout)).ConfigureAwait(false);
        releaseInitialize.SetResult();

        await first.WaitAsync(GuardTimeout);
        await Assert.That(secondCompleted).IsSameReferenceAs(second);
        await Assert.That(second.IsCanceled).IsTrue();

        await Assert.That(store.InitializeCalls).IsEqualTo(1);
        await Assert.That(transport.ConnectCalls).IsEqualTo(1);
    }

    /// <summary>Verifies registration uses a finite stream cap and rejects duplicate participants.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegisterParticipantRejectsDuplicateAndOverCapacityStreams()
    {
        await using var duplicateEngine = CreateEngine(maxRegisteredStreams: 2);
        using var first = duplicateEngine.RegisterParticipant(new RecordingParticipant());

        await Assert.That(() => duplicateEngine.RegisterParticipant(new RecordingParticipant()))
            .ThrowsExactly<InvalidOperationException>();

        await using var cappedEngine = CreateEngine(maxRegisteredStreams: 1);
        using var capped = cappedEngine.RegisterParticipant(new RecordingParticipant());

        await Assert.That(() => cappedEngine.RegisterParticipant(new RecordingParticipant { StreamId = new("sync/other") }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies blocked capacity waiters are bounded by count and retained bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CapacityReleaseWaitersAreBoundedByCountAndRetainedBytes()
    {
        var options = OccasionallyConnectedOptions.Default with { Outbox = new() { MaximumBlockedPublishers = 1, MaxBytes = TestQueueBytes } };
        await using var engine = CreateEngine(options: options);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var generation = engine.GetCapacityReleaseGeneration(Stream);
        using CancellationTokenSource waiting = new();

        var wait = engine.WaitForCapacityReleaseAsync(Stream, generation, TestQueueBytes, waiting.Token).AsTask();

        await Assert.That(() => engine.WaitForCapacityReleaseAsync(Stream, generation, retainedBytes: 1, CancellationToken.None).AsTask())
            .ThrowsExactly<QueueCapacityExceededException>();
        engine.NotifyCapacityReleased(Stream);
        await wait;

        var nextGeneration = engine.GetCapacityReleaseGeneration(Stream);
        await Assert.That(() => engine.WaitForCapacityReleaseAsync(Stream, nextGeneration, retainedBytes: 5, CancellationToken.None).AsTask())
            .ThrowsExactly<QueueCapacityExceededException>();
    }

    /// <summary>Verifies capacity waits return immediately when a release happened before registration.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForCapacityReleaseReturnsImmediatelyWhenGenerationChanged()
    {
        await using var engine = CreateEngine();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var generation = engine.GetCapacityReleaseGeneration(Stream);

        engine.NotifyCapacityReleased(Stream);
        await engine.WaitForCapacityReleaseAsync(Stream, generation, TestQueueBytes, CancellationToken.None);

        await Assert.That(engine.GetCapacityReleaseGeneration(Stream)).IsEqualTo(generation + 1);
    }

    /// <summary>Verifies engine validation and store forwarding branches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineValidationRejectsUnknownStreamsInvalidBytesAndCanceledLifecycleRequests()
    {
        var store = new RecordingStore();
        await using var engine = CreateEngine(store);
        using CancellationTokenSource canceled = new();
        await canceled.CancelAsync();

        await Assert.That(async () => await engine.EnqueueOperationAsync(CreateOperation(streamId: new("sync/missing")), CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => engine.EnterLocalCommitAsync(Stream, 0, CancellationToken.None).AsTask())
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => engine.GetCapacityReleaseGeneration(Stream))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => engine.StartStreamAsync(Stream, canceled.Token).AsTask())
            .ThrowsExactly<OperationCanceledException>();
        await Assert.That(() => engine.StopStreamAsync(Stream, CancellationToken.None).AsTask())
            .ThrowsExactly<InvalidOperationException>();

        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var subscription = await engine.EnsureSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var status = await engine.GetOperationStatusAsync(Operation, CancellationToken.None);

        await Assert.That(subscription).IsEqualTo(Subscription);
        await Assert.That(store.LastPreferredSubscriptionId).IsEqualTo(Subscription);
        await Assert.That(status).IsNull();
    }

    /// <summary>Verifies trigger sync honors cancellation and disposal while remaining callable on a live engine.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAsyncCompletesForLiveEngineAndRejectsCancellationAndDisposal()
    {
        await using var engine = CreateEngine();
        using CancellationTokenSource canceled = new();
        await canceled.CancelAsync();

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);
        await Assert.That(() => engine.TriggerSyncAsync(canceled.Token).AsTask())
            .ThrowsExactly<OperationCanceledException>();

        await engine.DisposeAsync();

        await Assert.That(() => engine.TriggerSyncAsync(CancellationToken.None).AsTask())
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies stream lifecycle requests are independent from the engine global lifecycle.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StreamLifecycleRequestsValidateRegistrationWithoutStoppingEngine()
    {
        await using var engine = CreateEngine();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await engine.StartAsync(CancellationToken.None);

        await engine.StopStreamAsync(Stream, CancellationToken.None);
        var receipt = await engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None);
        await engine.StartStreamAsync(Stream, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(Operation);
    }

    /// <summary>Verifies capacity waiters leave bounded state when canceled or participant registration is removed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CapacityWaitersCancelAndRegistrationRemovalFaultsPendingWaiters()
    {
        await using var engine = CreateEngine();
        var registration = engine.RegisterParticipant(new RecordingParticipant());
        var generation = engine.GetCapacityReleaseGeneration(Stream);
        using CancellationTokenSource canceled = new();

        var canceledWait = engine.WaitForCapacityReleaseAsync(Stream, generation, TestQueueBytes, canceled.Token).AsTask();
        await canceled.CancelAsync();
        await Assert.That(async () => await canceledWait.WaitAsync(GuardTimeout).ConfigureAwait(false))
            .ThrowsExactly<TaskCanceledException>();

        var nextGeneration = engine.GetCapacityReleaseGeneration(Stream);
        var removedWait = engine.WaitForCapacityReleaseAsync(Stream, nextGeneration, TestQueueBytes, CancellationToken.None).AsTask();
        registration.Dispose();
        registration.Dispose();

        await Assert.That(async () => await removedWait.WaitAsync(GuardTimeout).ConfigureAwait(false))
            .ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => engine.GetCapacityReleaseGeneration(Stream))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies disposal closes observer registries and handles an active transport session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAfterStartDisposesSessionAndRejectsNewObservers()
    {
        var transport = new RecordingTransport();
        var observer = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using var subscription = engine.SyncStates.Subscribe(observer);
        await engine.StartAsync(CancellationToken.None);

        await engine.DisposeAsync();

        await Assert.That(transport.Session?.DisposeCalls).IsEqualTo(1);
        await Assert.That(() => engine.SyncStates.Subscribe(new RecordingObserver<SyncState>()))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposal during startup disposes the newly connected session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringStartDisposesSessionCreatedAfterDisposalBegins()
    {
        TaskCompletionSource connectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCreatedSession = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource sessionCreated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport { ConnectEntered = connectEntered, ReleaseCreatedSession = releaseCreatedSession, SessionCreated = sessionCreated };

        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        var start = engine.StartAsync(CancellationToken.None).AsTask();
        await connectEntered.Task.WaitAsync(GuardTimeout);
        await sessionCreated.Task.WaitAsync(GuardTimeout);
        var dispose = engine.DisposeAsync().AsTask();
        releaseCreatedSession.SetResult();
        await dispose.WaitAsync(GuardTimeout);

        await Assert.That(async () => await start.WaitAsync(GuardTimeout).ConfigureAwait(false))
            .ThrowsExactly<ObjectDisposedException>();
        await Assert.That(transport.Session?.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies raw admission is bounded while shared store initialization is blocked and cancellation releases the reservation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BlockedInitializerAdmissionReservationIsBoundedAndCancellationReleasesIt()
    {
        TaskCompletionSource initializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInitialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new RecordingStore { InitializeEntered = initializeEntered, ReleaseInitialize = releaseInitialize };
        var options = OccasionallyConnectedOptions.Default with { Outbox = new() { MaximumBlockedPublishers = 1, MaxBytes = BlockedAdmissionBytes } };
        await using var engine = CreateEngine(store, options: options);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using CancellationTokenSource blocked = new();

        var first = engine.EnqueueOperationAsync(CreateOperation(payload: TestPayload), blocked.Token).AsTask();
        await initializeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(async () => await engine.EnqueueOperationAsync(CreateOperation(payload: TestPayload), CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<QueueCapacityExceededException>();

        await blocked.CancelAsync();
        await Assert.That(first.IsCanceled).IsTrue();

        var second = engine.EnqueueOperationAsync(CreateOperation(payload: TestPayload), CancellationToken.None).AsTask();
        await Assert.That(second.IsCompleted).IsFalse();
        releaseInitialize.SetResult();
        var receipt = await second.WaitAsync(GuardTimeout);

        await Assert.That(receipt.OperationId).IsEqualTo(Operation);
    }

    /// <summary>Verifies operation status queries before start use the shared store initialization path.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetOperationStatusBeforeStartInitializesStoreOnce()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        await using var engine = CreateEngine(store);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        var status = await engine.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(status?.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(store.InitializeCalls).IsEqualTo(1);
        await Assert.That(store.StatusQueryCalls).IsEqualTo(1);
    }

    /// <summary>Verifies store initialization is published before the store callback can reenter the engine.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoreInitializationRunsOutsideEngineGateAndAllowsReentrantAdapters()
    {
        SyncEngine? engine = null;
        var store = new RecordingStore { OnInitialize = () => _ = engine!.GetCapacityReleaseGeneration(Stream) };
        await using var created = CreateEngine(store);
        engine = created;
        using var registration = created.RegisterParticipant(new RecordingParticipant());

        var receipt = await created.EnqueueOperationAsync(CreateOperation(), CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(Operation);
        await Assert.That(store.InitializeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies disposal preserves the first synchronous failure while still attempting later cleanup stages.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncAttemptsAllCleanupStagesWhenDependenciesThrowSynchronously()
    {
        var store = new RecordingStore { ThrowOnDispose = true };
        var transport = new RecordingTransport { ThrowOnDispose = true };
        var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await engine.StartAsync(CancellationToken.None);

        await Assert.That(async () => await engine.DisposeAsync().ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(transport.Session?.DisposeCalls).IsEqualTo(1);
        await Assert.That(transport.DisposeCalls).IsEqualTo(1);
        await Assert.That(store.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies post-commit status query failures cannot suppress capacity wakeups.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPostCommitStatusFailureStillNotifiesCapacityReleased()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes);
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store, failStatusQueriesAfterReconcile: true));
        using var faults = engine.Faults.Subscribe(new RecordingObserver<OccasionallyConnectedFault>());
        await engine.StartAsync(CancellationToken.None);
        var generation = engine.GetCapacityReleaseGeneration(Stream);

        var wait = engine.WaitForCapacityReleaseAsync(Stream, generation, retainedBytes: 1, CancellationToken.None).AsTask();
        await engine.TriggerSyncAsync(CancellationToken.None);

        await wait.WaitAsync(GuardTimeout);
        await Assert.That(store.StatusQueryCalls).IsGreaterThan(1);
    }

    /// <summary>Verifies missing post-reconciliation status is treated as an invariant failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPostCommitMissingStatusPublishesInvariantFaultWithoutFakeSynchronizedStatus()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes);
        var transport = new RecordingTransport { SessionOverride = session };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operations = new RecordingObserver<SyncOperationStatus>();
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { OnApplySyncResult = (_, _) => store.Statuses.Clear() });
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operations);
        await engine.StartAsync(CancellationToken.None);
        var generation = engine.GetCapacityReleaseGeneration(Stream);

        var wait = engine.WaitForCapacityReleaseAsync(Stream, generation, retainedBytes: 1, CancellationToken.None).AsTask();
        await engine.TriggerSyncAsync(CancellationToken.None);

        await wait.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Count == 1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Engine.UploadPostCommit");
        await Assert.That(operations.Values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a dwell-limited head waits on virtual time instead of spinning and resetting readiness.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPumpWaitsForDwellDeadlineBeforePreparingPartialBatch()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var dwell = TimeSpan.FromSeconds(1);
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with { MaximumOperations = ExpectedTwoOperations, MaximumDwellTime = dwell },
        };
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
        };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => clock.TimerCount > 0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);

        clock.Advance(dwell);
        await WaitForConditionAsync(() => session.PrepareCalls == 1);
        await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies an explicit synchronization flushes a partial batch without changing dwell timestamps.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncFlushesPartialBatchWithStationaryMinValueClock()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.MinValue);
        var dwell = TimeSpan.FromMinutes(1);
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with { MaximumOperations = ExpectedTwoOperations, MaximumDwellTime = dwell },
        };
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
        };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.MinValue);
        await Assert.That(faults.Values).IsEmpty();
        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
    }

    /// <summary>Verifies an explicit synchronization flushes an existing dwell-delayed head.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncFlushesPendingDwellHeadWithStationaryClock()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var dwell = TimeSpan.FromMinutes(1);
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with { MaximumOperations = ExpectedTwoOperations, MaximumDwellTime = dwell },
        };
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
        };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(dwell));
        await Assert.That(session.PrepareCalls).IsEqualTo(0);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
    }

    /// <summary>Verifies an explicit synchronization made during an active upload flushes the next partial lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncDuringInflightAttemptFlushesNextPartialBatchAfterClockRollback()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var dwell = TimeSpan.FromMinutes(1);
        var firstOperation = CreateOperation();
        var secondOperation = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var thirdOperation = CreateOperation(sequence: ExpectedTwoOperations + 1, operationId: OperationId.New());
        var store = CreateUploadStore([firstOperation, secondOperation], timeProvider: clock);
        store.Leases.Enqueue(CreateLease([thirdOperation], clock));
        store.Statuses[thirdOperation.OperationId] = new(
            thirdOperation.OperationId,
            thirdOperation.StreamId,
            SyncOperationState.QueuedForUpload,
            Attempt: 0,
            DateTimeOffset.UnixEpoch,
            ReasonCode: null);
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with { MaximumOperations = ExpectedTwoOperations, MaximumDwellTime = dwell },
        };
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, firstOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(dwell));
        clock.Advance(dwell);
        await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);

        clock.SetUtcNow(DateTimeOffset.UnixEpoch);

        var trigger = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        session.ReleasePausedSendAttempt();
        await trigger.WaitAsync(GuardTimeout);

        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.PreparedBatches[0].Operations[0]).IsSameReferenceAs(firstOperation);
        await Assert.That(session.PreparedBatches[0].Operations[1]).IsSameReferenceAs(secondOperation);
        await Assert.That(session.PreparedBatches[1].Operations[0]).IsSameReferenceAs(thirdOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
    }

    /// <summary>Verifies stop removes pending dwell heads so restart can schedule the stream again.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task StopAsyncClearsPendingUploadHeadBeforeRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        var first = CreateBatchSession(ExpectedTwoOperations);
        var second = CreateBatchSession(ExpectedTwoOperations);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
        await engine.StopAsync(CancellationToken.None);

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
        clock.Advance(options.Batching.MaximumDwellTime);
        await WaitForConditionAsync(() => second.SentBatches.Count == ExpectedSingleOperation);

        await Assert.That(first.PrepareCalls).IsEqualTo(0);
        await Assert.That(second.PreparedBatches[0].Operations[0]).IsSameReferenceAs(operation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a commit that finishes after stop is uploaded after restart.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task StopAsyncDefersAdmittedCommitWakeUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var first = CreateBatchSession(ExpectedSingleOperation);
        var second = CreateBatchSession(ExpectedSingleOperation);
        var transport = new RecordingTransport();
        var commitEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(
            store,
            commitEntered: commitEntered,
            releaseCommit: releaseCommit));

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(operation, CancellationToken.None).AsTask();
        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);
            var stop = engine.StopAsync(CancellationToken.None).AsTask();
            _ = releaseCommit.TrySetResult();
            await stop.WaitAsync(GuardTimeout);
            _ = await publish.WaitAsync(GuardTimeout);

            await engine.StartAsync(CancellationToken.None);
            clock.Advance(options.Batching.MaximumDwellTime);
            await WaitForConditionAsync(() => second.SentBatches.Count == ExpectedSingleOperation);

            await Assert.That(first.PrepareCalls).IsEqualTo(0);
            await Assert.That(second.PreparedBatches[0].Operations[0]).IsSameReferenceAs(operation);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

            await engine.StopAsync(CancellationToken.None);
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
        }
    }

    /// <summary>Verifies an inflight upload stopped before send completion is resumed after restart.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task StopAsyncDefersInflightUploadWakeUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var first = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        var second = CreateBatchSession(ExpectedSingleOperation);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        await WaitForConditionAsync(() => HasUploadDwellProgress(clock, first, faults: null));
        AdvanceUploadDwellIfStillPending(clock, first, faults: null);
        await first.PausedSendEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await sync.WaitAsync(GuardTimeout);

            await engine.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => second.SentBatches.Count == ExpectedSingleOperation || HasUploadDwellProgress(clock, second, faults: null));
            AdvanceUploadDwellIfStillPending(clock, second, faults: null);
            await WaitForConditionAsync(() => second.SentBatches.Count == ExpectedSingleOperation);

            await Assert.That(first.SentBatches.Count).IsEqualTo(0);
            await Assert.That(second.PreparedBatches[0].Operations[0]).IsSameReferenceAs(operation);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

            await engine.StopAsync(CancellationToken.None);
        }
        finally
        {
            first.ReleasePausedSendAttempt();
        }
    }

    /// <summary>Verifies stale upload completion cannot remove a reregistered stream's new upload head.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadCompletionAfterReregisterPreservesNewPendingHead()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var firstOperation = CreateOperation(operationId: OperationId.New());
        var secondOperation = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStoreWithSingleOperationLeases([firstOperation, secondOperation], timeProvider: clock);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var first = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        var second = first;
        var transport = new RecordingTransport { SessionOverride = first };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var firstRegistration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, firstOperation);
        await WaitForConditionAsync(() => HasUploadDwellProgress(clock, first, faults: null));
        AdvanceUploadDwellIfStillPending(clock, first, faults: null);
        try
        {
            await first.PausedSendEntered.Task.WaitAsync(GuardTimeout);
            firstRegistration.Dispose();
            using var secondRegistration = engine.RegisterParticipant(CreateUploadParticipant(store));
            engine.NotifyLocalCommitReady(Stream, secondOperation);
            first.ReleasePausedSendAttempt();
            await WaitForConditionAsync(() => second.PreparedBatches.Count == ExpectedCapacityCommitAttempts || clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            if (second.PreparedBatches.Count != ExpectedCapacityCommitAttempts)
            {
                clock.Advance(options.Batching.MaximumDwellTime);
            }

            await WaitForConditionAsync(() => second.SentBatches.Count == ExpectedCapacityCommitAttempts);

            await Assert.That(first.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(first.SentBatches[0].Operations[0]).IsSameReferenceAs(firstOperation);
            await Assert.That(first.SentBatches[ExpectedSingleOperation].Operations[0]).IsSameReferenceAs(secondOperation);
            await Assert.That(second.PreparedBatches[ExpectedSingleOperation].Operations[0]).IsSameReferenceAs(secondOperation);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

            await engine.StopAsync(CancellationToken.None);
        }
        finally
        {
            first.ReleasePausedSendAttempt();
            firstRegistration.Dispose();
        }
    }

    /// <summary>Verifies explicit synchronization requests are rejected until global startup succeeds.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAsyncBeforeStartRejectsWithoutSchedulingWork()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = CreateBatchSession(ExpectedSingleOperation);
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await Assert.That(() => engine.TriggerSyncAsync(CancellationToken.None).AsTask())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.LeaseRequests.Count).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
    }
}
