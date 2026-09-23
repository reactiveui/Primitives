// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics integration tests for <see cref="SyncEngine"/>.</summary>
[NotInParallel]
public sealed partial class SyncEngineTests
{
    /// <summary>The stable diagnostics source name used by engine metrics and activities.</summary>
    private const string EngineDiagnosticsName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The elapsed milliseconds used by diagnostics duration tests.</summary>
    private const int DiagnosticsElapsedMilliseconds = 250;

    /// <summary>The diagnostics recovered snapshot revision.</summary>
    private const int DiagnosticsRecoveredRevision = 1;

    /// <summary>The diagnostics recovered counter value.</summary>
    private const int DiagnosticsRecoveredCounter = 7;

    /// <summary>The diagnostics local publish counter value.</summary>
    private const int DiagnosticsLocalCounter = 3;

    /// <summary>The diagnostics in-memory store byte capacity.</summary>
    private const int DiagnosticsStoreBytes = 4096;

    /// <summary>The synchronized operations metric name.</summary>
    private const string SynchronizedOperationsMetricName = "oc.operations.synchronized";

    /// <summary>The pending queue count metric name.</summary>
    private const string QueuePendingMetricName = "oc.queue.pending";

    /// <summary>The queue retained bytes metric name.</summary>
    private const string QueueBytesMetricName = "oc.queue.bytes";

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte2 = 2;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte3 = 3;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte4 = 4;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte5 = 5;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte6 = 6;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte7 = 7;

    /// <summary>The diagnostics second payload byte.</summary>
    private const byte DiagnosticsSecondPayloadByte8 = 8;

    /// <summary>The receive stream state contract used by diagnostics recovery fixtures.</summary>
    private const string ReceiveCounterStateContract = "counter-state";

    /// <summary>The receive stream content type used by diagnostics recovery fixtures.</summary>
    private const string ReceiveCounterContentType = "application/json";

    /// <summary>Verifies successful local commits emit real metrics and a store commit activity without tags.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecordLocalCommitOutcomeWithoutTags()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var elapsed = TimeSpan.FromMilliseconds(DiagnosticsElapsedMilliseconds);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        using var activities = CreateEngineActivityListener(out var activityCapture);
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(options: options, timeProvider: clock);
        var participant = new RecordingParticipant { OnCommit = () => clock.Advance(elapsed) };
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
        await Assert.That(metricCapture.HasMeasurement("oc.operations.published", ExpectedSingleOperation)).IsTrue();
        await Assert.That(metricCapture.HasMeasurement("oc.store.commit.duration", elapsed.TotalMilliseconds)).IsTrue();
        await Assert.That(metricCapture.AllMeasurementsUntagged).IsTrue();
        await Assert.That(activityCapture.HasActivity("oc.store.commit")).IsTrue();
        await Assert.That(activityCapture.AllActivitiesUntagged).IsTrue();
    }

    /// <summary>Verifies durable upload reconciliation emits synchronized and queue-release diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecordUploadReconciliationOutcomeWithoutTags()
    {
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        using var activities = CreateEngineActivityListener(out var activityCapture);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes);
        var transport = new RecordingTransport { SessionOverride = session };
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, transport, options);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);

        await Assert.That(metricCapture.HasMeasurement(SynchronizedOperationsMetricName, ExpectedSingleOperation)).IsTrue();
        await Assert.That(metricCapture.HasMeasurement("oc.sync.batch.size", ExpectedSingleOperation)).IsTrue();
        await Assert.That(metricCapture.HasNonNegativeMeasurement("oc.sync.duration")).IsTrue();
        await Assert.That(metricCapture.HasMeasurement("oc.connection.state_changes", ExpectedSingleOperation)).IsTrue();
        await Assert.That(metricCapture.AllMeasurementsUntagged).IsTrue();
        await Assert.That(activityCapture.HasActivity("oc.transport.connect")).IsTrue();
        await Assert.That(activityCapture.HasActivity("oc.sync.push")).IsTrue();
        await Assert.That(activityCapture.AllActivitiesUntagged).IsTrue();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies typed publishes record queue deltas through the real committer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecordTypedPublishQueueDeltaFromRealCommitter()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, options: options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var receipt = await stream.PublishAsync(
            new(DiagnosticsLocalCounter),
            CreateVolatilePublishOptions(),
            CancellationToken.None);

        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(metricCapture.HasMeasurement(QueuePendingMetricName, ExpectedSingleOperation))
            .IsTrue();
        await Assert.That(metricCapture.HasPositiveMeasurement(QueueBytesMetricName)).IsTrue();
    }

    /// <summary>Verifies direct enqueue through a typed stream records its authoritative queue snapshot once.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsDirectTypedEnqueueCountsSavedCommitOnce()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, options: options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var states = new RecordingObserver<SyncState>();
        using var subscription = engine.SyncStates.Subscribe(states);
        var operation = CreateOperation(payload: "3"u8.ToArray()) with
        {
            Policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile },
        };

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);
        await WaitForConditionAsync(() => states.Values.Exists(static state => state.PendingOperations == ExpectedSingleOperation));
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        var saved = recovered.PendingOperations.Single();
        var state = states.Values.Last(static item => item.PendingOperations == ExpectedSingleOperation);

        await Assert.That(receipt.OperationId).IsEqualTo(saved.OperationId);
        await Assert.That(state.PendingBytes).IsEqualTo(SyncEngine.GetOperationRetainedBytes(saved));
        await Assert.That(metricCapture.Sum("oc.operations.published")).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(state.PendingBytes);
    }

    /// <summary>Verifies reordered upload results release bytes by identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsReleaseQueueBytesByOperationIdForReorderedResults()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var first = CreateOperation(payload: [1], operationId: OperationId.New());
        var second = CreateLargeDiagnosticsOperation();
        var firstRetainedBytes = SyncEngine.GetOperationRetainedBytes(first);
        var expectedReleaseBytes = SyncEngine.GetOperationRetainedBytes(second);
        var recoveredPendingBytes = firstRetainedBytes + expectedReleaseBytes;
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var store = CreateUploadStore([first, second], timeProvider: clock);
        var session = CreateReorderedDiagnosticsSession(second, first);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        engine.RecordRecoveredQueueAggregate(
            Stream,
            new(ExpectedTwoOperations, recoveredPendingBytes, DiagnosticsRecoveredRevision));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, first);
        await TriggerAndDrainUploadWithTraceAsync(
            engine,
            clock,
            store,
            session,
            faults,
            operationStates);

        var faultTrace = string.Join(",", faults.Values.Select(static item => item.Code));
        var statusTrace = string.Join(
            ",",
            operationStates.Values.Select(static item => item.State));
        await Assert.That(faultTrace).IsEqualTo(string.Empty);
        await Assert.That(statusTrace).Contains(nameof(SyncOperationState.Synchronized));
        await Assert.That(SyncEngine.GetOperationRetainedBytes(second))
            .IsGreaterThan(SyncEngine.GetOperationRetainedBytes(first));
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.Statuses.ContainsKey(second.OperationId)).IsTrue();
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(firstRetainedBytes);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies accepted typed uploads are not re-added by the next publish.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsDoNotReAddAcceptedTypedPublishAfterSecondPublish()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        await engine.StartAsync(CancellationToken.None);

        _ = await PublishVolatileCounterAsync(stream);
        await TriggerAndDrainUploadWithTraceAsync(
            engine,
            clock,
            session,
            faults,
            operationStates);

        var faultTrace = string.Join(",", faults.Values.Select(static item => item.Code));
        var statusTrace = string.Join(
            ",",
            operationStates.Values.Select(static item => item.State));
        await Assert.That(faultTrace).IsEqualTo(string.Empty);
        await Assert.That(statusTrace).Contains(nameof(SyncOperationState.Synchronized));
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);

        _ = await PublishVolatileCounterAsync(stream);

        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a typed publish racing after durable reconciliation is not double-released by engine accounting.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsTypedPublishDuringReconciliationDoesNotDoubleReleaseQueueAggregate()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes) { PauseBeforeSendNumber = ExpectedCapacityCommitAttempts };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        var coordinator = new CapturingStreamCoordinator(engine);
        await using var stream = CreateUploadOnlyCounterStream(store, coordinator, new(), clock);
        var participant = new PausingApplySyncResultParticipant(coordinator.Participant);
        using var registration = engine.RegisterParticipant(participant);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        await engine.StartAsync(CancellationToken.None);

        _ = await PublishVolatileCounterAsync(stream);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        PublishReceipt secondReceipt;
        try
        {
            await AdvanceDwellIfUploadRaceIsWaitingAsync(
                sync,
                participant.ApplyCompleted.Task,
                clock,
                options.Batching.MaximumDwellTime);
            await participant.ApplyCompleted.Task.WaitAsync(GuardTimeout);
            secondReceipt = await PublishVolatileCounterAsync(stream);
            participant.ReleaseApply();
            await AdvanceDwellIfUploadRaceIsWaitingAsync(
                sync,
                session.PausedSendEntered.Task,
                clock,
                options.Batching.MaximumDwellTime);
            await WaitForConditionAsync(
                () => sync.IsCompleted || session.PausedSendEntered.Task.IsCompleted)
                .WaitAsync(GuardTimeout);
            await AssertTypedReconciliationRaceOutcomeAsync(
                sync,
                store,
                secondReceipt,
                metricCapture,
                faults);
        }
        finally
        {
            participant.ReleaseApply();
            session.ReleasePausedSendAttempt();
        }
    }

    /// <summary>Verifies typed dead-letter accounting is not double-released by a racing publish.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">The dead-letter phase was not reached before the guard.</exception>
    [Test]
    public async Task EngineDiagnosticsTypedPublishDuringDeadLetterDoesNotDoubleReleaseQueueAggregate()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var session = CreateOversizedDiagnosticsSession();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        var coordinator = new CapturingStreamCoordinator(engine);
        await using var stream = CreateUploadOnlyCounterStream(store, coordinator, new(), clock);
        var participant = new PausingDeadLetterParticipant(coordinator.Participant);
        using var registration = engine.RegisterParticipant(participant);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        await engine.StartAsync(CancellationToken.None);

        _ = await PublishVolatileCounterAsync(stream);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        PublishReceipt secondReceipt;
        try
        {
            try
            {
                await AdvanceDwellIfUploadRaceIsWaitingAsync(
                    sync,
                    participant.DeadLetterCompleted.Task,
                    clock,
                    options.Batching.MaximumDwellTime);
                await participant.DeadLetterCompleted.Task.WaitAsync(GuardTimeout);
            }
            catch (TimeoutException exception)
            {
                throw new TimeoutException(CreateUploadTrace(session, faults, operationStates), exception);
            }

            secondReceipt = await PublishVolatileCounterAsync(stream);
            participant.ReleaseDeadLetter();
            await AdvanceDwellIfUploadRaceIsWaitingAsync(
                sync,
                session.PausedSendEntered.Task,
                clock,
                options.Batching.MaximumDwellTime);
            await WaitForConditionAsync(
                () => sync.IsCompleted || session.PausedSendEntered.Task.IsCompleted)
                .WaitAsync(GuardTimeout);
            await AssertTypedDeadLetterPausedRaceOutcomeAsync(store, secondReceipt, metricCapture);
        }
        finally
        {
            participant.ReleaseDeadLetter();
            session.ReleasePausedSendAttempt();
        }
    }

    /// <summary>Verifies receive inclusion keeps queue diagnostics pending until terminal upload acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">The receive retry timer did not arm before the guard.</exception>
    [Test]
    public async Task EngineDiagnosticsReceiveInclusionReleasesQueueAfterTerminalUploadAcknowledgement()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        await using var store = await CreateDiagnosticsSqliteStoreAsync();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var session = new ReceiveSession { AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(store, transport, CreateReceiveRetryOptions(), timeProvider: clock);
        var projection = new ReceiveCounterProjection();
        var coordinator = new CapturingStreamCoordinator(engine);
        await using var stream = CreateReceiveCounterStream(store, coordinator, projection, clock);
        var participant = new CapturingRemoteApplyParticipant(coordinator.Participant);
        using var registration = engine.RegisterParticipant(participant);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        await SeedReceiveAuthoritativeCheckpointAsync(stream, store);
        var receipt = await stream.PublishAsync(new(DiagnosticsLocalCounter), cancellationToken: CancellationToken.None);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(ReceiveCursor);
        session.Batches.Add(CreateCompletedReceiveBatch(receipt.OperationId));

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await Assert.That(session.SubscribeRequests[0].Cursor).IsEqualTo(ReceiveCursor);
        await Assert.That(session.SubscribeRequests[0].InitialPosition).IsEqualTo(StartPosition.Latest);
        try
        {
            await AwaitReceiveRetryTimerWithTraceAsync(session, projection, clock, faults, operationStates);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateCapturedRemoteApplyTrace(participant), exception);
        }

        recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(recovered.ReplayOperations).IsEmpty();
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        await engine.StopAsync(CancellationToken.None);
        await AssertTerminalUploadAcknowledgementReleasesQueueOnceAsync(store, clock);
    }

    /// <summary>Verifies terminal conflict results emit conflict diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecordTerminalConflictResult()
    {
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes) { ResultKind = OperationResultKind.Conflict };

        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);

        await Assert.That(metricCapture.HasMeasurement("oc.conflicts", ExpectedSingleOperation)).IsTrue();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies disabled diagnostics options prevent engine metrics and activities.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRespectDisabledOptions()
    {
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        using var activities = CreateEngineActivityListener(out var activityCapture);
        var options = CreateDiagnosticsOptions(enabled: false);
        await using var engine = CreateEngine(options: options);
        var participant = new RecordingParticipant();
        using var registration = engine.RegisterParticipant(participant);

        var receipt = await engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(Operation);
        await Assert.That(metricCapture.GetMeasurements()).Count().IsEqualTo(0);
        await Assert.That(activityCapture.GetActivities()).Count().IsEqualTo(0);
    }

    /// <summary>Verifies diagnostics listener failures cannot change durable commit outcomes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsListenerFailuresDoNotAlterDurableCommitOutcome()
    {
        using var metrics = CreateThrowingEngineMetricListener();
        using var activities = CreateThrowingEngineActivityListener();
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(options: options);
        var participant = new RecordingParticipant();
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
    }

    /// <summary>Verifies activity stop listener failures cannot change durable commit outcomes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsActivityStopFailuresDoNotAlterDurableCommitOutcome()
    {
        using var activities = CreateThrowingEngineActivityStopListener();
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(options: options);
        var participant = new RecordingParticipant();
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
    }

    /// <summary>Verifies owned diagnostics remain alive until dispose drains the lifecycle stop.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecordDisposeStopAfterLifecycleDrain()
    {
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var options = CreateDiagnosticsOptions(enabled: true);
        var engine = CreateEngine(options: options);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        await engine.StartAsync(CancellationToken.None);
        await engine.DisposeAsync();

        await Assert.That(metricCapture.Sum("oc.connection.state_changes")).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Verifies diagnostic durations use monotonic time when UTC moves backwards during a commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsUseMonotonicDurationWhenUtcRollsBack()
    {
        var clock = new DivergentTimeProvider(DateTimeOffset.UnixEpoch);
        var elapsed = TimeSpan.FromMilliseconds(DiagnosticsElapsedMilliseconds);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(options: options, timeProvider: clock);
        var participant = new RecordingParticipant { OnCommit = () => clock.AdvanceMonotonicAndRollbackUtc(elapsed, TimeSpan.FromSeconds(ReceiveRetryAfterSeconds)) };
        using var registration = engine.RegisterParticipant(participant);
        var operation = CreateOperation();

        var receipt = await engine.EnqueueOperationAsync(operation, CancellationToken.None);

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(metricCapture.HasMeasurement("oc.store.commit.duration", elapsed.TotalMilliseconds)).IsTrue();
    }

    /// <summary>Verifies recovered pending stream work seeds bounded queue gauges once during initialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsSeedRecoveredQueueAggregateOnStreamInitialization()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var operation = CreateOperation();
        var snapshot = CreateReceiveCounterSnapshot();
        var store = new RecordingStore { Recovery = new(Subscription, null, snapshot, [operation], [], FirstSequence + 1) };
        var session = new ReceiveSession();
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateReceiveCounterStream(store, engine, new(), clock);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(metricCapture.HasMeasurement(QueuePendingMetricName, ExpectedSingleOperation))
            .IsTrue();
        await Assert.That(metricCapture.HasPositiveMeasurement(QueueBytesMetricName)).IsTrue();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a recreated public stream facade recovers its own queue aggregate after unregister removes the previous registration.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRecreatedStreamRecoveryDoesNotInheritDisposedQueueAggregate()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = new InMemoryLocalStoreAdapter(
            clock,
            maximumRecordCount: DiagnosticsStoreBytes,
            maximumEncodedBytes: DiagnosticsStoreBytes,
            retentionOptions: new());
        var options = CreateDiagnosticsOptions(enabled: true);
        await using var engine = CreateEngine(store, options: options, timeProvider: clock);
        var first = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        try
        {
            _ = await first.PublishAsync(
                new(DiagnosticsLocalCounter),
                CreateVolatilePublishOptions(),
                CancellationToken.None);

            await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            await first.DisposeAsync();
        }

        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(0);

        await using var second = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        await second.StartAsync(CancellationToken.None);

        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies repeated synchronization does not resend or recount an operation already applied to the real store.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EngineDiagnosticsRepeatedSyncDoesNotRecountDurablySynchronizedOperation()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var store = CreateDiagnosticsMemoryStore(clock);
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var receipt = await PublishVolatileCounterAsync(stream);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var synchronized = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(synchronized!.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(metricCapture.Sum(SynchronizedOperationsMetricName)).IsEqualTo(ExpectedSingleOperation);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);

        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metricCapture.Sum(SynchronizedOperationsMetricName)).IsEqualTo(ExpectedSingleOperation);
    }
}
