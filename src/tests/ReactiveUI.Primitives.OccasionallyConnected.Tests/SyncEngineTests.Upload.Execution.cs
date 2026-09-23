// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload execution tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a denied durable attempt barrier releases the lease without sending a prepared upload.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDeniedBarrierReleasesLeaseWithoutSendingPreparedBatch()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        store.DenyRemoteAttempt = true;
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a later denied barrier prevents sending the whole prepared batch.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptLaterDeniedBarrierDoesNotSendPartiallyAuthorizedBatch()
    {
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStore([first, second]);
        store.DenyRemoteAttemptOnCall = ExpectedTwoOperations;
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateBatchOptions(ExpectedTwoOperations));
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PreparedBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies exactly-once retry-anchor lookup failures fault and release the lease before remote effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptFaultsRetryAnchorLookupFailureBeforePreparingRemotePush()
    {
        var operation = CreateExactlyOnceOperation();
        var store = CreateUploadStore([operation]);
        store.RetryStateQueryException = new InvalidOperationException("retry lookup failed");
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies exactly-once retry-anchor status lookup failures release the lease before remote effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptFaultsRetryAnchorStatusLookupFailureBeforePreparingRemotePush()
    {
        var operation = CreateExactlyOnceOperation();
        var store = CreateUploadStore([operation]);
        store.StatusQueryException = new InvalidOperationException("retry status lookup failed");
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies retry-anchor save failures fault and release exactly-once work before remote effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptFaultsRetryAnchorSaveFailureBeforePreparingRemotePush()
    {
        var operation = CreateExactlyOnceOperation();
        var store = CreateUploadStore([operation]);
        store.RetryStateSaveException = new InvalidOperationException("retry save failed");
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.RetryStates.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a faulted upload publishes lease-release failure before the original upload fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPublishesLeaseReleaseFailureBeforeUploadFault()
    {
        var operation = CreateExactlyOnceOperation();
        var store = CreateUploadStore([operation]);
        store.RetryStateQueryException = new InvalidOperationException("retry lookup failed");
        store.ReleaseLeaseException = new InvalidOperationException("release failed");
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedTwoOperations, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Engine.UploadLeaseRelease");
        await Assert.That(faults.Values[1].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies malformed partial remote results release their lease without queue-accounting release.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadMalformedPartialResultReleasesLeaseWithoutQueueAccountingRelease()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var retainedBytes = SyncEngine.GetOperationRetainedBytes(first) + SyncEngine.GetOperationRetainedBytes(second);
        var store = CreateUploadStore([first, second], timeProvider: clock);
        var sendCount = 0;
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
            ResultFactory = batch =>
            {
                var currentSend = sendCount;
                sendCount++;
                return currentSend == 0
                    ? CreateSingleAcceptedUploadResult(batch, first.OperationId)
                    : CreateAcceptedUploadResult(batch);
            },
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, retainedBytes, DiagnosticsRecoveredRevision));

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates);
        await AssertMalformedPartialResultLeftQueuePendingAsync(session, store, operationStates, faults, metricCapture, retainedBytes);

        store.Leases.Enqueue(CreateLease([first, second], clock));
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates);

        var states = operationStates.Values.Select(static status => status.State).ToArray();
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(states.Length).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(states[0]).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(states[1]).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(0);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies retryable remote results keep queue accounting pending until a later terminal result.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadRetryableResultKeepsQueueAccountingPendingUntilAccepted()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var operation = CreateOperation();
        var retainedBytes = SyncEngine.GetOperationRetainedBytes(operation);
        var store = CreateUploadStore([operation], timeProvider: clock);
        var sendCount = 0;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            ResultFactory = batch =>
            {
                var currentSend = sendCount;
                sendCount++;
                return currentSend == 0
                    ? CreateRetryableUploadResult(batch)
                    : CreateAcceptedUploadResult(batch);
            },
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateResultAwareUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, retainedBytes, DiagnosticsRecoveredRevision));

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates);
        await AssertRetryableUploadLeftQueuePendingAsync(session, operationStates, metricCapture, retainedBytes);

        store.Leases.Enqueue(CreateLease([operation], clock));
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates);

        var final = operationStates.Values.Last(status => status.OperationId == operation.OperationId);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(final.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(0);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(0);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies byte-prefix planning releases a legitimate payload-byte lease before taking a remote barrier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPlanningSendsReadyMinimumRetainedBytePrefixAfterReleasingLease()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(payload: EmptyPayload, operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, payload: EmptyPayload, operationId: OperationId.New());
        var store = CreateUploadStore([first, second], timeProvider: clock);
        store.Leases.Enqueue(CreateLease([first], clock));
        var session = new PreparedSession(ExpectedTwoOperations, ExpectedSingleOperation)
            { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, ExpectedSingleOperation) };
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with
            {
                MaximumOperations = ExpectedTwoOperations,
                MaximumDwellTime = TimeSpan.FromMinutes(ExpectedSingleOperation),
                MaximumBytes = ExpectedSingleOperation,
            },
        };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        var started = false;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

            await Assert.That(first.Payload.PayloadLength + second.Payload.PayloadLength).IsEqualTo(0);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.LeaseRequests[0].MaximumBytes).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.LeaseRequests[1].MaximumOperations).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            if (started)
            {
                await engine.StopAsync(CancellationToken.None);
            }
        }
    }

    /// <summary>Verifies a leased oversized head is dead-lettered and the retained tail lease is released.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadPlanningDeadLettersOversizedHeadAndReleasesTailLease()
    {
        var oversizedPayload = TestPayload.Concat(TestPayload).ToArray();
        var oversized = CreateOperation(payload: oversizedPayload, operationId: OperationId.New());
        var tail = CreateOperation(
            sequence: ExpectedTwoOperations,
            payload: EmptyPayload,
            operationId: OperationId.New());
        var store = CreateUploadStore([oversized, tail]);
        var participant = new RecordingParticipant();
        var session = new PreparedSession(ExpectedTwoOperations, ExpectedSingleOperation)
            { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, ExpectedSingleOperation) };
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with
            {
                MaximumOperations = ExpectedTwoOperations,
                MaximumBytes = ExpectedSingleOperation,
            },
        };
        var fixtureLease = store.Leases.Peek();
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options);
        using var registration = engine.RegisterParticipant(participant);

        await Assert.That(options.Batching.MaximumOperations).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(options.Batching.MaximumBytes).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.NegotiatedCapabilities.MaximumBatchOperations).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.NegotiatedCapabilities.MaximumBatchBytes).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(oversized.Payload.PayloadLength).IsGreaterThan(ExpectedSingleOperation);
        await Assert.That(tail.Payload.PayloadLength).IsLessThanOrEqualTo(ExpectedSingleOperation);
        await Assert.That(fixtureLease.Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(fixtureLease.Operations[0]).IsSameReferenceAs(oversized);
        await Assert.That(fixtureLease.Operations[1]).IsSameReferenceAs(tail);

        var started = false;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

            await Assert.That(participant.DeadLetterCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(participant.LastDeadLetterOperationId).IsEqualTo(oversized.OperationId);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.BarrierCalls).IsEqualTo(0);
            await Assert.That(store.LeaseRequests[0].MaximumOperations).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(store.LeaseRequests[0].MaximumBytes).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PrepareCalls).IsEqualTo(0);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        }
        finally
        {
            if (started)
            {
                await engine.StopAsync(CancellationToken.None);
            }
        }
    }

    /// <summary>Verifies engine disposal waits for an in-flight upload send before dropping continuations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForInflightUploadCompletionAndDropsContinuation()
    {
        var sendEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseSend = new();
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { OnSend = EnterAndWaitForSendRelease };
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateResultAwareUploadParticipant(store));
        Task? dispose = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, operation);
            await WaitForConditionAsync(() => session.PrepareCalls == ExpectedSingleOperation);
            await sendEntered.Task.WaitAsync(GuardTimeout);

            dispose = engine.DisposeAsync().AsTask();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => dispose.WaitAsync(StopCallbackObservationWindow));

            releaseSend.Set();
            await dispose.WaitAsync(GuardTimeout);

            await Assert.That(store.LeaseRequests.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
            await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        }
        finally
        {
            releaseSend.Set();
            if (dispose is not null)
            {
                await ObserveTaskCompletionAsync(dispose).ConfigureAwait(false);
            }
        }

        void EnterAndWaitForSendRelease()
        {
            _ = sendEntered.TrySetResult();
            releaseSend.Wait();
        }
    }

    /// <summary>Verifies store lease enumeration failures fault the upload attempt before remote effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPublishesFaultWhenStoreLeasePreparationFails()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        store.LeasePendingOperationsException = new InvalidOperationException("lease enumeration failed");
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.LeaseRequests.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Creates an exactly-once operation for retry-anchor tests.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateExactlyOnceOperation() =>
        CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce } };

    /// <summary>Creates an accepted result for every operation in the batch.</summary>
    /// <param name="batch">The uploaded batch.</param>
    /// <returns>The accepted result.</returns>
    private static RemoteSyncResult CreateAcceptedUploadResult(SyncBatch batch) =>
        new(
            batch.BatchId,
            batch.Operations
                .Select(static operation => new OperationSyncResult(
                    operation.OperationId,
                    OperationResultKind.Accepted,
                    ReasonCode: null,
                    ServerVersion: "v1"))
                .ToArray(),
            serverCursor: null,
            retryAfter: null);

    /// <summary>Creates an accepted result for one operation from a larger batch.</summary>
    /// <param name="batch">The uploaded batch.</param>
    /// <param name="operationId">The accepted operation identity.</param>
    /// <returns>The partial accepted result.</returns>
    private static RemoteSyncResult CreateSingleAcceptedUploadResult(SyncBatch batch, OperationId operationId) =>
        new(
            batch.BatchId,
            [new(operationId, OperationResultKind.Accepted, ReasonCode: null, ServerVersion: "v1")],
            serverCursor: null,
            retryAfter: null);

    /// <summary>Creates a retryable result for every operation in the batch.</summary>
    /// <param name="batch">The uploaded batch.</param>
    /// <returns>The retryable result.</returns>
    private static RemoteSyncResult CreateRetryableUploadResult(SyncBatch batch) =>
        new(
            batch.BatchId,
            batch.Operations
                .Select(static operation => new OperationSyncResult(
                    operation.OperationId,
                    OperationResultKind.Retryable,
                    ReasonCode: "retryable",
                    ServerVersion: "v1"))
                .ToArray(),
            serverCursor: null,
            retryAfter: null);

    /// <summary>Asserts that a malformed partial result did not mutate durable status or release queue accounting.</summary>
    /// <param name="session">The prepared transport session.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="operationStates">The operation status observer.</param>
    /// <param name="faults">The fault observer.</param>
    /// <param name="metricCapture">The metric capture.</param>
    /// <param name="retainedBytes">The original retained queue bytes.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedPartialResultLeftQueuePendingAsync(
        PreparedSession session,
        RecordingStore store,
        RecordingObserver<SyncOperationStatus> operationStates,
        RecordingObserver<OccasionallyConnectedFault> faults,
        EngineMetricCapture metricCapture,
        long retainedBytes)
    {
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(operationStates.Values.Count).IsEqualTo(0);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(retainedBytes);
    }

    /// <summary>Asserts that a retryable result kept upload queue accounting pending.</summary>
    /// <param name="session">The prepared transport session.</param>
    /// <param name="operationStates">The operation status observer.</param>
    /// <param name="metricCapture">The metric capture.</param>
    /// <param name="retainedBytes">The operation retained bytes.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRetryableUploadLeftQueuePendingAsync(
        PreparedSession session,
        RecordingObserver<SyncOperationStatus> operationStates,
        EngineMetricCapture metricCapture,
        long retainedBytes)
    {
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(operationStates.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(operationStates.Values[0].State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(retainedBytes);
    }

    /// <summary>Creates a participant that records upload outcomes according to each remote result kind.</summary>
    /// <param name="store">The store receiving result-aware statuses.</param>
    /// <returns>The recording participant.</returns>
    private static RecordingParticipant CreateResultAwareUploadParticipant(RecordingStore store) =>
        new()
        {
            OnApplySyncResult = (batch, result) =>
            {
                foreach (var operationResult in result.Operations)
                {
                    var operation = batch.Operations.Single(candidate => candidate.OperationId == operationResult.OperationId);
                    store.Statuses[operation.OperationId] = new(
                        operation.OperationId,
                        operation.StreamId,
                        ToSyncOperationState(operationResult.Kind),
                        Attempt: 1,
                        DateTimeOffset.UnixEpoch,
                        operationResult.ReasonCode);
                }
            },
        };

    /// <summary>Maps a remote upload result kind to the durable test status.</summary>
    /// <param name="kind">The remote upload result kind.</param>
    /// <returns>The matching durable status.</returns>
    private static SyncOperationState ToSyncOperationState(OperationResultKind kind) =>
        kind switch
        {
            OperationResultKind.Accepted => SyncOperationState.Synchronized,
            OperationResultKind.Conflict => SyncOperationState.Conflict,
            OperationResultKind.Rejected => SyncOperationState.Rejected,
            _ => SyncOperationState.QueuedForUpload,
        };
}
