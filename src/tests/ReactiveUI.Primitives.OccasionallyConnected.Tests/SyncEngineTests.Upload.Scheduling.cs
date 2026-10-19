// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload scheduling tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The initial denied lease, queued wake retry, and post-success empty probe lease.</summary>
    private const int ExpectedDeniedBarrierLeaseRequests = 3;

    /// <summary>Verifies repeated local wakes merge while an existing upload head is in flight.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InflightUploadMergesQueuedLocalWakesIntoNextScheduledHead()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var third = CreateOperation(sequence: ExpectedTwoOperations + 1, operationId: OperationId.New());
        var store = CreateUploadStore([first], timeProvider: clock);
        EnqueueQueuedOperations(store, clock, second, third);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            engine.NotifyLocalCommitReady(Stream, first);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults: null, operationStates: null);
            await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);

            engine.NotifyLocalCommitReady(Stream, second);
            engine.NotifyLocalCommitReady(Stream, third);
            session.ReleasePausedSendAttempt();
            await WaitForUploadConditionWithTraceAsync(
                () => session.SentBatches.Count == ExpectedTwoOperations,
                store,
                session,
                faults: null,
                operationStates: null);

            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches[1].Operations.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.SentBatches[1].Operations[0]).IsSameReferenceAs(second);
            await Assert.That(session.SentBatches[1].Operations[1]).IsSameReferenceAs(third);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
        }
        finally
        {
            session.ReleasePausedSendAttempt();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies a local wake queued during a denied upload attempt is scheduled after the attempt releases ownership.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeniedInflightUploadSchedulesQueuedWakeWithoutRetryReschedule()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var barrierEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStore([first], timeProvider: clock);
        store.DenyRemoteAttemptOnCall = ExpectedSingleOperation;
        store.BarrierEntered = barrierEntered;
        store.ReleaseBarrier = releaseBarrier;
        EnqueueQueuedOperations(store, clock, second);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            engine.NotifyLocalCommitReady(Stream, first);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults: null, operationStates: null);
            await barrierEntered.Task.WaitAsync(GuardTimeout);
            engine.NotifyLocalCommitReady(Stream, second);
            _ = releaseBarrier.TrySetResult();
            await AdvanceDwellAfterDeniedAttemptReleasesAsync(clock, store, session, options);
            await Assert.That(session.PreparedBatches.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(second);
            await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.LeaseRequests.Count).IsEqualTo(ExpectedDeniedBarrierLeaseRequests);
        }
        finally
        {
            _ = releaseBarrier.TrySetResult();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies an explicit sync still uploads a queued head after the UTC clock moves backward.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExplicitSyncClampsElapsedHeadAgeAfterUtcClockRollback()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation(operationId: OperationId.New());
        var store = CreateUploadStore([operation], timeProvider: clock);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        clock.SetUtcNow(DateTimeOffset.UnixEpoch.Subtract(TimeSpan.FromSeconds(ExpectedSingleOperation)));
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches[0].Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies failed lease preparation classifies authentication and storage faults before remote work.</summary>
    /// <param name="authenticationFailure">Whether the store reports an authentication failure.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task UploadLeaseFailureClassifiesSecurityAndStorageFaults(bool authenticationFailure)
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        store.LeasePendingOperationsException = authenticationFailure
            ? new LocalStoreRecordAuthenticationException("The local record failed authentication.")
            : new DurableStorageException("The storage medium refused the lease read.");
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);

        await Assert.That(faults.Values[0].Category).IsEqualTo(authenticationFailure ? FaultCategory.Security : FaultCategory.Storage);
        await Assert.That(faults.Values[0].Severity).IsEqualTo(authenticationFailure ? FaultSeverity.Critical : FaultSeverity.Error);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies fault diagnostics bound an unusually long exception type name.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadLeaseFailureBoundsDiagnosticExceptionTypeName()
    {
        const int maximumDiagnosticTypeNameLength = 256;
        var store = CreateUploadStore([CreateOperation()]);
        store.LeasePendingOperationsException = new VerboseDiagnosticException<
            Dictionary<string, Dictionary<string, string>>,
            Dictionary<string, Dictionary<string, string>>,
            Dictionary<string, Dictionary<string, string>>,
            Dictionary<string, Dictionary<string, string>>>();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);

        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
        await Assert.That(faults.Values[0].Exception?.Message.Length).IsEqualTo(maximumDiagnosticTypeNameLength);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Waits for denied-attempt cleanup and advances the pending local wake dwell.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AdvanceDwellAfterDeniedAttemptReleasesAsync(
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        OccasionallyConnectedOptions options)
    {
        await WaitForUploadConditionWithTraceAsync(
            () => store.ReleaseLeaseCalls == ExpectedSingleOperation
                && clock.HasTimerDueIn(options.Batching.MaximumDwellTime),
            store,
            session,
            faults: null,
            operationStates: null);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        clock.Advance(options.Batching.MaximumDwellTime);
        await WaitForUploadConditionWithTraceAsync(
            () => session.SentBatches.Count == ExpectedSingleOperation
                && store.LeaseRequests.Count == ExpectedDeniedBarrierLeaseRequests,
            store,
            session,
            faults: null,
            operationStates: null);
    }

    /// <summary>Adds queued leases and status rows to an upload store fixture.</summary>
    /// <param name="store">The store to update.</param>
    /// <param name="clock">The lease clock.</param>
    /// <param name="operations">The queued operations.</param>
    private static void EnqueueQueuedOperations(RecordingStore store, TimeProvider clock, params SyncOperation[] operations)
    {
        store.Leases.Enqueue(CreateLease(operations, clock));
        foreach (var operation in operations)
        {
            store.Statuses[operation.OperationId] = new(
                operation.OperationId,
                operation.StreamId,
                SyncOperationState.QueuedForUpload,
                0,
                DateTimeOffset.UnixEpoch,
                null);
        }
    }

    /// <summary>Provides an exception type with diagnostic metadata longer than the engine's bound.</summary>
    /// <typeparam name="TFirst">The first nested diagnostic type.</typeparam>
    /// <typeparam name="TSecond">The second nested diagnostic type.</typeparam>
    /// <typeparam name="TThird">The third nested diagnostic type.</typeparam>
    /// <typeparam name="TFourth">The fourth nested diagnostic type.</typeparam>
    private sealed class VerboseDiagnosticException<TFirst, TSecond, TThird, TFourth> : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="VerboseDiagnosticException{TFirst, TSecond, TThird, TFourth}"/> class.</summary>
        public VerboseDiagnosticException()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="VerboseDiagnosticException{TFirst, TSecond, TThird, TFourth}"/> class.</summary>
        /// <param name="message">The error message.</param>
        public VerboseDiagnosticException(string message)
            : base(message)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="VerboseDiagnosticException{TFirst, TSecond, TThird, TFourth}"/> class.</summary>
        /// <param name="message">The error message.</param>
        /// <param name="innerException">The cause.</param>
        public VerboseDiagnosticException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>Gets the type arguments that make this diagnostic exception distinct.</summary>
        public static (Type First, Type Second, Type Third, Type Fourth) DiagnosticTypes =>
            (typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth));
    }
}
