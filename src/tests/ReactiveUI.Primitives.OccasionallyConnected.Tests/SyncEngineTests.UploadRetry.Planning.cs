// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload retry planning tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies an ordinary upload signal wakes the pump without being mistaken for terminal drain completion.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadPumpContinuesAfterOrdinaryWakeAndSendsReadyWork()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(
            maximumBatchOperations: ExpectedSingleOperation,
            maximumBatchBytes: PreparedUploadBytes);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);

        await engine.StartAsync(CancellationToken.None);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        engine.NotifyLocalCommitReady(Stream, operation);
        try
        {
            await WaitForConditionAsync(
                () => session.SentBatches.Count == ExpectedSingleOperation
                    || clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            if (session.SentBatches.Count == 0)
            {
                clock.Advance(options.Batching.MaximumDwellTime);
            }

            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults: null, operationStates: null), exception);
        }

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a session without batch push is leased as single-operation work.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptUsesSingleOperationLeaseWhenSessionLacksBatchPush()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(
            maximumBatchOperations: ExpectedTwoOperations,
            maximumBatchBytes: PreparedUploadBytes);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateBatchOptions(ExpectedTwoOperations),
            timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);
        try
        {
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates: null), exception);
        }

        await Assert.That(store.LeaseRequests[0].MaximumOperations).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies typed transient upload failures preserve Retry-After and retry after durable lease release.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPersistsTypedTransientRetryAfterAndRetriesReleasedLease()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            SendException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(UploadRetryAfterSeconds)),
        };
        var transport = new RecordingTransport { SessionOverride = session };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => clock.TimerCount > 0,
            store,
            session,
            faults,
            operationStates: null);

        var retryState = store.RetryStates[operation.OperationId];
        var firstSendUtc = DateTimeOffset.UnixEpoch + options.Batching.MaximumDwellTime;
        await Assert.That(retryState.DueUtc).IsEqualTo(firstSendUtc.AddSeconds(UploadRetryAfterSeconds));
        await Assert.That(retryState.PreviousDelay).IsEqualTo(TimeSpan.FromSeconds(UploadRetryAfterSeconds));
        await Assert.That(retryState.TransientAttemptCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        clock.Advance(TimeSpan.FromSeconds(UploadEarlyAdvanceSeconds));
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);

        clock.Advance(TimeSpan.FromSeconds(1));
        await WaitForUploadConditionWithTraceAsync(
            () => session.PrepareCalls == ExpectedCapacityCommitAttempts,
            store,
            session,
            faults,
            operationStates: null);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies trigger and commit wakes do not pull a retry-backoff head before its due time.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TriggerSyncAndCommitDuringRetryBackoffRetainRetryDue()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            SendException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(UploadRetryAfterSeconds)),
        };
        var transport = new RecordingTransport { SessionOverride = session };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.Count == ExpectedSingleOperation && clock.TimerCount > 0,
            store,
            session,
            faults,
            operationStates: null);

        var retryState = store.RetryStates[operation.OperationId];
        var firstSendUtc = DateTimeOffset.UnixEpoch + options.Batching.MaximumDwellTime;
        var trigger = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        engine.NotifyLocalCommitReady(Stream, CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New()));
        await Task.Delay(PollMilliseconds).ConfigureAwait(false);

        await Assert.That(retryState.DueUtc).IsEqualTo(firstSendUtc.AddSeconds(UploadRetryAfterSeconds));
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(trigger.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(UploadEarlyAdvanceSeconds));
        await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(trigger.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));
        await WaitForUploadConditionWithTraceAsync(
            () => session.PrepareCalls == ExpectedCapacityCommitAttempts,
            store,
            session,
            faults,
            operationStates: null);

        await engine.StopAsync(CancellationToken.None);
        await trigger.WaitAsync(GuardTimeout);
    }

    /// <summary>Verifies an explicit trigger keeps a retry lease parked until the original dwell deadline.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExplicitTriggerRetryReleasesPartialLeaseUntilOriginalDwellDeadline()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var dwell = retryDelay + TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var sendCount = 0;
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
            OnSend = () => ThrowFirstUploadAttempt(ref sendCount, retryDelay),
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateRetryDwellOptions(retryDelay, dwell);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        Task? trigger = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            trigger = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await WaitForUploadConditionWithTraceAsync(
                () => store.RetryStates.Count == ExpectedSingleOperation
                    && store.ReleaseLeaseCalls == ExpectedSingleOperation
                    && clock.HasTimerDueIn(retryDelay),
                store,
                session,
                faults,
                operationStates: null);

            await AssertRetryLeasePausedAtBackoffAsync(session, trigger);
            clock.Advance(retryDelay);
            await WaitForRetryDwellReleaseAsync(clock, dwell - retryDelay, store, session, faults);
            await AssertRetryLeasePausedForDwellAsync(store, session, trigger);

            clock.Advance(dwell - retryDelay);
            await WaitForUploadConditionWithTraceAsync(
                () => session.SentBatches.Count == ExpectedTwoOperations,
                store,
                session,
                faults,
                operationStates: null);

            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(faults.Values.Count).IsEqualTo(0);
        }
        finally
        {
            await StopEngineThenJoinUploadTriggerAsync(engine, trigger, store, session, faults);
        }
    }

    /// <summary>Creates retry and dwell options that allow one partial leased operation.</summary>
    /// <param name="retryDelay">The transient retry delay.</param>
    /// <param name="dwell">The maximum dwell interval.</param>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateRetryDwellOptions(TimeSpan retryDelay, TimeSpan dwell) =>
        CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes) with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with
            {
                MaximumOperations = ExpectedTwoOperations,
                MaximumBytes = PreparedUploadBytes,
                MaximumDwellTime = dwell,
            },
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };

    /// <summary>Throws on the first upload attempt and succeeds on later attempts.</summary>
    /// <param name="sendCount">The mutable send count.</param>
    /// <param name="retryDelay">The transient retry delay.</param>
    private static void ThrowFirstUploadAttempt(ref int sendCount, TimeSpan retryDelay)
    {
        if (sendCount == 0)
        {
            sendCount++;
            throw CreateTransportFailure(RetryFailureKind.Transient, retryDelay);
        }

        sendCount++;
    }

    /// <summary>Asserts the first failed trigger is paused by retry backoff.</summary>
    /// <param name="session">The prepared remote session.</param>
    /// <param name="trigger">The explicit trigger task.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRetryLeasePausedAtBackoffAsync(PreparedSession session, Task trigger)
    {
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(trigger.IsCompleted).IsFalse();
    }

    /// <summary>Waits for a retry lease to be released while dwell remains pending.</summary>
    /// <param name="clock">The manual clock.</param>
    /// <param name="remainingDwell">The expected remaining dwell.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The assertion task.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static Task WaitForRetryDwellReleaseAsync(
        ManualTimerTimeProvider clock,
        TimeSpan remainingDwell,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault> faults) =>
        WaitForUploadConditionWithTraceAsync(
            () => store.ReleaseLeaseCalls == ExpectedTwoOperations && clock.HasTimerDueIn(remainingDwell),
            store,
            session,
            faults,
            operationStates: null);

    /// <summary>Asserts a retry lease is parked for dwell without remote side effects.</summary>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="trigger">The explicit trigger task.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRetryLeasePausedForDwellAsync(RecordingStore store, PreparedSession session, Task trigger)
    {
        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(trigger.IsCompleted).IsFalse();
    }

    /// <summary>Stops an engine and joins an explicit upload trigger even when stop faults.</summary>
    /// <param name="engine">The engine to stop.</param>
    /// <param name="trigger">The optional explicit trigger.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The cleanup task.</returns>
    /// <exception cref="Exception">The engine stop failed.</exception>
    /// <exception cref="TimeoutException">The trigger did not finish within the guard timeout.</exception>
    private static async Task StopEngineThenJoinUploadTriggerAsync(
        SyncEngine engine,
        Task? trigger,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        Exception? stopFailure = null;

        try
        {
            await engine.StopAsync(CancellationToken.None);
        }
        catch (Exception exception)
        {
            stopFailure = exception;
        }

        if (trigger is not null)
        {
            try
            {
                await trigger.WaitAsync(GuardTimeout);
            }
            catch (Exception exception) when (stopFailure is null)
            {
                throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates: null), exception);
            }
        }

        if (stopFailure is not null)
        {
            throw stopFailure;
        }
    }
}
