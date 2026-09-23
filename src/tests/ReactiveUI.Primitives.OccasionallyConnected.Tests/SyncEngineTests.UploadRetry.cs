// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload retry classification tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The retry-after delay in seconds used by upload tests.</summary>
    private const int UploadRetryAfterSeconds = 3;

    /// <summary>The short retry delay in milliseconds used by upload tests.</summary>
    private const int UploadShortRetryMilliseconds = 100;

    /// <summary>The early retry advance in seconds used by upload tests.</summary>
    private const int UploadEarlyAdvanceSeconds = 2;

    /// <summary>The short exactly-once retention window in seconds used by upload tests.</summary>
    private const int UploadShortRetentionSeconds = 2;

    /// <summary>The advance that expires the short exactly-once retention window.</summary>
    private const int UploadExpiredRetentionAdvanceSeconds = 3;

    /// <summary>The in-memory restart store maximum record count.</summary>
    private const int UploadRestartStoreRecordCount = 64;

    /// <summary>The in-memory restart store byte multiplier.</summary>
    private const int UploadRestartStoreByteMultiplier = 16;

    /// <summary>The upload attempt fault code.</summary>
    private const string UploadAttemptFaultCode = "OC.Engine.UploadAttempt";

    /// <summary>The test store application name.</summary>
    private const string SyncEngineTestsStoreName = "sync-engine-tests";

    /// <summary>Verifies typed ambiguous upload outcomes are retryable through the Core failure seam.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRetriesTypedAmbiguousTransportOutcome()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            SendException = CreateTransportFailure(RetryFailureKind.AmbiguousTransportOutcome, TimeSpan.FromSeconds(1)),
        };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults: null, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.Count == ExpectedSingleOperation,
            store,
            session,
            faults: null,
            operationStates: null);
        var firstRetryState = store.RetryStates[operation.OperationId];
        var retryDelay = firstRetryState.DueUtc.GetValueOrDefault() - clock.GetUtcNow();
        if (retryDelay > TimeSpan.Zero)
        {
            await WaitForUploadConditionWithTraceAsync(
                () => clock.HasTimerDueIn(retryDelay),
                store,
                session,
                faults: null,
                operationStates: null);
            clock.Advance(retryDelay);
        }

        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.TryGetValue(operation.OperationId, out var retryState) && retryState.TransientAttemptCount == ExpectedCapacityCommitAttempts,
            store,
            session,
            faults: null,
            operationStates: null);

        await Assert.That(store.RetryStates[operation.OperationId].TransientAttemptCount).IsEqualTo(ExpectedCapacityCommitAttempts);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a stale shared remote session renews once and reuses durable upload work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRenewsExpiredRemoteSessionBeforeRetryPolicyFault()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var renewedSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(expiredSession);
        transport.Sessions.Enqueue(renewedSession);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized || faults.Values.Count != 0,
            store,
            expiredSession,
            faults,
            operationStates: null);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(expiredSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewedSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewedSession.SentBatches[0].Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(store.RetryStates).IsEmpty();
        await Assert.That(faults.Values).IsEmpty();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies exactly-once upload retry age is anchored before a long first send.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptStopsRetryWhenExactlyOnceFirstAttemptOutlivesServerRetention()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
        var retryAfter = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention),
            SendException = CreateTransportFailure(RetryFailureKind.Transient, retryAfter),
            OnSend = () => clock.Advance(TimeSpan.FromSeconds(UploadExpiredRetentionAdvanceSeconds)),
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retention = OccasionallyConnectedOptions.Default.Retention with
            {
                InboxDeduplicationRetention = TimeSpan.FromMinutes(1),
            },
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
                MaximumRetryAge = TimeSpan.FromMinutes(1),
            },
        };
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.RetryStates.Count).IsEqualTo(ExpectedSingleOperation);
        var firstSendUtc = DateTimeOffset.UnixEpoch + options.Batching.MaximumDwellTime;
        await Assert.That(store.RetryStates[operation.OperationId].StartedUtc).IsEqualTo(firstSendUtc);
        await Assert.That(store.RetryStates[operation.OperationId].TransientAttemptCount).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies recovered ambiguous exactly-once work does not reset its retry anchor before resend.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDoesNotResetExactlyOnceRetryAnchorRecoveredAfterAmbiguousBarrier()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RetryStates[operation.OperationId] = RetryState.Start(DateTimeOffset.UnixEpoch);
        store.Statuses[operation.OperationId] = new(
            operation.OperationId,
            operation.StreamId,
            SyncOperationState.Ambiguous,
            Attempt: ExpectedSingleOperation,
            DateTimeOffset.UnixEpoch,
            ReasonCode: null);
        clock.Advance(TimeSpan.FromSeconds(UploadExpiredRetentionAdvanceSeconds));
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention),
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retention = OccasionallyConnectedOptions.Default.Retention with
            {
                InboxDeduplicationRetention = TimeSpan.FromMinutes(1),
            },
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
                MaximumRetryAge = TimeSpan.FromMinutes(1),
            },
        };
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.RetryStates[operation.OperationId].StartedUtc).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies recovered exactly-once work with a prior attempt but no retry anchor fails closed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRejectsAttemptedExactlyOnceOperationWhenRetryAnchorIsMissing()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.Statuses[operation.OperationId] = new(
            operation.OperationId,
            operation.StreamId,
            SyncOperationState.Uploading,
            Attempt: ExpectedSingleOperation,
            DateTimeOffset.UnixEpoch,
            ReasonCode: null);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(TimeSpan.FromSeconds(UploadShortRetentionSeconds)),
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates: null);

        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.RetryStates.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies the in-memory store fails exactly-once upload closed before prepare because it lacks durable capabilities.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRejectsExactlyOnceOnInMemoryStoreBeforePrepare()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        await using var store = await CreateEngineInMemoryRestartStoreAsync(clock);
        await SeedRestartUploadOperationAsync(store, operation);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var session = await RunExpiredExactlyOnceInMemoryUploadAttemptAsync(store, clock, faults, serverRetention);

        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
    }

    /// <summary>Verifies a SQLite recovered exactly-once attempt keeps its original retry anchor after reopening.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDoesNotResetExactlyOnceRetryAnchorAcrossSqliteRestart()
    {
        var directory = Directory.CreateTempSubdirectory("oc-engine-upload-restart-");
        try
        {
            await AssertExactlyOnceSqliteRestartPreservesRetryAnchorAsync(Path.Combine(directory.FullName, "local.db"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies recovered upload-only work is not attempted when the session lacks its delivery guarantee.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptRejectsRecoveredExactlyOnceOperationBeforeBarrierOnWeakSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce } };
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates: null);
        try
        {
            await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, null), exception);
        }

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies the same recovered exactly-once fixture uploads when the session satisfies its policy.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptSendsRecoveredExactlyOnceOperationOnStrongSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce } };
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults: null, operationStates: null);

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies one stronger policy rejects an entire mixed lease before any upload side effect.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptRejectsMixedLeaseBeforeBarrierWhenOnePolicyNeedsMissingCapabilities()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var atMostOnce = CreateOperation(operationId: OperationId.New()) with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce } };
        var exactlyOncePolicy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce };
        var exactlyOnce = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New()) with { Policy = exactlyOncePolicy };
        var store = CreateUploadStore([atMostOnce, exactlyOnce], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates: null);

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies upload validation releases leases when the store lacks a required capability.</summary>
    /// <param name="missingCapability">The omitted store capability.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    [Arguments(LocalStoreCapabilities.DurableLocalCommit)]
    [Arguments(LocalStoreCapabilities.DurableInbox)]
    public async Task UploadAttemptRejectsRecoveredOperationWhenStoreLacksRequiredCapabilityBeforeBarrier(
        LocalStoreCapabilities missingCapability)
    {
        var operation = CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce } };

        await AssertUploadLeaseRejectedBeforeBarrierAsync(
            [operation],
            RecordingStoreUploadCapabilities & ~missingCapability,
            CreateExactlyOnceCapabilities(),
            ExpectedSingleOperation);
    }

    /// <summary>Verifies malformed recovered policies release their lease before upload side effects.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptReleasesMalformedRecoveredPolicyBeforeBarrier()
    {
        var operation = CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = (DeliveryGuarantee)int.MaxValue } };

        await AssertUploadLeaseRejectedBeforeBarrierAsync(
            [operation],
            RecordingStoreUploadCapabilities,
            CreateExactlyOnceCapabilities(),
            ExpectedSingleOperation);
    }

    /// <summary>Verifies streams registered after Start cannot upload stronger work than the negotiated session permits.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">No upload attempt signal was observed before the guard.</exception>
    [Test]
    public async Task UploadAttemptRejectsLaterRegisteredExactlyOnceOperationBeforeBarrierOnWeakSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation() with { Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce } };
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        engine.NotifyLocalCommitReady(Stream, operation);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates: null);

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies disposed transport session failures use transient retry classification.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptRetriesObjectDisposedSessionFailureAsTransient()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = new ObjectDisposedException("prepared-session") };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedSingleOperation,
            },
        };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.TryGetValue(operation.OperationId, out var state)
                && state.TransientAttemptCount == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        clock.Advance(retryDelay);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies transient upload failures fault after the configured retry attempt limit.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task UploadAttemptPublishesFaultAfterConfiguredTransientRetryLimit()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { SendException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedSingleOperation,
            },
        };
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.TryGetValue(operation.OperationId, out var state)
                && state.TransientAttemptCount == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        clock.Advance(retryDelay);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.RetryStates[operation.OperationId].TransientAttemptCount)
            .IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies typed permanent upload failures are faulted without durable retry state.</summary>
    /// <param name="failureKind">The permanent failure kind.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(RetryFailureKind.AuthorizationDenied)]
    [Arguments(RetryFailureKind.SchemaIncompatible)]
    [Arguments(RetryFailureKind.ValidationRejected)]
    public async Task UploadAttemptDoesNotRetryTypedPermanentTransportFailure(RetryFailureKind failureKind)
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { SendException = CreateTransportFailure(failureKind) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.RetryStates.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Creates a test engine that borrows an existing store across restart.</summary>
    /// <param name="store">The borrowed store.</param>
    /// <param name="transport">The owned transport.</param>
    /// <param name="options">The runtime options.</param>
    /// <param name="timeProvider">The test time provider.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateBorrowedStoreEngine(
        ILocalStoreAdapter store,
        IRemoteTransportAdapter transport,
        OccasionallyConnectedOptions options,
        TimeProvider timeProvider) =>
        new(new()
        {
            Store = store,
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Borrowed,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = options,
            StoreInitialization = new(SyncEngineTestsStoreName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = timeProvider,
            MaxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });

    /// <summary>Asserts an exactly-once upload retry anchor survives a real SQLite close and reopen.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertExactlyOnceSqliteRestartPreservesRetryAnchorAsync(string databasePath)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var serverRetention = TimeSpan.FromSeconds(UploadShortRetentionSeconds);
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };

        await RunFirstExactlyOnceSqliteUploadAttemptAsync(databasePath, clock, operation, serverRetention);
        clock.Advance(TimeSpan.FromSeconds(UploadExpiredRetentionAdvanceSeconds));
        await AssertExpiredExactlyOnceSqliteUploadDoesNotPrepareAsync(databasePath, clock, operation.OperationId, serverRetention);
    }

    /// <summary>Creates an initialized in-memory store for sync engine restart tests.</summary>
    /// <param name="clock">The shared manual clock.</param>
    /// <returns>The initialized in-memory store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateEngineInMemoryRestartStoreAsync(TimeProvider clock)
    {
        var store = new InMemoryLocalStoreAdapter(
            clock,
            maximumRecordCount: UploadRestartStoreRecordCount,
            maximumEncodedBytes: PreparedUploadBytes * UploadRestartStoreByteMultiplier,
            retentionOptions: new());
        await InitializeRestartStoreAsync(store);
        return store;
    }

    /// <summary>Runs the second expired exactly-once upload attempt against an in-memory store.</summary>
    /// <param name="store">The initialized store.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <param name="faults">The fault observer.</param>
    /// <param name="serverRetention">The server retention window.</param>
    /// <returns>The second prepared session.</returns>
    private static async Task<PreparedSession> RunExpiredExactlyOnceInMemoryUploadAttemptAsync(
        ILocalStoreAdapter store,
        ManualTimerTimeProvider clock,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TimeSpan serverRetention)
    {
        var secondSession = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention) };
        await using var secondEngine = CreateBorrowedStoreEngine(
            store,
            new RecordingTransport { SessionOverride = secondSession },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var registration = secondEngine.RegisterParticipant(new RecordingParticipant());
        using var faultSubscription = secondEngine.Faults.Subscribe(faults);
        await secondEngine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(secondEngine, clock, secondSession, faults, operationStates: null);
        return secondSession;
    }

    /// <summary>Runs the first ambiguous exactly-once upload attempt against SQLite.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <param name="operation">The operation to upload.</param>
    /// <param name="serverRetention">The server retention window.</param>
    /// <returns>The assertion task.</returns>
    private static async Task RunFirstExactlyOnceSqliteUploadAttemptAsync(
        string databasePath,
        ManualTimerTimeProvider clock,
        SyncOperation operation,
        TimeSpan serverRetention)
    {
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        await SeedRestartUploadOperationAsync(store, operation);
        var session = await RunFirstExactlyOnceUploadAttemptAsync(store, clock, operation.OperationId, serverRetention);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var retryState = await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(status?.Attempt).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(retryState?.StartedUtc).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Runs the first ambiguous exactly-once upload attempt against a restart store.</summary>
    /// <param name="store">The initialized store.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="serverRetention">The server retention window.</param>
    /// <returns>The prepared session.</returns>
    /// <exception cref="TimeoutException">The first upload attempt did not reach dwell, retry anchor, or completion.</exception>
    private static async Task<PreparedSession> RunFirstExactlyOnceUploadAttemptAsync(
        ILocalStoreAdapter store,
        ManualTimerTimeProvider clock,
        OperationId operationId,
        TimeSpan serverRetention)
    {
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention),
            SendException = CreateTransportFailure(
                RetryFailureKind.AmbiguousTransportOutcome,
                TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds)),
        };
        await using var engine = CreateBorrowedStoreEngine(
            store,
            new RecordingTransport { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        try
        {
            await WaitForConditionAsync(() => HasUploadDwellProgress(clock, session, faults));
        }
        catch (TimeoutException exception)
        {
            if (sync.IsCompleted)
            {
                await AwaitSyncWithUploadTraceAsync(sync, session, faults);
            }

            throw new TimeoutException(CreateUploadTrace(session, faults, operationStates: null), exception);
        }

        AdvanceUploadDwellIfStillPending(clock, session, faults);
        _ = await WaitForRetryAnchorWithTraceAsync(store, operationId, session, faults, operationStates: null);
        try
        {
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(session, faults, operationStates: null), exception);
        }

        _ = await WaitForRetryStateWithTraceAsync(
            store,
            operationId,
            session,
            faults,
            operationStates: null,
            static retryState => retryState.TransientAttemptCount > 0);
        await engine.StopAsync(CancellationToken.None);
        await AwaitSyncWithUploadTraceAsync(sync, session, faults);
        return session;
    }

    /// <summary>Asserts the reopened SQLite store refuses an expired exactly-once upload before prepare.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="serverRetention">The server retention window.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertExpiredExactlyOnceSqliteUploadDoesNotPrepareAsync(
        string databasePath,
        ManualTimerTimeProvider clock,
        OperationId operationId,
        TimeSpan serverRetention)
    {
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities(serverRetention) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateBorrowedStoreEngine(
            store,
            new RecordingTransport { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, session, faults, operationStates: null);

        var retryState = await store.GetRetryStateAsync(operationId, CancellationToken.None);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(retryState?.StartedUtc).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
    }

    /// <summary>Creates an initialized SQLite store for sync engine restart tests.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <returns>The initialized SQLite store.</returns>
    private static async Task<SqliteLocalStoreAdapter> CreateEngineSqliteStoreAsync(
        string databasePath,
        TimeProvider clock)
    {
        var store = new SqliteLocalStoreAdapter(databasePath, new() { TimeProvider = clock });
        await InitializeRestartStoreAsync(store);
        return store;
    }

    /// <summary>Initializes a restart test store.</summary>
    /// <param name="store">The store to initialize.</param>
    /// <returns>The initialization task.</returns>
    private static async Task InitializeRestartStoreAsync(ILocalStoreAdapter store) =>
        await store.InitializeAsync(
            new(SyncEngineTestsStoreName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            CancellationToken.None);

    /// <summary>Seeds one exactly-once upload operation into a restart store.</summary>
    /// <param name="store">The initialized restart store.</param>
    /// <param name="operation">The operation to seed.</param>
    /// <returns>The seed task.</returns>
    private static async Task SeedRestartUploadOperationAsync(ILocalStoreAdapter store, SyncOperation operation)
    {
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(
            operation,
            new(
                Stream,
                new("counter-state", 1, "application/json", TestPayload, "hash"),
                FormatVersion: 1,
                ExpectedRevision: 0),
            CancellationToken.None);
    }

    /// <summary>Asserts a leased upload is rejected before remote upload side effects.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="storeCapabilities">The modeled store capabilities.</param>
    /// <param name="negotiatedCapabilities">The acquired session capabilities.</param>
    /// <param name="maximumOperations">The attempted operation limit.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload retry progress is not observed before the guard timeout.</exception>
    private static async Task AssertUploadLeaseRejectedBeforeBarrierAsync(
        IReadOnlyList<SyncOperation> operations,
        LocalStoreCapabilities storeCapabilities,
        NegotiatedCapabilities negotiatedCapabilities,
        int maximumOperations)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateUploadStore(operations, storeCapabilities, clock);
        var session = new PreparedSession(maximumOperations, PreparedUploadBytes) { NegotiatedCapabilities = negotiatedCapabilities };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(maximumOperations, PreparedUploadBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            options,
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, session, faults, operationStates: null);

        await Assert.That(store.LeaseRequests.Count).IsGreaterThan(0);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }
}
