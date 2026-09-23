// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload stale-session renewal tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies stop cancels a stale-session renewal connect without publishing an upload fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncCancelsExpiredSessionRenewalConnectWithoutUploadFault()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(expiredSession);
        var transport = new CancelableReconnectTransport(innerTransport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngineWithStoreAndTransportAdapter(store, transport, options, clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var stopped = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, operation);
            await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
            await transport.ReconnectEntered.Task.WaitAsync(GuardTimeout);
            var stopTask = engine.StopAsync(CancellationToken.None).AsTask();
            await transport.ReconnectCanceled.Task.WaitAsync(GuardTimeout);
            await stopTask.WaitAsync(GuardTimeout);
            stopped = true;

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(innerTransport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(expiredSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(faults.Values).IsEmpty();
        }
        finally
        {
            if (!stopped)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies a stale-session renewal rejects weaker replacement capabilities.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRejectsExpiredSessionRenewalCapabilityDowngrade()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var downgradedSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = new(new(1, 0), RemoteTransportCapabilities.None, ExpectedSingleOperation, PreparedUploadBytes, null, null),
        };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(expiredSession);
        transport.Sessions.Enqueue(downgradedSession);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(expiredSession.DisposeCalls).IsEqualTo(0);
        await Assert.That(downgradedSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(expiredSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies lower or absent renewed peer inbox requirements are accepted.</summary>
    /// <param name="absentRenewedRequirement">Whether the renewed session omits the peer requirement.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UploadAttemptAcceptsExpiredSessionRenewalWithNonIncreasingClientInboxRequirement(
        bool absentRenewedRequirement)
    {
        TimeSpan? renewedRequirement = absentRenewedRequirement
            ? null
            : TimeSpan.FromMinutes(ExpectedSingleOperation);

        await AssertExpiredSessionRenewalCompletesWithClientInboxRequirementAsync(renewedRequirement);
    }

    /// <summary>Verifies an excessive renewed peer inbox requirement is rejected before publication.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task UploadAttemptRejectsExpiredSessionRenewalWhenPeerInboxRequirementExceedsRetention() =>
        AssertExpiredSessionRenewalRejectsClientInboxRequirementAsync(
            TimeSpan.FromMinutes(ExpectedCapacityCommitAttempts),
            CreateOptionsWithInboxRetention(TimeSpan.FromMinutes(ExpectedSingleOperation)),
            RecordingStoreUploadCapabilities);

    /// <summary>Verifies invalid peer inbox requirements are rejected before publication.</summary>
    /// <param name="maximumValueRequirement">Whether the peer requirement uses <see cref="TimeSpan.MaxValue"/>.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task UploadAttemptRejectsExpiredSessionRenewalWhenPeerInboxRequirementIsInvalid(
        bool maximumValueRequirement) =>
        AssertExpiredSessionRenewalRejectsClientInboxRequirementAsync(
            maximumValueRequirement ? TimeSpan.MaxValue : TimeSpan.Zero,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            RecordingStoreUploadCapabilities);

    /// <summary>Verifies peer inbox requirements need a durable inbox store.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task UploadAttemptRejectsExpiredSessionRenewalWhenStoreLacksDurableInbox() =>
        AssertExpiredSessionRenewalRejectsClientInboxRequirementAsync(
            TimeSpan.FromMinutes(ExpectedSingleOperation),
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            RecordingStoreUploadCapabilities & ~LocalStoreCapabilities.DurableInbox);

    /// <summary>Verifies stop disposes a connected renewal candidate when capability validation is canceled.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncDisposesConnectedRenewalCandidateWhenCapabilityValidationCancels()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var candidateReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expiredInner = CreateExpiredUploadSession();
        var expiredSession = new CapabilityThrowingAfterRenewalCandidateSession(
            expiredInner,
            candidateReturned.Task);
        var candidate = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(expiredSession);
        innerTransport.Sessions.Enqueue(candidate);
        var transport = new RenewalCancellationTrackingTransport(innerTransport, candidateReturned);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngineWithStoreAndTransportAdapter(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var stopped = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, operation);
            await DriveUploadDwellWithTraceAsync(clock, store, expiredInner, faults, operationStates: null);
            await expiredSession.ThrowingCapabilityReadEntered.Task.WaitAsync(GuardTimeout);
            var stopTask = engine.StopAsync(CancellationToken.None).AsTask();
            await transport.RenewalCancellationObserved.Task.WaitAsync(GuardTimeout);
            expiredSession.ReleaseThrowingCapabilityRead.SetResult();
            await stopTask.WaitAsync(GuardTimeout);
            stopped = true;
        }
        finally
        {
            _ = expiredSession.ReleaseThrowingCapabilityRead.TrySetResult();
            if (!stopped)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(candidate.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values).IsEmpty();
    }

    /// <summary>Verifies a failed stale-session renewal connect publishes the renewal failure and upload failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPublishesFaultWhenExpiredSessionRenewalConnectFails()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(null);
        transport.ConnectFailures.Enqueue(new InvalidOperationException("renew connect failed"));
        transport.Sessions.Enqueue(expiredSession);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
        await WaitForConditionAsync(() => faults.Values.Count >= ExpectedCapacityCommitAttempts);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == "OC.Engine.SessionRenewal"))
            .IsTrue();
        await Assert.That(faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode))
            .IsTrue();
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies repeated stale-session upload failures do not renew again without durable remote progress.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDoesNotRenewRepeatedExpiredSessionWithoutRemoteProgress()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var first = CreateExpiredUploadSession();
        var second = CreateExpiredUploadSession();
        var unused = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        transport.Sessions.Enqueue(unused);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, store, first, faults, operationStates: null);

        await Assert.That(faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode)).IsTrue();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(first.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(unused.SentBatches.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies AtMostOnce ambiguity survives renewal while another stream can use the renewed session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPreservesAtMostOnceAmbiguityAcrossExpiredSessionRenewal()
    {
        var directory = Directory.CreateTempSubdirectory("oc-engine-renewal-atmostonce-");
        try
        {
            await AssertAtMostOnceAmbiguityAcrossExpiredSessionRenewalAsync(Path.Combine(directory.FullName, "local.db"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies old-generation receive progress cannot reset a renewed session stale-failure budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OldGenerationReceiveProgressAfterRenewalDoesNotPermitAnotherExpiredSessionRenewal()
    {
        var directory = Directory.CreateTempSubdirectory("oc-engine-renewal-old-generation-");
        try
        {
            await AssertOldGenerationReceiveProgressAfterRenewalAsync(Path.Combine(directory.FullName, "local.db"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Asserts old-generation receive progress cannot reopen stale-session renewal with a stream-aware store.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOldGenerationReceiveProgressAfterRenewalAsync(string databasePath)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var operation = CreateOperation();
        await SeedRestartUploadOperationAsync(store, operation);
        var receiveStream = new StreamId("sync/engine/renewal-old-receive");
        var oldReceiveApplyEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOldReceiveApply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (first, second, unused, transport) = CreateOldGenerationReceiveTransport(receiveStream);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateBorrowedStoreEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var uploadRegistration = engine.RegisterParticipant(CreateSqliteUploadParticipant(store, Stream));
        using var receiveRegistration = engine.RegisterParticipant(CreateReceiveParticipant(
            receiveStream,
            oldReceiveApplyEntered,
            releaseOldReceiveApply));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        Task? trigger = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await oldReceiveApplyEntered.Task.WaitAsync(GuardTimeout);
            trigger = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await second.PausedSendEntered.Task.WaitAsync(GuardTimeout);
            releaseOldReceiveApply.SetResult();
            await first.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);
            second.ReleasePausedSendAttempt();
            await trigger.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode));
            await AssertOldGenerationDurableUploadStateAsync(store, operation);

            await AssertOldGenerationReceiveProgressDidNotRenewAgainAsync(transport, first, second, unused);
        }
        finally
        {
            _ = releaseOldReceiveApply.TrySetResult();
            second.ReleasePausedSendAttempt();
            await StopEngineAndObserveTriggerAsync(engine, trigger);
        }
    }

    /// <summary>Asserts failed renewal left the original upload work pending with no retry budget reset.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="operation">The original upload operation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOldGenerationDurableUploadStateAsync(
        SqliteLocalStoreAdapter store,
        SyncOperation operation)
    {
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var retryState = await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(status?.Attempt).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(retryState).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Creates the old-generation receive and renewed upload sessions.</summary>
    /// <param name="receiveStream">The receive stream identity.</param>
    /// <returns>The queued transport and its sessions.</returns>
    private static (ReceiveSession First, PreparedSession Second, PreparedSession Unused, RecordingTransport Transport)
        CreateOldGenerationReceiveTransport(StreamId receiveStream)
    {
        var first = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(receiveStream, previousCursor: null, nextCursor: "old-progress") },
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PreparedSendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var second = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
            SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var unused = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) };

        var transport = CreateQueuedReceiveTransport(first, second, unused);
        return (first, second, unused, transport);
    }

    /// <summary>Asserts old-generation receive progress did not reopen stale-session renewal budget.</summary>
    /// <param name="transport">The shared transport.</param>
    /// <param name="first">The expired shared session.</param>
    /// <param name="second">The renewed shared session.</param>
    /// <param name="unused">The unused third session.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertOldGenerationReceiveProgressDidNotRenewAgainAsync(
        RecordingTransport transport,
        ReceiveSession first,
        PreparedSession second,
        PreparedSession unused)
    {
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(first.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(unused.SentBatches.Count).IsEqualTo(0);
    }

    /// <summary>Asserts durable AtMostOnce ambiguity survives renewal against a SQLite store.</summary>
    /// <param name="databasePath">The physical SQLite database path.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertAtMostOnceAmbiguityAcrossExpiredSessionRenewalAsync(string databasePath)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var sqliteStore = await CreateEngineSqliteStoreAsync(databasePath, clock);
        var store = sqliteStore;
        var firstOperation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
        };
        await SeedRestartUploadOperationAsync(store, firstOperation);
        var expiredSession = CreateExpiredUploadSession();
        var renewedSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(expiredSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateBorrowedStoreEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            clock);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await engine.StartAsync(CancellationToken.None);
        try
        {
            await TriggerAndDrainUploadWithTraceAsync(engine, clock, expiredSession, faults: null, operationStates: null);
            var secondOperation = await SeedOtherStreamOperationAsync(store);
            using var otherRegistration = engine.RegisterParticipant(CreateSqliteUploadParticipant(store, secondOperation.StreamId));
            engine.NotifyLocalCommitReady(secondOperation.StreamId, secondOperation);
            await TriggerAndDrainUploadWithTraceAsync(engine, clock, renewedSession, faults: null, operationStates: null);

            var firstStatus = await store.GetOperationStatusAsync(firstOperation.OperationId, CancellationToken.None);
            var secondStatus = await WaitForSqliteOperationStateAsync(
                store,
                secondOperation.OperationId,
                SyncOperationState.Synchronized);
            await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Ambiguous);
            await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(expiredSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.SentBatches[0].Operations[0].OperationId).IsEqualTo(secondOperation.OperationId);
        }
        finally
        {
            await engine.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>Creates a test engine with a supplied store and composed transport adapter.</summary>
    /// <param name="store">The supplied recording store.</param>
    /// <param name="transport">The composed transport adapter.</param>
    /// <param name="options">The runtime options.</param>
    /// <param name="timeProvider">The test time provider.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateEngineWithStoreAndTransportAdapter(
        RecordingStore store,
        IRemoteTransportAdapter transport,
        OccasionallyConnectedOptions options,
        TimeProvider timeProvider) =>
        new(new()
        {
            Store = store,
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Owned,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = options,
            StoreInitialization = new(StoreInitializationName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = timeProvider,
            MaxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });

    /// <summary>Seeds a pending operation for a different stream.</summary>
    /// <param name="store">The initialized store.</param>
    /// <returns>The seeded operation.</returns>
    private static async Task<SyncOperation> SeedOtherStreamOperationAsync(SqliteLocalStoreAdapter store)
    {
        var otherStream = new StreamId("sync/engine/renewed-other");
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(otherStream, Subscription, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(otherStream, subscriptionId, CancellationToken.None);
        var operation = CreateOperation(
            streamId: otherStream,
            sequence: recovered.NextClientSequence,
            operationId: OperationId.New());
        _ = await store.CommitLocalOperationAsync(
            operation,
            new(
                otherStream,
                new("counter-state", 1, "application/json", TestPayload, "hash"),
                FormatVersion: 1,
                ExpectedRevision: 0),
            CancellationToken.None);
        return operation;
    }

    /// <summary>Creates a participant that reconciles upload results through a SQLite store.</summary>
    /// <param name="store">The durable store receiving reconciliation.</param>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The recording participant.</returns>
    private static RecordingParticipant CreateSqliteUploadParticipant(SqliteLocalStoreAdapter store, StreamId streamId) => new()
    {
        StreamId = streamId,
        OnApplySyncResultAsync = (batch, result, token) => ApplySqliteSyncResultAsync(store, batch, result, token),
    };

    /// <summary>Applies an upload result through the SQLite store.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="batch">The uploaded batch.</param>
    /// <param name="result">The remote upload result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The queue transition.</returns>
    private static async ValueTask<ParticipantQueueTransitionResult> ApplySqliteSyncResultAsync(
        SqliteLocalStoreAdapter store,
        SyncBatch batch,
        RemoteSyncResult result,
        CancellationToken cancellationToken)
    {
        await store.ApplySyncResultAsync(batch.BatchId, result, cancellationToken).ConfigureAwait(false);
        return ParticipantQueueTransitionResult.None;
    }

    /// <summary>Waits for a SQLite operation to reach the expected durable state.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="state">The expected state.</param>
    /// <returns>The observed status.</returns>
    /// <exception cref="TimeoutException">The expected state is not observed before the guard timeout.</exception>
    private static async Task<SyncOperationStatus> WaitForSqliteOperationStateAsync(
        SqliteLocalStoreAdapter store,
        OperationId operationId,
        SyncOperationState state)
    {
        SyncOperationStatus? status = null;
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            status = await store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            if (status?.State == state)
            {
                return status;
            }

            await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException($"Operation {operationId} did not reach {state}; observed {status?.State}.");
    }

    /// <summary>Creates a receive-only participant with a gated remote apply.</summary>
    /// <param name="streamId">The receive stream identity.</param>
    /// <param name="remoteApplyEntered">The signal raised when receive apply begins.</param>
    /// <param name="releaseRemoteApply">The gate that releases receive apply.</param>
    /// <returns>The recording participant.</returns>
    private static RecordingParticipant CreateReceiveParticipant(
        StreamId streamId,
        TaskCompletionSource remoteApplyEntered,
        TaskCompletionSource releaseRemoteApply) => new()
    {
        StreamId = streamId,
        ReceiveSubscription = CreateReceiveSubscription(streamId, null),
        RemoteApplyEntered = remoteApplyEntered,
        ReleaseRemoteApply = releaseRemoteApply,
    };

    /// <summary>Observes a cleanup task without replacing the primary test failure.</summary>
    /// <param name="task">The task to observe.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task ObserveCleanupTaskAsync(Task? task)
    {
        if (task is null)
        {
            return;
        }

        try
        {
            await task.WaitAsync(GuardTimeout).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            GC.KeepAlive(exception);
            GC.KeepAlive(task.Exception);
        }
    }

    /// <summary>Stops the engine and observes a cleanup trigger task.</summary>
    /// <param name="engine">The engine to stop.</param>
    /// <param name="trigger">The trigger task to observe after stop.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task StopEngineAndObserveTriggerAsync(SyncEngine engine, Task? trigger)
    {
        try
        {
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
        finally
        {
            await ObserveCleanupTaskAsync(trigger);
        }
    }

    /// <summary>Asserts stale-session renewal succeeds with a non-increasing peer inbox requirement.</summary>
    /// <param name="renewedRequirement">The renewed session peer inbox requirement.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertExpiredSessionRenewalCompletesWithClientInboxRequirementAsync(
        TimeSpan? renewedRequirement)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateClientInboxRequirementCapabilities(
                TimeSpan.FromMinutes(ExpectedCapacityCommitAttempts)),
            SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var renewedSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { NegotiatedCapabilities = CreateClientInboxRequirementCapabilities(renewedRequirement) };
        var transport = CreateRenewalTransport(expiredSession, renewedSession);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(
            store,
            transport,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized
                || faults.Values.Count != 0,
            store,
            expiredSession,
            faults,
            operationStates: null);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(renewedSession.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewedSession.SentBatches[0].Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(faults.Values).IsEmpty();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Asserts stale-session renewal rejects an unsupported peer inbox requirement.</summary>
    /// <param name="renewedRequirement">The renewed session peer inbox requirement.</param>
    /// <param name="options">The engine options.</param>
    /// <param name="storeCapabilities">The store capabilities.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertExpiredSessionRenewalRejectsClientInboxRequirementAsync(
        TimeSpan renewedRequirement,
        OccasionallyConnectedOptions options,
        LocalStoreCapabilities storeCapabilities)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], storeCapabilities, clock);
        store.RequeueReleasedLeases = true;
        var expiredSession = CreateExpiredUploadSession();
        var rejectedSession = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { NegotiatedCapabilities = CreateClientInboxRequirementCapabilities(renewedRequirement) };
        var transport = CreateRenewalTransport(expiredSession, rejectedSession);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, expiredSession, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode),
            store,
            expiredSession,
            faults,
            operationStates: null);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(rejectedSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(rejectedSession.SentBatches.Count).IsEqualTo(0);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == "OC.Engine.SessionRenewal"))
            .IsTrue();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Creates a transport that returns an expired session and then a renewed session.</summary>
    /// <param name="expiredSession">The expired session.</param>
    /// <param name="renewedSession">The renewed session.</param>
    /// <returns>The recording transport.</returns>
    private static RecordingTransport CreateRenewalTransport(
        IRemoteTransportSession expiredSession,
        IRemoteTransportSession renewedSession)
    {
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(expiredSession);
        transport.Sessions.Enqueue(renewedSession);
        return transport;
    }

    /// <summary>Creates options with a specific client inbox retention window.</summary>
    /// <param name="inboxRetention">The configured local inbox retention.</param>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateOptionsWithInboxRetention(TimeSpan inboxRetention) =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retention = OccasionallyConnectedOptions.Default.Retention with
            {
                InboxDeduplicationRetention = inboxRetention,
            },
        };

    /// <summary>Creates capabilities with an explicit peer inbox retention requirement.</summary>
    /// <param name="clientInboxRetentionRequired">The peer-required client inbox retention window.</param>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateClientInboxRequirementCapabilities(TimeSpan? clientInboxRetentionRequired) =>
        new(
            new(1, 0),
            RecordingTransportUploadCapabilities,
            ExpectedSingleOperation,
            PreparedUploadBytes,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            clientInboxRetentionRequired);

    /// <summary>Creates a prepared session that reports the shared remote session as expired.</summary>
    /// <returns>The prepared session.</returns>
    private static PreparedSession CreateExpiredUploadSession() =>
        new(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
}
