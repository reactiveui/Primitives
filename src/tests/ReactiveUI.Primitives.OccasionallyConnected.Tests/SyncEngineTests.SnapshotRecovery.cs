// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery orchestration tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The expired cursor used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryExpiredCursor = "expired-cursor";

    /// <summary>The recovered cursor used by legacy single-stream snapshot recovery tests.</summary>
    private const string SnapshotRecoveryRecoveredCursor = "recovered-frontier";

    /// <summary>The features supported by the snapshot recovery test peer.</summary>
    private const RemoteTransportCapabilities SnapshotRecoveryRemoteFeatures =
        RecordingTransportUploadCapabilities | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The rejected counter value used by reversed disposition tests.</summary>
    private const int SnapshotRecoveryRejectedCounterValue = 2;

    /// <summary>The unknown counter value used by reversed disposition tests.</summary>
    private const int SnapshotRecoveryUnknownCounterValue = 3;

    /// <summary>The disposition count used by reversed snapshot recovery results.</summary>
    private const int SnapshotRecoveryReversedDispositionCount = 3;

    /// <summary>The tiny byte budget used by bounded recovery limit tests.</summary>
    private const int TinySnapshotRecoveryBytes = 16;

    /// <summary>The state contract identifier used by counter snapshot recovery tests.</summary>
    private const string SnapshotRecoveryCounterStateContractId = "counter-state";

    /// <summary>The independent stream that proves recovery parking is scoped to one stream.</summary>
    private static readonly StreamId SnapshotRecoveryOtherStream = new("sync/engine/other");

    /// <summary>The subscription identifier for the independent stream.</summary>
    private static readonly SubscriptionId SnapshotRecoveryOtherSubscription = new(Guid.Parse("0e547814-37f8-4f2e-9b7f-5912e2afcb87"));

    /// <summary>The next stream that proves canceled recovery admission does not leak capacity.</summary>
    private static readonly StreamId SnapshotRecoveryNextStream = new("sync/engine/next");

    /// <summary>The subscription identifier for the next recovery stream.</summary>
    private static readonly SubscriptionId SnapshotRecoveryNextSubscription = new(Guid.Parse("d78f8f61-33f1-4775-82cc-031e55bb35a7"));

    /// <summary>Verifies retained-history gap recovery waits for existing upload ownership before bounded capture.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryWaitsForInflightUploadBeforeBoundedCaptureRequest()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        await Assert.That(store.InitializeCalls).IsEqualTo(ExpectedSingleOperation);
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySession(releaseGap, releaseRecovery, ExpectedSingleOperation);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        store.TrackExistingUpload(receipt.OperationId);

        Task? uploadSync = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(session, uploadSync);
            releaseGap.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await Assert.That(store.CaptureRequestCount).IsEqualTo(0);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);

            session.ReleasePausedSendAttempt();
            await uploadSync.WaitAsync(GuardTimeout);
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(store.CaptureRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.ExistingUploadStatusAtFirstCapture?.State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(store.TrackedUploadSyncResultCompletedAtFirstCapture).IsTrue();
            await Assert.That(store.TrackedUploadSyncResultLeaseAtFirstCapture.HasValue).IsTrue();
            await Assert.That(session.GetSnapshotRecoveryRequest(0).ExpiredCursor).IsEqualTo(SnapshotRecoveryExpiredCursor);
            await Assert.That(session.GetSnapshotRecoveryRequest(0).PendingOperations.Count).IsEqualTo(0);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            session.ReleasePausedSendAttempt();
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Verifies recovery transport does not block local publication or release same-stream upload parking.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryKeepsLocalPublicationResponsiveWhileNetworkHeld()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        await Assert.That(store.InitializeCalls).IsEqualTo(ExpectedSingleOperation);
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySession(releaseGap, releaseRecovery);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        var observer = new RecordingObserver<ReceiveCounterState>();
        await using var recovering = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await using var independent = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            SnapshotRecoveryOtherStream,
            SnapshotRecoveryOtherSubscription,
            receiveEnabled: false);
        using var subscription = recovering.Local.Subscribe(observer);
        await recovering.StartAsync(CancellationToken.None);
        await independent.StartAsync(CancellationToken.None);
        var recoveryCallsBeforeGap = store.GetRecoverStreamCalls(Stream);
        Task? independentSync = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);

            var localReceipt = await recovering.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None)
                .AsTask()
                .WaitAsync(GuardTimeout);
            var independentReceipt = await independent.PublishAsync(
                    new(ExpectedSingleOperation),
                    CreateVolatilePublishOptions(SnapshotRecoveryOtherStream),
                    CancellationToken.None)
                .AsTask()
                .WaitAsync(GuardTimeout);
            independentSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await session.SentBatchEntered.Task.WaitAsync(GuardTimeout);
            await WaitForOperationStateAsync(store, independentReceipt.OperationId, SyncOperationState.Synchronized);

            await Assert.That(localReceipt.ClientSequence).IsEqualTo(FirstSequence);
            await Assert.That(observer.Values[^1].Sum).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.HasSentBatchForStream(SnapshotRecoveryOtherStream)).IsTrue();
            await Assert.That(session.HasPreparedBatchForStream(Stream)).IsFalse();
            await Assert.That(store.GetRecoverStreamCalls(Stream)).IsEqualTo(recoveryCallsBeforeGap);
            await AssertSnapshotRecoveryRequestAsync(session.GetSnapshotRecoveryRequest(0));
        }
        finally
        {
            await ReleaseSnapshotRecoveryFixtureAsync(releaseGap, releaseRecovery, independentSync).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies recovered dispositions are matched by operation identity and publish terminal states.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryMatchesReversedDispositionsByOperationIdentity()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        var operationObserver = new RecordingObserver<SyncOperationStatus>();
        using var operationSubscription = stream.OperationStates.Subscribe(operationObserver);
        await stream.StartAsync(CancellationToken.None);
        var accepted = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        var rejected = await stream.PublishAsync(new(SnapshotRecoveryRejectedCounterValue), CreateVolatilePublishOptions(Stream, "rv"), CancellationToken.None);
        var unknown = await stream.PublishAsync(
            new(SnapshotRecoveryUnknownCounterValue),
            CreateVolatilePublishOptions(Stream, "retained-replay-version"),
            CancellationToken.None);
        session.SnapshotRecoveryResult = await CreateRecoveredSnapshotResultAsync(accepted, rejected, unknown);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
        releaseGap.SetResult();
        await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
        releaseRecovery.SetResult();
        await session.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(accepted.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(unknown.OperationId);
        await Assert.That(SyncEngine.GetOperationRetainedBytes(recovered.PendingOperations[0])).IsGreaterThan(0);
        await Assert.That(SyncEngine.GetOperationRetainedBytes(recovered.PendingOperations[1])).IsGreaterThan(0);
        await Assert.That(session.SnapshotRecoveryResult.OperationDispositions.Count).IsEqualTo(SnapshotRecoveryReversedDispositionCount);
        await Assert.That((await store.GetOperationStatusAsync(accepted.OperationId, CancellationToken.None))?.State)
            .IsEqualTo(SyncOperationState.Conflict);
        await Assert.That((await store.GetOperationStatusAsync(rejected.OperationId, CancellationToken.None))?.State)
            .IsEqualTo(SyncOperationState.Rejected);
        await Assert.That((await store.GetOperationStatusAsync(unknown.OperationId, CancellationToken.None))?.State)
            .IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(HasObservedOperationState(operationObserver, accepted.OperationId, SyncOperationState.Conflict)).IsTrue();
        await Assert.That(HasObservedOperationState(operationObserver, rejected.OperationId, SyncOperationState.Rejected)).IsTrue();
        await Assert.That(HasObservedOperationState(operationObserver, unknown.OperationId, SyncOperationState.SavedLocally)).IsTrue();
    }

    /// <summary>Verifies snapshot recovery honors configured message limits before remote request allocation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryUsesConfiguredMessageLimitBeforeRemoteRequest()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySession(releaseGap, releaseRecovery);
        var options = CreateTinySnapshotRecoveryOptions();
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
        releaseGap.SetResult();
        await WaitForConditionAsync(() => store.CaptureRequestCount > 0);

        await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);
        _ = releaseRecovery.TrySetResult();
    }

    /// <summary>Verifies canceled recovery admission does not leak capacity or block later recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryAdmissionCancelAllowsNextQueuedRecoveryAfterFirstRelease()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateRecoveredSnapshotResultFactoryAsync();
        var session = CreateSnapshotRecoveryAdmissionSession(releaseGap, releaseRecovery, resultFactory);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes) with { MaxConcurrentStreams = 1 };
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var active = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await active.StartAsync(CancellationToken.None);
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>? queued = null;
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>? next = null;
        Task? stopQueued = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(CreateSnapshotRecoveryRequestTrace(session)).IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream));
            var captureCountAfterActiveRecovery = store.CaptureRequestCount;
            queued = CreateSnapshotRecoveryStream(store, engine, clock, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription);
            await queued.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.CaptureRequestCount).IsEqualTo(captureCountAfterActiveRecovery);

            stopQueued = queued.StopAsync(CancellationToken.None).AsTask();
            await stopQueued.WaitAsync(GuardTimeout);
            await Assert.That(CreateSnapshotRecoveryRequestTrace(session)).IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream));
            await Assert.That(CreateCanceledSnapshotRecoveryRequestTrace(session)).IsEqualTo(string.Empty);

            next = CreateSnapshotRecoveryStream(store, engine, clock, SnapshotRecoveryNextStream, SnapshotRecoveryNextSubscription);
            await next.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
            await Assert.That(CreateCanceledSnapshotRecoveryRequestTrace(session)).IsEqualTo(string.Empty);
            await Assert.That(CreateSnapshotRecoveryRequestTrace(session)).IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream));

            releaseRecovery.SetResult();
            await WaitForConditionAsync(() => session.SnapshotRecoveryRequestCount == ExpectedCapacityCommitAttempts);
            await WaitForRecoveredCursorAsync(
                store,
                SnapshotRecoveryNextStream,
                SnapshotRecoveryNextSubscription,
                CreateSnapshotRecoveryCursor(SnapshotRecoveryNextStream));
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
        finally
        {
            await ReleaseSnapshotRecoveryFixtureAsync(releaseGap, releaseRecovery, stopQueued, queued, next, releaseCommit: null).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies restarted queued recovery keeps upload work parked until recovery commits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryRestartKeepsQueuedUploadParkedUntilRecoveryCommit()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateUnknownRecoveredSnapshotResultFactoryAsync();
        var session = CreateSnapshotRecoveryAdmissionSession(releaseGap, releaseRecovery, resultFactory);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes) with { MaxConcurrentStreams = 1 };
        var commitGate = CreateSnapshotRecoveryCommitGate(store, SnapshotRecoveryOtherStream);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var active = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await active.StartAsync(CancellationToken.None);
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>? queued = null;
        Task? stopQueued = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(CreateSnapshotRecoveryRequestTrace(session)).IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream));
            queued = CreateSnapshotRecoveryStream(store, engine, clock, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription);
            await queued.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts);
            var receipt = await queued.PublishAsync(
                new(ExpectedSingleOperation),
                CreateVolatilePublishOptions(SnapshotRecoveryOtherStream),
                CancellationToken.None);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            stopQueued = queued.StopAsync(CancellationToken.None).AsTask();
            await stopQueued.WaitAsync(GuardTimeout);
            await queued.StartAsync(CancellationToken.None);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);

            releaseRecovery.SetResult();
            await commitGate.Entered.Task.WaitAsync(GuardTimeout);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await AssertRecoveredCursorAsync(store, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription, null);
            commitGate.Release.SetResult();
            await WaitForRecoveredCursorAsync(
                store,
                SnapshotRecoveryOtherStream,
                SnapshotRecoveryOtherSubscription,
                CreateSnapshotRecoveryCursor(SnapshotRecoveryOtherStream));
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            await ReleaseSnapshotRecoveryFixtureAsync(releaseGap, releaseRecovery, stopQueued, queued, null, commitGate.Release)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Verifies durable recovery completion releases parked uploads even when cursor acknowledgement is lost.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryReleasesParkedUploadsAfterRecoveredCommitWhenAcknowledgementFails()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var result = await CreateRecoveredSnapshotResultAsync();
        var session = CreateSnapshotRecoveryLostAcknowledgementSession(releaseGap, releaseRecovery, result);
        var options = CreateSnapshotRecoveryRetryOptions();
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        Task? uploadSync = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            releaseRecovery.SetResult();
            await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));
            clock.Advance(TimeSpan.FromSeconds(1));
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

            var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Verifies parked uploads survive stream stop during recovery and resume after restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryDefersParkedUploadAfterStreamStopUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoveryBatchingSession(releaseGap, releaseRecovery);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        session.SnapshotRecoveryResult = await CreateUnknownRecoveredSnapshotResultAsync(receipt);
        Task? stopStream = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);

            stopStream = stream.StopAsync(CancellationToken.None).AsTask();
            releaseRecovery.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

            await Assert.That(session.SentBatches.Count).IsEqualTo(0);

            await stream.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => session.SnapshotRecoveryRequestCount == ExpectedCapacityCommitAttempts);
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, SnapshotRecoveryRecoveredCursor);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Creates a receive session configured for snapshot recovery tests.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The configured session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReceiveSession CreateSnapshotRecoverySession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery) =>
        CreateSnapshotRecoverySessionCore(releaseGap, releaseRecovery, pauseBeforeSendNumber: 0);

    /// <summary>Creates a receive session configured for snapshot recovery tests.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="pauseBeforeSendNumber">The one-based send attempt to pause.</param>
    /// <returns>The configured session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReceiveSession CreateSnapshotRecoverySession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        int pauseBeforeSendNumber) =>
        CreateSnapshotRecoverySessionCore(releaseGap, releaseRecovery, pauseBeforeSendNumber);

    /// <summary>Creates a receive session configured for snapshot recovery tests.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="pauseBeforeSendNumber">The one-based send attempt to pause.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoverySessionCore(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        int pauseBeforeSendNumber) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            PauseBeforeSendNumber = pauseBeforeSendNumber,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates a receive session with two-operation batch negotiation.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoveryBatchingSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedCapacityCommitAttempts,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates a receive session that emits request-matched recovery gaps.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="resultFactory">The request-matched recovered snapshot result factory.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoveryAdmissionSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory) =>
        new()
        {
            SubscriptionGapFactory = CreateSnapshotRecoveryGap,
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedCapacityCommitAttempts + ExpectedSingleOperation,
            SnapshotRecoveryResultFactory = resultFactory,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates a snapshot recovery session whose first cursor acknowledgement is lost.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="result">The recovered snapshot result.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoveryLostAcknowledgementSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        RemoteSnapshotRecoveryResult result) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)),
            AcknowledgeExceptionLimit = ExpectedSingleOperation,
            SnapshotRecoveryResult = result,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates options with a tiny recovery message budget.</summary>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateTinySnapshotRecoveryOptions() =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, TinySnapshotRecoveryBytes) with
        {
            Security = OccasionallyConnectedOptions.Default.Security with
            {
                MaximumPayloadBytes = TinySnapshotRecoveryBytes,
                MaximumMessageBytes = TinySnapshotRecoveryBytes,
            },
        };

    /// <summary>Creates options with deterministic receive retry delay.</summary>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateSnapshotRecoveryRetryOptions() =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts + ExpectedSingleOperation,
            },
        };

    /// <summary>Creates a recovered snapshot response with no pending operations.</summary>
    /// <returns>The remote recovery result.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateRecoveredSnapshotResultAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, default(ReceiveCounterState), CancellationToken.None)
            .ConfigureAwait(false);
        return new() { Status = RemoteSnapshotRecoveryStatus.Recovered, Checkpoint = CreateRecoveredCheckpoint(payload), OperationDispositions = [] };
    }

    /// <summary>Creates a recovered snapshot response with deliberately reversed dispositions.</summary>
    /// <param name="accepted">The accepted local receipt.</param>
    /// <param name="rejected">The rejected local receipt.</param>
    /// <param name="unknown">The unknown local receipt.</param>
    /// <returns>The remote recovery result.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateRecoveredSnapshotResultAsync(
        PublishReceipt accepted,
        PublishReceipt rejected,
        PublishReceipt unknown)
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, new ReceiveCounterState(1), CancellationToken.None)
            .ConfigureAwait(false);
        return new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions =
            [
                new() { OperationId = unknown.OperationId, Kind = SnapshotOperationDispositionKind.Unknown },
                CreateRejectedDisposition(rejected.OperationId),
                CreateConflictDisposition(accepted.OperationId),
            ],
        };
    }

    /// <summary>Creates request-matched recovered snapshot responses with no pending operations.</summary>
    /// <returns>The remote recovery result factory.</returns>
    private static async ValueTask<Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>> CreateRecoveredSnapshotResultFactoryAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, default(ReceiveCounterState), CancellationToken.None)
            .ConfigureAwait(false);
        return request => new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload, request.StreamId, request.SubscriptionId),
            OperationDispositions = [],
        };
    }

    /// <summary>Creates request-matched recovered snapshot responses that preserve all pending operations.</summary>
    /// <returns>The remote recovery result factory.</returns>
    private static async ValueTask<Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>> CreateUnknownRecoveredSnapshotResultFactoryAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, default(ReceiveCounterState), CancellationToken.None)
            .ConfigureAwait(false);
        return request =>
        {
            var dispositions = new List<SnapshotOperationDisposition>(request.PendingOperations.Count);
            foreach (var operation in request.PendingOperations)
            {
                dispositions.Add(new() { OperationId = operation.OperationId, Kind = SnapshotOperationDispositionKind.Unknown });
            }

            return new()
            {
                Status = RemoteSnapshotRecoveryStatus.Recovered,
                Checkpoint = CreateRecoveredCheckpoint(payload, request.StreamId, request.SubscriptionId),
                OperationDispositions = dispositions,
            };
        };
    }

    /// <summary>Creates a recovered snapshot response that preserves one pending operation.</summary>
    /// <param name="unknown">The operation preserved by recovery.</param>
    /// <returns>The remote recovery result.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateUnknownRecoveredSnapshotResultAsync(
        PublishReceipt unknown)
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, default(ReceiveCounterState), CancellationToken.None)
            .ConfigureAwait(false);
        return new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions =
            [
                new() { OperationId = unknown.OperationId, Kind = SnapshotOperationDispositionKind.Unknown },
            ],
        };
    }

    /// <summary>Creates a recovered checkpoint payload binding.</summary>
    /// <param name="payload">The checkpoint state payload.</param>
    /// <returns>The recovered checkpoint.</returns>
    private static RemoteSnapshotCheckpoint CreateRecoveredCheckpoint(PayloadEnvelope payload) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = Subscription,
            FrontierCursor = SnapshotRecoveryRecoveredCursor,
            ServerVersion = "snapshot-version",
            SnapshotFormatVersion = ExpectedSingleOperation,
            ClientState = payload,
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };

    /// <summary>Creates a recovered checkpoint payload binding.</summary>
    /// <param name="payload">The checkpoint state payload.</param>
    /// <param name="streamId">The checkpoint stream identity.</param>
    /// <param name="subscriptionId">The checkpoint subscription identity.</param>
    /// <returns>The recovered checkpoint.</returns>
    private static RemoteSnapshotCheckpoint CreateRecoveredCheckpoint(
        PayloadEnvelope payload,
        StreamId streamId,
        SubscriptionId subscriptionId) =>
        new()
        {
            StreamId = streamId,
            SubscriptionId = subscriptionId,
            FrontierCursor = CreateSnapshotRecoveryCursor(streamId),
            ServerVersion = "snapshot-version",
            SnapshotFormatVersion = ExpectedSingleOperation,
            ClientState = payload,
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };

    /// <summary>Creates a deterministic recovered cursor for a stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The recovered cursor.</returns>
    private static string CreateSnapshotRecoveryCursor(StreamId streamId) =>
        $"recovered-frontier-{streamId.Value.Replace('/', '-')}";

    /// <summary>Creates a conflict recovery disposition.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <returns>The conflict disposition.</returns>
    private static SnapshotOperationDisposition CreateConflictDisposition(OperationId operationId) =>
        new()
        {
            OperationId = operationId,
            Kind = SnapshotOperationDispositionKind.IncludedAccepted,
            Result = new(operationId, OperationResultKind.Conflict, ReasonCode: "snapshot-conflict", ServerVersion: "conflict-version"),
        };

    /// <summary>Creates a rejected recovery disposition.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <returns>The rejected disposition.</returns>
    private static SnapshotOperationDisposition CreateRejectedDisposition(OperationId operationId) =>
        new()
        {
            OperationId = operationId,
            Kind = SnapshotOperationDispositionKind.TerminalRejected,
            Result = new(operationId, OperationResultKind.Rejected, ReasonCode: "snapshot-rejected", ServerVersion: null),
        };

    /// <summary>Checks whether an operation status observer saw a state.</summary>
    /// <param name="observer">The status observer.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="state">The expected state.</param>
    /// <returns>Whether the status was observed.</returns>
    private static bool HasObservedOperationState(
        RecordingObserver<SyncOperationStatus> observer,
        OperationId operationId,
        SyncOperationState state)
    {
        var values = observer.Values;
        for (var index = 0; index < values.Count; index++)
        {
            if (values[index].OperationId == operationId && values[index].State == state)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Creates the retained-history gap used by snapshot recovery tests.</summary>
    /// <returns>The gap exception.</returns>
    private static RemoteSubscriptionRetentionGapException CreateSnapshotRecoveryGap() =>
        new(Stream, Subscription, SnapshotRecoveryExpiredCursor, reasonCode: "retention-gap");

    /// <summary>Creates a retained-history gap that matches a subscribe request.</summary>
    /// <param name="request">The subscribe request that observed the gap.</param>
    /// <returns>The gap exception.</returns>
    private static RemoteSubscriptionRetentionGapException CreateSnapshotRecoveryGap(RemoteSubscribeRequest request) =>
        new(request.StreamId, request.SubscriptionId, SnapshotRecoveryExpiredCursor, reasonCode: "retention-gap");

    /// <summary>Waits for the tracked upload to reach the paused send gate.</summary>
    /// <param name="session">The receive session that pauses the upload send.</param>
    /// <param name="uploadSync">The explicit upload trigger.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The upload did not reach the paused send gate.</exception>
    private static async Task AwaitPausedSendEnteredAsync(ReceiveSession session, Task uploadSync)
    {
        try
        {
            await session.PausedSendEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateSnapshotRecoveryUploadTrace(session, uploadSync), exception);
        }
    }

    /// <summary>Waits for the trigger task during cleanup after all recovery gates are released.</summary>
    /// <param name="uploadSync">The optional upload trigger.</param>
    /// <returns>The wait task.</returns>
    private static async Task AwaitTriggerCompletionAsync(Task? uploadSync)
    {
        if (uploadSync is null)
        {
            return;
        }

        await uploadSync.WaitAsync(GuardTimeout).ConfigureAwait(false);
    }

    /// <summary>Creates a compact timeout trace for the inflight upload gate.</summary>
    /// <param name="session">The receive session under observation.</param>
    /// <param name="uploadSync">The explicit upload trigger.</param>
    /// <returns>The trace text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSnapshotRecoveryUploadTrace(ReceiveSession session, Task uploadSync) =>
        $"prepare={session.PrepareCalls};prepared={session.PreparedBatches.Count};"
        + $"sent={session.SentBatches.Count};sync={uploadSync.Status};"
        + $"gapReady={session.SubscriptionGapReady.Task.IsCompleted};"
        + $"paused={session.PausedSendEntered.Task.IsCompleted};"
        + $"snapshot={session.SnapshotRecoveryEntered.Task.IsCompleted}";

    /// <summary>Creates the snapshot recovery request stream trace.</summary>
    /// <param name="session">The receive session under observation.</param>
    /// <returns>The request stream trace.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSnapshotRecoveryRequestTrace(ReceiveSession session) =>
        CreateSnapshotRecoveryRequestTrace(session.GetSnapshotRecoveryRequestStreams());

    /// <summary>Creates the snapshot recovery request stream trace.</summary>
    /// <param name="streamIds">The stream identifiers to format.</param>
    /// <returns>The request stream trace.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSnapshotRecoveryRequestTrace(params StreamId[] streamIds) => string.Join("|", streamIds);

    /// <summary>Creates the canceled snapshot recovery request stream trace.</summary>
    /// <param name="session">The receive session under observation.</param>
    /// <returns>The canceled request stream trace.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateCanceledSnapshotRecoveryRequestTrace(ReceiveSession session) =>
        CreateSnapshotRecoveryRequestTrace(session.GetCanceledSnapshotRecoveryRequestStreams());

    /// <summary>Waits for a recovered stream cursor to become durable.</summary>
    /// <param name="store">The store containing the recovered stream state.</param>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="subscriptionId">The subscription identity.</param>
    /// <param name="cursor">The expected durable cursor.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The durable cursor was not observed before the guard timeout.</exception>
    private static async Task WaitForRecoveredCursorAsync(
        ILocalStoreAdapter store,
        StreamId streamId,
        SubscriptionId subscriptionId,
        string cursor)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            var recovered = await store.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None).ConfigureAwait(false);
            if (recovered.ServerCursor == cursor)
            {
                return;
            }

            await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("The expected recovered cursor was not observed.");
    }

    /// <summary>Asserts the current recovered stream cursor.</summary>
    /// <param name="store">The store containing the recovered stream state.</param>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="subscriptionId">The subscription identity.</param>
    /// <param name="cursor">The expected durable cursor.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveredCursorAsync(
        ILocalStoreAdapter store,
        StreamId streamId,
        SubscriptionId subscriptionId,
        string? cursor)
    {
        var recovered = await store.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None).ConfigureAwait(false);
        await Assert.That(recovered.ServerCursor).IsEqualTo(cursor);
    }

    /// <summary>Waits for an operation to reach a target state.</summary>
    /// <param name="store">The store containing the operation status.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="state">The expected state.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The operation did not reach the requested state before the guard timeout.</exception>
    private static async Task WaitForOperationStateAsync(
        ILocalStoreAdapter store,
        OperationId operationId,
        SyncOperationState state)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            if (status?.State == state)
            {
                return;
            }

            await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("The expected operation state was not observed.");
    }

    /// <summary>Asserts the remote request carries bounded client-state metadata.</summary>
    /// <param name="request">The request to assert.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSnapshotRecoveryRequestAsync(RemoteSnapshotRecoveryRequest request)
    {
        await Assert.That(request.StreamId).IsEqualTo(Stream);
        await Assert.That(request.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(request.ClientStateContractId).IsEqualTo("counter-state");
        await Assert.That(request.ClientStateSchemaVersion).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(request.SnapshotFormatVersion).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(request.MaximumResponseBytes).IsGreaterThan(0);
    }
}
