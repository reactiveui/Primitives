// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Engine lifecycle tests for <see cref="SyncEngine"/> snapshot recovery.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a failed inflight head or pre-gap flush stays parked until recovery completes.</summary>
    /// <param name="explicitFlushBeforeGap">Whether a second explicit flush is pending when the send fails.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotRecoveryParksTerminalInflightSendFailure(bool explicitFlushBeforeGap)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var session = CreateFailedPausedSnapshotRecoverySession(releaseGap, releaseRecovery, resultFactory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        Task? uploadSync = null;
        Task? explicitFlush = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(session, uploadSync);
            explicitFlush = explicitFlushBeforeGap ? engine.TriggerSyncAsync(CancellationToken.None).AsTask() : null;
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await Assert.That(explicitFlush is null || !explicitFlush.IsCompleted).IsTrue();
            session.ReleasePausedSendAttempt();
            await uploadSync.WaitAsync(GuardTimeout);
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.GetSnapshotRecoveryRequest(0).PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);

            releaseRecovery.SetResult();
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, CreateSnapshotRecoveryCursor(Stream));
            await AwaitTriggerCompletionAsync(explicitFlush);
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedCapacityCommitAttempts);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
            await WaitForConditionAsync(() => faults.Values.Count == ExpectedCapacityCommitAttempts);
            await Assert.That(faults.Values.TrueForAll(static fault => fault.Code == "OC.Engine.UploadAttempt")).IsTrue();
            await engine.StopAsync(CancellationToken.None);
            var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
            await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
            var status = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            session.ReleasePausedSendAttempt();
            await AwaitTriggerCompletionAsync(explicitFlush);
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Verifies disposal cancels recovery and drops parked network work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryEngineDisposeDropsParkedUploadWithoutRemoteSend()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(
            new(ExpectedSingleOperation),
            CreateVolatilePublishOptions(Stream),
            CancellationToken.None);
        Task? dispose = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);

            dispose = engine.DisposeAsync().AsTask();
            await dispose.WaitAsync(GuardTimeout);
            await Assert.That(session.SnapshotRecoveryCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await AssertRecoveredCursorAsync(store, Stream, Subscription, null);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.SavedLocally);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            if (dispose is not null)
            {
                await ObserveTaskCompletionAsync(dispose).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies stopping the engine preserves a parked upload for the next start.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryEngineStopDefersParkedUploadUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(
            new(ExpectedSingleOperation),
            CreateVolatilePublishOptions(Stream),
            CancellationToken.None);
        Task? stop = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);

            stop = engine.StopAsync(CancellationToken.None).AsTask();
            await stop.WaitAsync(GuardTimeout);
            await Assert.That(session.SnapshotRecoveryCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await AssertRecoveredCursorAsync(store, Stream, Subscription, null);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.SavedLocally);

            releaseRecovery.SetResult();
            await engine.StartAsync(CancellationToken.None);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies stopping a FIFO-admitted recovery releases its permit for the next stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryStopAfterQueuedAdmissionReleasesPermitForNextStream()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateRecoveredSnapshotResultFactoryAsync();
        var session = CreateSnapshotRecoveryAdmissionSession(releaseGap, releaseRecovery, resultFactory);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes) with { MaxConcurrentStreams = 1 };
        var commitGate = CreateSnapshotRecoveryCommitGate(store, SnapshotRecoveryOtherStream);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        await using var first = CreateSnapshotRecoveryStream(store, engine, clock, Stream, Subscription);
        await first.StartAsync(CancellationToken.None);
        Task? stopped = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await using var admitted = CreateSnapshotRecoveryStream(
                store,
                engine,
                clock,
                SnapshotRecoveryOtherStream,
                SnapshotRecoveryOtherSubscription);
            await admitted.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);

            releaseRecovery.SetResult();
            await commitGate.Entered.Task.WaitAsync(GuardTimeout);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedCapacityCommitAttempts);
            stopped = admitted.StopAsync(CancellationToken.None).AsTask();
            await stopped.WaitAsync(GuardTimeout);
            await AssertRecoveredCursorAsync(store, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription, null);

            await AssertSuccessorRecoveryAdmittedAsync(store, engine, clock);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            _ = commitGate.Release.TrySetResult();
            if (stopped is not null)
            {
                await ObserveTaskCompletionAsync(stopped).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies the next stream can recover after the granted recovery is stopped.</summary>
    /// <param name="store">The durable local store.</param>
    /// <param name="engine">The coordinating engine.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSuccessorRecoveryAdmittedAsync(
        InstrumentedSnapshotRecoveryStore store,
        SyncEngine engine,
        ManualTimerTimeProvider clock)
    {
        await using var successor = CreateSnapshotRecoveryStream(
            store,
            engine,
            clock,
            SnapshotRecoveryNextStream,
            SnapshotRecoveryNextSubscription);
        await successor.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForRecoveredCursorAsync(
            store,
            SnapshotRecoveryNextStream,
            SnapshotRecoveryNextSubscription,
            CreateSnapshotRecoveryCursor(SnapshotRecoveryNextStream));
    }

    /// <summary>Creates a paused recovery session whose upload send fails without a retry continuation.</summary>
    /// <param name="releaseGap">The retained-history gap gate.</param>
    /// <param name="releaseRecovery">The remote snapshot response gate.</param>
    /// <param name="resultFactory">The recovered snapshot response factory.</param>
    /// <returns>The configured recovery session.</returns>
    private static ReceiveSession CreateFailedPausedSnapshotRecoverySession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            PauseBeforeSendNumber = ExpectedSingleOperation,
            PreparedSendException = new InvalidOperationException("The prepared send was rejected."),
            SnapshotRecoveryResultFactory = resultFactory,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };
}
