// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Engine-owned snapshot recovery coverage tests for public recovery coordination paths.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The radix that makes local projection order visible.</summary>
    private const int OrderedProjectionRadix = 10;

    /// <summary>The state from replaying pending inputs two then three after the authoritative checkpoint.</summary>
    private const int ExpectedOrderedRecoveryState = 23;

    /// <summary>Stores the missing diagnostic marker.</summary>
    private const string MissingDiagnosticValue = "<none>";

    /// <summary>Verifies recovered commits skip cursor acknowledgement when the peer did not negotiate it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryWithoutReceiveAcknowledgementsCommitsWithoutRemoteAck()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoveryNoAcknowledgementSession(releaseGap, releaseRecovery);
        session.SnapshotRecoveryResult = await CreateRecoveredCounterStateResultAsync(ExpectedSingleOperation);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        await using var stream = CreateAtMostOnceSnapshotRecoveryCounterStream(store, engine, clock);
        var local = new RecordingObserver<ReceiveCounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        await stream.StartAsync(CancellationToken.None);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            releaseRecovery.SetResult();
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, SnapshotRecoveryRecoveredCursor);

            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNotNull();
            await AssertRecoveredCounterStateAsync(store, ExpectedSingleOperation).ConfigureAwait(false);
            await AssertLocalCounterStateAsync(local, ExpectedSingleOperation).ConfigureAwait(false);
            await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));
            await Assert.That(session.Acknowledgements).IsEmpty();
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Verifies a negotiated recovery facet must be implemented by the active receive session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryRejectsNegotiatedSessionWithoutRecoveryFacetBeforeCapture()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateNonSnapshotRecoverySession(releaseGap);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with { MaximumRetryAttempts = 0 },
        };
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);

        await stream.StartAsync(CancellationToken.None);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await WaitForConditionAsync(() => HasReceivePumpFault<InvalidOperationException>(faults, Stream));

            await Assert.That(store.CaptureRequestCount).IsEqualTo(0);
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await AssertRecoveredCursorAsync(store, Stream, Subscription, null).ConfigureAwait(false);
            await Assert.That(session.Acknowledgements).IsEmpty();
        }
        finally
        {
            _ = releaseGap.TrySetResult();
        }
    }

    /// <summary>Verifies a scheduled same-stream upload is parked while snapshot recovery owns the stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryParksScheduledSameStreamUploadUntilRecoveryCompletes()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var session = CreateSnapshotRecoveryAdmissionSession(releaseGap, releaseRecovery, resultFactory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        Task? uploadSync = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            var receipt = await stream.PublishAsync(
                new(ExpectedSingleOperation),
                CreateVolatilePublishOptions(Stream),
                CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);

            await Assert.That(session.HasSentBatchForStream(Stream)).IsFalse();
            releaseRecovery.SetResult();
            await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));
            clock.Advance(TimeSpan.FromSeconds(1));
            await WaitForConditionAsync(() => session.SnapshotRecoveryRequestCount == ExpectedCapacityCommitAttempts);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
            await WaitForConditionAsync(() => session.HasSentBatchForStream(Stream));
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies canceling a queued recovery waiter removes it without blocking the next stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryCancelQueuedAdmissionAllowsNextQueuedRecoveryAfterFirstRelease()
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
        await using var active = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: true);
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
            var captureCount = store.CaptureRequestCount;
            queued = CreateSnapshotRecoveryStream(store, engine, clock, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription);
            await queued.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts);
            stopQueued = queued.StopAsync(CancellationToken.None).AsTask();
            await stopQueued.WaitAsync(GuardTimeout);
            next = CreateSnapshotRecoveryStream(store, engine, clock, SnapshotRecoveryNextStream, SnapshotRecoveryNextSubscription);
            await next.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => session.SubscribeCompletedCount >= ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
            await AssertQueuedAdmissionBeforeFirstReleaseAsync(session, store, captureCount).ConfigureAwait(false);

            releaseRecovery.SetResult();
            await WaitForConditionAsync(() => session.SnapshotRecoveryRequestCount == ExpectedCapacityCommitAttempts);
            await Assert.That(CreateSnapshotRecoveryRequestTrace(session))
                .IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream, SnapshotRecoveryNextStream));
            await WaitForRecoveredCursorAsync(
                store,
                SnapshotRecoveryNextStream,
                SnapshotRecoveryNextSubscription,
                CreateSnapshotRecoveryCursor(SnapshotRecoveryNextStream));
        }
        finally
        {
            await ReleaseSnapshotRecoveryFixtureAsync(releaseGap, releaseRecovery, stopQueued, queued, next, releaseCommit: null)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Verifies unregistering a stream drops upload work parked for incomplete recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryUnregisterDropsParkedUploadWithoutRemoteSend()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var session = CreateSnapshotRecoveryAdmissionSession(releaseGap, releaseRecovery, resultFactory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        Task? uploadSync = null;
        var streamDisposed = false;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            var receipt = await stream.PublishAsync(
                new(ExpectedSingleOperation),
                CreateVolatilePublishOptions(Stream),
                CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
            await Assert.That(session.HasSentBatchForStream(Stream)).IsFalse();

            await stream.DisposeAsync().AsTask().WaitAsync(GuardTimeout).ConfigureAwait(false);
            streamDisposed = true;
            await WaitForConditionAsync(() => session.SnapshotRecoveryCancellationCount == ExpectedSingleOperation);
            releaseRecovery.SetResult();
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.HasSentBatchForStream(Stream)).IsFalse();
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.SavedLocally).ConfigureAwait(false);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
            if (!streamDisposed)
            {
                await stream.DisposeAsync().AsTask().WaitAsync(GuardTimeout).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a retained-history gap for another subscription is not recovered as this stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryIgnoresRetainedGapForDifferentSubscription()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateMismatchedSnapshotRecoveryGapSession(releaseGap);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await WaitForConditionAsync(() => HasReceivePumpFault<InvalidOperationException>(faults, Stream));
            await Assert.That(GetReceivePumpFaultDiagnosticMessage(faults, Stream))
                .IsEqualTo(typeof(RemoteSubscriptionRetentionGapException).ToString());

            await Assert.That(store.CaptureRequestCount).IsEqualTo(0);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);
            await Assert.That(session.Acknowledgements).IsEmpty();
            await AssertRecoveredCursorAsync(store, Stream, Subscription, null).ConfigureAwait(false);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
        }
    }

    /// <summary>Verifies active upload completion parks a pending same-stream flush until recovery commits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryActiveUploadCompletionParksPendingExplicitFlush()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var session = CreatePausedSnapshotRecoverySession(releaseGap, releaseRecovery, resultFactory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStreamWithProjection(
            store,
            engine,
            new ReceiveOrderedProjection(),
            clock,
            new(Stream, Subscription, ReceiveEnabled: true, WorkCapacity: ExpectedCapacityCommitAttempts + ExpectedSingleOperation));
        await stream.StartAsync(CancellationToken.None);
        _ = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        Task? uploadSync = null;
        Task? parkedSync = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(session, uploadSync).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            var parked = await stream.PublishAsync(new(ExpectedTwoOperations), CreateVolatilePublishOptions(Stream), CancellationToken.None);
            var later = await stream.PublishAsync(
                new(ExpectedCapacityCommitAttempts + ExpectedSingleOperation),
                CreateVolatilePublishOptions(Stream),
                CancellationToken.None);
            parkedSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await Assert.That(uploadSync.IsCompleted).IsFalse();
            await Assert.That(parkedSync.IsCompleted).IsFalse();
            await AssertSinglePreparedBatchForStreamAsync(session).ConfigureAwait(false);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);
            await WaitForOperationStateAsync(store, parked.OperationId, SyncOperationState.SavedLocally);
            session.ReleasePausedSendAttempt();
            await uploadSync.WaitAsync(GuardTimeout);
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await AssertOrderedSnapshotRecoveryAsync(store, session, faults, releaseRecovery).ConfigureAwait(false);
            await AwaitTriggerCompletionAsync(parkedSync).ConfigureAwait(false);
            await WaitForOperationStateAsync(store, parked.OperationId, SyncOperationState.Synchronized);
            await WaitForOperationStateAsync(store, later.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            session.ReleasePausedSendAttempt();
            await AwaitTriggerCompletionAsync(parkedSync).ConfigureAwait(false);
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies active upload completion does not duplicate work when no flush is pending.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryActiveUploadCompletionWithoutPendingFlushDoesNotResend()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var session = CreatePausedSnapshotRecoverySession(releaseGap, releaseRecovery, resultFactory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, CreateSnapshotRecoveryRetryOptions());
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(
            new(ExpectedSingleOperation),
            CreateVolatilePublishOptions(Stream),
            CancellationToken.None);
        Task? uploadSync = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(session, uploadSync).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseGap.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            session.ReleasePausedSendAttempt();
            await uploadSync.WaitAsync(GuardTimeout);
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            releaseRecovery.SetResult();
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, CreateSnapshotRecoveryCursor(Stream));
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            session.ReleasePausedSendAttempt();
            await AwaitTriggerCompletionAsync(uploadSync).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies stopping during a gated recovery commit defers parked upload work until restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryStopDefersParkedUploadUntilRestartDuringCommitWait()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoveryBatchingSession(releaseGap, releaseRecovery);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        var commitGate = CreateSnapshotRecoveryCommitGate(store, Stream);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        var coordinator = new StopObservingStreamCoordinator(engine);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, coordinator, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(
            new(ExpectedSingleOperation),
            CreateVolatilePublishOptions(Stream),
            CancellationToken.None);
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
            releaseRecovery.SetResult();
            await commitGate.Entered.Task.WaitAsync(GuardTimeout);
            stopStream = stream.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await AssertRecoveredCursorAsync(store, Stream, Subscription, null).ConfigureAwait(false);
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await coordinator.StopEntered.Task.WaitAsync(GuardTimeout);
            commitGate.Release.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await stream.StartAsync(CancellationToken.None);
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, SnapshotRecoveryRecoveredCursor);
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            _ = commitGate.Release.TrySetResult();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Asserts queued recovery has not captured or requested a remote snapshot before admission is released.</summary>
    /// <param name="session">The observed receive session.</param>
    /// <param name="store">The observed local store.</param>
    /// <param name="captureCount">The capture count after the active recovery request.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertQueuedAdmissionBeforeFirstReleaseAsync(
        ReceiveSession session,
        InstrumentedSnapshotRecoveryStore store,
        int captureCount)
    {
        await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(CreateSnapshotRecoveryRequestTrace(session)).IsEqualTo(CreateSnapshotRecoveryRequestTrace(Stream));
        await Assert.That(CreateCanceledSnapshotRecoveryRequestTrace(session)).IsEqualTo(string.Empty);
        await Assert.That(store.CaptureRequestCount).IsEqualTo(captureCount);
    }

    /// <summary>Asserts exactly one prepared batch exists for the recovering stream.</summary>
    /// <param name="session">The observed receive session.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSinglePreparedBatchForStreamAsync(ReceiveSession session)
    {
        await Assert.That(session.PreparedBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.HasPreparedBatchForStream(Stream)).IsTrue();
    }

    /// <summary>Creates request-matched results that prove replay inclusion and preserve pending operations.</summary>
    /// <returns>The remote recovery result factory.</returns>
    private static async ValueTask<Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>>
        CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(
                SnapshotRecoveryCounterStateContractId,
                ExpectedSingleOperation,
                default(ReceiveCounterState),
                CancellationToken.None)
            .ConfigureAwait(false);
        return request => new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload, request.StreamId, request.SubscriptionId),
            OperationDispositions = CreateReplayAcceptedPendingUnknownDispositions(request),
        };
    }

    /// <summary>Creates a pause-capable receive session with request-matched recovery results.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="resultFactory">The request-matched recovered snapshot result factory.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreatePausedSnapshotRecoverySession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            PauseBeforeSendNumber = ExpectedSingleOperation,
            SnapshotRecoveryResultFactory = resultFactory,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates full-union dispositions for replay and pending request operations.</summary>
    /// <param name="request">The request whose operation union must be proven.</param>
    /// <returns>The full operation disposition list.</returns>
    private static SnapshotOperationDisposition[] CreateReplayAcceptedPendingUnknownDispositions(
        RemoteSnapshotRecoveryRequest request)
    {
        var dispositions = new List<SnapshotOperationDisposition>(request.PendingOperations.Count + request.ReplayOperations.Count);
        for (var replayIndex = 0; replayIndex < request.ReplayOperations.Count; replayIndex++)
        {
            var operation = request.ReplayOperations[replayIndex];
            if (ContainsPendingOperation(request, operation.OperationId))
            {
                continue;
            }

            dispositions.Add(new()
            {
                OperationId = operation.OperationId,
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(
                    operation.OperationId,
                    OperationResultKind.Accepted,
                    ReasonCode: null,
                    ServerVersion: "snapshot-replay"),
            });
        }

        for (var pendingIndex = 0; pendingIndex < request.PendingOperations.Count; pendingIndex++)
        {
            dispositions.Add(new() { OperationId = request.PendingOperations[pendingIndex].OperationId, Kind = SnapshotOperationDispositionKind.Unknown });
        }

        return [.. dispositions];
    }

    /// <summary>Checks whether the request has a pending operation identity.</summary>
    /// <param name="request">The snapshot recovery request.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <returns><see langword="true"/> when the operation is pending.</returns>
    private static bool ContainsPendingOperation(RemoteSnapshotRecoveryRequest request, OperationId operationId)
    {
        for (var pendingIndex = 0; pendingIndex < request.PendingOperations.Count; pendingIndex++)
        {
            if (request.PendingOperations[pendingIndex].OperationId == operationId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Waits for focused recovery cursor and reports bounded public diagnostics on timeout.</summary>
    /// <param name="store">The observed local store.</param>
    /// <param name="session">The observed receive session.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The durable cursor was not observed before the guard timeout.</exception>
    private static async Task WaitForRecoveredCursorDiagnosticAsync(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        try
        {
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, CreateSnapshotRecoveryCursor(Stream)).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            var diagnostic = await CreateFocusedRecoveryDiagnosticAsync(store, session, faults).ConfigureAwait(false);
            throw new TimeoutException(diagnostic, exception);
        }
    }

    /// <summary>Creates the focused recovery timeout diagnostic without payload contents.</summary>
    /// <param name="store">The observed local store.</param>
    /// <param name="session">The observed receive session.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <returns>The diagnostic message.</returns>
    private static async Task<string> CreateFocusedRecoveryDiagnosticAsync(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false);
        var request = session.SnapshotRecoveryRequestCount == 0 ? null : session.GetSnapshotRecoveryRequest(0);
        var pending = await FormatRequestOperationsAsync(store, request?.PendingOperations).ConfigureAwait(false);
        var replay = await FormatRequestOperationsAsync(store, request?.ReplayOperations).ConfigureAwait(false);
        return string.Join(
            "; ",
            "The expected recovered cursor was not observed",
            $"actualCursor={recovered.ServerCursor ?? MissingDiagnosticValue}",
            $"requestCount={session.SnapshotRecoveryRequestCount}",
            $"captureCount={store.CaptureRequestCount}",
            $"sentCount={session.SentBatches.Count}",
            $"preparedCount={session.PreparedBatches.Count}",
            $"commitPresent={store.LastSnapshotRecoveryCommit is not null}",
            $"commitException={store.LastSnapshotRecoveryException?.GetType().Name ?? MissingDiagnosticValue}",
            $"pending={pending}",
            $"replay={replay}",
            $"faults={FormatFaults(faults.Values)}");
    }

    /// <summary>Formats operation identities and durable states without payload contents.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="operations">The request operations.</param>
    /// <returns>The formatted operation diagnostic.</returns>
    private static async Task<string> FormatRequestOperationsAsync(
        ILocalStoreAdapter store,
        IReadOnlyList<SyncOperation>? operations)
    {
        if (operations is null)
        {
            return MissingDiagnosticValue;
        }

        var values = new string[operations.Count];
        for (var index = 0; index < operations.Count; index++)
        {
            var operation = operations[index];
            var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None).ConfigureAwait(false);
            values[index] = FormatOperationStatus(operation.OperationId, status);
        }

        return string.Join(",", values);
    }

    /// <summary>Formats one operation status without payload contents.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="status">The durable operation status.</param>
    /// <returns>The formatted operation status.</returns>
    private static string FormatOperationStatus(OperationId operationId, SyncOperationStatus? status) =>
        status is null
            ? $"{operationId}:status={MissingDiagnosticValue}"
            : $"{operationId}:state={status.State}:attempt={status.Attempt}:reason={status.ReasonCode ?? MissingDiagnosticValue}";

    /// <summary>Formats public fault details without payload contents.</summary>
    /// <param name="faults">The observed faults.</param>
    /// <returns>The formatted fault diagnostic.</returns>
    private static string FormatFaults(List<OccasionallyConnectedFault> faults)
    {
        if (faults.Count == 0)
        {
            return MissingDiagnosticValue;
        }

        var values = new string[faults.Count];
        for (var index = 0; index < faults.Count; index++)
        {
            var fault = faults[index];
            values[index] = string.Join(
                "|",
                fault.Code,
                fault.StreamId?.ToString() ?? MissingDiagnosticValue,
                fault.Exception?.GetType().Name ?? MissingDiagnosticValue,
                fault.Message,
                fault.Exception?.Message ?? MissingDiagnosticValue);
        }

        return string.Join(",", values);
    }

    /// <summary>Creates a receive session that emits a mismatched retained-history gap.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateMismatchedSnapshotRecoveryGapSession(TaskCompletionSource releaseGap) =>
        new()
        {
            SubscriptionGap = new(
                SnapshotRecoveryOtherStream,
                SnapshotRecoveryOtherSubscription,
                SnapshotRecoveryExpiredCursor,
                reasonCode: "retention-gap"),
            ReleaseSubscriptionGap = releaseGap,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates a recovered snapshot result with a specific counter state.</summary>
    /// <param name="sum">The recovered counter sum.</param>
    /// <returns>The recovered snapshot result.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateRecoveredCounterStateResultAsync(int sum)
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(
                SnapshotRecoveryCounterStateContractId,
                ExpectedSingleOperation,
                new ReceiveCounterState(sum),
                CancellationToken.None)
            .ConfigureAwait(false);
        return new() { Status = RemoteSnapshotRecoveryStatus.Recovered, Checkpoint = CreateRecoveredCheckpoint(payload), OperationDispositions = [] };
    }

    /// <summary>Creates an at-most-once stream that can recover a retained-history gap without acknowledgements.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="timeProvider">The shared test clock.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>
        CreateAtMostOnceSnapshotRecoveryCounterStream(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        TimeProvider timeProvider)
    {
        var serializer = new ReceiveCounterSerializer();
        return new(new()
        {
            Definition = new()
            {
                StreamId = Stream,
                SubscriptionId = Subscription,
                Projection = new ReceiveCounterProjection(),
                InputContractId = "counter-input",
                StateContractId = SnapshotRecoveryCounterStateContractId,
                Subscription = new() { StreamId = Stream, SubscriptionId = Subscription, DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
            },
            Store = store,
            Serializer = serializer,
            TimeProvider = timeProvider,
            OperationIdSource = ReceiveOperationIdSource.Instance,
            Coordinator = coordinator,
            InputProducer = new ReceiveInputProducer(),
            LocalStateSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateState(payload)),
            RemoteInputSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateInput(payload)),
            NotificationScheduler = InlineObserverScheduler.Instance,
            NotificationOptions = new(
                ReceiveReplayNotificationCapacity,
                ReceiveReplayNotificationBytes,
                ObserverNotificationOverflowMode.CoalesceLatest),
            WorkCapacity = ExpectedCapacityCommitAttempts,
            LocalAdmissionRetainedBytes = PreparedUploadBytes,
            ClientId = EngineClientId,
        });
    }

    /// <summary>Creates a receive session that supports snapshot recovery without receive acknowledgements.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoveryNoAcknowledgementSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures & ~RemoteTransportCapabilities.ReceiveAcknowledgements,
            },
        };

    /// <summary>Creates a receive session that negotiates snapshot recovery without implementing the facet.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <returns>The configured session.</returns>
    private static NonSnapshotRecoveryReceiveSession CreateNonSnapshotRecoverySession(TaskCompletionSource releaseGap) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Checks the exact recovery roles and the ordered optimistic state after the durable commit.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="session">The receive session.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The recovered cursor did not produce a snapshot.</exception>
    private static async Task AssertOrderedSnapshotRecoveryAsync(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseRecovery)
    {
        var request = session.GetSnapshotRecoveryRequest(0);
        await Assert.That(request.PendingOperations.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(request.ReplayOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        releaseRecovery.SetResult();
        await WaitForRecoveredCursorDiagnosticAsync(store, session, faults).ConfigureAwait(false);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false);
        var snapshot = recovered.Snapshot ?? throw new InvalidOperationException("The recovered snapshot is missing.");
        await Assert.That(ReceiveCounterSerializer.CreateState(snapshot.State).Sum).IsEqualTo(ExpectedOrderedRecoveryState);
    }

    /// <summary>Appends input digits so replay order and accepted-operation removal are visible.</summary>
    private sealed class ReceiveOrderedProjection : ILocalProjection<ReceiveCounterState, ReceiveCounterInput>
    {
        /// <inheritdoc/>
        public ReceiveCounterState InitialState => new(0);

        /// <inheritdoc/>
        public ReceiveCounterState ApplyLocal(ReceiveCounterState state, ReceiveCounterInput input, SyncOperation operation) =>
            new((state.Sum * OrderedProjectionRadix) + input.Delta);

        /// <inheritdoc/>
        public ReceiveCounterState ApplyRemote(ReceiveCounterState state, ReceiveCounterInput input, RemoteEvent remoteEvent) =>
            new((state.Sum * OrderedProjectionRadix) + input.Delta);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReceiveCounterState Reconcile(ReceiveCounterState state, ConflictResolutionResult result) => state;
    }
}
