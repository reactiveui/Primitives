// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Per-stream lifecycle tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The observation window while cancellation callbacks remain held.</summary>
    private static readonly TimeSpan StopCallbackObservationWindow = TimeSpan.FromMilliseconds(100);

    /// <summary>Verifies a registered stream still participates in global startup until explicitly stopped.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegisteredStreamStartsReceiveOnGlobalStartUntilExplicitlyStopped()
    {
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a stream stopped before the global engine starts stays parked until the stream is explicitly resumed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncBeforeGlobalStartParksReceiveUntilStartStreamAsync()
    {
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StopStreamAsync(Stream, CancellationToken.None);
        await engine.StartAsync(CancellationToken.None);

        await Assert.That(session.SubscribeEntered.Task.IsCompleted).IsFalse();
        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies repeated inactive stream stops complete immediately without starting receive work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncAlreadyInactiveCompletesWithoutReceiveWork()
    {
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(CreateReceiveParticipant());

        await engine.StopStreamAsync(Stream, CancellationToken.None);
        var secondStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();

        await Assert.That(secondStop.IsCompletedSuccessfully).IsTrue();
        await secondStop.WaitAsync(GuardTimeout);
        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(0);

        await engine.StartAsync(CancellationToken.None);
        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies stopping one stream cancels its receive loop without disposing the shared transport session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncCancelsReceivePumpWithoutDisposingSharedSession()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);

            releaseCanceledSubscribe.SetResult();
            releaseBatches.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies stream stop and restart use the same shared session and subscription identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartStreamAsyncAfterStopRestartsReceiveOnSameSharedSession()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        session.SubscriptionBatches.Enqueue([]);
        session.SubscriptionBatches.Enqueue([]);
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            await Assert.That(stopStream.IsCompleted).IsFalse();

            releaseCanceledSubscribe.SetResult();
            releaseBatches.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        await WaitForConditionAsync(() => session.SubscribeCompletedCount == ExpectedSingleOperation);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.SubscribeRequests[1].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies a repeated stream stop joins the receive pump cancellation already in progress.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncConcurrentCallersSharePendingReceiveDrain()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? firstStop = null;
        Task? secondStop = null;
        try
        {
            firstStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            secondStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();

            await Assert.That(firstStop.IsCompleted).IsFalse();
            await Assert.That(secondStop.IsCompleted).IsFalse();

            releaseCanceledSubscribe.SetResult();
            releaseBatches.SetResult();
            await firstStop.WaitAsync(GuardTimeout);
            await secondStop.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
            if (firstStop is not null)
            {
                await ObserveTaskCompletionAsync(firstStop);
            }

            if (secondStop is not null)
            {
                await ObserveTaskCompletionAsync(secondStop);
            }
        }

        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies repeated stream stop waits for cancellation callbacks after receive iteration completes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncConcurrentCallerWaitsForBlockedCancellationCallbackAfterReceiveCompletes()
    {
        using var fixture = new BlockedCancellationFixture();
        var releaseCanceledSubscribe = fixture.ReleaseCanceledSubscribe;
        var cancellationCallbackEntered = fixture.CancellationCallbackEntered;
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? firstStop = null;
        Task? secondStop = null;
        Task? thirdStop = null;
        try
        {
            firstStop = StopStreamOnDedicatedThread(engine);
            await cancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            releaseCanceledSubscribe.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            secondStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();

            await Assert.That(firstStop.IsCompleted).IsFalse();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => secondStop.WaitAsync(StopCallbackObservationWindow));
            await Assert.That(firstStop.IsCompleted).IsFalse();

            thirdStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => thirdStop.WaitAsync(StopCallbackObservationWindow));

            fixture.ReleaseCancellationCallbacks();
            await firstStop.WaitAsync(GuardTimeout);
            await secondStop.WaitAsync(GuardTimeout);
            await thirdStop.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            await ObserveStreamStopTasksAsync([firstStop, secondStop, thirdStop]);
        }

        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies stop-start-stop joins the original cancellation callback drain before receive restart admission.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStartStopDuringBlockedCancellationCallbackWaitsForOriginalDrain()
    {
        using var fixture = new BlockedCancellationFixture();
        fixture.EnqueueEmptySubscriptionBatches(ExpectedCapacityCommitAttempts);
        var releaseCanceledSubscribe = fixture.ReleaseCanceledSubscribe;
        var cancellationCallbackEntered = fixture.CancellationCallbackEntered;
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? firstStop = null;
        Task? secondStop = null;
        try
        {
            firstStop = StopStreamOnDedicatedThread(engine);
            await cancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            releaseCanceledSubscribe.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => firstStop.WaitAsync(StopCallbackObservationWindow));
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            secondStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();

            await Assert.That(firstStop.IsCompleted).IsFalse();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => secondStop.WaitAsync(StopCallbackObservationWindow));

            fixture.ReleaseCancellationCallbacks();
            await firstStop.WaitAsync(GuardTimeout);
            await secondStop.WaitAsync(GuardTimeout);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            await ObserveStreamStopTasksAsync([firstStop, secondStop]);
        }

        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies global cleanup owns a stream stop whose cancellation callback is still draining.</summary>
    /// <param name="disposeEngine">Whether to verify disposal instead of stop.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GlobalCleanupWaitsForBlockedStreamStopCancellationCallbackBeforeSessionDispose(bool disposeEngine)
    {
        using var fixture = new BlockedCancellationFixture();
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? streamStop = null;
        Task? globalCleanup = null;
        try
        {
            streamStop = StopStreamOnDedicatedThread(engine);
            await fixture.CancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            fixture.ReleaseCanceledSubscribe.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            globalCleanup = disposeEngine
                ? engine.DisposeAsync().AsTask()
                : engine.StopAsync(CancellationToken.None).AsTask();
            if (streamStop is not { } activeStreamStop || globalCleanup is not { } activeGlobalCleanup)
            {
                await Assert.That(streamStop).IsNotNull();
                await Assert.That(globalCleanup).IsNotNull();
                return;
            }

            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => activeGlobalCleanup.WaitAsync(StopCallbackObservationWindow));
            await Assert.That(activeStreamStop.IsCompleted).IsFalse();
            await Assert.That(session.DisposeCalls).IsEqualTo(0);

            fixture.ReleaseCancellationCallbacks();
            await Task.WhenAll(activeStreamStop, activeGlobalCleanup).WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = fixture.ReleaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            await ObserveStreamStopTasksAsync([streamStop, globalCleanup]);
        }

        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies a stream stop admitted after global receive cleanup joins the accepted global stop.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncDuringGlobalSessionDisposeWaitsForAcceptedStopWithoutLateDriver()
    {
        using var fixture = new GlobalStopDisposeFixture();
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? globalStop = null;
        Task? lateStop = null;
        try
        {
            globalStop = engine.StopAsync(CancellationToken.None).AsTask();
            await WaitForGlobalStopToReachSessionDisposeAsync(fixture);

            lateStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await AssertLateStreamStopJoinsGlobalStopAsync(lateStop, globalStop, session, transport, faults);

            fixture.ReleaseDispose.SetResult();
            await Task.WhenAll(globalStop, lateStop).WaitAsync(GuardTimeout);
        }
        finally
        {
            fixture.ReleaseAll();
            await ObserveStreamStopTasksAsync([globalStop, lateStop]);
        }

        await AssertSingleGlobalStopNoLateRestartOrFaultAsync(session, transport, faults);
    }

    /// <summary>Verifies a stream start requested while stop drains restarts receive after the old pump exits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartStreamAsyncDuringStopDrainRestartsReceiveOnSameSharedSession()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        session.SubscriptionBatches.Enqueue([]);
        session.SubscriptionBatches.Enqueue([]);
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await Assert.That(stopStream.IsCompleted).IsFalse();
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

            releaseCanceledSubscribe.SetResult();
            releaseBatches.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.SubscribeRequests[1].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies restart intent recorded during cancellation callbacks starts receive after cancellation completes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartStreamAsyncDuringBlockedCancellationCallbackRetriesPendingReceiveRestart()
    {
        using var fixture = new BlockedCancellationFixture();
        fixture.EnqueueEmptySubscriptionBatches(ExpectedCapacityCommitAttempts);
        var releaseCanceledSubscribe = fixture.ReleaseCanceledSubscribe;
        var cancellationCallbackEntered = fixture.CancellationCallbackEntered;
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = StopStreamOnDedicatedThread(engine);
            await cancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            releaseCanceledSubscribe.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
            fixture.ReleaseCancellationCallbacks();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.SubscribeRequests[1].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies cancellation callback failures publish faults while stop cleanup still permits restart and unregister.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncCallbackFaultDrainsThenAllowsRestartAndUnregisterCleanup()
    {
        var callbackFailure = new InvalidOperationException("subscription cancellation callback failed");
        using var fixture = new BlockedCancellationFixture(callbackFailure);
        fixture.EnqueueEmptySubscriptionBatches(ExpectedCapacityCommitAttempts);
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = StopStreamOnDedicatedThread(engine);
            await fixture.CancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            fixture.ReleaseCancellationCallbacks();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            fixture.ReleaseCanceledSubscribe.SetResult();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = fixture.ReleaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        await WaitForConditionAsync(() => faults.Values.Exists(static fault =>
            fault.Code == ReceivePumpFaultCode && fault.Exception?.Message == typeof(AggregateException).FullName));
        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        registration.Dispose();
        await WaitForConditionAsync(() => session.SubscribeCompletedCount == ExpectedCapacityCommitAttempts);
        var failureTrace = CreateExceptionTrace(null, faults);

        await Assert.That(failureTrace).Contains(nameof(InvalidOperationException));
        await Assert.That(session.SubscribeCancellationFailures.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(() => engine.StartStreamAsync(Stream, CancellationToken.None).AsTask()).ThrowsExactly<InvalidOperationException>();

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies unregister waits for a stopped generation's callback drain before a fresh stream reuses the identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnregisterDuringBlockedStopCallbackDoesNotClobberFreshStreamRegistration()
    {
        using var fixture = new BlockedCancellationFixture();
        fixture.EnqueueEmptySubscriptionBatches(ExpectedCapacityCommitAttempts);
        var session = fixture.Session;
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Task? stopStream = null;
        try
        {
            stopStream = StopStreamOnDedicatedThread(engine);
            await fixture.CancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            fixture.ReleaseCanceledSubscribe.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            registration.Dispose();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
                () => stopStream.WaitAsync(StopCallbackObservationWindow));
            fixture.ReleaseCancellationCallbacks();
            await stopStream.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = fixture.ReleaseCanceledSubscribe.TrySetResult();
            fixture.ReleaseCancellationCallbacks();
            if (stopStream is not null)
            {
                await ObserveTaskCompletionAsync(stopStream);
            }
        }

        using var replacement = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });
        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        await Assert.That(session.SubscribeRequests[1].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies stopped-stream upload notifications merge into one deferred head and drain after restart.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task StoppedStreamMergesDeferredUploadWakeUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStore([first, second], timeProvider: clock);
        var session = CreateBatchSession(ExpectedTwoOperations);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StopStreamAsync(Stream, CancellationToken.None);
        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, first);
        engine.NotifyLocalCommitReady(Stream, second);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.LeaseRequests.Count).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => session.SentBatches.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(first);
        await Assert.That(session.SentBatches[0].Operations[1]).IsSameReferenceAs(second);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies local publication remains admitted while an explicit stream stop parks remote upload work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncParksUploadButKeepsLocalPublicationAdmitted()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var session = new ReceiveSession();
        var transport = new RecordingTransport { SessionOverride = session };
        var activeStream = new StreamId("sync/engine-active");
        var activeOperation = CreateOperation(activeStream, operationId: OperationId.New());
        var stoppedOperation = CreateOperation(operationId: OperationId.New());
        var store = CreateUploadStore([activeOperation], timeProvider: clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedCapacityCommitAttempts, PreparedUploadBytes);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var activeRegistration = engine.RegisterParticipant(CreateUploadParticipant(store, streamId: activeStream));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StopStreamAsync(Stream, CancellationToken.None);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(activeStream, activeOperation);
        await WaitForReceiveSessionUploadAsync(clock, session, faults);

        await Assert.That(session.SentBatches[0].Operations[0].StreamId).IsEqualTo(activeStream);
        var receipt = await engine.EnqueueOperationAsync(stoppedOperation, CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);

        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.LeaseRequests.Exists(static request => request.StreamId == Stream)).IsFalse();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies unregistering an active receive subscription cancels the pump without disposing the shared session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnregisterActiveReceiveSubscriptionCancelsPumpWithoutDisposingSharedSession()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        var transport = new RecordingTransport { SessionOverride = session };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport);
        var registration = engine.RegisterParticipant(CreateReceiveParticipant());
        using var faultSubscription = engine.Faults.Subscribe(faults);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

            registration.Dispose();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);

            await Assert.That(session.DisposeCalls).IsEqualTo(0);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);

            releaseCanceledSubscribe.SetResult();
            releaseBatches.SetResult();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
            registration.Dispose();
        }

        using var replacement = engine.RegisterParticipant(CreateReceiveParticipant());
        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

        await Assert.That(session.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();

        await engine.StopAsync(CancellationToken.None);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Waits until global stop owns receive cleanup and reaches session disposal.</summary>
    /// <param name="fixture">The global stop probe gates.</param>
    /// <returns>The wait task.</returns>
    private static async Task WaitForGlobalStopToReachSessionDisposeAsync(GlobalStopDisposeFixture fixture)
    {
        await fixture.CancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
        await fixture.Session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
        fixture.ReleaseCanceledSubscribe.SetResult();
        fixture.ReleaseCancellationCallback.Set();
        await fixture.Session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
        await fixture.DisposeEntered.Task.WaitAsync(GuardTimeout);
    }

    /// <summary>Asserts a late stream stop waits for the already accepted global stop.</summary>
    /// <param name="lateStop">The late per-stream stop task.</param>
    /// <param name="globalStop">The accepted global stop task.</param>
    /// <param name="session">The receive session.</param>
    /// <param name="transport">The recording transport.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertLateStreamStopJoinsGlobalStopAsync(
        Task lateStop,
        Task globalStop,
        ReceiveSession session,
        RecordingTransport transport,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        _ = await Assert.ThrowsExactlyAsync<TimeoutException>(
            () => lateStop.WaitAsync(StopCallbackObservationWindow));

        await Assert.That(globalStop.IsCompleted).IsFalse();
        await AssertSingleGlobalStopNoLateRestartOrFaultAsync(session, transport, faults);
    }

    /// <summary>Asserts global stop did not restart receive work or publish faults.</summary>
    /// <param name="session">The receive session.</param>
    /// <param name="transport">The recording transport.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSingleGlobalStopNoLateRestartOrFaultAsync(
        ReceiveSession session,
        RecordingTransport transport,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Count).IsEqualTo(0);
    }

    /// <summary>Runs a stream stop on a dedicated thread for cancellation callback probes.</summary>
    /// <param name="engine">The engine to stop.</param>
    /// <returns>The stop operation.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static Task StopStreamOnDedicatedThread(SyncEngine engine) =>
        Task.Factory.StartNew(
            static state => ((SyncEngine)state!).StopStreamAsync(Stream, CancellationToken.None).AsTask(),
            engine,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

    /// <summary>Observes every stop operation created by a cancellation-drain probe.</summary>
    /// <param name="tasks">The operations that were started before cleanup.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task ObserveStreamStopTasksAsync(Task?[] tasks)
    {
        foreach (var task in tasks)
        {
            if (task is not null)
            {
                await ObserveTaskCompletionAsync(task);
            }
        }
    }

    /// <summary>Stores gates for a global stop that blocks during session disposal.</summary>
    private sealed class GlobalStopDisposeFixture : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="GlobalStopDisposeFixture"/> class.</summary>
        internal GlobalStopDisposeFixture() =>
            Session = new()
            {
                ReleaseSubscribeCancellation = ReleaseCanceledSubscribe,
                SubscribeCancellationCallbackEntered = CancellationCallbackEntered,
                ReleaseSubscribeCancellationCallback = ReleaseCancellationCallback,
                DisposeEntered = DisposeEntered,
                ReleaseDispose = ReleaseDispose,
            };

        /// <summary>Gets the signal that releases canceled subscription completion.</summary>
        internal TaskCompletionSource ReleaseCanceledSubscribe { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal raised from inside a cancellation callback.</summary>
        internal TaskCompletionSource CancellationCallbackEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate that blocks a cancellation callback.</summary>
        internal ManualResetEventSlim ReleaseCancellationCallback { get; } = new();

        /// <summary>Gets the signal raised when session disposal starts.</summary>
        internal TaskCompletionSource DisposeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate that releases session disposal.</summary>
        internal TaskCompletionSource ReleaseDispose { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the receive session.</summary>
        internal ReceiveSession Session { get; }

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Dispose() => ReleaseCancellationCallback.Dispose();

        /// <summary>Releases retained asynchronous gates.</summary>
        internal void ReleaseAll()
        {
            _ = ReleaseCanceledSubscribe.TrySetResult();
            ReleaseCancellationCallback.Set();
            _ = ReleaseDispose.TrySetResult();
            Session.DisposeSubscribeCancellationCallbacks();
        }
    }

    /// <summary>Stores reusable gates for blocked receive cancellation callback probes.</summary>
    private sealed class BlockedCancellationFixture : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="BlockedCancellationFixture"/> class.</summary>
        /// <param name="subscribeCancellationException">The exception thrown by cancellation callbacks.</param>
        internal BlockedCancellationFixture(Exception? subscribeCancellationException = null) =>
            Session = new()
            {
                ReleaseSubscribeCancellation = ReleaseCanceledSubscribe,
                SubscribeCancellationCallbackEntered = CancellationCallbackEntered,
                ReleaseSubscribeCancellationCallback = ReleaseCancellationCallback,
                SubscribeCancellationException = subscribeCancellationException,
            };

        /// <summary>Gets the signal that releases canceled subscription completion.</summary>
        internal TaskCompletionSource ReleaseCanceledSubscribe { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal raised from inside the cancellation callback.</summary>
        internal TaskCompletionSource CancellationCallbackEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate that blocks the cancellation callback.</summary>
        internal ManualResetEventSlim ReleaseCancellationCallback { get; } = new();

        /// <summary>Gets the receive session.</summary>
        internal ReceiveSession Session { get; }

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Dispose() => ReleaseCancellationCallback.Dispose();

        /// <summary>Adds empty subscription batch entries.</summary>
        /// <param name="count">The number of batch entries.</param>
        internal void EnqueueEmptySubscriptionBatches(int count)
        {
            for (var i = 0; i < count; i++)
            {
                Session.SubscriptionBatches.Enqueue([]);
            }
        }

        /// <summary>Releases and disposes retained cancellation callback registrations.</summary>
        internal void ReleaseCancellationCallbacks()
        {
            ReleaseCancellationCallback.Set();
            Session.DisposeSubscribeCancellationCallbacks();
        }
    }
}
