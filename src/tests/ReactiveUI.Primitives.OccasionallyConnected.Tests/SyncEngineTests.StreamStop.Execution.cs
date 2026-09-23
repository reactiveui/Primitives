// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Stream stop execution tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a restart requested while stop drains cancellation callbacks starts after the drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartStreamAsyncDuringStopStreamDrainRestartsReceiveAfterCallbackReleases()
    {
        TaskCompletionSource callbackEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseCallback = new();
        var session = CreateCallbackGatedReceiveSession(callbackEntered, releaseCallback);
        await using var engine = CreateEngine(transport: new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateReceiveParticipant());
        Task? stop = null;

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            stop = Task.Run(async () => await engine.StopStreamAsync(Stream, CancellationToken.None).ConfigureAwait(false));
            await callbackEntered.Task.WaitAsync(GuardTimeout);

            await engine.StartStreamAsync(Stream, CancellationToken.None);

            await Assert.That(stop.IsCompleted).IsFalse();
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

            releaseCallback.Set();
            await stop.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

            await Assert.That(session.DisposeCalls).IsEqualTo(0);
        }
        finally
        {
            releaseCallback.Set();
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies duplicate stream stops join the same receive callback drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentStopStreamAsyncCallsShareReceiveCallbackDrain()
    {
        TaskCompletionSource callbackEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseCallback = new();
        var session = CreateCallbackGatedReceiveSession(callbackEntered, releaseCallback);
        await using var engine = CreateEngine(transport: new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateReceiveParticipant());
        Task? firstStop = null;
        Task? secondStop = null;

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            firstStop = Task.Run(async () => await engine.StopStreamAsync(Stream, CancellationToken.None).ConfigureAwait(false));
            await callbackEntered.Task.WaitAsync(GuardTimeout);
            secondStop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();

            await Assert.That(firstStop.IsCompleted).IsFalse();
            await Assert.That(secondStop.IsCompleted).IsFalse();
            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);

            releaseCallback.Set();
            await Task.WhenAll(firstStop, secondStop).WaitAsync(GuardTimeout);

            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);
        }
        finally
        {
            releaseCallback.Set();
            if (firstStop is not null)
            {
                await ObserveTaskCompletionAsync(firstStop).ConfigureAwait(false);
            }

            if (secondStop is not null)
            {
                await ObserveTaskCompletionAsync(secondStop).ConfigureAwait(false);
            }
        }

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies caller cancellation only abandons the wait for an accepted stream stop.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncCallerCancellationDoesNotCancelAcceptedStreamStop()
    {
        TaskCompletionSource callbackEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseCallback = new();
        using CancellationTokenSource caller = new();
        var session = CreateCallbackGatedReceiveSession(callbackEntered, releaseCallback);
        await using var engine = CreateEngine(transport: new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateReceiveParticipant());
        Task? stop = null;

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            stop = Task.Run(async () => await engine.StopStreamAsync(Stream, caller.Token).ConfigureAwait(false));
            await callbackEntered.Task.WaitAsync(GuardTimeout);
            await caller.CancelAsync();

            var cancellation = await Assert.ThrowsAsync<OperationCanceledException>(
                async () => await stop.WaitAsync(GuardTimeout).ConfigureAwait(false));
            await Assert.That(cancellation?.CancellationToken).IsEqualTo(caller.Token);

            releaseCallback.Set();
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);
        }
        finally
        {
            releaseCallback.Set();
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies stopped streams defer upload work until stream restart instead of preparing it while inactive.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncDefersPendingUploadWakeUntilStreamRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = CreateBatchSession(ExpectedSingleOperation);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.StopStreamAsync(Stream, CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.Statuses.TryGetValue(operation.OperationId, out var status)
                && status.State == SyncOperationState.Synchronized,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PreparedBatches[0].Operations[0]).IsSameReferenceAs(operation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a stream stopped during an upload parks later upload wakes until stream restart.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task StopStreamAsyncParksInflightUploadAndDeferredWakeUntilStreamRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStoreWithSingleOperationLeases([first, second], timeProvider: clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        Task? trigger = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, first);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
            await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);

            await engine.StopStreamAsync(Stream, CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, second);
            trigger = engine.TriggerSyncAsync(CancellationToken.None).AsTask();

            await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches.Count).IsEqualTo(0);
            await Assert.That(trigger.IsCompleted).IsFalse();

            session.ReleasePausedSendAttempt();
            await trigger.WaitAsync(GuardTimeout);

            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.Statuses[second.OperationId].State).IsEqualTo(SyncOperationState.QueuedForUpload);

            await AssertRestartedStreamUploadsDeferredWakeAsync(engine, clock, store, session, faults, options, second);
        }
        finally
        {
            session.ReleasePausedSendAttempt();
            if (trigger is not null)
            {
                await ObserveTaskCompletionAsync(trigger).ConfigureAwait(false);
            }
        }

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies a retry backoff from an in-flight failed upload is deferred until stream restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncDefersRetryBackoffFromInflightFailedUploadUntilRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
            SendException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay),
        };
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
        var started = false;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            engine.NotifyLocalCommitReady(Stream, operation);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
            await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);

            await engine.StopStreamAsync(Stream, CancellationToken.None);
            session.ReleasePausedSendAttempt();
            await WaitForUploadConditionWithTraceAsync(
                () => store.RetryStates.ContainsKey(operation.OperationId),
                store,
                session,
                faults,
                operationStates: null);

            await AssertStoppedRetryRunsAfterStreamRestartAsync(engine, clock, retryDelay, store, session, faults);
        }
        finally
        {
            session.ReleasePausedSendAttempt();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None);
            }
        }
    }

    /// <summary>Verifies upload retry exhaustion releases the lease and publishes a single terminal fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptStopsAfterConfiguredTransientRetryLimit()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromMilliseconds(UploadShortRetryMilliseconds);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { SendException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
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

        clock.Advance(retryDelay);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Asserts a stopped retry remains parked until the stream restarts.</summary>
    /// <param name="engine">The synchronization engine.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="retryDelay">The configured retry delay.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertStoppedRetryRunsAfterStreamRestartAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        TimeSpan retryDelay,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        clock.Advance(retryDelay);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.LeaseRequests.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
    }

    /// <summary>Restarts a stopped stream and asserts the deferred upload wake is sent.</summary>
    /// <param name="engine">The synchronization engine.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The fault observer.</param>
    /// <param name="options">The engine options.</param>
    /// <param name="operation">The operation expected after restart.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRestartedStreamUploadsDeferredWakeAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        OccasionallyConnectedOptions options,
        SyncOperation operation)
    {
        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForUploadConditionWithTraceAsync(
            () => session.SentBatches.Count == ExpectedCapacityCommitAttempts
                || clock.HasTimerDueIn(options.Batching.MaximumDwellTime),
            store,
            session,
            faults,
            operationStates: null);
        if (session.SentBatches.Count != ExpectedCapacityCommitAttempts)
        {
            clock.Advance(options.Batching.MaximumDwellTime);
        }

        await WaitForUploadConditionWithTraceAsync(
            () => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(session.SentBatches[ExpectedSingleOperation].Operations[0]).IsSameReferenceAs(operation);
        await Assert.That(faults.Values.Count).IsEqualTo(0);
    }

    /// <summary>Creates a receive participant for stream stop lifecycle tests.</summary>
    /// <returns>The receive participant.</returns>
    private static RecordingParticipant CreateReceiveParticipant() =>
        new() { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };

    /// <summary>Creates a receive session whose cancellation callback is controlled by the test.</summary>
    /// <param name="callbackEntered">The signal set when the callback starts.</param>
    /// <param name="releaseCallback">The gate that releases the callback.</param>
    /// <returns>The gated session.</returns>
    private static ReceiveSession CreateCallbackGatedReceiveSession(
        TaskCompletionSource callbackEntered,
        ManualResetEventSlim releaseCallback) =>
        new() { SubscribeCancellationCallbackEntered = callbackEntered, ReleaseSubscribeCancellationCallback = releaseCallback };
}
