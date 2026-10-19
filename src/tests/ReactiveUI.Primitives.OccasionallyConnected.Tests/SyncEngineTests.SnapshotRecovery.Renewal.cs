// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Cross-generation snapshot recovery tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies an upload-triggered session renewal restarts recovery on the new generation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryWaitingForExpiredUploadRestartsOnRenewedSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var recoveredSnapshot = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var releaseOldGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOldRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRenewedGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldSession = CreateRecoveryRenewalSession(releaseOldGap, releaseOldRecovery, recoveredSnapshot, expiresOnSend: true);
        var renewedSession = CreateRecoveryRenewalSession(releaseRenewedGap, null, recoveredSnapshot, expiresOnSend: false);
        var transport = new RecordingTransport { Capabilities = SnapshotRecoveryRemoteFeatures };
        transport.Sessions.Enqueue(oldSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateRecoveryRenewalEngine(store, transport, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        Task? uploadSync = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await oldSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(oldSession, uploadSync);
            releaseOldGap.SetResult();
            await oldSession.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await Assert.That(oldSession.SnapshotRecoveryRequestCount).IsEqualTo(0);

            oldSession.ReleasePausedSendAttempt();
            await renewedSession.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await renewedSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseRenewedGap.SetResult();
            await renewedSession.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await WaitForRenewedRecoveryCursorAsync(store, renewedSession, receipt.OperationId);
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await WaitForConditionAsync(() => renewedSession.Acknowledgements.Count == ExpectedSingleOperation);
            await uploadSync.WaitAsync(GuardTimeout);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(oldSession.SnapshotRecoveryRequestCount).IsEqualTo(0);
            await Assert.That(renewedSession.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.GetSnapshotRecoveryRequest(0).ExpiredCursor).IsEqualTo(SnapshotRecoveryExpiredCursor);
        }
        finally
        {
            _ = releaseOldGap.TrySetResult();
            _ = releaseOldRecovery.TrySetResult();
            _ = releaseRenewedGap.TrySetResult();
            oldSession.ReleasePausedSendAttempt();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Verifies a retention gap emitted after renewal does not recover through the retired session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryGapAfterRenewalRestartsBeforeOldSessionRecovery()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var recoveredSnapshot = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var releaseOldGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRenewedGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldSession = CreateRecoveryRenewalSession(releaseOldGap, null, recoveredSnapshot, expiresOnSend: true);
        var renewedSession = CreateRecoveryRenewalSession(releaseRenewedGap, null, recoveredSnapshot, expiresOnSend: false);
        var transport = new RecordingTransport { Capabilities = SnapshotRecoveryRemoteFeatures };
        transport.Sessions.Enqueue(oldSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateRecoveryRenewalEngine(store, transport, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        Task? uploadSync = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await oldSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(oldSession, uploadSync);
            oldSession.ReleasePausedSendAttempt();
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await uploadSync.WaitAsync(GuardTimeout);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

            releaseOldGap.SetResult();
            await oldSession.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await renewedSession.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await renewedSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseRenewedGap.SetResult();
            await renewedSession.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await WaitForRenewedRecoveryCursorAsync(store, renewedSession, receipt.OperationId);
            await WaitForConditionAsync(() => renewedSession.Acknowledgements.Count == ExpectedSingleOperation);

            await Assert.That(oldSession.SnapshotRecoveryRequestCount).IsEqualTo(0);
            await Assert.That(renewedSession.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            _ = releaseOldGap.TrySetResult();
            _ = releaseRenewedGap.TrySetResult();
            oldSession.ReleasePausedSendAttempt();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Verifies a stale gap joins cancellation already owned by stream stop.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryStaleGapJoinsExistingReceiveCancellationDrain()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var recoveredSnapshot = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var releaseOldGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationCallbackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseCancellationCallback = new ManualResetEventSlim(false);
        var oldSession = CreateRecoveryRenewalSession(
            releaseOldGap,
            null,
            recoveredSnapshot,
            expiresOnSend: true,
            cancellationCallbackEntered: cancellationCallbackEntered,
            releaseCancellationCallback: releaseCancellationCallback,
            ignoreGapCancellation: true);
        var renewedSession = new ReceiveSession { NegotiatedCapabilities = oldSession.NegotiatedCapabilities };
        var transport = new RecordingTransport { Capabilities = SnapshotRecoveryRemoteFeatures };
        transport.Sessions.Enqueue(oldSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateRecoveryRenewalEngine(store, transport, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        Task? uploadSync = null;
        Task? stop = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await oldSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(oldSession, uploadSync);
            oldSession.ReleasePausedSendAttempt();
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await uploadSync.WaitAsync(GuardTimeout);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

            stop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await cancellationCallbackEntered.Task.WaitAsync(GuardTimeout);
            releaseOldGap.SetResult();
            await oldSession.SubscribeCompleted.Task.WaitAsync(GuardTimeout);
            await Assert.That(stop.IsCompleted).IsFalse();
            releaseCancellationCallback.Set();
            await stop.WaitAsync(GuardTimeout);
            await Assert.That(oldSession.SnapshotRecoveryRequestCount).IsEqualTo(0);
        }
        finally
        {
            _ = releaseOldGap.TrySetResult();
            releaseCancellationCallback.Set();
            oldSession.ReleasePausedSendAttempt();
            if (stop is not null)
            {
                await stop.WaitAsync(GuardTimeout);
            }

            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await AwaitTriggerCompletionAsync(uploadSync);
        }
    }

    /// <summary>Waits for the renewed recovery cursor with a durable-state trace on failure.</summary>
    /// <param name="store">The instrumented durable store.</param>
    /// <param name="session">The renewed receive session.</param>
    /// <param name="operationId">The local upload operation.</param>
    /// <returns>The convergence task.</returns>
    /// <exception cref="TimeoutException">The durable recovered cursor was not observed.</exception>
    private static async Task WaitForRenewedRecoveryCursorAsync(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        OperationId operationId)
    {
        try
        {
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, CreateSnapshotRecoveryCursor(Stream)).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false);
            var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            throw new TimeoutException(
                $"capture={store.CaptureRequestCount};requested={session.SnapshotRecoveryRequestCount};"
                + $"committed={store.LastSnapshotRecoveryCommit is not null};cursor={recovered.ServerCursor};"
                + $"pending={recovered.PendingOperations.Count};status={status?.State};ack={session.Acknowledgements.Count}",
                exception);
        }
    }

    /// <summary>Creates one gap-capable shared session for the renewal transition.</summary>
    /// <param name="releaseGap">The gate holding the gap until upload is active.</param>
    /// <param name="releaseRecovery">The optional gate holding an old-generation recovery request.</param>
    /// <param name="resultFactory">The request-matched recovered checkpoint response.</param>
    /// <param name="expiresOnSend">Whether the first prepared upload expires this session.</param>
    /// <param name="cancellationCallbackEntered">The optional cancellation callback signal.</param>
    /// <param name="releaseCancellationCallback">The optional gate holding cancellation callback completion.</param>
    /// <param name="ignoreGapCancellation">Whether remote gap emission ignores receive cancellation.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateRecoveryRenewalSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource? releaseRecovery,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory,
        bool expiresOnSend,
        TaskCompletionSource? cancellationCallbackEntered = null,
        ManualResetEventSlim? releaseCancellationCallback = null,
        bool ignoreGapCancellation = false) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            IgnoreSubscriptionGapCancellation = ignoreGapCancellation,
            SubscribeCancellationCallbackEntered = cancellationCallbackEntered,
            ReleaseSubscribeCancellationCallback = releaseCancellationCallback,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            ReleaseSnapshotRecovery = releaseRecovery,
            SnapshotRecoveryResultFactory = resultFactory,
            PauseBeforeSendNumber = expiresOnSend ? ExpectedSingleOperation : 0,
            PreparedSendException = expiresOnSend ? CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) : null,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates a recovery-capable engine with queued old and renewed shared sessions.</summary>
    /// <param name="store">The instrumented durable store.</param>
    /// <param name="transport">The queued transport.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The configured engine.</returns>
    private static SyncEngine CreateRecoveryRenewalEngine(
        InstrumentedSnapshotRecoveryStore store,
        RecordingTransport transport,
        TimeProvider clock) =>
        new(new()
        {
            Store = store,
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Borrowed,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = CreateSnapshotRecoveryRetryOptions(),
            StoreInitialization = new("sync-engine-tests", RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = clock,
            MaxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });
}
