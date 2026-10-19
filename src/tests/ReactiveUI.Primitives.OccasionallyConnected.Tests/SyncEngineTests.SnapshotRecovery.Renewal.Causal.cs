// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Cross-stream shared-session renewal coverage for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies renewal cancels an active snapshot request and restarts it on the replacement session.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadExpiryOnOtherStreamRestartsActiveSnapshotRecoveryOnRenewedSession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var releaseOldGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOldRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRenewedGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldSession = CreateRecoveryRenewalSession(releaseOldGap, releaseOldRecovery, resultFactory, expiresOnSend: true);
        var renewedSession = CreateRecoveryRenewalSession(releaseRenewedGap, null, resultFactory, expiresOnSend: false);
        var transport = new RecordingTransport { Capabilities = SnapshotRecoveryRemoteFeatures };
        transport.Sessions.Enqueue(oldSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateRecoveryRenewalEngine(store, transport, clock);
        await using var recovering = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await using var uploading = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription, receiveEnabled: false);
        await recovering.StartAsync(CancellationToken.None);
        await uploading.StartAsync(CancellationToken.None);
        Task? uploadSync = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await oldSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseOldGap.SetResult();
            await oldSession.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            var receipt = await uploading.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(SnapshotRecoveryOtherStream), CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(oldSession, uploadSync);
            oldSession.ReleasePausedSendAttempt();
            await WaitForConditionAsync(() => oldSession.SnapshotRecoveryCancellationCount == ExpectedSingleOperation);
            await renewedSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseRenewedGap.SetResult();
            await renewedSession.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
            await WaitForRecoveredCursorAsync(store, Stream, Subscription, CreateSnapshotRecoveryCursor(Stream));
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await uploadSync.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => oldSession.DisposeCalls == ExpectedSingleOperation);

            await Assert.That(oldSession.SnapshotRecoveryCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(oldSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(renewedSession.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(oldSession.GetSnapshotRecoveryRequest(0).StreamId).IsEqualTo(Stream);
            await Assert.That(renewedSession.GetSnapshotRecoveryRequest(0).SubscriptionId).IsEqualTo(Subscription);
            await Assert.That(renewedSession.GetSnapshotRecoveryRequest(0).ExpiredCursor).IsEqualTo(SnapshotRecoveryExpiredCursor);
            await Assert.That(renewedSession.DisposeCalls).IsEqualTo(0);
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
}
