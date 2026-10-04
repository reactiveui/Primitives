// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Independent receive retry ownership tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies upload renewal leaves a separately owned receive retry session running.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadRenewalDoesNotCancelOwnedReceiveRetrySession()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var uploadStream = new StreamId("sync/engine/renewal-upload");
        var operation = CreateOperation(uploadStream, operationId: OperationId.New());
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var batch = CreateReceiveBatch();
        var shared = new ReceiveSession
        {
            Batches = { batch },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)),
            PreparedSendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var ownedRetry = new ReceiveSession { Batches = { batch } };
        var renewed = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(shared, ownedRetry, renewed);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, transport, CreateSnapshotRecoveryRetryOptions(), timeProvider: clock);
        using var receiveRegistration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });
        using var uploadRegistration = engine.RegisterParticipant(CreateUploadParticipant(store, streamId: uploadStream));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await shared.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));
        clock.Advance(TimeSpan.FromSeconds(1));
        await ownedRetry.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await ownedRetry.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        engine.NotifyLocalCommitReady(uploadStream, operation);
        await WaitForReceiveSessionUploadAsync(clock, shared, faults);
        await WaitForReceiveSessionUploadAsync(clock, renewed, faults);
        await WaitForConditionAsync(() => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
        await Assert.That(ownedRetry.SubscribeCompleted.Task.IsCompleted).IsFalse();
        await Assert.That(ownedRetry.SubscribeCanceled.Task.IsCompleted).IsFalse();
        await Assert.That(ownedRetry.LastSubscriptionCancellationToken.IsCancellationRequested).IsFalse();
        await Assert.That(ownedRetry.DisposeCalls).IsEqualTo(0);
        await engine.StopStreamAsync(Stream, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(ownedRetry.LastSubscriptionCancellationToken.IsCancellationRequested).IsTrue();
        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(ownedRetry.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
