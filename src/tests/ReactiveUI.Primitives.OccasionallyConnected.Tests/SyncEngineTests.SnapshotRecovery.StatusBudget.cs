// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Observer retention controls for snapshot recovery statuses.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>A budget between a short terminal status and one with a 128-character reason.</summary>
    private const int SnapshotRecoveryStatusObserverBytes = 256;

    /// <summary>Verifies recovery status retention charges the persisted reason after durable commit.</summary>
    /// <param name="oversizedReason">Whether the durable reason exceeds the observer budget.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotRecoveryBoundsPersistedTerminalStatusNotification(bool oversizedReason)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var session = new ReceiveSession();
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStreamWithProjection(
            store,
            engine,
            new ReceiveCounterProjection(),
            clock,
            new(Stream, Subscription, true, ExpectedCapacityCommitAttempts),
            new(ReceiveReplayNotificationCapacity, SnapshotRecoveryStatusObserverBytes, ObserverNotificationOverflowMode.CoalesceLatest));
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(statuses);
        var reason = oversizedReason ? new string('r', SnapshotRecoveryMaximumReasonLength) : "rejected";
        session.SnapshotRecoveryResult = await CreateRejectedSnapshotRecoveryResultAsync(receipt.OperationId, reason);

        var recovered = await stream.RecoverSnapshotAsync(session, SnapshotRecoveryExpiredCursor, new(), CancellationToken.None);
        if (oversizedReason)
        {
            await WaitForConditionAsync(() => statuses.Error is ObserverNotificationOverflowException);
            await Assert.That(statuses.Values.Count).IsEqualTo(0);
            await Assert.That(statuses.Error).IsTypeOf<ObserverNotificationOverflowException>();
        }
        else
        {
            await statuses.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
            await Assert.That(statuses.Error).IsNull();
            await Assert.That(statuses.Values[0].ReasonCode).IsEqualTo(reason);
        }

        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(recovered.QueueSnapshot.PendingOperations).IsEqualTo(0);
        await Assert.That(persisted?.State).IsEqualTo(SyncOperationState.Rejected);
        await Assert.That(persisted?.ReasonCode).IsEqualTo(reason);
    }
}
