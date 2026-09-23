// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Public paired projection controls for snapshot recovery.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The maximum valid ASCII reason length for default snapshot recovery limits.</summary>
    private const int SnapshotRecoveryMaximumReasonLength = 128;

    /// <summary>Verifies a committed recovery clears pending state in both public projections.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryPublishesCommittedQueuePairToPublicObservers()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var session = new ReceiveSession();
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        var pending = new RecordingObserver<PendingSyncSummary>();
        var synchronized = new RecordingObserver<ReceiveCounterState>();
        using var pendingSubscription = stream.ObservePending().Subscribe(pending);
        using var synchronizedSubscription = stream.WhereSynchronized().Subscribe(synchronized);
        await stream.StartAsync(CancellationToken.None);
        await pending.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await synchronized.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(synchronized.Values[0].Sum).IsEqualTo(0);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        await pending.WaitForCountAsync(ExpectedTwoOperations, GuardTimeout);

        var recoveredState = new ReceiveCounterState(SnapshotRecoveryRejectedCounterValue);
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, recoveredState, CancellationToken.None)
            .ConfigureAwait(false);
        session.SnapshotRecoveryResult = new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions = [CreateRejectedDisposition(receipt.OperationId)],
        };

        _ = await stream.RecoverSnapshotAsync(session, SnapshotRecoveryExpiredCursor, new(), CancellationToken.None);
        await pending.WaitForCountAsync(SnapshotRecoveryReversedDispositionCount, GuardTimeout);
        await synchronized.WaitForCountAsync(ExpectedTwoOperations, GuardTimeout);

        await Assert.That(pending.Values[0].OperationCount).IsEqualTo(0);
        await Assert.That(pending.Values[1].OperationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(pending.Values[ExpectedTwoOperations].OperationCount).IsEqualTo(0);
        await Assert.That(synchronized.Values[^1].Sum).IsEqualTo(SnapshotRecoveryRejectedCounterValue);
        var durable = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(durable.PendingOperations).IsEmpty();
    }

    /// <summary>Verifies a gap-driven terminal notification reproduces the committed durable status.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryTerminalStatusPreservesPersistedAttemptAndReason()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        store.AfterSnapshotRecoveryCommit = () => clock.Advance(TimeSpan.FromMinutes(ExpectedSingleOperation));
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        var observed = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(observed);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        await observed.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        await Assert.That(observed.Values[0].OperationId).IsEqualTo(receipt.OperationId);

        var leaseCount = 0;
        await foreach (var lease in store.LeasePendingOperationsAsync(
            new(Stream, ExpectedSingleOperation, PreparedUploadBytes, TimeSpan.FromMinutes(ExpectedSingleOperation)),
            CancellationToken.None))
        {
            leaseCount++;
            await Assert.That(lease.Operations[0].OperationId).IsEqualTo(receipt.OperationId);
            var attempt = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, receipt.OperationId, ExpectedSingleOperation, CancellationToken.None);
            await Assert.That(attempt.Attempt).IsEqualTo(ExpectedSingleOperation);
            await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        }

        await Assert.That(leaseCount).IsEqualTo(ExpectedSingleOperation);
        var reasonCode = new string('r', SnapshotRecoveryMaximumReasonLength);
        session.SnapshotRecoveryResult = await CreateRejectedSnapshotRecoveryResultAsync(receipt.OperationId, reasonCode);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
        _ = releaseGap.TrySetResult();
        await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout);
        _ = releaseRecovery.TrySetResult();
        await session.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);
        await observed.WaitForCountAsync(ExpectedTwoOperations, GuardTimeout);

        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(persisted).IsNotNull();
        var terminal = observed.Values.Single(value => value.OperationId == receipt.OperationId && value.State == SyncOperationState.Rejected);
        await Assert.That(terminal).IsEqualTo(persisted);
        await Assert.That(terminal.Attempt).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(terminal.ChangedAtUtc).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(terminal.ReasonCode).IsEqualTo(reasonCode);
    }

    /// <summary>Verifies a postcommit status read failure reports a fault without retrying durable recovery.</summary>
    /// <param name="throws">Whether the read throws instead of returning no status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotRecoveryStatusReadFailurePreservesCommittedRecovery(bool throws)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var session = new ReceiveSession();
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        await using var stream = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var statusSubscription = stream.OperationStates.Subscribe(statuses);
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        await statuses.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);

        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(SnapshotRecoveryCounterStateContractId, ExpectedSingleOperation, new ReceiveCounterState(ExpectedSingleOperation), CancellationToken.None)
            .ConfigureAwait(false);
        session.SnapshotRecoveryResult = new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions = [CreateRejectedDisposition(receipt.OperationId)],
        };
        store.PostRecoveryStatusReadOverride = throws
            ? static _ => ValueTask.FromException<SyncOperationStatus?>(new InvalidOperationException("Postcommit status read failed."))
            : static _ => new((SyncOperationStatus?)null);

        var recovered = await stream.RecoverSnapshotAsync(session, SnapshotRecoveryExpiredCursor, new(), CancellationToken.None);
        await faults.WaitForCountAsync(ExpectedSingleOperation, GuardTimeout);
        store.PostRecoveryStatusReadOverride = null;
        var durable = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);

        await Assert.That(recovered.QueueSnapshot.PendingOperations).IsEqualTo(0);
        await Assert.That(store.LastSnapshotRecoveryCommit).IsNotNull();
        await Assert.That(durable?.State).IsEqualTo(SyncOperationState.Rejected);
        await Assert.That(faults.Values[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(statuses.Values.Count).IsEqualTo(ExpectedSingleOperation);
    }
}
