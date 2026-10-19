// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests durable result notifications through the typed stream.</summary>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The maximum bytes in a result fixture lease.</summary>
    private const long ResultLeaseMaximumBytes = 4096;

    /// <summary>The number of edits in a mixed result fixture.</summary>
    private const int MixedResultOperationCount = 2;

    /// <summary>Verifies an unknown remote result kind cannot consume a durable edit or publish a status.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsUnknownSqliteResultKindBeforeDurableMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(statuses);
        var lease = await LeaseStreamResultAsync(store, 1);
        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        var before = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var batch = new SyncBatch(lease.LeaseId, lease.Operations);
        var invalid = new RemoteSyncResult(
            batch.BatchId,
            [new(receipt.OperationId, (OperationResultKind)int.MaxValue, null, null)],
            null,
            null);

        await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => participant.ApplySyncResultAsync(batch, invalid, CancellationToken.None).AsTask());
        scheduler.RunAll();

        var after = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var retained = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(after).IsEqualTo(before);
        await Assert.That(retained.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(retained.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(statuses.Values.Count).IsEqualTo(0);

        var accepted = new RemoteSyncResult(batch.BatchId, [new(receipt.OperationId, OperationResultKind.Accepted, null, null)], null, null);
        var transition = await participant.ApplySyncResultAsync(batch, accepted, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(transition.QueueSnapshot?.PendingOperations).IsEqualTo(0);
        await Assert.That(transition.QueueSnapshot?.PendingBytes).IsEqualTo(0);
        await Assert.That(statuses.Values.Count).IsEqualTo(1);
        await Assert.That(statuses.Values[0].State).IsEqualTo(SyncOperationState.Synchronized);
        var completed = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(completed?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies malformed acknowledgements preserve edits and valid retryable results allow later progress.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsPartialSqliteResultAndRetainsRetryableEdit()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var first = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        var second = await stream.PublishAsync(new(SecondValue), null, CancellationToken.None);
        scheduler.RunAll();
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        var lease = await LeaseStreamResultAsync(store, MixedResultOperationCount);
        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [new(second.OperationId, OperationResultKind.Accepted, null, null)],
            null,
            null);

        var batch = new SyncBatch(lease.LeaseId, lease.Operations);
        await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => participant.ApplySyncResultAsync(batch, result, CancellationToken.None).AsTask());
        var before = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(before.PendingOperations.Count).IsEqualTo(MixedResultOperationCount);
        var completeResult = new RemoteSyncResult(
            lease.LeaseId,
            [new(second.OperationId, OperationResultKind.Accepted, null, null), new(first.OperationId, OperationResultKind.Retryable, null, null)],
            null,
            null);
        var transition = await participant.ApplySyncResultAsync(batch, completeResult, CancellationToken.None);
        scheduler.RunAll();
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(transition.QueueSnapshot?.PendingOperations).IsEqualTo(1);
        await Assert.That(transition.QueueSnapshot?.PendingBytes).IsEqualTo(SyncEngine.GetOperationRetainedBytes(lease.Operations[0]));
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(local.Values.Count).IsGreaterThan(0);
        await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue + SecondValue);

        var retry = await LeaseStreamResultAsync(store, MixedResultOperationCount);
        await Assert.That(retry.Operations.Count).IsEqualTo(1);
        await Assert.That(retry.Operations[0].OperationId).IsEqualTo(first.OperationId);
        var accepted = new RemoteSyncResult(
            retry.LeaseId,
            [new(first.OperationId, OperationResultKind.Accepted, null, null)],
            null,
            null);
        var completed = await participant.ApplySyncResultAsync(new(retry.LeaseId, retry.Operations), accepted, CancellationToken.None);
        await Assert.That(completed.QueueSnapshot?.PendingOperations).IsEqualTo(0);
        await Assert.That(completed.QueueSnapshot?.PendingBytes).IsEqualTo(0);
        var final = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(final.PendingOperations.Count).IsEqualTo(0);
        var status = await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies a retryable response preserves the edit and bounds retained status notifications.</summary>
    /// <param name="oversizedReason">Whether the reason exceeds the observer byte capacity.</param>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ApplySyncResultAsyncPreservesRetryableEditAndBoundsStatusNotifications(bool oversizedReason)
    {
        var retryReason = oversizedReason ? new string('x', NotificationCapacityBytes) : "server-busy";
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(statuses);
        var local = new RecordingObserver<CounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        LeasedOperationBatch lease;
        await using (var leases = store.LeasePendingOperationsAsync(
            new(Stream, 1, ResultLeaseMaximumBytes, TimeSpan.FromMinutes(1)),
            CancellationToken.None).GetAsyncEnumerator())
        {
            await Assert.That(await leases.MoveNextAsync()).IsTrue();
            lease = leases.Current;
        }

        var batch = new SyncBatch(lease.LeaseId, lease.Operations);
        var result = new RemoteSyncResult(batch.BatchId, [new(receipt.OperationId, OperationResultKind.Retryable, retryReason, null)], null, null);
        var participant = (IOccasionallyConnectedStreamParticipant)stream;
        _ = await participant.ApplySyncResultAsync(batch, result, CancellationToken.None);
        scheduler.RunAll();
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);

        if (oversizedReason)
        {
            await Assert.That(statuses.Values.Count).IsEqualTo(0);
            await Assert.That(statuses.Error).IsTypeOf<ObserverNotificationOverflowException>();
        }
        else
        {
            await Assert.That(statuses.Values.Count).IsEqualTo(1);
            await Assert.That(statuses.Values[0].OperationId).IsEqualTo(receipt.OperationId);
            await Assert.That(statuses.Values[0].State).IsEqualTo(SyncOperationState.QueuedForUpload);
            await Assert.That(statuses.Values[0].ReasonCode).IsEqualTo(retryReason);
        }

        await Assert.That(persisted?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(persisted?.ReasonCode).IsEqualTo(retryReason);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(local.Values.Count).IsGreaterThan(0);
        await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies dead-letter status delivery respects retained bytes after durable reconciliation.</summary>
    /// <param name="oversizedReason">Whether the reason exceeds the observer byte capacity.</param>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DeadLetterOperationAsyncBoundsStatusNotificationAfterDurableReconciliation(bool oversizedReason)
    {
        var reason = oversizedReason ? new string('x', NotificationCapacityBytes) : "cannot-upload";
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        scheduler.RunAll();
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(statuses);
        var local = new RecordingObserver<CounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        var lease = await LeaseStreamResultAsync(store, 1);
        var participant = (IOccasionallyConnectedStreamParticipant)stream;

        var transition = await participant.DeadLetterOperationAsync(lease.LeaseId, receipt.OperationId, reason, CancellationToken.None);
        scheduler.RunAll();

        if (oversizedReason)
        {
            await Assert.That(statuses.Values.Count).IsEqualTo(0);
            await Assert.That(statuses.Error).IsTypeOf<ObserverNotificationOverflowException>();
        }
        else
        {
            await Assert.That(statuses.Values.Count).IsEqualTo(1);
            await Assert.That(statuses.Values[0].State).IsEqualTo(SyncOperationState.DeadLettered);
            await Assert.That(statuses.Values[0].ReasonCode).IsEqualTo(reason);
        }

        await Assert.That(transition.QueueSnapshot?.PendingOperations).IsEqualTo(0);
        await Assert.That(transition.QueueSnapshot?.PendingBytes).IsEqualTo(0);
        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(persisted?.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(persisted?.ReasonCode).IsEqualTo(reason);
        await Assert.That(local.Values.Count).IsGreaterThan(0);
        await Assert.That(local.Values[^1].Sum).IsEqualTo(0);
    }

    /// <summary>Leases the pending operations used by a stream result test.</summary>
    /// <param name="store">The initialized local store.</param>
    /// <param name="maximumOperations">The maximum batch count.</param>
    /// <returns>The first durable lease.</returns>
    private static async Task<LeasedOperationBatch> LeaseStreamResultAsync(SqliteLocalStoreAdapter store, int maximumOperations)
    {
        await using var leases = store.LeasePendingOperationsAsync(
            new(Stream, maximumOperations, ResultLeaseMaximumBytes, TimeSpan.FromMinutes(1)),
            CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await leases.MoveNextAsync()).IsTrue();
        return leases.Current;
    }
}
