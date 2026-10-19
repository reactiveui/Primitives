// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb.Tests;

/// <summary>Workflow and state-transition tests for the BLite adapter.</summary>
public sealed partial class BliteDbLocalStoreAdapterTests
{
    /// <summary>Verifies sync results update operation states and snapshot replacements.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SyncResultsUpdateOperationAndSnapshotStates()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("sync-results");
        const int leaseOperationLimit = FourthRevision;
        await using var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);

        var accepted = CreateOperation(stream, FirstSequence);
        var conflicted = CreateOperation(stream, SecondSequence);
        var rejected = CreateOperation(stream, ThirdSequence);
        var retryable = CreateOperation(stream, FourthSequence);
        _ = await adapter.CommitLocalOperationAsync(accepted, CreateSnapshotMutation(stream, "s0", InitialRevision), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(conflicted, CreateSnapshotMutation(stream, "s1", FirstRevision), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(rejected, CreateSnapshotMutation(stream, "s2", SecondRevision), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(retryable, CreateSnapshotMutation(stream, "s3", ThirdRevision), CancellationToken.None);

        var lease = await ReadOptionalLeaseAsync(
            adapter,
            new(stream, leaseOperationLimit, LeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease).IsNotNull();
        var changed = await adapter.ApplySyncResultAsync(
            lease!.LeaseId,
            new(
                Guid.NewGuid(),
                [
                    new OperationSyncResult(accepted.OperationId, OperationResultKind.Accepted, null, null),
                    new OperationSyncResult(conflicted.OperationId, OperationResultKind.Conflict, "conflict", null),
                    new OperationSyncResult(rejected.OperationId, OperationResultKind.Rejected, "invalid", null),
                    new OperationSyncResult(retryable.OperationId, OperationResultKind.Retryable, "retry", null),
                ],
                null,
                null),
            [CreateSnapshotMutation(stream, "reconciled", FourthRevision)],
            CancellationToken.None);

        await Assert.That(changed).HasSingleItem();
        await Assert.That(changed[0].Revision).IsEqualTo(FifthRevision);
        await Assert.That((await adapter.GetOperationStatusAsync(accepted.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That((await adapter.GetOperationStatusAsync(conflicted.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.Conflict);
        await Assert.That((await adapter.GetOperationStatusAsync(rejected.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.Rejected);
        await Assert.That((await adapter.GetOperationStatusAsync(retryable.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies retry state and ambiguous at-most-once barriers persist durably.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryStateAndAttemptBarrierPersistAcrossReopen()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("attempt-barrier");
        const int initialAttemptNumber = FirstRevision;
        const int retryAttemptNumber = SecondRevision;
        var operation = CreateOperation(stream, FirstSequence) with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
        };
        var clock = new FixedTimeProvider(FixedTimestamp);

        await using (var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath, clock))
        {
            await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(
                operation,
                CreateSnapshotMutation(stream, "state", InitialRevision),
                CancellationToken.None);

            var retryState = RetryState.Start(FixedTimestamp);
            await adapter.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None);
            var lease = await ReadOptionalLeaseAsync(adapter, new(stream, 1, LeaseBytes, TimeSpan.FromMinutes(1)));
            await Assert.That(lease).IsNotNull();

            var firstAttempt = await adapter.TryBeginRemoteAttemptAsync(
                lease!.LeaseId,
                operation.OperationId,
                initialAttemptNumber,
                CancellationToken.None);
            await Assert.That(firstAttempt.MaySend).IsTrue();

            var secondAttempt = await adapter.TryBeginRemoteAttemptAsync(
                lease.LeaseId,
                operation.OperationId,
                retryAttemptNumber,
                CancellationToken.None);
            await Assert.That(secondAttempt.MaySend).IsFalse();
            await Assert.That(secondAttempt.ReasonCode).IsEqualTo("OC.AmbiguousAtMostOnce");
        }

        await using var reopened = new BliteDbLocalStoreAdapter(directory.DatabasePath, clock);
        await reopened.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var reopenedRetryState = await reopened.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        var reopenedStatus = await reopened.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(reopenedRetryState).IsNotNull();
        await Assert.That(reopenedStatus!.State).IsEqualTo(SyncOperationState.Ambiguous);
    }

    /// <summary>Verifies remote inbox deduplication and locally originated completions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RemoteBatchDeduplicatesEventsAndCompletesOperations()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("remote-transitions");
        await using var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        var localOperation = CreateOperation(stream, FirstSequence);
        _ = await adapter.CommitLocalOperationAsync(
            localOperation,
            CreateSnapshotMutation(stream, "initial", InitialRevision),
            CancellationToken.None);

        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            stream,
            FirstCursor,
            FixedTimestamp,
            null,
            CreatePayload("event"),
            new Dictionary<string, string>());
        var duplicateBatch = new RemoteEventBatch(Guid.NewGuid(), stream, null, FirstCursor, [remoteEvent, remoteEvent]);
        var firstApply = await adapter.ApplyRemoteBatchAsync(
            duplicateBatch,
            CreateSnapshotMutation(stream, "remote-1", FirstRevision),
            CancellationToken.None);
        await Assert.That(firstApply.AppliedCount).IsEqualTo(FirstRevision);
        await Assert.That(firstApply.DuplicateCount).IsEqualTo(FirstRevision);

        var completedOperation = CreateOperation(stream, SecondSequence);
        _ = await adapter.CommitLocalOperationAsync(
            completedOperation,
            CreateSnapshotMutation(stream, "local-2", SecondRevision),
            CancellationToken.None);
        RemoteEventBatch completionBatch = new(Guid.NewGuid(), stream, FirstCursor, SecondCursor, [])
        {
            CompletedOperations = [new(new RemoteEventOrigin(ClientIdentity, completedOperation.OperationId), [])],
        };
        _ = await adapter.ApplyRemoteBatchAsync(
            completionBatch,
            CreateSnapshotMutation(stream, "remote-2", ThirdRevision),
            CancellationToken.None);
        await Assert.That((await adapter.GetOperationStatusAsync(completedOperation.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That((await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None)).ServerCursor)
            .IsEqualTo(SecondCursor);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(
            stream,
            [remoteEvent.EventId, Guid.NewGuid()],
            CancellationToken.None);
        await Assert.That(unapplied).HasSingleItem();
    }

    /// <summary>Verifies lease renewal and release gate future leasing correctly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RenewedAndReleasedLeaseControlsReleasingOperations()
    {
        using var directory = new TestDirectory();
        var clock = new FixedTimeProvider(FixedTimestamp);
        var stream = new StreamId("lease-renewal");
        const int shortLeaseSeconds = SecondRevision;
        const int leaseAdvanceSeconds = ThirdRevision;
        var operation = CreateOperation(stream, FirstSequence);
        await using var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath, clock);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(stream, "snapshot", InitialRevision),
            CancellationToken.None);

        var lease = await ReadOptionalLeaseAsync(adapter, new(stream, 1, LeaseBytes, TimeSpan.FromSeconds(shortLeaseSeconds)));
        await Assert.That(lease).IsNotNull();
        await adapter.RenewLeaseAsync(lease!.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(leaseAdvanceSeconds));
        var unavailable = await ReadOptionalLeaseAsync(adapter, new(stream, 1, LeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(unavailable).IsNull();

        await adapter.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var released = await ReadOptionalLeaseAsync(adapter, new(stream, 1, LeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(released).IsNotNull();
        await Assert.That(released!.Operations).HasSingleItem();
    }

    /// <summary>Verifies dead-letter transitions and compaction remove terminal records.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeadLetterOperationsCanBeCompacted()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("dead-letter");
        await using var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);

        var deadLetter = CreateOperation(stream, FirstSequence);
        _ = await adapter.CommitLocalOperationAsync(
            deadLetter,
            CreateSnapshotMutation(stream, "local", InitialRevision),
            CancellationToken.None);
        var lease = await ReadOptionalLeaseAsync(adapter, new(stream, 1, LeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease).IsNotNull();
        _ = await adapter.DeadLetterOperationAsync(
            lease!.LeaseId,
            deadLetter.OperationId,
            "permanent",
            CreateSnapshotMutation(stream, "replacement", FirstRevision),
            CancellationToken.None);
        await Assert.That((await adapter.GetOperationStatusAsync(deadLetter.OperationId, CancellationToken.None))!.State)
            .IsEqualTo(SyncOperationState.DeadLettered);

        var compacted = await adapter.CompactAsync(
            new(stream, DateTimeOffset.MaxValue, 0),
            CancellationToken.None);
        await Assert.That(compacted.RecordsRemoved).IsEqualTo(1);
        await Assert.That(await adapter.GetOperationStatusAsync(deadLetter.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies canceled commits do not change durable state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledCommitDoesNotChangeDurableState()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("canceled-commit");
        var operation = CreateOperation(stream, FirstSequence);
        await using var adapter = new BliteDbLocalStoreAdapter(directory.DatabasePath);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.That(async () =>
        {
            _ = await adapter.CommitLocalOperationAsync(
                operation,
                CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision),
                cancellation.Token);
        }).Throws<OperationCanceledException>();

        var recovered = await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.Snapshot).IsNull();
    }
}
