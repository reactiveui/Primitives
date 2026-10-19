// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Reopen assertions for the <see cref="SqliteCommitCheckpoint"/> crash matrix.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Reopens the killed writer's database and asserts the invariants for the case checkpoint.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static Task AssertCrashMatrixRecoveryAsync(CrashMatrixCase matrixCase) => matrixCase.Checkpoint switch
    {
        SqliteCommitCheckpoint.LocalCommitBeforeCommit => AssertLocalCommitRolledBackAsync(matrixCase),
        SqliteCommitCheckpoint.LocalCommitAfterCommit => AssertLocalCommitDurableAsync(matrixCase),
        SqliteCommitCheckpoint.AttemptBarrierBeforeCommit => AssertAttemptBarrierRolledBackAsync(matrixCase),
        SqliteCommitCheckpoint.AttemptBarrierAfterCommit or SqliteCommitCheckpoint.SyncResultBeforeCommit =>
            AssertAttemptBarrierRecoveryAsync(matrixCase.DatabasePath, matrixCase.OperationId, matrixCase.SubscriptionId, matrixCase.DeliveryGuarantee),
        SqliteCommitCheckpoint.SyncResultAfterCommit => AssertSyncResultDurableAsync(matrixCase),
        SqliteCommitCheckpoint.RemoteApplyBeforeCommit => AssertRemoteApplyRolledBackAsync(matrixCase),
        SqliteCommitCheckpoint.RemoteApplyAfterCommit => AssertRemoteApplyDurableAsync(matrixCase),
        SqliteCommitCheckpoint.DeadLetterBeforeCommit => AssertDeadLetterRolledBackAsync(matrixCase),
        SqliteCommitCheckpoint.DeadLetterAfterCommit => AssertDeadLetterDurableAsync(matrixCase),
        _ => AssertCompactionRecoveryAsync(matrixCase),
    };

    /// <summary>Opens and initializes a parent-side adapter at the supplied time.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <param name="timestamp">The adapter clock value.</param>
    /// <returns>The initialized adapter.</returns>
    private static async Task<SqliteLocalStoreAdapter> ReopenCrashMatrixAdapterAsync(CrashMatrixCase matrixCase, DateTimeOffset timestamp)
    {
        var adapter = CreateAdapter(matrixCase.DatabasePath, new FixedTimeProvider(timestamp));
        await adapter.InitializeAsync(CreateCrashMatrixInitialization(matrixCase.Checkpoint), CancellationToken.None);
        return adapter;
    }

    /// <summary>Asserts a local commit killed inside its transaction left no row and did not consume its sequence.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertLocalCommitRolledBackAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(matrixCase.OperationId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.Snapshot).IsNull();
        await Assert.That(status).IsNull();

        var retried = await reopened.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.OperationId, FirstClientSequence, matrixCase.DeliveryGuarantee, "matrix"),
            CreateCrashMatrixInitialMutation(),
            CancellationToken.None);
        var afterRetry = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);

        await Assert.That(retried.ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(afterRetry.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterRetry.PendingOperations[0].OperationId).IsEqualTo(matrixCase.OperationId);
        await Assert.That(afterRetry.NextClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Asserts a local commit killed after its transaction is durable exactly once.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertLocalCommitDurableAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(matrixCase.OperationId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(matrixCase.OperationId);
        await Assert.That(recovered.PendingOperations[0].ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations[0].Policy.DeliveryGuarantee).IsEqualTo(matrixCase.DeliveryGuarantee);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);

        var replayed = await reopened.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.OperationId, FirstClientSequence, matrixCase.DeliveryGuarantee, "matrix"),
            CreateCrashMatrixInitialMutation(),
            CancellationToken.None);
        var afterReplay = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);

        await Assert.That(replayed.ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(afterReplay.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterReplay.NextClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Asserts an attempt barrier killed inside its transaction leaves the operation unsent and sendable.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertAttemptBarrierRolledBackAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryExpiredLeaseTimestamp);
        var status = await reopened.GetOperationStatusAsync(matrixCase.OperationId, CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(status?.State).IsNotEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(matrixCase.OperationId);

        var lease = await LeaseFirstCrashMatrixBatchAsync(reopened, FirstAttempt);
        var retry = await reopened.TryBeginRemoteAttemptAsync(lease.LeaseId, matrixCase.OperationId, FirstAttempt, CancellationToken.None);

        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(matrixCase.OperationId);
        await Assert.That(retry.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(retry.MaySend).IsTrue();
    }

    /// <summary>Asserts an upload result killed after its transaction stays terminal and fences the old lease.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertSyncResultDurableAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryExpiredLeaseTimestamp);
        var status = await reopened.GetOperationStatusAsync(matrixCase.OperationId, CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var leases = await ReadLeasesAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(leases.Count).IsEqualTo(0);
    }

    /// <summary>Asserts a receive batch killed inside its transaction left cursor, inbox, and snapshot untouched.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertRemoteApplyRolledBackAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var unapplied = await reopened.GetUnappliedEventIdsAsync(Stream, [matrixCase.EventId], CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(CrashRecoveryAuthoritativeInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(1);

        var redelivered = await reopened.ApplyRemoteBatchAsync(
            CreateCrashMatrixRemoteBatch(matrixCase, matrixCase.BatchId, null, CrashRecoveryRemoteCursor),
            CreateCrashMatrixRemoteMutation(FirstClientSequence),
            CancellationToken.None);
        var afterRedelivery = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var unappliedAfterRedelivery = await reopened.GetUnappliedEventIdsAsync(Stream, [matrixCase.EventId], CancellationToken.None);

        await Assert.That(redelivered.AppliedCount).IsEqualTo(1);
        await Assert.That(redelivered.DuplicateCount).IsEqualTo(0);
        await AssertRemoteApplyCrashRecoveryAsync(afterRedelivery, matrixCase.OperationId, unappliedAfterRedelivery);
    }

    /// <summary>Asserts a receive batch killed after its transaction is durable and a redelivery is a duplicate.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertRemoteApplyDurableAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var unapplied = await reopened.GetUnappliedEventIdsAsync(Stream, [matrixCase.EventId], CancellationToken.None);
        await AssertRemoteApplyCrashRecoveryAsync(recovered, matrixCase.OperationId, unapplied);

        var redelivered = await reopened.ApplyRemoteBatchAsync(
            CreateCrashMatrixRemoteBatch(matrixCase, Guid.NewGuid(), CrashRecoveryRemoteCursor, CrashMatrixRedeliveryCursor),
            CreateSnapshotMutation(CrashRecoveryReceiveRevision, CrashRecoveryRemotePayloadText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeRemoteText) },
            CancellationToken.None);

        await Assert.That(redelivered.AppliedCount).IsEqualTo(0);
        await Assert.That(redelivered.DuplicateCount).IsEqualTo(1);
    }

    /// <summary>Asserts a dead-letter killed inside its transaction left both operations pending and can be repeated.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertDeadLetterRolledBackAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryExpiredLeaseTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var secondStatus = await reopened.GetOperationStatusAsync(matrixCase.SecondOperationId, CancellationToken.None);

        await Assert.That(secondStatus?.State).IsNotEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(CrashMatrixTwoCommitRevision);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(matrixCase.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(matrixCase.SecondOperationId);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(CrashMatrixTwoCommitRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticLocalText);

        await DeadLetterCrashMatrixSecondOperationAsync(reopened, matrixCase);
        var afterRetry = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);

        await Assert.That(afterRetry.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(afterRetry.DeadLetters[0].Operation.OperationId).IsEqualTo(matrixCase.SecondOperationId);
        await Assert.That(afterRetry.PendingOperations.Count).IsEqualTo(1);
    }

    /// <summary>Asserts a dead-letter killed after its transaction is durable.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertDeadLetterDurableAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var secondStatus = await reopened.GetOperationStatusAsync(matrixCase.SecondOperationId, CancellationToken.None);

        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(matrixCase.SecondOperationId);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(matrixCase.OperationId);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(CrashRecoveryDeadLetterRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(CrashRecoveryDeadLetterPayloadText);
    }

    /// <summary>Asserts compaction keeps pending work and a rebuildable snapshot across a crash at either side of commit.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertCompactionRecoveryAsync(CrashMatrixCase matrixCase)
    {
        await using var reopened = await ReopenCrashMatrixAdapterAsync(matrixCase, CrashRecoveryExpiredLeaseTimestamp);
        var recovered = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var firstStatus = await reopened.GetOperationStatusAsync(matrixCase.OperationId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(matrixCase.SecondOperationId);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(CrashMatrixThirdClientSequence);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(CrashMatrixTwoCommitRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(CrashMatrixCompactionPayloadText);
        if (matrixCase.Checkpoint == SqliteCommitCheckpoint.CompactionBeforeCommit)
        {
            await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        }

        _ = await reopened.CompactAsync(new(Stream, CrashRecoveryExpiredLeaseTimestamp, TargetBytes: 0), CancellationToken.None);
        var afterCompaction = await reopened.RecoverStreamAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        var lease = await LeaseFirstCrashMatrixBatchAsync(reopened, FirstAttempt);

        await Assert.That(afterCompaction.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterCompaction.PendingOperations[0].OperationId).IsEqualTo(matrixCase.SecondOperationId);
        await Assert.That(afterCompaction.Snapshot?.Revision).IsEqualTo(CrashMatrixTwoCommitRevision);
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(matrixCase.SecondOperationId);
    }
}
