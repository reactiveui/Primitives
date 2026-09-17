// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Crash recovery tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The child crash recovery mode marker environment variable.</summary>
    private const string CrashRecoveryChildModeVariable = "RXUI_SQLITE_CRASH_RECOVERY_CHILD";

    /// <summary>The child crash recovery database path environment variable.</summary>
    private const string CrashRecoveryDatabasePathVariable = "RXUI_SQLITE_CRASH_RECOVERY_DATABASE";

    /// <summary>The child crash recovery signal path environment variable.</summary>
    private const string CrashRecoverySignalPathVariable = "RXUI_SQLITE_CRASH_RECOVERY_SIGNAL";

    /// <summary>The child crash recovery boundary environment variable.</summary>
    private const string CrashRecoveryBoundaryVariable = "RXUI_SQLITE_CRASH_RECOVERY_BOUNDARY";

    /// <summary>The child crash recovery operation identifier environment variable.</summary>
    private const string CrashRecoveryOperationIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_OPERATION";

    /// <summary>The child crash recovery second operation identifier environment variable.</summary>
    private const string CrashRecoverySecondOperationIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_SECOND_OPERATION";

    /// <summary>The child crash recovery third operation identifier environment variable.</summary>
    private const string CrashRecoveryThirdOperationIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_THIRD_OPERATION";

    /// <summary>The child crash recovery subscription identifier environment variable.</summary>
    private const string CrashRecoverySubscriptionIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_SUBSCRIPTION";

    /// <summary>The child crash recovery event identifier environment variable.</summary>
    private const string CrashRecoveryEventIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_EVENT";

    /// <summary>The child crash recovery receive batch identifier environment variable.</summary>
    private const string CrashRecoveryBatchIdVariable = "RXUI_SQLITE_CRASH_RECOVERY_BATCH";

    /// <summary>The child crash recovery delivery guarantee environment variable.</summary>
    private const string CrashRecoveryDeliveryGuaranteeVariable = "RXUI_SQLITE_CRASH_RECOVERY_GUARANTEE";

    /// <summary>The child crash recovery environment failure message.</summary>
    private const string CrashRecoveryEnvironmentIncompleteMessage = "The child crash recovery environment is incomplete.";

    /// <summary>The crash recovery signal failure message.</summary>
    private const string CrashRecoverySignalMalformedMessage = "The crash recovery signal is malformed.";

    /// <summary>The marker value that enables child crash recovery mode.</summary>
    private const string CrashRecoveryChildMode = "1";

    /// <summary>The pre-send attempt crash boundary name.</summary>
    private const string AttemptBoundary = "attempt";

    /// <summary>The remote receive crash boundary name.</summary>
    private const string RemoteApplyBoundary = "remote-apply";

    /// <summary>The upload result crash boundary name.</summary>
    private const string SyncResultBoundary = "sync-result";

    /// <summary>The dead-letter crash boundary name.</summary>
    private const string DeadLetterBoundary = "dead-letter";

    /// <summary>The ambiguous-attempt reason recorded for at-most-once operations.</summary>
    private const string AttemptAmbiguousReason = "OC.AttemptAmbiguous";

    /// <summary>The optimistic payload committed by receive crash recovery tests.</summary>
    private const string CrashRecoveryRemotePayloadText = "crash-remote";

    /// <summary>The optimistic payload committed by result crash recovery tests.</summary>
    private const string CrashRecoveryResultPayloadText = "crash-result";

    /// <summary>The optimistic payload committed by dead-letter crash recovery tests.</summary>
    private const string CrashRecoveryDeadLetterPayloadText = "crash-dead-letter";

    /// <summary>The initial authoritative payload for crash recovery tests.</summary>
    private const string CrashRecoveryAuthoritativeInitialText = "crash-authoritative-initial";

    /// <summary>The receive authoritative payload for crash recovery tests.</summary>
    private const string CrashRecoveryAuthoritativeRemoteText = "crash-authoritative-remote";

    /// <summary>The cursor used by receive crash recovery tests.</summary>
    private const string CrashRecoveryRemoteCursor = "crash-cursor";

    /// <summary>The stable rejected reason for result crash recovery tests.</summary>
    private const string CrashRecoveryRejectedReason = "OC.CrashRejected";

    /// <summary>The stable dead-letter reason for crash recovery tests.</summary>
    private const string CrashRecoveryDeadLetterReason = "OC.CrashDeadLetter";

    /// <summary>The snapshot revision after one local commit and one receive apply.</summary>
    private const int CrashRecoveryReceiveRevision = 2;

    /// <summary>The snapshot revision after result reconciliation of three local commits.</summary>
    private const int CrashRecoveryResultRevision = 4;

    /// <summary>The snapshot revision after dead-lettering one of two local commits.</summary>
    private const int CrashRecoveryDeadLetterRevision = 3;

    /// <summary>The third client sequence value.</summary>
    private const int ThirdClientSequence = 3;

    /// <summary>The crash recovery signal line count.</summary>
    private const int CrashRecoverySignalLineCount = 12;

    /// <summary>The child crash recovery test tree node filter.</summary>
    private const string CrashRecoveryChildTestTreeNodeFilter = $"/*/*/*/{nameof(WhenCrashRecoveryChildCommitsBoundaryAndWaits_ThenSignalIsPublished)}";

    /// <summary>The fixed timestamp used by child process crash recovery writes.</summary>
    private static readonly DateTimeOffset CrashRecoveryTimestamp = new(2026, 5, 6, 7, 8, 9, TimeSpan.Zero);

    /// <summary>The fixed timestamp used after the child-owned lease has expired.</summary>
    private static readonly DateTimeOffset CrashRecoveryExpiredLeaseTimestamp = CrashRecoveryTimestamp.AddMinutes(2);

    /// <summary>Verifies a process death after the durable pre-send barrier recovers according to delivery policy.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee under test.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    [Arguments(DeliveryGuarantee.AtMostOnce)]
    [Arguments(DeliveryGuarantee.AtLeastOnce)]
    [Arguments(DeliveryGuarantee.ExactlyOnce)]
    public async Task WhenWriterProcessDiesAfterAttemptBarrier_ThenRecoveryHonorsDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
    {
        using var database = TempDatabase.Create();
        var signalPath = CreateCrashRecoverySignalPath(database.Path, AttemptBoundary);
        var operationId = OperationId.New();
        var subscriptionId = SubscriptionId.New();
        await PrepareAttemptBoundaryDatabaseAsync(database.Path, operationId, subscriptionId, deliveryGuarantee);

        await RunCrashRecoveryChildUntilSignalAsync(new(database.Path, signalPath, AttemptBoundary, operationId, subscriptionId, deliveryGuarantee));

        var signal = await ReadCrashRecoverySignalAsync(signalPath);
        await Assert.That(signal.Boundary).IsEqualTo(AttemptBoundary);
        await Assert.That(signal.OperationId).IsEqualTo(operationId.Value);
        await Assert.That(signal.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(signal.MaySend).IsTrue();

        await AssertAttemptBarrierRecoveryAsync(database.Path, operationId, subscriptionId, deliveryGuarantee);
    }

    /// <summary>Verifies a process death after receive apply preserves receive state and fences stale storage replay.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    public async Task WhenWriterProcessDiesAfterRemoteBatchApply_ThenReopenPreservesCursorAndRejectsStaleReplay()
    {
        using var database = TempDatabase.Create();
        var signalPath = CreateCrashRecoverySignalPath(database.Path, RemoteApplyBoundary);
        var operationId = OperationId.New();
        var subscriptionId = SubscriptionId.New();
        var eventId = Guid.NewGuid();
        var batchId = Guid.NewGuid();
        await PrepareSingleOperationDatabaseAsync(database.Path, operationId, subscriptionId, DeliveryGuarantee.AtLeastOnce);

        await RunCrashRecoveryChildUntilSignalAsync(new(database.Path, signalPath, RemoteApplyBoundary, operationId, subscriptionId, DeliveryGuarantee.AtLeastOnce)
        {
            EventId = eventId,
            BatchId = batchId,
        });

        var signal = await ReadCrashRecoverySignalAsync(signalPath);
        await Assert.That(signal.Boundary).IsEqualTo(RemoteApplyBoundary);
        await Assert.That(signal.OperationId).IsEqualTo(operationId.Value);
        await Assert.That(signal.BatchId).IsEqualTo(batchId);
        await Assert.That(signal.SnapshotRevision).IsEqualTo(CrashRecoveryReceiveRevision);
        await Assert.That(signal.AppliedCount).IsEqualTo(FirstAttempt);
        await Assert.That(signal.DuplicateCount).IsEqualTo(0);
        await Assert.That(signal.Cursor).IsEqualTo(CrashRecoveryRemoteCursor);

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await reopened.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None);
        await AssertRemoteApplyCrashRecoveryAsync(recovered, operationId, unapplied);

        var staleReplayEvent = CreateCrashRecoveryRemoteEvent(eventId, CrashRecoveryRemoteCursor, operationId, ClientId);
        var staleReplayBatch = CreateCrashRecoveryRemoteBatch(batchId, previousCursor: null, CrashRecoveryRemoteCursor, staleReplayEvent, operationId);
        Func<Task> staleReplay = async () => _ = await reopened.ApplyRemoteBatchAsync(
            staleReplayBatch,
            CreateSnapshotMutation(CrashRecoveryReceiveRevision, CrashRecoveryRemotePayloadText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeRemoteText) },
            CancellationToken.None);
        await Assert.That(staleReplay).ThrowsExactly<InvalidOperationException>();

        var afterStaleReplay = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unappliedAfterStaleReplay = await reopened.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None);
        await AssertRemoteApplyCrashRecoveryAsync(afterStaleReplay, operationId, unappliedAfterStaleReplay);
    }

    /// <summary>Verifies a process death after upload result reconciliation recovers terminal outcomes and remaining FIFO work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    public async Task WhenWriterProcessDiesAfterSyncResultApply_ThenReopenKeepsTerminalOutcomesAndRemainingOrder()
    {
        using var database = TempDatabase.Create();
        var signalPath = CreateCrashRecoverySignalPath(database.Path, SyncResultBoundary);
        var first = OperationId.New();
        var second = OperationId.New();
        var third = OperationId.New();
        var subscriptionId = SubscriptionId.New();
        await PrepareThreeOperationDatabaseAsync(database.Path, first, second, third, subscriptionId);

        await RunCrashRecoveryChildUntilSignalAsync(new(database.Path, signalPath, SyncResultBoundary, first, subscriptionId, DeliveryGuarantee.AtLeastOnce)
        {
            SecondOperationId = second,
            ThirdOperationId = third,
        });

        var signal = await ReadCrashRecoverySignalAsync(signalPath);
        await Assert.That(signal.Boundary).IsEqualTo(SyncResultBoundary);
        await Assert.That(signal.OperationId).IsEqualTo(first.Value);
        await Assert.That(signal.SecondOperationId).IsEqualTo(second.Value);
        await Assert.That(signal.ThirdOperationId).IsEqualTo(third.Value);
        await Assert.That(signal.SnapshotRevision).IsEqualTo(CrashRecoveryResultRevision);

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var firstStatus = await reopened.GetOperationStatusAsync(first, CancellationToken.None);
        var secondStatus = await reopened.GetOperationStatusAsync(second, CancellationToken.None);
        var thirdStatus = await reopened.GetOperationStatusAsync(third, CancellationToken.None);
        Func<Task> originalLeaseRetry = async () => _ = await reopened.ApplySyncResultAsync(
            signal.LeaseId,
            new(signal.LeaseId, [new(first, OperationResultKind.Accepted, null, ServerVersion), new(second, OperationResultKind.Rejected, CrashRecoveryRejectedReason, null)], null, null),
            [CreateSnapshotMutation(CrashRecoveryResultRevision, CrashRecoveryResultPayloadText)],
            CancellationToken.None).AsTask();
        var nextLease = await ReadSingleLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(originalLeaseRetry).ThrowsExactly<InvalidOperationException>();
        await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Rejected);
        await Assert.That(thirdStatus?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(third);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(TwoWorkerCommands);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(third);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(CrashRecoveryResultPayloadText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(CrashRecoveryAuthoritativeInitialText);
        await Assert.That(nextLease.Operations.Count).IsEqualTo(1);
        await Assert.That(nextLease.Operations[0].OperationId).IsEqualTo(third);
    }

    /// <summary>Verifies a process death after never-attempted dead-lettering recovers terminal state and remaining lease membership.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    public async Task WhenWriterProcessDiesAfterDeadLetter_ThenReopenKeepsDeadLetterAndRemainingLease()
    {
        using var database = TempDatabase.Create();
        var signalPath = CreateCrashRecoverySignalPath(database.Path, DeadLetterBoundary);
        var first = OperationId.New();
        var second = OperationId.New();
        var subscriptionId = SubscriptionId.New();
        await PrepareTwoOperationDatabaseAsync(database.Path, first, second, subscriptionId);

        await RunCrashRecoveryChildUntilSignalAsync(new(database.Path, signalPath, DeadLetterBoundary, first, subscriptionId, DeliveryGuarantee.AtLeastOnce) { SecondOperationId = second });

        var signal = await ReadCrashRecoverySignalAsync(signalPath);
        await Assert.That(signal.Boundary).IsEqualTo(DeadLetterBoundary);
        await Assert.That(signal.OperationId).IsEqualTo(first.Value);
        await Assert.That(signal.SecondOperationId).IsEqualTo(second.Value);
        await Assert.That(signal.SnapshotRevision).IsEqualTo(CrashRecoveryDeadLetterRevision);

        await using var reopened = CreateAdapter(database.Path, new FixedTimeProvider(CrashRecoveryTimestamp));
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var secondStatus = await reopened.GetOperationStatusAsync(second, CancellationToken.None);

        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(second);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(CrashRecoveryDeadLetterReason);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(0);
        await Assert.That(recovered.DeadLetters[0].DeadLetteredAtUtc).IsEqualTo(CrashRecoveryTimestamp);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(CrashRecoveryDeadLetterPayloadText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(CrashRecoveryAuthoritativeInitialText);

        await reopened.ApplySyncResultAsync(
            signal.LeaseId,
            new(signal.LeaseId, [new(first, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var afterRemainingAck = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(afterRemainingAck.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(afterRemainingAck.DeadLetters.Count).IsEqualTo(1);
    }

    /// <summary>Child workflow used by crash recovery parent process tests.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child crash recovery environment is incomplete.</exception>
    [Test]
    public async Task WhenCrashRecoveryChildCommitsBoundaryAndWaits_ThenSignalIsPublished()
    {
        var childContext = ReadCrashRecoveryChildContext();
        if (childContext is null)
        {
            await Assert.That(Environment.GetEnvironmentVariable(CrashRecoveryChildModeVariable)).IsNull();
            return;
        }

        await using var adapter = CreateAdapter(childContext.DatabasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(CreateCrashRecoveryInitialization(childContext.Boundary), CancellationToken.None);
        var signal = childContext.Boundary switch
        {
            AttemptBoundary => await CommitAttemptBoundaryAsync(adapter, childContext),
            RemoteApplyBoundary => await CommitRemoteApplyBoundaryAsync(adapter, childContext),
            SyncResultBoundary => await CommitSyncResultBoundaryAsync(adapter, childContext),
            DeadLetterBoundary => await CommitDeadLetterBoundaryAsync(adapter, childContext),
            _ => throw new InvalidOperationException("The child crash recovery boundary is unknown."),
        };
        await PublishCrashRecoverySignalAsync(childContext.SignalPath, signal);
        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Commits the child process attempt boundary.</summary>
    /// <param name="adapter">The live child adapter.</param>
    /// <param name="context">The child context.</param>
    /// <returns>The published boundary signal.</returns>
    private static async Task<CrashRecoverySignal> CommitAttemptBoundaryAsync(SqliteLocalStoreAdapter adapter, CrashRecoveryChildContext context)
    {
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var barrier = await adapter.TryBeginRemoteAttemptAsync(lease.LeaseId, context.OperationId, FirstAttempt, CancellationToken.None);
        return CrashRecoverySignal.ForAttempt(context.Boundary, context.OperationId, lease.LeaseId, barrier);
    }

    /// <summary>Commits the child process remote apply boundary.</summary>
    /// <param name="adapter">The live child adapter.</param>
    /// <param name="context">The child context.</param>
    /// <returns>The published boundary signal.</returns>
    private static async Task<CrashRecoverySignal> CommitRemoteApplyBoundaryAsync(SqliteLocalStoreAdapter adapter, CrashRecoveryChildContext context)
    {
        var remoteEvent = CreateCrashRecoveryRemoteEvent(context.EventId, CrashRecoveryRemoteCursor, context.OperationId, ClientId);
        var batch = CreateCrashRecoveryRemoteBatch(context.BatchId, previousCursor: null, CrashRecoveryRemoteCursor, remoteEvent, context.OperationId);
        var result = await adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(FirstClientSequence, CrashRecoveryRemotePayloadText) with
            {
                AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeRemoteText),
            },
            CancellationToken.None);
        return CrashRecoverySignal.ForRemoteApply(context.Boundary, context.OperationId, batch.BatchId, result);
    }

    /// <summary>Commits the child process result reconciliation boundary.</summary>
    /// <param name="adapter">The live child adapter.</param>
    /// <param name="context">The child context.</param>
    /// <returns>The published boundary signal.</returns>
    private static async Task<CrashRecoverySignal> CommitSyncResultBoundaryAsync(SqliteLocalStoreAdapter adapter, CrashRecoveryChildContext context)
    {
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var result = new RemoteSyncResult(
            lease.LeaseId,
            [
                new(context.OperationId, OperationResultKind.Accepted, null, ServerVersion),
                new(context.SecondOperationId, OperationResultKind.Rejected, CrashRecoveryRejectedReason, null),
            ],
            null,
            null);
        var snapshots = await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            result,
            [CreateSnapshotMutation(ThirdClientSequence, CrashRecoveryResultPayloadText)],
            CancellationToken.None);
        return CrashRecoverySignal.ForSnapshot(context.Boundary, context.OperationId, context.SecondOperationId, context.ThirdOperationId, lease.LeaseId, snapshots[0]);
    }

    /// <summary>Commits the child process dead-letter boundary.</summary>
    /// <param name="adapter">The live child adapter.</param>
    /// <param name="context">The child context.</param>
    /// <returns>The published boundary signal.</returns>
    private static async Task<CrashRecoverySignal> CommitDeadLetterBoundaryAsync(SqliteLocalStoreAdapter adapter, CrashRecoveryChildContext context)
    {
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var snapshot = await adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            context.SecondOperationId,
            CrashRecoveryDeadLetterReason,
            CreateSnapshotMutation(SecondClientSequence, CrashRecoveryDeadLetterPayloadText),
            CancellationToken.None);
        return CrashRecoverySignal.ForSnapshot(context.Boundary, context.OperationId, context.SecondOperationId, context.ThirdOperationId, lease.LeaseId, snapshot);
    }

    /// <summary>Creates initialization options for the child-owned live adapter.</summary>
    /// <param name="boundary">The boundary under test.</param>
    /// <returns>The initialization options.</returns>
    private static LocalStoreInitialization CreateCrashRecoveryInitialization(string boundary) =>
        boundary == RemoteApplyBoundary
            ? new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }
            : new(StoreIdentity, SchemaVersion, false);

    /// <summary>Prepares a database for the attempt barrier crash boundary.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareAttemptBoundaryDatabaseAsync(
        string databasePath,
        OperationId operationId,
        SubscriptionId subscriptionId,
        DeliveryGuarantee deliveryGuarantee)
    {
        await using var adapter = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(operationId, FirstClientSequence, deliveryGuarantee, "attempt"),
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeInitialText) },
            CancellationToken.None);
    }

    /// <summary>Prepares a database with one committed local operation.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareSingleOperationDatabaseAsync(
        string databasePath,
        OperationId operationId,
        SubscriptionId subscriptionId,
        DeliveryGuarantee deliveryGuarantee)
    {
        await using var adapter = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(operationId, FirstClientSequence, deliveryGuarantee, "single"),
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeInitialText) },
            CancellationToken.None);
    }

    /// <summary>Prepares a database with three committed local operations.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="first">The first operation identifier.</param>
    /// <param name="second">The second operation identifier.</param>
    /// <param name="third">The third operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareThreeOperationDatabaseAsync(
        string databasePath,
        OperationId first,
        OperationId second,
        OperationId third,
        SubscriptionId subscriptionId)
    {
        await using var adapter = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(first, FirstClientSequence, DeliveryGuarantee.AtLeastOnce, "first"),
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(second, SecondClientSequence, DeliveryGuarantee.AtLeastOnce, "second"),
            CreateSnapshotMutation(FirstClientSequence, ResultOptimisticLocalText),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(third, ThirdClientSequence, DeliveryGuarantee.AtLeastOnce, "third"),
            CreateSnapshotMutation(SecondClientSequence, "optimistic-third"),
            CancellationToken.None);
    }

    /// <summary>Prepares a database with two committed local operations.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="first">The first operation identifier.</param>
    /// <param name="second">The second operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareTwoOperationDatabaseAsync(
        string databasePath,
        OperationId first,
        OperationId second,
        SubscriptionId subscriptionId)
    {
        await using var adapter = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(first, FirstClientSequence, DeliveryGuarantee.AtLeastOnce, "first"),
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(second, SecondClientSequence, DeliveryGuarantee.AtLeastOnce, "second"),
            CreateSnapshotMutation(FirstClientSequence, ResultOptimisticLocalText),
            CancellationToken.None);
    }

    /// <summary>Asserts recovered attempt barrier state after the child process is killed.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AssertAttemptBarrierRecoveryAsync(
        string databasePath,
        OperationId operationId,
        SubscriptionId subscriptionId,
        DeliveryGuarantee deliveryGuarantee)
    {
        await using (var activeReopen = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryTimestamp)))
        {
            await activeReopen.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var activeRecovered = await activeReopen.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
            var activeStatus = await activeReopen.GetOperationStatusAsync(operationId, CancellationToken.None);
            var activeRetryLeases = await ReadLeasesAsync(activeReopen, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

            await Assert.That(activeStatus?.Attempt).IsEqualTo(FirstAttempt);
            await Assert.That(activeRecovered.PendingOperations.Count).IsEqualTo(1);
            await Assert.That(activeRecovered.PendingOperations[0].OperationId).IsEqualTo(operationId);
            await Assert.That(activeRecovered.PendingOperations[0].ClientSequence).IsEqualTo(FirstClientSequence);
            await Assert.That(activeRecovered.PendingOperations[0].Policy.DeliveryGuarantee).IsEqualTo(deliveryGuarantee);
            await Assert.That(activeRecovered.NextClientSequence).IsEqualTo(SecondClientSequence);
            await Assert.That(activeRetryLeases.Count).IsEqualTo(0);
        }

        await using var expiredReopen = CreateAdapter(databasePath, new FixedTimeProvider(CrashRecoveryExpiredLeaseTimestamp));
        await expiredReopen.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await expiredReopen.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await expiredReopen.GetOperationStatusAsync(operationId, CancellationToken.None);

        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovered.PendingOperations[0].ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations[0].Policy.DeliveryGuarantee).IsEqualTo(deliveryGuarantee);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);

        if (deliveryGuarantee == DeliveryGuarantee.AtMostOnce)
        {
            var retryLease = await ReadLeasesAsync(expiredReopen, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
            await Assert.That(status?.ReasonCode).IsEqualTo(AttemptAmbiguousReason);
            await Assert.That(retryLease.Count).IsEqualTo(0);
            return;
        }

        var lease = await ReadSingleLeaseAsync(expiredReopen, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var retry = await expiredReopen.TryBeginRemoteAttemptAsync(lease.LeaseId, operationId, SecondClientSequence, CancellationToken.None);
        var afterRetryStatus = await expiredReopen.GetOperationStatusAsync(operationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(lease.Operations.Count).IsEqualTo(1);
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(retry.Attempt).IsEqualTo(SecondClientSequence);
        await Assert.That(retry.MaySend).IsTrue();
        await Assert.That(afterRetryStatus?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(afterRetryStatus?.Attempt).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Asserts recovered receive state after the child process is killed.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="unapplied">The duplicate inbox lookup result.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertRemoteApplyCrashRecoveryAsync(RecoveredStream recovered, OperationId operationId, IReadOnlyList<Guid> unapplied)
    {
        await Assert.That(recovered.ServerCursor).IsEqualTo(CrashRecoveryRemoteCursor);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(CrashRecoveryReceiveRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(CrashRecoveryRemotePayloadText);
        await Assert.That(PayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(CrashRecoveryAuthoritativeRemoteText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Starts and kills a crash recovery child after the signal appears.</summary>
    /// <param name="start">The child start arguments.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to signal.</exception>
    private static async Task RunCrashRecoveryChildUntilSignalAsync(CrashRecoveryChildStart start)
    {
        using var child = StartCrashRecoveryChild(start);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        CrashReceiptChildOutput? output = null;
        try
        {
            var signaled = await WaitForSignalAsync(start.SignalPath, child, SignalWaitTimeout);
            if (!signaled)
            {
                output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
                throw new InvalidOperationException(CreateCrashRecoverySignalTimeoutMessage(output));
            }

            output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        }
        finally
        {
            output ??= await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        }
    }

    /// <summary>Starts the owned child process for a crash recovery boundary.</summary>
    /// <param name="start">The child start arguments.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child test process did not start.</exception>
    private static Process StartCrashRecoveryChild(CrashRecoveryChildStart start)
    {
        var testAssembly = Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(CrashRecoveryChildTestTreeNodeFilter);
        startInfo.ArgumentList.Add("--output");
        startInfo.ArgumentList.Add("Detailed");
        startInfo.Environment[CrashRecoveryChildModeVariable] = CrashRecoveryChildMode;
        startInfo.Environment[CrashRecoveryDatabasePathVariable] = start.DatabasePath;
        startInfo.Environment[CrashRecoverySignalPathVariable] = start.SignalPath;
        startInfo.Environment[CrashRecoveryBoundaryVariable] = start.Boundary;
        startInfo.Environment[CrashRecoveryOperationIdVariable] = start.OperationId.Value.ToString("D");
        startInfo.Environment[CrashRecoverySecondOperationIdVariable] = start.SecondOperationId.Value.ToString("D");
        startInfo.Environment[CrashRecoveryThirdOperationIdVariable] = start.ThirdOperationId.Value.ToString("D");
        startInfo.Environment[CrashRecoverySubscriptionIdVariable] = start.SubscriptionId.Value.ToString("D");
        startInfo.Environment[CrashRecoveryEventIdVariable] = start.EventId.ToString("D");
        startInfo.Environment[CrashRecoveryBatchIdVariable] = start.BatchId.ToString("D");
        startInfo.Environment[CrashRecoveryDeliveryGuaranteeVariable] = ((int)start.DeliveryGuarantee).ToString(CultureInfo.InvariantCulture);

        var child = Process.Start(startInfo);
        return child ?? throw new InvalidOperationException("The child test process did not start.");
    }

    /// <summary>Reads child process settings from environment variables.</summary>
    /// <returns>The child context, or null during a normal test run.</returns>
    /// <exception cref="InvalidOperationException">The child crash recovery environment is incomplete.</exception>
    private static CrashRecoveryChildContext? ReadCrashRecoveryChildContext() =>
        !string.Equals(Environment.GetEnvironmentVariable(CrashRecoveryChildModeVariable), CrashRecoveryChildMode, StringComparison.Ordinal)
            ? null
            : new(
            ReadRequiredCrashRecoveryValue(CrashRecoveryDatabasePathVariable),
            ReadRequiredCrashRecoveryValue(CrashRecoverySignalPathVariable),
            ReadRequiredCrashRecoveryValue(CrashRecoveryBoundaryVariable),
            new(ParseCrashRecoveryGuid(CrashRecoveryOperationIdVariable)),
            new(ParseCrashRecoveryGuid(CrashRecoverySecondOperationIdVariable)),
            new(ParseCrashRecoveryGuid(CrashRecoveryThirdOperationIdVariable)),
            new(ParseCrashRecoveryGuid(CrashRecoverySubscriptionIdVariable)),
            ParseCrashRecoveryGuid(CrashRecoveryEventIdVariable),
            ParseCrashRecoveryGuid(CrashRecoveryBatchIdVariable),
            ParseCrashRecoveryDeliveryGuarantee());

    /// <summary>Creates a diagnostic timeout message from child process output.</summary>
    /// <param name="output">The child process output.</param>
    /// <returns>The timeout message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateCrashRecoverySignalTimeoutMessage(CrashReceiptChildOutput output) =>
        string.Join(
            Environment.NewLine,
            "The child process did not publish the crash recovery signal.",
            $"HasExited: {output.HasExited.ToString(CultureInfo.InvariantCulture)}",
            "StandardOutput:",
            output.StandardOutput,
            "StandardError:",
            output.StandardError);

    /// <summary>Reads a required child environment value.</summary>
    /// <param name="name">The environment variable name.</param>
    /// <returns>The environment variable value.</returns>
    /// <exception cref="InvalidOperationException">The value is missing.</exception>
    private static string ReadRequiredCrashRecoveryValue(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException(CrashRecoveryEnvironmentIncompleteMessage)
            : value;
    }

    /// <summary>Parses a required child environment GUID value.</summary>
    /// <param name="name">The environment variable name.</param>
    /// <returns>The parsed GUID.</returns>
    /// <exception cref="InvalidOperationException">The value is malformed.</exception>
    private static Guid ParseCrashRecoveryGuid(string name)
    {
        var value = ReadRequiredCrashRecoveryValue(name);
        return Guid.TryParse(value, out var parsed)
            ? parsed
            : throw new InvalidOperationException(CrashRecoveryEnvironmentIncompleteMessage);
    }

    /// <summary>Parses the child environment delivery guarantee.</summary>
    /// <returns>The parsed delivery guarantee.</returns>
    /// <exception cref="InvalidOperationException">The value is malformed.</exception>
    private static DeliveryGuarantee ParseCrashRecoveryDeliveryGuarantee()
    {
        var value = ReadRequiredCrashRecoveryValue(CrashRecoveryDeliveryGuaranteeVariable);
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? (DeliveryGuarantee)parsed
            : throw new InvalidOperationException(CrashRecoveryEnvironmentIncompleteMessage);
    }

    /// <summary>Creates the deterministic operation shared by the parent and child process.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateCrashRecoveryOperation(
        OperationId operationId,
        long clientSequence,
        DeliveryGuarantee deliveryGuarantee,
        string payloadText) =>
        CreateOperation(clientSequence) with
        {
            OperationId = operationId,
            Payload = CreatePayload(payloadText),
            Policy = new(deliveryGuarantee, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        };

    /// <summary>Creates a deterministic remote event for crash recovery tests.</summary>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="operationId">The origin operation identifier.</param>
    /// <param name="clientId">The origin client identifier.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateCrashRecoveryRemoteEvent(Guid eventId, string serverCursor, OperationId operationId, string clientId) =>
        new(eventId, Stream, serverCursor, DateTimeOffset.UnixEpoch, operationId, CreatePayload("remote"), new Dictionary<string, string>()) { Origin = new(clientId, operationId) };

    /// <summary>Creates a deterministic remote event batch for crash recovery receive replay.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="operationId">The completed operation identifier.</param>
    /// <returns>The remote event batch.</returns>
    private static RemoteEventBatch CreateCrashRecoveryRemoteBatch(Guid batchId, string? previousCursor, string nextCursor, RemoteEvent remoteEvent, OperationId operationId) =>
        new(batchId, Stream, previousCursor, nextCursor, [remoteEvent]) { CompletedOperations = [new(new(ClientId, operationId), [remoteEvent.EventId])] };

    /// <summary>Reads all leased operation batches from the adapter.</summary>
    /// <param name="adapter">The local store adapter.</param>
    /// <param name="request">The lease request.</param>
    /// <returns>The leased operation batches.</returns>
    private static async ValueTask<IReadOnlyList<LeasedOperationBatch>> ReadLeasesAsync(SqliteLocalStoreAdapter adapter, OutboxLeaseRequest request)
    {
        List<LeasedOperationBatch> batches = [];
        await foreach (var batch in adapter.LeasePendingOperationsAsync(request, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches;
    }

    /// <summary>Creates a crash recovery signal path beside the temporary database.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="boundary">The boundary name.</param>
    /// <returns>The signal path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateCrashRecoverySignalPath(string databasePath, string boundary) =>
        Path.ChangeExtension(databasePath, $"{boundary}-{Guid.NewGuid():N}.signal");

    /// <summary>Reads the crash recovery signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>The deserialized signal.</returns>
    private static async Task<CrashRecoverySignal> ReadCrashRecoverySignalAsync(string signalPath)
    {
        var text = await File.ReadAllTextAsync(signalPath);
        return CrashRecoverySignal.Parse(text);
    }

    /// <summary>Atomically publishes the child crash recovery signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="signal">The signal payload.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PublishCrashRecoverySignalAsync(string signalPath, CrashRecoverySignal signal)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId}.tmp";
        await File.WriteAllTextAsync(temporaryPath, signal.ToSignalText());
        File.Move(temporaryPath, signalPath);
    }

    /// <summary>The child process crash recovery start arguments.</summary>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    /// <param name="Boundary">The boundary to commit.</param>
    /// <param name="OperationId">The primary operation identifier.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    /// <param name="DeliveryGuarantee">The delivery guarantee.</param>
    private sealed record CrashRecoveryChildStart(
        string DatabasePath,
        string SignalPath,
        string Boundary,
        OperationId OperationId,
        SubscriptionId SubscriptionId,
        DeliveryGuarantee DeliveryGuarantee)
    {
        /// <summary>Gets the optional second operation identifier.</summary>
        public OperationId SecondOperationId { get; init; }

        /// <summary>Gets the optional third operation identifier.</summary>
        public OperationId ThirdOperationId { get; init; }

        /// <summary>Gets the optional event identifier.</summary>
        public Guid EventId { get; init; }

        /// <summary>Gets the optional receive batch identifier.</summary>
        public Guid BatchId { get; init; }
    }

    /// <summary>The child process crash recovery context.</summary>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    /// <param name="Boundary">The boundary to commit.</param>
    /// <param name="OperationId">The primary operation identifier.</param>
    /// <param name="SecondOperationId">The second operation identifier.</param>
    /// <param name="ThirdOperationId">The third operation identifier.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    /// <param name="EventId">The event identifier.</param>
    /// <param name="BatchId">The receive batch identifier.</param>
    /// <param name="DeliveryGuarantee">The delivery guarantee.</param>
    private sealed record CrashRecoveryChildContext(
        string DatabasePath,
        string SignalPath,
        string Boundary,
        OperationId OperationId,
        OperationId SecondOperationId,
        OperationId ThirdOperationId,
        SubscriptionId SubscriptionId,
        Guid EventId,
        Guid BatchId,
        DeliveryGuarantee DeliveryGuarantee);

    /// <summary>The crash recovery boundary signal.</summary>
    /// <param name="Boundary">The committed boundary.</param>
    /// <param name="OperationId">The primary operation identifier.</param>
    /// <param name="SecondOperationId">The second operation identifier.</param>
    /// <param name="ThirdOperationId">The third operation identifier.</param>
    /// <param name="BatchId">The remote receive batch identifier.</param>
    /// <param name="LeaseId">The lease identifier.</param>
    /// <param name="Attempt">The attempt number.</param>
    /// <param name="MaySend">Whether the attempt barrier allowed a send.</param>
    /// <param name="SnapshotRevision">The committed snapshot revision.</param>
    /// <param name="AppliedCount">The remote apply count.</param>
    /// <param name="DuplicateCount">The remote duplicate count.</param>
    /// <param name="Cursor">The committed cursor.</param>
    private sealed record CrashRecoverySignal(
        string Boundary,
        Guid OperationId,
        Guid SecondOperationId,
        Guid ThirdOperationId,
        Guid BatchId,
        Guid LeaseId,
        int Attempt,
        bool MaySend,
        long SnapshotRevision,
        int AppliedCount,
        int DuplicateCount,
        string? Cursor)
    {
        /// <summary>Creates an attempt signal.</summary>
        /// <param name="boundary">The boundary.</param>
        /// <param name="operationId">The operation identifier.</param>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="barrier">The barrier result.</param>
        /// <returns>The signal.</returns>
        public static CrashRecoverySignal ForAttempt(string boundary, OperationId operationId, Guid leaseId, AttemptBarrierResult barrier) =>
            new(boundary, operationId.Value, Guid.Empty, Guid.Empty, Guid.Empty, leaseId, barrier.Attempt, barrier.MaySend, 0, 0, 0, barrier.ReasonCode);

        /// <summary>Creates a remote apply signal.</summary>
        /// <param name="boundary">The boundary.</param>
        /// <param name="operationId">The operation identifier.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <param name="result">The remote apply result.</param>
        /// <returns>The signal.</returns>
        public static CrashRecoverySignal ForRemoteApply(string boundary, OperationId operationId, Guid batchId, RemoteApplyResult result) =>
            new(boundary, operationId.Value, Guid.Empty, Guid.Empty, batchId, Guid.Empty, 0, false, result.SnapshotRevision, result.AppliedCount, result.DuplicateCount, result.NextCursor);

        /// <summary>Creates a snapshot signal.</summary>
        /// <param name="boundary">The boundary.</param>
        /// <param name="operationId">The primary operation identifier.</param>
        /// <param name="secondOperationId">The second operation identifier.</param>
        /// <param name="thirdOperationId">The third operation identifier.</param>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="snapshot">The snapshot.</param>
        /// <returns>The signal.</returns>
        public static CrashRecoverySignal ForSnapshot(
            string boundary,
            OperationId operationId,
            OperationId secondOperationId,
            OperationId thirdOperationId,
            Guid leaseId,
            LocalSnapshot snapshot) =>
            new(boundary, operationId.Value, secondOperationId.Value, thirdOperationId.Value, Guid.Empty, leaseId, 0, false, snapshot.Revision, 0, 0, snapshot.ServerCursor);

        /// <summary>Parses a crash recovery signal from signal text.</summary>
        /// <param name="text">The signal text.</param>
        /// <returns>The parsed signal.</returns>
        /// <exception cref="InvalidOperationException">The signal text is malformed.</exception>
        public static CrashRecoverySignal Parse(string text)
        {
            var lines = text.Split('\n', StringSplitOptions.TrimEntries);
            if (lines.Length != CrashRecoverySignalLineCount)
            {
                throw new InvalidOperationException(CrashRecoverySignalMalformedMessage);
            }

            return new(
                lines[0],
                ParseSignalGuid(lines[1]),
                ParseSignalGuid(lines[2]),
                ParseSignalGuid(lines[3]),
                ParseSignalGuid(lines[4]),
                ParseSignalGuid(lines[5]),
                ParseSignalInt(lines[6]),
                ParseSignalBool(lines[7]),
                ParseSignalLong(lines[8]),
                ParseSignalInt(lines[9]),
                ParseSignalInt(lines[10]),
                lines[11].Length == 0 ? null : lines[11]);
        }

        /// <summary>Formats a crash recovery signal as invariant signal text.</summary>
        /// <returns>The signal text.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ToSignalText() =>
            string.Join(
                '\n',
                Boundary,
                OperationId.ToString("D"),
                SecondOperationId.ToString("D"),
                ThirdOperationId.ToString("D"),
                BatchId.ToString("D"),
                LeaseId.ToString("D"),
                Attempt.ToString(CultureInfo.InvariantCulture),
                MaySend.ToString(CultureInfo.InvariantCulture),
                SnapshotRevision.ToString(CultureInfo.InvariantCulture),
                AppliedCount.ToString(CultureInfo.InvariantCulture),
                DuplicateCount.ToString(CultureInfo.InvariantCulture),
                Cursor ?? string.Empty);

        /// <summary>Parses a signal GUID field.</summary>
        /// <param name="value">The field value.</param>
        /// <returns>The parsed GUID.</returns>
        /// <exception cref="InvalidOperationException">The field is malformed.</exception>
        private static Guid ParseSignalGuid(string value) =>
            Guid.TryParse(value, out var parsed) ? parsed : throw new InvalidOperationException(CrashRecoverySignalMalformedMessage);

        /// <summary>Parses a signal integer field.</summary>
        /// <param name="value">The field value.</param>
        /// <returns>The parsed integer.</returns>
        /// <exception cref="InvalidOperationException">The field is malformed.</exception>
        private static int ParseSignalInt(string value) =>
            int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw new InvalidOperationException(CrashRecoverySignalMalformedMessage);

        /// <summary>Parses a signal boolean field.</summary>
        /// <param name="value">The field value.</param>
        /// <returns>The parsed boolean.</returns>
        /// <exception cref="InvalidOperationException">The field is malformed.</exception>
        private static bool ParseSignalBool(string value) =>
            bool.TryParse(value, out var parsed) ? parsed : throw new InvalidOperationException(CrashRecoverySignalMalformedMessage);

        /// <summary>Parses a signal long field.</summary>
        /// <param name="value">The field value.</param>
        /// <returns>The parsed long.</returns>
        /// <exception cref="InvalidOperationException">The field is malformed.</exception>
        private static long ParseSignalLong(string value) =>
            long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw new InvalidOperationException(CrashRecoverySignalMalformedMessage);
    }
}
