// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Durable operation lifecycle tests for <see cref="SqliteLocalCommitStore"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The first upload attempt.</summary>
    private const int FirstAttempt = 1;

    /// <summary>The second upload attempt.</summary>
    private const int SecondAttempt = 2;

    /// <summary>The operation id parameter name.</summary>
    private const string OperationIdParameter = "$operationId";

    /// <summary>The reason code parameter name.</summary>
    private const string ReasonCodeParameter = "$reasonCode";

    /// <summary>The transient retry reason used by operation state tests.</summary>
    private const string TransientReasonCode = "OC.Transient";

    /// <summary>An undefined persisted enum value.</summary>
    private const int UndefinedEnumValue = 99;

    /// <summary>The retry delay used by operation state tests.</summary>
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(5);

    /// <summary>The timeout used for operation state coordination.</summary>
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies committed operation status and retry state survive reopening the store.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStatusAndRetryStateArePersisted_ThenReopenedStoreReadsThem()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var retryState = RetryState.Start(clock.GetUtcNow()) with
        {
            DueUtc = clock.GetUtcNow().Add(RetryDelay),
            PreviousDelay = RetryDelay,
            TransientAttemptCount = FirstAttempt,
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = "credential-v2",
        };

        await store.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None);
        using var reopened = CreateInitializedStore(database.Path, clock);
        var status = reopened.GetOperationStatus(operation.OperationId, CancellationToken.None);
        var retry = reopened.GetRetryState(operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(status?.ChangedAtUtc).IsEqualTo(clock.GetUtcNow());
        await Assert.That(retry).IsEqualTo(retryState);
    }

    /// <summary>Verifies at-most-once ambiguity is persisted before send and blocks later sends after reopen.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAtMostOnceAttemptBarrierCommits_ThenReopenCannotSendAgain()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(
            store,
            Stream,
            clientSequence: 1,
            OperationPayloadText,
            DeliveryGuarantee.AtMostOnce);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        var first = store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None);
        using var reopened = CreateInitializedStore(database.Path, clock);
        var second = reopened.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, SecondAttempt, CancellationToken.None);
        var status = reopened.GetOperationStatus(operation.OperationId, CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));
        var leasedAgain = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(first.MaySend).IsTrue();
        await Assert.That(second.MaySend).IsFalse();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(leasedAgain).IsNull();
    }

    /// <summary>Verifies retryable at-most-once results do not permit a second send after reopen.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAtMostOnceResultIsRetryable_ThenReopenCannotLeaseOrSendAgain()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(
            store,
            Stream,
            clientSequence: 1,
            OperationPayloadText,
            DeliveryGuarantee.AtMostOnce);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None);
        var result = CreateSyncResult(
            lease.LeaseId,
            new OperationSyncResult(operation.OperationId, OperationResultKind.Retryable, TransientReasonCode, null));

        await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None);
        using var reopened = CreateInitializedStore(database.Path);
        var leasedAgain = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        SetOperationState(database.Path, operation.OperationId, SyncOperationState.QueuedForUpload);
        var forcedLease = InsertSingleLease(database.Path, operation.OperationId, Stream);
        var second = reopened.TryBeginRemoteAttempt(forcedLease, operation.OperationId, SecondAttempt, CancellationToken.None);

        await Assert.That(leasedAgain).IsNull();
        await Assert.That(second.MaySend).IsFalse();
        await Assert.That(second.ReasonCode).IsEqualTo("OC.AtMostOnceAttempted");
        await Assert.That(reopened.GetOperationStatus(operation.OperationId, CancellationToken.None)?.Attempt).IsEqualTo(FirstAttempt);
    }

    /// <summary>Verifies retry due time blocks a stream head without letting later operations overtake it.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateIsNotDue_ThenLaterSequenceDoesNotOvertakeHead()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var first = CommitOperation(store, Stream, clientSequence: 1, "a");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = store.TryBeginRemoteAttempt(lease.LeaseId, first.OperationId, FirstAttempt, CancellationToken.None);
        await store.SaveRetryStateAsync(
            first.OperationId,
            RetryState.Start(clock.GetUtcNow()) with { DueUtc = clock.GetUtcNow().Add(RetryDelay) },
            CancellationToken.None);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);

        var beforeDue = await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        clock.Advance(RetryDelay);
        var afterDue = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(beforeDue).IsNull();
        await Assert.That(afterDue.Operations.Count).IsEqualTo(TwoOperations);
        await Assert.That(afterDue.Operations[0].OperationId).IsEqualTo(first.OperationId);
    }

    /// <summary>Verifies batch result application validates the lease id as the expected batch id before mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSyncResultBatchIdDiffersFromLease_ThenOperationStatesRemainUnchanged()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var first = CommitOperation(store, Stream, clientSequence: 1, "a");
        var second = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            Guid.NewGuid(),
            new(first.OperationId, OperationResultKind.Accepted, null, "v1"),
            new(second.OperationId, OperationResultKind.Accepted, null, "v2"));

        await Assert.That(async () => await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None))
            .ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(store.GetOperationStatus(first.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(store.GetOperationStatus(second.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(TwoOperations);
    }

    /// <summary>Verifies sync results are applied atomically and remove completed rows from recovery pending work.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSyncResultApplies_ThenTerminalAndRetryableStatesCommitAtomically()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = CommitOperation(store, Stream, clientSequence: 1, "a");
        var second = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            lease.LeaseId,
            new(first.OperationId, OperationResultKind.Accepted, null, "v1"),
            new(second.OperationId, OperationResultKind.Retryable, TransientReasonCode, null));

        await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None);
        var synchronized = store.GetOperationStatus(first.OperationId, CancellationToken.None);
        var retryable = store.GetOperationStatus(second.OperationId, CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(synchronized?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(retryable?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(retryable?.ReasonCode).IsEqualTo(TransientReasonCode);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(0);
    }

    /// <summary>Verifies frozen schema version four databases backfill lifecycle rows during migration.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaFourMigratesToCurrent_ThenOperationStateIsBackfilled()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        var operation = CreateOperation(clientSequence: 1);
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
            SqliteStoreSchemaTests.CreateLeaseSchema(connection, transaction);
            InsertLegacyLocalCommitRows(connection, transaction, subscriptionId, operation, CreateSnapshotMutation(expectedRevision: 0));
            await transaction.CommitAsync();
        }

        using var store = CreateInitializedStore(database.Path);
        var status = store.GetOperationStatus(operation.OperationId, CancellationToken.None);
        var batch = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ChangedAtUtc).IsEqualTo(operation.TimestampUtc);
        await Assert.That(batch.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies missing operations have no persisted status or retry state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationIsUnknown_ThenStatusAndRetryStateAreMissing()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operationId = OperationId.New();

        var status = store.GetOperationStatus(operationId, CancellationToken.None);
        var retry = store.GetRetryState(operationId, CancellationToken.None);

        await Assert.That(status).IsNull();
        await Assert.That(retry).IsNull();
    }

    /// <summary>Verifies an operation without retry metadata returns no retry state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationHasNoRetryMetadata_ThenRetryStateIsMissing()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);

        var retry = store.GetRetryState(operation.OperationId, CancellationToken.None);

        await Assert.That(retry).IsNull();
    }

    /// <summary>Verifies expired leases cannot open an upload attempt barrier.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpiresBeforeAttemptBarrier_ThenAttemptIsRejected()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));

        await Assert.That(() => store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.GetOperationStatus(operation.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies expired leases cannot apply a remote result.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpiresBeforeSyncResult_ThenResultIsRejected()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            lease.LeaseId,
            new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, "v1"));
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));

        await Assert.That(async () => await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.GetOperationStatus(operation.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies bounded writer waits fail closed for attempt and result paths.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenWriterLockRemainsHeld_ThenAttemptAndResultTimeoutWithoutMutation()
    {
        using var attemptDatabase = TempDatabase.Create();
        using var attemptStore = CreateInitializedStore(attemptDatabase.Path);
        var attemptOperation = CommitOperation(attemptStore, Stream, clientSequence: 1, OperationPayloadText);
        var attemptLease = RequireBatch(await LeaseSingleBatch(attemptStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await using var attemptBlocker = OpenRawConnection(attemptDatabase.Path);
        await using var attemptTransaction = (SqliteTransaction)await attemptBlocker.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);
        InsertBlockingIdentity(attemptBlocker, attemptTransaction);

        await Assert.That(() => attemptStore.TryBeginRemoteAttempt(attemptLease.LeaseId, attemptOperation.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<TimeoutException>();
        await attemptTransaction.RollbackAsync();

        using var resultDatabase = TempDatabase.Create();
        using var resultStore = CreateInitializedStore(resultDatabase.Path);
        var resultOperation = CommitOperation(resultStore, Stream, clientSequence: 1, OperationPayloadText);
        var resultLease = RequireBatch(await LeaseSingleBatch(resultStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            resultLease.LeaseId,
            new OperationSyncResult(resultOperation.OperationId, OperationResultKind.Accepted, null, "v1"));
        await using var resultBlocker = OpenRawConnection(resultDatabase.Path);
        await using var resultTransaction = (SqliteTransaction)await resultBlocker.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);
        InsertBlockingIdentity(resultBlocker, resultTransaction);

        await Assert.That(async () => await resultStore.ApplySyncResultAsync(resultLease.LeaseId, result, CancellationToken.None))
            .ThrowsExactly<TimeoutException>();
        await resultTransaction.RollbackAsync();

        await Assert.That(attemptStore.GetOperationStatus(attemptOperation.OperationId, CancellationToken.None)?.Attempt).IsEqualTo(0);
        await Assert.That(resultStore.GetOperationStatus(resultOperation.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies lease expiry is checked with a clock sample captured after writer contention clears.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpiresWhileAttemptBarrierWaitsForWriter_ThenAttemptFailsClosed()
    {
        using var database = TempDatabase.Create();
        var clock = new SignalingManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await blocker.BeginTransactionAsync();
        InsertBlockingIdentity(blocker, transaction);
        var validationSample = clock.SignalNextRead();
        var blockedAttempt = Task.Run(() => store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None));

        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));
        await transaction.RollbackAsync();
        await validationSample.WaitAsync(TestTimeout);

        await Assert.That(async () => await blockedAttempt).ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.GetOperationStatus(operation.OperationId, CancellationToken.None)?.Attempt).IsEqualTo(0);
    }

    /// <summary>Verifies result application checks lease expiry after writer contention clears.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpiresWhileSyncResultWaitsForWriter_ThenResultFailsClosed()
    {
        using var database = TempDatabase.Create();
        var clock = new SignalingManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            lease.LeaseId,
            new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, "v1"));
        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await blocker.BeginTransactionAsync();
        InsertBlockingIdentity(blocker, transaction);
        var validationSample = clock.SignalNextRead();
        var blockedApply = Task.Run(async () => await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None));

        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));
        await transaction.RollbackAsync();
        await validationSample.WaitAsync(TestTimeout);

        await Assert.That(async () => await blockedApply).ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.GetOperationStatus(operation.OperationId, CancellationToken.None)?.State)
            .IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies terminal states and repeated attempt numbers deny upload attempts.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAttemptBarrierFindsTerminalOrRepeatedAttempt_ThenSendIsDenied()
    {
        using var terminalDatabase = TempDatabase.Create();
        using var terminalStore = CreateInitializedStore(terminalDatabase.Path);
        var terminalOperation = CommitOperation(terminalStore, Stream, clientSequence: 1, OperationPayloadText);
        var terminalLease = RequireBatch(await LeaseSingleBatch(terminalStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        SetOperationState(terminalDatabase.Path, terminalOperation.OperationId, SyncOperationState.Synchronized);

        var terminal = terminalStore.TryBeginRemoteAttempt(terminalLease.LeaseId, terminalOperation.OperationId, FirstAttempt, CancellationToken.None);

        using var repeatedDatabase = TempDatabase.Create();
        using var repeatedStore = CreateInitializedStore(repeatedDatabase.Path);
        var repeatedOperation = CommitOperation(repeatedStore, Stream, clientSequence: 1, OperationPayloadText);
        var repeatedLease = RequireBatch(await LeaseSingleBatch(repeatedStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = repeatedStore.TryBeginRemoteAttempt(repeatedLease.LeaseId, repeatedOperation.OperationId, FirstAttempt, CancellationToken.None);
        var repeated = repeatedStore.TryBeginRemoteAttempt(repeatedLease.LeaseId, repeatedOperation.OperationId, FirstAttempt, CancellationToken.None);

        using var ambiguousDatabase = TempDatabase.Create();
        using var ambiguousStore = CreateInitializedStore(ambiguousDatabase.Path);
        var ambiguousOperation = CommitOperation(ambiguousStore, Stream, clientSequence: 1, OperationPayloadText, DeliveryGuarantee.AtMostOnce);
        var ambiguousLease = RequireBatch(await LeaseSingleBatch(ambiguousStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        SetOperationState(ambiguousDatabase.Path, ambiguousOperation.OperationId, SyncOperationState.Ambiguous);
        var ambiguous = ambiguousStore.TryBeginRemoteAttempt(ambiguousLease.LeaseId, ambiguousOperation.OperationId, FirstAttempt, CancellationToken.None);

        await Assert.That(terminal.MaySend).IsFalse();
        await Assert.That(terminal.ReasonCode).IsEqualTo("OC.OperationTerminal");
        await Assert.That(repeated.MaySend).IsFalse();
        await Assert.That(repeated.ReasonCode).IsEqualTo("OC.AttemptNotAdvanced");
        await Assert.That(ambiguous.MaySend).IsFalse();
        await Assert.That(ambiguous.ReasonCode).IsEqualTo("OC.AtMostOnceAmbiguous");
    }

    /// <summary>Verifies an attempt barrier rejects an operation outside the lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAttemptBarrierOperationIsNotInLease_ThenItFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var first = CommitOperation(store, Stream, clientSequence: 1, "a");
        var second = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(() => store.TryBeginRemoteAttempt(lease.LeaseId, second.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(first.OperationId);
    }

    /// <summary>Verifies conflict and rejection results persist their terminal states.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSyncResultConflictsOrRejects_ThenTerminalStatesArePersisted()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var first = CommitOperation(store, Stream, clientSequence: 1, "a");
        var second = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            lease.LeaseId,
            new(first.OperationId, OperationResultKind.Conflict, "OC.Conflict", null),
            new(second.OperationId, OperationResultKind.Rejected, "OC.Rejected", null));

        await store.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None);

        await Assert.That(store.GetOperationStatus(first.OperationId, CancellationToken.None)?.State).IsEqualTo(SyncOperationState.Conflict);
        await Assert.That(store.GetOperationStatus(second.OperationId, CancellationToken.None)?.State).IsEqualTo(SyncOperationState.Rejected);
    }

    /// <summary>Verifies retry persistence rejects terminal and at-most-once ambiguous states.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateTargetsClosedOperation_ThenItFailsClosed()
    {
        var retryState = RetryState.Start(DateTimeOffset.UnixEpoch);
        using var terminalDatabase = TempDatabase.Create();
        using var terminalStore = CreateInitializedStore(terminalDatabase.Path);
        var terminal = CommitOperation(terminalStore, Stream, clientSequence: 1, OperationPayloadText);
        SetOperationState(terminalDatabase.Path, terminal.OperationId, SyncOperationState.Synchronized);

        using var conflictDatabase = TempDatabase.Create();
        using var conflictStore = CreateInitializedStore(conflictDatabase.Path);
        var conflict = CommitOperation(conflictStore, Stream, clientSequence: 1, OperationPayloadText);
        SetOperationState(conflictDatabase.Path, conflict.OperationId, SyncOperationState.Conflict);

        using var ambiguousDatabase = TempDatabase.Create();
        using var ambiguousStore = CreateInitializedStore(ambiguousDatabase.Path);
        var ambiguous = CommitOperation(ambiguousStore, Stream, clientSequence: 1, OperationPayloadText, DeliveryGuarantee.AtMostOnce);
        var ambiguousLease = RequireBatch(await LeaseSingleBatch(ambiguousStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = ambiguousStore.TryBeginRemoteAttempt(ambiguousLease.LeaseId, ambiguous.OperationId, FirstAttempt, CancellationToken.None);

        await Assert.That(async () => await terminalStore.SaveRetryStateAsync(terminal.OperationId, retryState, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await conflictStore.SaveRetryStateAsync(conflict.OperationId, retryState, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await ambiguousStore.SaveRetryStateAsync(ambiguous.OperationId, retryState, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies retry input validation rejects invalid values before state is written.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryOrAttemptInputIsInvalid_ThenValidationRejectsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(() => store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, nextAttempt: 0, CancellationToken.None))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(async () => await store.SaveRetryStateAsync(
            operation.OperationId,
            RetryState.Start(DateTimeOffset.UnixEpoch) with { TransientAttemptCount = -1 },
            CancellationToken.None)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(async () => await store.SaveRetryStateAsync(
            operation.OperationId,
            RetryState.Start(DateTimeOffset.UnixEpoch) with { PreviousDelay = TimeSpan.FromTicks(-1) },
            CancellationToken.None)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(async () => await store.SaveRetryStateAsync(
            operation.OperationId,
            RetryState.Start(DateTimeOffset.UnixEpoch) with { AuthenticationState = (RetryAuthenticationState)UndefinedEnumValue },
            CancellationToken.None)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies nullable retry fields round-trip when no retry delay has been chosen.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateHasNullableFields_ThenItRoundTrips()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var retryState = RetryState.Start(DateTimeOffset.UnixEpoch);

        await store.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None);
        var persisted = store.GetRetryState(operation.OperationId, CancellationToken.None);

        await Assert.That(persisted).IsEqualTo(retryState);
    }

    /// <summary>Verifies malformed state rows fail closed during status and retry reads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateRowsAreMalformed_ThenLookupsFailClosed()
    {
        using var invalidStateDatabase = TempDatabase.Create();
        using var invalidStateStore = CreateInitializedStore(invalidStateDatabase.Path);
        var invalidState = CommitOperation(invalidStateStore, Stream, clientSequence: 1, OperationPayloadText);
        SetOperationStateValue(invalidStateDatabase.Path, invalidState.OperationId, UndefinedEnumValue);

        using var emptyReasonDatabase = TempDatabase.Create();
        using var emptyReasonStore = CreateInitializedStore(emptyReasonDatabase.Path);
        var emptyReason = CommitOperation(emptyReasonStore, Stream, clientSequence: 1, OperationPayloadText);
        SetOperationReasonCode(emptyReasonDatabase.Path, emptyReason.OperationId, string.Empty);

        using var invalidRetryDatabase = TempDatabase.Create();
        using var invalidRetryStore = CreateInitializedStore(invalidRetryDatabase.Path);
        var invalidRetry = CommitOperation(invalidRetryStore, Stream, clientSequence: 1, OperationPayloadText);
        await invalidRetryStore.SaveRetryStateAsync(invalidRetry.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None);
        SetRetryAuthenticationState(invalidRetryDatabase.Path, invalidRetry.OperationId, UndefinedEnumValue);

        using var negativeAttemptDatabase = TempDatabase.Create();
        using var negativeAttemptStore = CreateInitializedStore(negativeAttemptDatabase.Path);
        var negativeAttempt = CommitOperation(negativeAttemptStore, Stream, clientSequence: 1, OperationPayloadText);
        SetOperationAttempt(negativeAttemptDatabase.Path, negativeAttempt.OperationId, -1);

        await Assert.That(() => invalidStateStore.GetOperationStatus(invalidState.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => emptyReasonStore.GetOperationStatus(emptyReason.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => invalidRetryStore.GetRetryState(invalidRetry.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => negativeAttemptStore.GetOperationStatus(negativeAttempt.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies malformed retry delay values fail closed during retry reads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetryDelayIsMalformed_ThenRetryLookupFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        await store.SaveRetryStateAsync(
            operation.OperationId,
            RetryState.Start(DateTimeOffset.UnixEpoch) with { PreviousDelay = TimeSpan.FromTicks(1) },
            CancellationToken.None);
        SetRetryPreviousDelayTicks(database.Path, operation.OperationId, -1);

        await Assert.That(() => store.GetRetryState(operation.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing state rows fail closed for retry and result mutation paths.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateRowIsMissing_ThenMutationsFailClosed()
    {
        using var retryDatabase = TempDatabase.Create();
        using var retryStore = CreateInitializedStore(retryDatabase.Path);
        var retry = CommitOperation(retryStore, Stream, clientSequence: 1, OperationPayloadText);
        DeleteOperationState(retryDatabase.Path, retry.OperationId);

        using var resultDatabase = TempDatabase.Create();
        using var resultStore = CreateInitializedStore(resultDatabase.Path);
        var resultOperation = CommitOperation(resultStore, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(resultStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        DeleteOperationState(resultDatabase.Path, resultOperation.OperationId);
        var result = CreateSyncResult(
            lease.LeaseId,
            new OperationSyncResult(resultOperation.OperationId, OperationResultKind.Accepted, null, "v1"));

        await Assert.That(() => retryStore.GetOperationStatus(retry.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => retryStore.GetRetryState(retry.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await retryStore.SaveRetryStateAsync(retry.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await resultStore.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unresolved state at a stream head blocks later operations while other streams can progress.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStreamHeadHasUnresolvedState_ThenLaterOperationDoesNotOvertakeIt()
    {
        await VerifyUnresolvedHeadBlocksStream(SyncOperationState.Conflict);
        await VerifyUnresolvedHeadBlocksStream(SyncOperationState.GuaranteeExpired);
    }

    /// <summary>Verifies malformed lease membership and delivery guarantee values fail closed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseOwnershipMetadataIsMalformed_ThenAttemptBarrierFailsClosed()
    {
        using var membershipDatabase = TempDatabase.Create();
        using var membershipStore = CreateInitializedStore(membershipDatabase.Path);
        var first = CommitOperation(membershipStore, Stream, clientSequence: 1, "a");
        var second = CommitOperation(membershipStore, Stream, clientSequence: SecondClientSequence, "b");
        var membershipLease = RequireBatch(await LeaseSingleBatch(membershipStore, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        UpdateLeaseMemberCount(membershipDatabase.Path, membershipLease.LeaseId, first.OperationId, 1);

        using var guaranteeDatabase = TempDatabase.Create();
        using var guaranteeStore = CreateInitializedStore(guaranteeDatabase.Path);
        var guaranteeOperation = CommitOperation(guaranteeStore, Stream, clientSequence: 1, OperationPayloadText);
        var guaranteeLease = RequireBatch(await LeaseSingleBatch(guaranteeStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        SetDeliveryGuaranteeValue(guaranteeDatabase.Path, guaranteeOperation.OperationId, UndefinedEnumValue);

        await Assert.That(() => membershipStore.TryBeginRemoteAttempt(membershipLease.LeaseId, second.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => guaranteeStore.TryBeginRemoteAttempt(guaranteeLease.LeaseId, guaranteeOperation.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing or invalid stream-head state fails closed during leasing.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStreamHeadStateIsMissingOrInvalid_ThenLeasingFailsClosed()
    {
        using var missingDatabase = TempDatabase.Create();
        using var missingStore = CreateInitializedStore(missingDatabase.Path);
        var missingHead = CommitOperation(missingStore, Stream, clientSequence: 1, "a");
        _ = CommitOperation(missingStore, Stream, clientSequence: SecondClientSequence, "b");
        DeleteOperationState(missingDatabase.Path, missingHead.OperationId);

        using var invalidDatabase = TempDatabase.Create();
        using var invalidStore = CreateInitializedStore(invalidDatabase.Path);
        var invalidHead = CommitOperation(invalidStore, Stream, clientSequence: 1, "a");
        _ = CommitOperation(invalidStore, Stream, clientSequence: SecondClientSequence, "b");
        SetOperationStateValue(invalidDatabase.Path, invalidHead.OperationId, UndefinedEnumValue);

        await Assert.That(async () => await LeaseSingleBatch(missingStore, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await LeaseSingleBatch(invalidStore, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies defensive update-count checks fail closed when a trigger removes the row during mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateRowDisappearsDuringUpdate_ThenMutationsFailClosed()
    {
        using var retryDatabase = TempDatabase.Create();
        using var retryStore = CreateInitializedStore(retryDatabase.Path);
        var retry = CommitOperation(retryStore, Stream, clientSequence: 1, OperationPayloadText);
        CreateDeleteStateBeforeUpdateTrigger(retryDatabase.Path);

        using var resultDatabase = TempDatabase.Create();
        using var resultStore = CreateInitializedStore(resultDatabase.Path);
        var resultOperation = CommitOperation(resultStore, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(resultStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = CreateSyncResult(
            lease.LeaseId,
            new OperationSyncResult(resultOperation.OperationId, OperationResultKind.Accepted, null, "v1"));
        CreateDeleteStateBeforeUpdateTrigger(resultDatabase.Path);

        await Assert.That(async () => await retryStore.SaveRetryStateAsync(retry.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await resultStore.ApplySyncResultAsync(lease.LeaseId, result, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies invalid operation result kinds cannot be applied by the internal state mapper.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationResultKindIsInvalid_ThenStateMapperFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        var result = CreateSyncResult(
            Guid.NewGuid(),
            new OperationSyncResult(operation.OperationId, (OperationResultKind)UndefinedEnumValue, "OC.Invalid", null));

        await Assert.That(() => SqliteLocalCommitSql.ApplySyncResult(connection, transaction, StoreIdentity, result, DateTimeOffset.UnixEpoch))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies lease schema validation detects a mismatched metadata schema version.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseSchemaMetadataVersionDiffers_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchemaTests.CreateLeaseSchema(connection, transaction);
        SetSchemaMetadataVersion(connection, transaction, SchemaVersion);

        await Assert.That(() => SqliteStoreSchema.ValidateLeaseSchema(connection, transaction))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a synchronization result for operation state tests.</summary>
    /// <param name="batchId">The synchronization batch identifier.</param>
    /// <param name="results">The operation results.</param>
    /// <returns>The synchronization result.</returns>
    private static RemoteSyncResult CreateSyncResult(Guid batchId, params OperationSyncResult[] results) =>
        new(batchId, results, null, null);

    /// <summary>Verifies an unresolved same-stream head blocks later work while another stream remains eligible.</summary>
    /// <param name="state">The unresolved state.</param>
    /// <returns>The asynchronous test.</returns>
    private static async Task VerifyUnresolvedHeadBlocksStream(SyncOperationState state)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var otherStream = new StreamId($"sensor/z-other-{state}");
        var head = CommitOperation(store, Stream, clientSequence: 1, "a");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var other = CommitOperation(store, otherStream, clientSequence: 1, "c");
        SetOperationState(database.Path, head.OperationId, state);

        var sameStream = await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        var anyStream = RequireBatch(await LeaseSingleBatch(store, new(null, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(sameStream).IsNull();
        await Assert.That(anyStream.Operations[0].OperationId).IsEqualTo(other.OperationId);
    }

    /// <summary>Inserts one forced lease row for direct barrier validation.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="streamId">The stream id.</param>
    /// <returns>The forced lease id.</returns>
    private static Guid InsertSingleLease(string path, OperationId operationId, StreamId streamId)
    {
        var leaseId = Guid.NewGuid();
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO oc_outbox_leases
                (store_identity, lease_id, operation_id, stream_id, client_sequence, lease_expires_at_utc, lease_member_count)
            VALUES
                ($storeIdentity, $leaseId, $operationId, $streamId, 1, '2099-01-01T00:00:00.0000000+00:00', 1);
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        _ = command.ExecuteNonQuery();
        return leaseId;
    }

    /// <summary>Sets an operation state value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="state">The state.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetOperationState(string path, OperationId operationId, SyncOperationState state) =>
        SetOperationStateValue(path, operationId, (int)state);

    /// <summary>Sets a raw operation state value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="state">The raw state.</param>
    private static void SetOperationStateValue(string path, OperationId operationId, int state)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $state
            WHERE operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$state", state);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the operation attempt count.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="attempt">The raw attempt count.</param>
    private static void SetOperationAttempt(string path, OperationId operationId, int attempt)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox_operation_states SET attempt_count = $attempt WHERE operation_id = $operationId;";
        _ = command.Parameters.AddWithValue("$attempt", attempt);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the delivery guarantee value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="guarantee">The raw delivery guarantee.</param>
    private static void SetDeliveryGuaranteeValue(string path, OperationId operationId, int guarantee)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET policy_delivery_guarantee = $guarantee WHERE operation_id = $operationId;";
        _ = command.Parameters.AddWithValue("$guarantee", guarantee);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Updates the lease member count for one operation.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease id.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="memberCount">The raw member count.</param>
    private static void UpdateLeaseMemberCount(string path, Guid leaseId, OperationId operationId, int memberCount)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox_leases SET lease_member_count = $memberCount WHERE lease_id = $leaseId AND operation_id = $operationId;";
        _ = command.Parameters.AddWithValue("$memberCount", memberCount);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the operation reason code.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="reasonCode">The reason code.</param>
    private static void SetOperationReasonCode(string path, OperationId operationId, string reasonCode)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET reason_code = $reasonCode
            WHERE operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(ReasonCodeParameter, reasonCode);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the retry authentication state.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="state">The raw authentication state.</param>
    private static void SetRetryAuthenticationState(string path, OperationId operationId, int state)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET retry_authentication_state = $state
            WHERE operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$state", state);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets retry previous delay ticks.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="ticks">The raw tick value.</param>
    private static void SetRetryPreviousDelayTicks(string path, OperationId operationId, long ticks)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET retry_previous_delay_ticks = $ticks
            WHERE operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$ticks", ticks);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes an operation state row.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    private static void DeleteOperationState(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_outbox_operation_states
            WHERE operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes an operation state before it is updated.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteStateBeforeUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_operation_state_delete_before_update
            BEFORE UPDATE ON oc_outbox_operation_states
            BEGIN
                DELETE FROM oc_outbox_operation_states
                WHERE store_identity = OLD.store_identity AND operation_id = OLD.operation_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the metadata schema version for an open transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="version">The schema version.</param>
    private static void SetSchemaMetadataVersion(SqliteConnection connection, SqliteTransaction transaction, int version)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_metadata
            SET value = $version
            WHERE key = 'schema_version';
            """;
        _ = command.Parameters.AddWithValue("$version", version.ToString(System.Globalization.CultureInfo.InvariantCulture));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>A manual clock that signals after capturing a timestamp to return.</summary>
    private sealed class SignalingManualTimeProvider : TimeProvider
    {
        /// <summary>The current UTC ticks.</summary>
        private long _utcTicks;

        /// <summary>The next read signal.</summary>
        private TaskCompletionSource? _nextRead;

        /// <summary>Initializes a new instance of the <see cref="SignalingManualTimeProvider"/> class.</summary>
        /// <param name="initial">The initial UTC timestamp.</param>
        internal SignalingManualTimeProvider(DateTimeOffset initial) => _utcTicks = initial.UtcDateTime.Ticks;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            var captured = new DateTimeOffset(new DateTime(Interlocked.Read(ref _utcTicks), DateTimeKind.Utc));
            _ = Interlocked.Exchange(ref _nextRead, null)?.TrySetResult();
            return captured;
        }

        /// <summary>Advances the clock.</summary>
        /// <param name="duration">The duration.</param>
        internal void Advance(TimeSpan duration) => _ = Interlocked.Add(ref _utcTicks, duration.Ticks);

        /// <summary>Signals on the next clock read after capturing the returned value.</summary>
        /// <returns>The signal task.</returns>
        internal Task SignalNextRead()
        {
            var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = Interlocked.Exchange(ref _nextRead, signal);
            return signal.Task;
        }
    }
}
