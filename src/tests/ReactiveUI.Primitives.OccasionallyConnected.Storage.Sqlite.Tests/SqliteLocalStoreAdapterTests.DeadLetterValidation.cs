// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Dead-letter validation tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The reason code used by SQLite dead-letter tests.</summary>
    private const string SqliteDeadLetterReasonCode = "OC.LocalPoison";

    /// <summary>The number of operations in mixed dead-letter validation scenarios.</summary>
    private const int DeadLetterTwoOperations = 2;

    /// <summary>The snapshot revision after a two-operation dead-letter rebuild.</summary>
    private const int DeadLetterRebuiltRevision = 3;

    /// <summary>The second stream used to corrupt lease membership.</summary>
    private static readonly StreamId DeadLetterOtherStream = new("sensor/other");

    /// <summary>Verifies invalid dead-letter snapshots preserve state, leases, and optimistic recovery.</summary>
    /// <param name="failure">The invalid mutation condition.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments("mismatched-stream")]
    [Arguments("missing-snapshot")]
    [Arguments("missing-authoritative")]
    [Arguments("stale")]
    [Arguments("authoritative")]
    public async Task WhenDeadLetterSnapshotFenceFails_ThenStateIsPreserved(string failure)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, failure != "missing-authoritative");
        if (failure == "missing-snapshot")
        {
            DeleteSnapshot(database.Path);
        }

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateInvalidDeadLetterMutation(failure),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a live lease cannot dead-letter an operation it never owned.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterLeaseDoesNotOwnOperation_ThenStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence);
        var second = CreateOperation(SecondClientSequence);
        _ = await adapter.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, ResultOptimisticLocalText), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            second.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(DeadLetterTwoOperations, ResultOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var secondStatus = await adapter.GetOperationStatusAsync(second.OperationId, CancellationToken.None);

        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(DeadLetterTwoOperations);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies an expired lease cannot dead-letter and leaves local state unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterLeaseExpiresBeforeCommit_ThenStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        ExpireLease(database.Path, created.Lease.LeaseId);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a missing operation state prevents dead-letter snapshot mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationStateIsMissing_ThenSnapshotIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        DeleteOperationState(database.Path, created.Operation.OperationId);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var snapshotRevision = ReadSnapshotRevision(database.Path, Stream);

        await Assert.That(snapshotRevision).IsEqualTo(1);
    }

    /// <summary>Verifies dead-letter snapshot validation fails when the operation row is absent.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationRowIsMissing_ThenSnapshotValidationFails()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, []),
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);

        Action action = () => AssertSqlDeadLetterSnapshotFails(database.Path, operation);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var snapshotRevision = ReadSnapshotRevision(database.Path, Stream);

        await Assert.That(snapshotRevision).IsEqualTo(1);
    }

    /// <summary>Verifies terminal operation states prevent stale lease dead-letter mutation.</summary>
    /// <param name="state">The terminal operation state.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(SyncOperationState.Conflict)]
    [Arguments(SyncOperationState.Synchronized)]
    [Arguments(SyncOperationState.Rejected)]
    [Arguments(SyncOperationState.DeadLettered)]
    [Arguments(SyncOperationState.GuaranteeExpired)]
    public async Task WhenDeadLetterOperationStateIsTerminal_ThenSnapshotIsPreserved(SyncOperationState state)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        SetOperationState(database.Path, created.Operation.OperationId, state);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(state);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies oversized dead-letter reasons fail admission before SQLite mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterReasonIsOversized_ThenInputIsRejected()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        var oversizedReason = new string('r', OversizedRetryTextLength);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            oversizedReason,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies recovered dead-letter rows without a durable reason fail closed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRecoveredDeadLetterReasonIsNull_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        _ = await adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None);
        NullDeadLetterReason(database.Path, created.Operation.OperationId);

        await Assert.That(RecoverAsync).ThrowsExactly<InvalidOperationException>();

        Task RecoverAsync() => adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None).AsTask();
    }

    /// <summary>Verifies low-level dead-letter status corruption fails closed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterStatusWriterSeesCorruption_ThenItFailsClosed()
    {
        using var terminalDatabase = TempDatabase.Create();
        await using var terminalAdapter = CreateAdapter(terminalDatabase.Path);
        await terminalAdapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var terminal = await CreateCommittedOperationAsync(terminalAdapter, includeAuthoritativeState: true);
        SetOperationState(terminalDatabase.Path, terminal.Operation.OperationId, SyncOperationState.DeadLettered);

        Action terminalWrite = () => AssertSqlDeadLetterStatusFails(terminalDatabase.Path, terminal.Operation.OperationId);

        await Assert.That(terminalWrite).ThrowsExactly<InvalidOperationException>();

        using var ignoredDatabase = TempDatabase.Create();
        await using var ignoredAdapter = CreateAdapter(ignoredDatabase.Path);
        await ignoredAdapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var ignored = await CreateCommittedOperationAsync(ignoredAdapter, includeAuthoritativeState: true);
        CreateDeadLetterUpdateIgnoreTrigger(ignoredDatabase.Path);

        Action ignoredWrite = () => AssertSqlDeadLetterStatusFails(ignoredDatabase.Path, ignored.Operation.OperationId);

        await Assert.That(ignoredWrite).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies one-row lease release validates the target row count.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterLeaseReleaseCannotFindRow_ThenItFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        DeleteLease(database.Path, created.Lease.LeaseId, created.Operation.OperationId);

        Action release = () => AssertSqlLeaseReleaseFails(database.Path, created.Lease.LeaseId, created.Operation.OperationId);

        await Assert.That(release).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies dead-letter recovery survives adapter reopen with remaining lease membership.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterCommitsAndStoreReopens_ThenSnapshotLeaseAndDeadLetterRecover()
    {
        using var database = TempDatabase.Create();
        var timeProvider = new FixedTimeProvider(DeadLetterTimestamp);
        SubscriptionId subscriptionId;
        SyncOperation first;
        SyncOperation second;
        LeasedOperationBatch lease;
        await using (var adapter = CreateAdapter(database.Path, timeProvider))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            first = CreateOperation(FirstClientSequence);
            second = CreateOperation(SecondClientSequence);
            _ = await adapter.CommitLocalOperationAsync(
                first,
                CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
                CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, ResultOptimisticLocalText), CancellationToken.None);
            lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

            var committed = await adapter.DeadLetterOperationAsync(
                lease.LeaseId,
                second.OperationId,
                SqliteDeadLetterReasonCode,
                CreateSnapshotMutation(DeadLetterTwoOperations, ResultOptimisticInitialText),
                CancellationToken.None);

            await Assert.That(committed.Revision).IsEqualTo(DeadLetterRebuiltRevision);
        }

        await using var reopened = CreateAdapter(database.Path, timeProvider);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await AssertDeadLetterRecoveryAfterReopenAsync(reopened, subscriptionId, first, second, lease.LeaseId);
    }

    /// <summary>Verifies status writer failure rolls back dead-letter snapshot and lease changes together.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterStatusUpdateFails_ThenStateLeaseAndSnapshotRollBack()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        CreateDeadLetterStatusRollbackTrigger(database.Path);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<SqliteException>();
        DropDeadLetterStatusRollbackTrigger(database.Path);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies stale terminal lease membership cannot rewrite operation state or snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationIsTerminal_ThenStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var atMostOnce = CreateOperation(FirstClientSequence) with
        {
            Policy = new(DeliveryGuarantee.AtMostOnce, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        };
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            atMostOnce,
            CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText) },
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var attempt = await adapter.TryBeginRemoteAttemptAsync(lease.LeaseId, atMostOnce.OperationId, FirstAttempt, CancellationToken.None);

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            atMostOnce.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(atMostOnce.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies ambiguous prior outcomes cannot be reclassified as local dead letters.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee with uncertain prior remote effects.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtLeastOnce)]
    [Arguments(DeliveryGuarantee.ExactlyOnce)]
    public async Task WhenAmbiguousOperationTargetsDeadLetter_ThenStateIsPreserved(DeliveryGuarantee deliveryGuarantee)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var policy = OperationPolicy.Default with { DeliveryGuarantee = deliveryGuarantee };
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, policy);
        SetOperationState(database.Path, created.Operation.OperationId, SyncOperationState.Ambiguous);

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies prior send-attempt evidence survives release, re-lease, and dead-letter rejection.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee that may have produced a remote effect.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtLeastOnce)]
    [Arguments(DeliveryGuarantee.ExactlyOnce)]
    public async Task WhenAttemptedOperationIsReLeasedForDeadLetter_ThenStateIsPreserved(DeliveryGuarantee deliveryGuarantee)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var policy = OperationPolicy.Default with { DeliveryGuarantee = deliveryGuarantee };
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, policy);
        var attempt = await adapter.TryBeginRemoteAttemptAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            FirstAttempt,
            CancellationToken.None);
        await adapter.ReleaseLeaseAsync(created.Lease.LeaseId, CancellationToken.None);
        var nextLease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            nextLease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State is SyncOperationState.Ambiguous or SyncOperationState.Uploading).IsTrue();
        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies queued state cannot hide prior upload attempt evidence during dead-lettering.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenQueuedOperationWithPriorAttemptTargetsDeadLetter_ThenStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        SetOperationAttempt(database.Path, created.Operation.OperationId, FirstAttempt);

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies active-lease send attempt evidence blocks local dead-lettering.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee that records uploading attempt state.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtLeastOnce)]
    [Arguments(DeliveryGuarantee.ExactlyOnce)]
    public async Task WhenAttemptedOperationTargetsDeadLetterOnSameLease_ThenStateIsPreserved(DeliveryGuarantee deliveryGuarantee)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var policy = OperationPolicy.Default with { DeliveryGuarantee = deliveryGuarantee };
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, policy);
        var attempt = await adapter.TryBeginRemoteAttemptAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            FirstAttempt,
            CancellationToken.None);

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(status?.Attempt).IsEqualTo(FirstAttempt);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies authoritative receive-before-ACK inclusion prevents stale lease dead-lettering.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationIsAlreadyIncluded_ThenStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        var remoteEvent = CreateReceiveOriginEvent(RemoteCursor, created.Operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, created.Operation.OperationId), [remoteEvent.EventId])],
        };
        _ = await adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(1, AuthoritativePayloadText) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);

        Func<Task> transition = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(DeadLetterIncludedRevision, ResultOptimisticLocalText)
                with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None).AsTask();

        await Assert.That(transition).ThrowsExactly<InvalidOperationException>();
        var beforeAck = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(beforeAck.Snapshot?.Revision).IsEqualTo(DeadLetterIncludedRevision);
        await Assert.That(PayloadText(beforeAck.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativePayloadText);
        await Assert.That(beforeAck.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(beforeAck.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(beforeAck.DeadLetters.Count).IsEqualTo(0);

        await adapter.ApplySyncResultAsync(
            created.Lease.LeaseId,
            new(created.Lease.LeaseId, [new(created.Operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var afterAck = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(afterAck.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(afterAck.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(afterAck.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies drift between a lease row and authoritative outbox stream fails before mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterLeaseStreamDriftsFromOutbox_ThenStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(DeadLetterOtherStream, null, CancellationToken.None);
        CorruptLeaseStream(database.Path, created.Lease.LeaseId, created.Operation.OperationId);

        Func<Task> action = () => adapter.DeadLetterOperationAsync(
            created.Lease.LeaseId,
            created.Operation.OperationId,
            SqliteDeadLetterReasonCode,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(created.Operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, created.SubscriptionId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Creates an invalid dead-letter mutation.</summary>
    /// <param name="failure">The invalid mutation condition.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateInvalidDeadLetterMutation(string failure) =>
        failure switch
        {
            "mismatched-stream" => new(DeadLetterOtherStream, CreatePayload(ResultOptimisticLocalText), FormatVersion: 1, ExpectedRevision: 1),
            "stale" => CreateSnapshotMutation(0, ResultOptimisticLocalText),
            "authoritative" => CreateSnapshotMutation(1, ResultOptimisticLocalText) with { AuthoritativeState = CreatePayload(ResultAuthoritativeChangedText) },
            _ => CreateSnapshotMutation(1, ResultOptimisticLocalText),
        };

    /// <summary>Creates a committed leased operation for dead-letter tests.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="includeAuthoritativeState">Whether to seed an authoritative checkpoint.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The committed operation test state.</returns>
    private static async Task<DeadLetterTarget> CreateCommittedOperationAsync(
        SqliteLocalStoreAdapter adapter,
        bool includeAuthoritativeState,
        OperationPolicy? policy = null)
    {
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = policy is null
            ? CreateOperation(FirstClientSequence)
            : CreateOperation(FirstClientSequence) with { Policy = policy };
        var mutation = CreateSnapshotMutation(0, ResultOptimisticInitialText) with
        {
            AuthoritativeState = includeAuthoritativeState ? CreatePayload(ResultAuthoritativeInitialText) : null,
        };
        _ = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        return new(operation, lease, subscriptionId);
    }

    /// <summary>Asserts reopened dead-letter state is durable and remaining lease membership survives.</summary>
    /// <param name="adapter">The reopened adapter.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="first">The surviving leased operation.</param>
    /// <param name="second">The dead-lettered operation.</param>
    /// <param name="leaseId">The original lease identifier.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task AssertDeadLetterRecoveryAfterReopenAsync(
        SqliteLocalStoreAdapter adapter,
        SubscriptionId subscriptionId,
        SyncOperation first,
        SyncOperation second,
        Guid leaseId)
    {
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var secondStatus = await adapter.GetOperationStatusAsync(second.OperationId, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(DeadLetterRebuiltRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(second.OperationId);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(SqliteDeadLetterReasonCode);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(0);
        await Assert.That(recovered.DeadLetters[0].DeadLetteredAtUtc).IsEqualTo(DeadLetterTimestamp);
        await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.DeadLettered);

        await adapter.ApplySyncResultAsync(
            leaseId,
            new(leaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var afterAck = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(afterAck.PendingOperations.Count).IsEqualTo(0);
    }

    /// <summary>Runs the low-level dead-letter status writer expecting failure.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void AssertSqlDeadLetterStatusFails(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction();
        SqliteLocalCommitSql.DeadLetterOperation(
            connection,
            transaction,
            StoreIdentity,
            operationId,
            SqliteDeadLetterReasonCode,
            DeadLetterTimestamp);
    }

    /// <summary>Runs the low-level dead-letter snapshot validator expecting failure.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operation">The operation.</param>
    private static void AssertSqlDeadLetterSnapshotFails(string path, SyncOperation operation)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction();
        _ = SqliteLocalCommitSql.CreateDeadLetterSnapshot(
            connection,
            transaction,
            StoreIdentity,
            operation,
            CreateSnapshotMutation(1, ResultOptimisticLocalText),
            DeadLetterTimestamp,
            NormalWorkerBytes);
    }

    /// <summary>Runs the low-level one-row lease release expecting failure.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void AssertSqlLeaseReleaseFails(string path, Guid leaseId, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction();
        SqliteLocalCommitSql.ReleaseLeaseOperation(connection, transaction, StoreIdentity, leaseId, operationId);
    }

    /// <summary>Creates a trigger that aborts dead-letter status updates.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeadLetterStatusRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_dead_letter_status_abort
            AFTER UPDATE OF operation_state ON oc_outbox_operation_states
            WHEN NEW.operation_state = 6
            BEGIN
                SELECT RAISE(ABORT, 'rollback dead letter status');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the trigger that aborts dead-letter status updates.</summary>
    /// <param name="path">The database path.</param>
    private static void DropDeadLetterStatusRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_dead_letter_status_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets one operation state through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The target operation state.</param>
    private static void SetOperationState(string path, OperationId operationId, SyncOperationState state)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $operationState,
                reason_code = $reasonCode
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$operationState", (int)state);
        _ = command.Parameters.AddWithValue(
            "$reasonCode",
            state == SyncOperationState.DeadLettered ? SqliteDeadLetterReasonCode : DBNull.Value);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets one operation attempt while preserving its current state.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="attempt">The target attempt count.</param>
    private static void SetOperationAttempt(string path, OperationId operationId, int attempt)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET attempt_count = $attempt
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$attempt", attempt);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that makes dead-letter status updates affect zero rows.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeadLetterUpdateIgnoreTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_dead_letter_status_ignore
            BEFORE UPDATE OF operation_state ON oc_outbox_operation_states
            WHEN NEW.operation_state = 6
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Removes the durable reason from a dead-letter operation.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void NullDeadLetterReason(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET reason_code = NULL
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the stream stored on a lease row.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void CorruptLeaseStream(string path, Guid leaseId, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET stream_id = $streamId
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StreamIdParameter, DeadLetterOtherStream.Value);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(LeaseIdParameter, leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Expires one lease through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    private static void ExpireLease(string path, Guid leaseId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_expires_at_utc = $leaseExpiresAtUtc
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        _ = command.Parameters.AddWithValue("$leaseExpiresAtUtc", SqliteLocalCommitSql.FormatDateTimeOffset(DateTimeOffset.UnixEpoch));
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(LeaseIdParameter, leaseId.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes one lease row through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void DeleteLease(string path, Guid leaseId, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(LeaseIdParameter, leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Adds one operation to an existing lease through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operation">The operation to add.</param>
    private static void AddOperationToLease(string path, Guid leaseId, SyncOperation operation)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_member_count = $leaseMemberCount
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;

            INSERT INTO oc_outbox_leases
                (store_identity, lease_id, operation_id, stream_id, client_sequence, lease_expires_at_utc, lease_member_count)
            SELECT store_identity, lease_id, $operationId, $streamId, $clientSequence, lease_expires_at_utc, $leaseMemberCount
            FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId
            LIMIT 1;
            """;
        _ = command.Parameters.AddWithValue("$leaseMemberCount", DeadLetterTwoOperations);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(LeaseIdParameter, leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operation.OperationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(StreamIdParameter, operation.StreamId.Value);
        _ = command.Parameters.AddWithValue("$clientSequence", operation.ClientSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes one operation state through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void DeleteOperationState(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_outbox_operation_states
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads the current snapshot revision through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The snapshot revision.</returns>
    /// <exception cref="InvalidOperationException">The snapshot revision is missing.</exception>
    private static long ReadSnapshotRevision(string path, StreamId streamId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT revision
            FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        return (long)(command.ExecuteScalar() ?? throw new InvalidOperationException("Expected a snapshot revision."));
    }

    /// <summary>Deletes the current snapshot row through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DeleteSnapshot(string path) => DeleteSnapshot(path, Stream);

    /// <summary>Deletes a snapshot row through raw SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="streamId">The stream identifier.</param>
    private static void DeleteSnapshot(string path, StreamId streamId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_snapshots
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>A committed operation, its current lease, and stream subscription.</summary>
    /// <param name="Operation">The operation.</param>
    /// <param name="Lease">The lease.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    private readonly record struct DeadLetterTarget(SyncOperation Operation, LeasedOperationBatch Lease, SubscriptionId SubscriptionId);
}
