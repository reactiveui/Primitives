// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Snapshot recovery tests for the SQLite local store adapter.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The authoritative checkpoint text used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryAuthoritativeText = "snapshot-recovery-authoritative";

    /// <summary>The optimistic recovery text used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryOptimisticText = "snapshot-recovery-optimistic";

    /// <summary>The recovery frontier cursor used by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryCursor = "snapshot-recovery-cursor";

    /// <summary>The cursor used to prove failed recovery does not move the frontier.</summary>
    private const string SnapshotRecoveryOriginalCursor = "snapshot-recovery-original-cursor";

    /// <summary>The Unicode replacement cursor used by ordinal fence tests.</summary>
    private const string SnapshotRecoveryReplacementCursor = "\uFFFD";

    /// <summary>The invalid surrogate cursor used by ordinal fence tests.</summary>
    private const string SnapshotRecoveryInvalidSurrogateCursor = "\uD800";

    /// <summary>The text used to prove stale recovery keeps the original snapshot.</summary>
    private const string SnapshotRecoveryOriginalText = "snapshot-recovery-original";

    /// <summary>The clock advance that expires a one-minute snapshot recovery test lease.</summary>
    private const int SnapshotRecoveryLeaseExpiryAdvanceMinutes = 2;

    /// <summary>The two-operation pending count expected by exact-set recovery tests.</summary>
    private const int TwoPendingOperations = 2;

    /// <summary>The six-operation pending count expected by state coverage tests.</summary>
    private const int SixPendingOperations = 6;

    /// <summary>The fourth client sequence used by state coverage tests.</summary>
    private const int FourthClientSequence = 4;

    /// <summary>The fifth client sequence used by state coverage tests.</summary>
    private const int FifthClientSequence = 5;

    /// <summary>The sixth client sequence used by state coverage tests.</summary>
    private const int SixthClientSequence = 6;

    /// <summary>The invalid operation state used by snapshot recovery tests.</summary>
    private const int InvalidSnapshotRecoveryOperationState = 99;

    /// <summary>The oversized corrupt operation identifier length used by snapshot recovery tests.</summary>
    private const int OversizedOperationIdentifierLength = 4096;

    /// <summary>The invalid but canonical-length operation identifier text used by snapshot recovery tests.</summary>
    private const string InvalidCanonicalOperationIdentifierText = "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz";

    /// <summary>Verifies the SQLite adapter advertises and implements durable atomic snapshot recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAdapterIsConstructed_ThenSnapshotRecoveryCapabilityIsBackedByInterface()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        object candidate = adapter;

        await Assert.That(candidate is ILocalSnapshotRecoveryStore).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AtomicSnapshotRecovery) != 0).IsTrue();
    }

    /// <summary>Verifies included and unknown dispositions commit durably through a real SQLite database.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCommits_ThenCheckpointAndPendingWorkSurviveRestart()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        OperationId includedOperationId;
        OperationId terminalOperationId;
        OperationId preservedOperationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var included = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            var terminal = await adapter.CommitLocalOperationAsync(
                CreateOperation(SecondClientSequence),
                CreateSnapshotMutation(FirstClientSequence),
                CancellationToken.None);
            var preserved = await adapter.CommitLocalOperationAsync(
                CreateOperation(ThirdClientSequence),
                CreateSnapshotMutation(SecondClientSequence),
                CancellationToken.None);
            includedOperationId = included.OperationId;
            terminalOperationId = terminal.OperationId;
            preservedOperationId = preserved.OperationId;
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: 3,
                expectedCursor: null,
                [
                    IncludedSnapshotDisposition(includedOperationId, OperationResultKind.Accepted),
                    RejectedSnapshotDisposition(terminalOperationId),
                    UnknownSnapshotDisposition(preservedOperationId),
                ]);

            var result = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);

            await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
            await Assert.That(result.TerminalOperationCount).IsEqualTo(1);
            await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var includedStatus = await reopened.GetOperationStatusAsync(includedOperationId, CancellationToken.None);
        var terminalStatus = await reopened.GetOperationStatusAsync(terminalOperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
        await Assert.That(SamePayload(recovered.Snapshot?.State, CreatePayload(SnapshotRecoveryOptimisticText))).IsTrue();
        await Assert.That(SamePayload(recovered.Snapshot?.AuthoritativeState, CreatePayload(SnapshotRecoveryAuthoritativeText))).IsTrue();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(preservedOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(preservedOperationId);
        await Assert.That(includedStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(terminalStatus?.State).IsEqualTo(SyncOperationState.Rejected);
    }

    /// <summary>Verifies stale recovery fences preserve cursor, snapshot, and pending work.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryRevisionFenceIsStale_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SnapshotRecoveryOriginalCursor, []),
            CreateSnapshotMutation(0) with { State = CreatePayload(SnapshotRecoveryOriginalText) },
            CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: 0,
            expectedCursor: SnapshotRecoveryOriginalCursor,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryOriginalCursor);
        await Assert.That(SamePayload(recovered.Snapshot?.State, CreateSnapshotMutation(FirstClientSequence).State)).IsTrue();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies an active SQLite lease blocks recovery without stealing ownership.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoverySeesActiveLease_ThenOwnershipIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        _ = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadLeaseOperationCount(database.Path, operation.OperationId)).IsEqualTo(1);
    }

    /// <summary>Verifies successful recovery reclaims expired lease rows and preserves unresolved work after restart.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoverySucceedsAfterLeaseExpiry_ThenExpiredLeaseIsReclaimedDurably()
    {
        using var database = TempDatabase.Create();
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        SubscriptionId subscriptionId;
        OperationId operationId;
        await using (var adapter = CreateAdapter(database.Path, clock))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
            operationId = operation.OperationId;
            _ = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            clock.Advance(TimeSpan.FromMinutes(SnapshotRecoveryLeaseExpiryAdvanceMinutes));
            var mutation = CreateSnapshotRecoveryMutation(
                subscriptionId,
                expectedRevision: FirstClientSequence,
                expectedCursor: null,
                [UnknownSnapshotDisposition(operationId)]);

            _ = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path, clock);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(lease.Operations.Count).IsEqualTo(1);
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operationId);
    }

    /// <summary>Verifies transaction rollback preserves the previous durable state when SQLite aborts recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoverySqliteWriteAborts_ThenTransactionRollsBack()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        CreateSnapshotRecoveryRollbackTrigger(database.Path);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Accepted)]);

        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<SqliteException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies cancellation is observed before a queued recovery command mutates SQLite.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryIsCancelledBeforeExecution_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Accepted)]);

        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, cancellation.Token).AsTask();

        await Assert.That(apply).ThrowsExactly<OperationCanceledException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies quarantined streams reject recovery without changing the quarantine record.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryTargetsQuarantinedStream_ThenQuarantineIsPreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var quarantine = await adapter.QuarantinePayloadAsync(
            new()
            {
                StreamId = Stream,
                SubscriptionId = subscriptionId,
                OperationId = operation.OperationId,
                Source = LocalPayloadQuarantineSource.OutboxOperation,
                Reason = LocalPayloadQuarantineReason.PayloadHashMismatch,
                ReasonCode = "OC.Test",
                Evidence = new(
                    "reading",
                    1,
                    "application/json",
                    operation.OperationId.Value.ToByteArray().Length,
                    "hash-quarantine",
                    operation.OperationId.Value.ToByteArray()),
                ObservedAtUtc = DateTimeOffset.UnixEpoch,
            },
            CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var stored = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(stored?.QuarantineId).IsEqualTo(quarantine.Record.QuarantineId);
    }

    /// <summary>Verifies cursor fences reject recovery without mutating durable state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCursorFenceIsStale_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SnapshotRecoveryOriginalCursor, []),
            CreateSnapshotMutation(0) with { State = CreatePayload(SnapshotRecoveryOriginalText) },
            CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: "stale-cursor",
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryOriginalCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies cursor fences reject longer expected cursors without mutating durable state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryExpectedCursorIsLongerThanStoredCursor_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SnapshotRecoveryOriginalCursor, []),
            CreateSnapshotMutation(0) with { State = CreatePayload(SnapshotRecoveryOriginalText) },
            CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: $"{SnapshotRecoveryOriginalCursor}-longer",
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryOriginalCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies a missing expected cursor rejects recovery when SQLite has a cursor.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryExpectedCursorIsMissing_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SnapshotRecoveryOriginalCursor, []),
            CreateSnapshotMutation(0) with { State = CreatePayload(SnapshotRecoveryOriginalText) },
            CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryOriginalCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies cursor fences use ordinal string identity, not replacement-encoded UTF-8 bytes.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCursorDiffersOnlyByInvalidSurrogateReplacement_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SnapshotRecoveryReplacementCursor, []),
            CreateSnapshotMutation(0) with { State = CreatePayload(SnapshotRecoveryOriginalText) },
            CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: SnapshotRecoveryInvalidSurrogateCursor,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryReplacementCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies subscription identity fences reject recovery before durable mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoverySubscriptionIdentityDoesNotMatch_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            SubscriptionId.New(),
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies recovery dispositions must exactly match pending operations in order.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryDispositionsDoNotMatchPendingSet_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var second = await adapter.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence),
            CreateSnapshotMutation(FirstClientSequence),
            CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(second.OperationId),
                UnknownSnapshotDisposition(first.OperationId),
            ]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<ArgumentException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies corrupt pending operation identifiers fail before snapshot recovery mutates SQLite state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryPendingOperationIdIsCorrupt_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);
        SetOutboxOperationIdOversized(database.Path);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        SetOutboxOperationId(database.Path, operation.OperationId);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.Snapshot?.ServerCursor).IsNull();
        await Assert.That(SamePayload(recovered.Snapshot?.State, CreateSnapshotMutation(0).State)).IsTrue();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies canonical-length malformed identifiers fail closed without snapshot mutation.</summary>
    /// <param name="storedOperationId">The corrupt stored operation identifier.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InvalidCanonicalOperationIdentifierText)]
    [Arguments("00000000-0000-0000-0000-000000000000")]
    public async Task WhenSnapshotRecoveryPendingOperationIdTextIsInvalid_ThenSqliteStateIsUnchanged(string storedOperationId)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);
        SetOutboxOperationIdText(database.Path, storedOperationId);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        SetOutboxOperationId(database.Path, operation.OperationId);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies invalid pending operation states fail closed before recovery mutates SQLite.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryPendingOperationStateIsInvalid_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetOutboxOperationState(database.Path, operation.OperationId, InvalidSnapshotRecoveryOperationState);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(operation.OperationId)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        SetOutboxOperationState(database.Path, operation.OperationId, (int)SyncOperationState.QueuedForUpload);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies missing operation state rows fail included recovery without partial mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryIncludedOperationStateIsMissing_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        DeleteOutboxOperationState(database.Path, operation.OperationId);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Accepted)]);

        Func<Task> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        SetOutboxOperationState(database.Path, operation.OperationId, (int)SyncOperationState.QueuedForUpload);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies every SQLite pending state accepted by snapshot recovery remains unresolved when marked unknown.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoverySeesEveryPendingState_ThenUnknownDispositionsRemainPending()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var second = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(FirstClientSequence), CancellationToken.None);
        var third = await adapter.CommitLocalOperationAsync(CreateOperation(ThirdClientSequence), CreateSnapshotMutation(SecondClientSequence), CancellationToken.None);
        var fourth = await adapter.CommitLocalOperationAsync(CreateOperation(FourthClientSequence), CreateSnapshotMutation(ThirdClientSequence), CancellationToken.None);
        var fifth = await adapter.CommitLocalOperationAsync(CreateOperation(FifthClientSequence), CreateSnapshotMutation(FourthClientSequence), CancellationToken.None);
        var sixth = await adapter.CommitLocalOperationAsync(CreateOperation(SixthClientSequence), CreateSnapshotMutation(FifthClientSequence), CancellationToken.None);
        SetOutboxOperationState(database.Path, first.OperationId, (int)SyncOperationState.SavedLocally);
        SetOutboxOperationState(database.Path, second.OperationId, (int)SyncOperationState.QueuedForUpload);
        SetOutboxOperationState(database.Path, third.OperationId, (int)SyncOperationState.Uploading);
        SetOutboxOperationState(database.Path, fourth.OperationId, (int)SyncOperationState.Conflict);
        SetOutboxOperationState(database.Path, fifth.OperationId, (int)SyncOperationState.Ambiguous);
        SetOutboxOperationState(database.Path, sixth.OperationId, (int)SyncOperationState.GuaranteeExpired);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SixthClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(first.OperationId),
                UnknownSnapshotDisposition(second.OperationId),
                UnknownSnapshotDisposition(third.OperationId),
                UnknownSnapshotDisposition(fourth.OperationId),
                UnknownSnapshotDisposition(fifth.OperationId),
                UnknownSnapshotDisposition(sixth.OperationId),
            ]);

        var result = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(SixPendingOperations);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(SixPendingOperations);
    }

    /// <summary>Verifies conflict result proofs map to durable conflict state during recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryIncludesConflict_ThenOperationStateBecomesConflict()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Conflict)]);

        _ = await RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Conflict);
    }

    /// <summary>Verifies disposition membership count and duplicate errors preserve durable state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryDispositionSetIsIncompleteOrDuplicated_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var second = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(FirstClientSequence), CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var incomplete = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [UnknownSnapshotDisposition(first.OperationId)]);
        var duplicated = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: SecondClientSequence,
            expectedCursor: null,
            [
                UnknownSnapshotDisposition(first.OperationId),
                UnknownSnapshotDisposition(first.OperationId),
            ]);

        Func<Task> applyIncomplete = () => recoveryStore.ApplySnapshotRecoveryAsync(incomplete, CancellationToken.None).AsTask();
        Func<Task> applyDuplicated = () => recoveryStore.ApplySnapshotRecoveryAsync(duplicated, CancellationToken.None).AsTask();

        await Assert.That(applyIncomplete).ThrowsExactly<ArgumentException>();
        await Assert.That(applyDuplicated).ThrowsExactly<ArgumentException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies the internal pending scan rejects non-positive row bounds.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryPendingScanBoundIsInvalid_ThenItIsRejected()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(CancellationToken.None);

        Action scan = () => SqliteLocalCommitSql.ReadSnapshotRecoveryPendingOperations(
            connection,
            transaction,
            StoreIdentity,
            Stream,
            0,
            CancellationToken.None);

        await Assert.That(scan).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies malformed recovery mutations are rejected before SQLite execution.</summary>
    /// <param name="scenario">The invalid mutation scenario.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments("checkpoint-stream")]
    [Arguments("negative-revision")]
    [Arguments("revision-overflow")]
    [Arguments("unknown-with-result")]
    [Arguments("result-operation")]
    [Arguments("included-rejected")]
    [Arguments("terminal-accepted")]
    [Arguments("snapshot-format")]
    [Arguments("checkpoint-format")]
    public async Task WhenSnapshotRecoveryMutationShapeIsInvalid_ThenItIsRejectedBeforeSqliteExecution(string scenario)
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        var subscriptionId = SubscriptionId.New();
        var operationId = OperationId.New();
        var mutation = CreateInvalidSnapshotRecoveryMutation(scenario, subscriptionId, operationId);

        Func<Task> apply = () => RequireSnapshotRecoveryStore(adapter).ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        if (scenario is "negative-revision" or "snapshot-format" or "checkpoint-format")
        {
            await Assert.That(apply).ThrowsExactly<ArgumentOutOfRangeException>();
        }
        else if (scenario == "revision-overflow")
        {
            await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        }
        else
        {
            await Assert.That(apply).ThrowsExactly<ArgumentException>();
        }
    }

    /// <summary>Verifies oversized recovery capture is rejected before SQLite mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryInputExceedsWorkerBytes_ThenAdmissionRejectsBeforeSqliteMutation()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        LocalCommitResult operation;
        await using (var setup = CreateAdapter(database.Path))
        {
            await setup.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await setup.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            operation = await setup.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        }

        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recoveryStore = RequireSnapshotRecoveryStore(adapter);
        var mutation = CreateSnapshotRecoveryMutation(
            subscriptionId,
            expectedRevision: FirstClientSequence,
            expectedCursor: null,
            [IncludedSnapshotDisposition(operation.OperationId, OperationResultKind.Accepted)]) with
        {
            OptimisticState = CreatePayload(new('x', OversizedPayloadLength)),
        };

        Func<Task<LocalSnapshotRecoveryResult>> apply = () => recoveryStore.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(apply);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }
}
