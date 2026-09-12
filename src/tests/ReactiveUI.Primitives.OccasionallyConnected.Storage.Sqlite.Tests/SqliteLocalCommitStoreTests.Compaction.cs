// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Transactional compaction tests for <see cref="SqliteLocalCommitStore"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The age for rows that should exceed terminal outbox retention.</summary>
    private const int EligibleTerminalAgeDays = -2;

    /// <summary>The age for dead-letter rows that are still inside dead-letter retention.</summary>
    private const int RetainedDeadLetterAgeDays = -20;

    /// <summary>The age for inbox rows older than inbox retention.</summary>
    private const int ExpiredInboxAgeDays = -8;

    /// <summary>The age for inbox rows exactly on the retention boundary.</summary>
    private const int InboxBoundaryAgeDays = -7;

    /// <summary>The payload text used for current snapshot producer rows.</summary>
    private const string CurrentCompactionPayloadText = "current";

    /// <summary>The payload text used for terminal test rows.</summary>
    private const string TerminalCompactionPayloadText = "terminal";

    /// <summary>The expected removed record count for whole-store compaction.</summary>
    private const int WholeStoreCompactionRemovedRecords = 3;

    /// <summary>The compaction clock timestamp.</summary>
    private static readonly DateTimeOffset CompactionNow = new(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The default compaction retention used by tests.</summary>
    private static readonly RetentionOptions CompactionRetention = new()
    {
        OutboxTerminalRetention = TimeSpan.FromDays(1),
        InboxDeduplicationRetention = TimeSpan.FromDays(7),
        DeadLetterRetention = TimeSpan.FromDays(30),
    };

    /// <summary>Verifies compacting terminal rows deletes only eligible historical outbox payloads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenTerminalOutboxRowsAreOlderThanBothCutoffs_ThenHistoricalRowsAreRemoved()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var first = CommitOperation(store, Stream, clientSequence: 1, "old");
        var second = CommitOperation(store, Stream, SecondClientSequence, "new");
        SetOperationStateAt(database.Path, first.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, second.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        var firstBytes = ReadOutboxEncodedBytes(database.Path, first.OperationId);

        var result = store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, reopened.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(result.BytesReclaimed).IsEqualTo(firstBytes);
        await Assert.That(OutboxOperationExists(database.Path, first.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, second.OperationId)).IsTrue();
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(ThirdClientSequence);
    }

    /// <summary>Verifies unresolved rows block stream compaction without being removed.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStreamContainsUnresolvedIntent_ThenCompactionPreservesTheStream()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var terminal = CommitOperation(store, Stream, clientSequence: 1, TerminalCompactionPayloadText);
        var conflict = CommitOperation(store, Stream, SecondClientSequence, "conflict");
        SetOperationStateAt(database.Path, terminal.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, conflict.OperationId, SyncOperationState.Conflict, CompactionNow.AddDays(EligibleTerminalAgeDays));

        var result = store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(OutboxOperationExists(database.Path, terminal.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, conflict.OperationId)).IsTrue();
    }

    /// <summary>Verifies dead-letter retention is independent from the terminal outbox window.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterIsInsideItsOwnRetention_ThenTerminalOutboxCompactionKeepsIt()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var terminal = CommitOperation(store, Stream, clientSequence: 1, TerminalCompactionPayloadText);
        var deadLetter = CommitOperation(store, Stream, SecondClientSequence, "dead");
        var current = CommitOperation(store, Stream, ThirdClientSequence, CurrentCompactionPayloadText);
        SetOperationStateAt(database.Path, terminal.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, deadLetter.OperationId, SyncOperationState.DeadLettered, CompactionNow.AddDays(RetainedDeadLetterAgeDays));
        SetOperationStateAt(database.Path, current.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));

        var result = store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(OutboxOperationExists(database.Path, terminal.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, deadLetter.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, current.OperationId)).IsTrue();
    }

    /// <summary>Verifies inbox retention uses local committed timestamps rather than the terminal outbox cutoff.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenInboxRowsExceedInboxRetention_ThenTerminalOutboxCutoffDoesNotKeepThem()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var terminal = CommitOperation(store, Stream, clientSequence: 1, TerminalCompactionPayloadText);
        SetOperationStateAt(database.Path, terminal.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(ExpiredInboxAgeDays));
        var oldEvent = CreateRemoteEvent(FirstRemoteCursor);
        var retainedEvent = CreateRemoteEvent(SecondRemoteCursor);
        InsertInboxEvent(database.Path, oldEvent);
        InsertInboxEvent(database.Path, retainedEvent);
        SetInboxCommittedAt(database.Path, oldEvent.EventId, CompactionNow.AddDays(ExpiredInboxAgeDays));
        SetInboxCommittedAt(database.Path, retainedEvent.EventId, CompactionNow.AddDays(InboxBoundaryAgeDays));

        var result = store.Compact(
            new(Stream, CompactionNow.AddYears(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        var unapplied = store.GetUnappliedEventIds(Stream, [oldEvent.EventId, retainedEvent.EventId], CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(result.BytesReclaimed).IsEqualTo(0);
        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(oldEvent.EventId);
        await Assert.That(OutboxOperationExists(database.Path, terminal.OperationId)).IsTrue();
    }

    /// <summary>Verifies remote-applied inbox rows expire independently of a satisfied outbox byte target.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOnlyInboxRowsExist_ThenInboxRetentionStillCompactsExpiredRows()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var oldEvent = CreateRemoteEvent(FirstRemoteCursor);
        var retainedEvent = CreateRemoteEvent(SecondRemoteCursor);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, SecondRemoteCursor, [oldEvent, retainedEvent]),
            new(Stream, CreatePayload("remote-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        SetInboxCommittedAt(database.Path, oldEvent.EventId, CompactionNow.AddDays(ExpiredInboxAgeDays));
        SetInboxCommittedAt(database.Path, retainedEvent.EventId, CompactionNow.AddDays(InboxBoundaryAgeDays));

        var result = store.Compact(
            new(Stream, CompactionNow.AddYears(-1), TargetBytes: 1),
            CompactionRetention,
            CancellationToken.None);

        var unapplied = store.GetUnappliedEventIds(Stream, [oldEvent.EventId, retainedEvent.EventId], CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(result.BytesReclaimed).IsEqualTo(0);
        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(oldEvent.EventId);
        await Assert.That(InboxEventExists(database.Path, retainedEvent.EventId)).IsTrue();
    }

    /// <summary>Verifies no eligible rows are removed when retained bytes already fit the target.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetainedBytesAlreadyFitTarget_ThenEligibleRowsRemain()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var old = CommitOperation(store, Stream, clientSequence: 1, "old");
        var current = CommitOperation(store, Stream, SecondClientSequence, CurrentCompactionPayloadText);
        SetOperationStateAt(database.Path, old.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, current.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        var targetBytes = ReadOutboxEncodedBytes(database.Path, Stream);

        var result = store.Compact(
            new(Stream, CompactionNow.AddDays(-1), targetBytes),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(result.BytesReclaimed).IsEqualTo(0);
        await Assert.That(OutboxOperationExists(database.Path, old.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, current.OperationId)).IsTrue();
    }

    /// <summary>Verifies the byte target stops after retained bytes fit the advisory budget.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenCompactionCanReachTargetBytes_ThenLaterEligibleRowsAreDeferred()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var first = CommitOperation(store, Stream, clientSequence: 1, "aa");
        var second = CommitOperation(store, Stream, SecondClientSequence, "bb");
        var current = CommitOperation(store, Stream, ThirdClientSequence, "cc");
        SetOperationStateAt(database.Path, first.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, second.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, current.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        var firstBytes = ReadOutboxEncodedBytes(database.Path, first.OperationId);
        var targetBytes = ReadOutboxEncodedBytes(database.Path, Stream) - firstBytes;

        var result = store.Compact(
            new(Stream, CompactionNow.AddDays(-1), targetBytes),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(result.BytesReclaimed).IsEqualTo(firstBytes);
        await Assert.That(OutboxOperationExists(database.Path, first.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, second.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, current.OperationId)).IsTrue();
    }

    /// <summary>Verifies a delete failure rolls the compaction transaction back.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenCompactionDeleteFails_ThenTransactionRollsBack()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var first = CommitOperation(store, Stream, clientSequence: 1, "old");
        var current = CommitOperation(store, Stream, SecondClientSequence, CurrentCompactionPayloadText);
        SetOperationStateAt(database.Path, first.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, current.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        CreateCompactionRollbackTrigger(database.Path);

        var action = () => store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<SqliteException>();
        DropCompactionRollbackTrigger(database.Path);
        await Assert.That(OutboxOperationExists(database.Path, first.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, current.OperationId)).IsTrue();
    }

    /// <summary>Verifies compaction stays inside the initialized store identity partition.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAnotherStoreIdentityHasEligibleRows_ThenCompactionLeavesThatPartitionUntouched()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var alpha = CreateInitializedStore(database.Path, clock);
        using var beta = CreateInitializedStore(database.Path, SecondaryStoreIdentity);
        var alphaOld = CommitOperation(alpha, Stream, clientSequence: 1, "alpha-old");
        var alphaCurrent = CommitOperation(alpha, Stream, SecondClientSequence, "alpha-current");
        var betaOld = CommitOperation(beta, Stream, clientSequence: 1, "beta-old");
        var betaCurrent = CommitOperation(beta, Stream, SecondClientSequence, "beta-current");
        SetOperationStateAt(database.Path, alphaOld.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, alphaCurrent.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, betaOld.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, betaCurrent.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));

        var result = alpha.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(OutboxOperationExists(database.Path, alphaOld.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, betaOld.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, betaCurrent.OperationId)).IsTrue();
    }

    /// <summary>Verifies a store-wide request prunes every eligible stream inside the store identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRequestDoesNotNameStream_ThenCompactionPrunesAllEligibleStreams()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var firstOld = CommitOperation(store, Stream, clientSequence: 1, "first-old");
        var firstCurrent = CommitOperation(store, Stream, SecondClientSequence, "first-current");
        var secondOld = CommitOperation(store, ReopenedStream, clientSequence: 1, "second-old");
        var secondCurrent = CommitOperation(store, ReopenedStream, SecondClientSequence, "second-current");
        var remoteEvent = CreateRemoteEvent(Guid.NewGuid(), ReopenedStream, FirstRemoteCursor, null);
        InsertInboxEvent(database.Path, remoteEvent);
        SetOperationStateAt(database.Path, firstOld.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, firstCurrent.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, secondOld.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, secondCurrent.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetInboxCommittedAt(database.Path, remoteEvent.EventId, CompactionNow.AddDays(ExpiredInboxAgeDays));
        var expectedBytes = ReadOutboxEncodedBytes(database.Path, firstOld.OperationId) + ReadOutboxEncodedBytes(database.Path, secondOld.OperationId);

        var result = store.Compact(
            new((StreamId?)null, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        var unapplied = store.GetUnappliedEventIds(ReopenedStream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(WholeStoreCompactionRemovedRecords);
        await Assert.That(result.BytesReclaimed).IsEqualTo(expectedBytes);
        await Assert.That(OutboxOperationExists(database.Path, firstOld.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, firstCurrent.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, secondOld.OperationId)).IsFalse();
        await Assert.That(OutboxOperationExists(database.Path, secondCurrent.OperationId)).IsTrue();
        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(remoteEvent.EventId);
    }

    /// <summary>Verifies compaction reports a defensive error when an outbox candidate delete affects no rows.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOutboxCandidateDeleteAffectsNoRows_ThenCompactionThrows()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        var old = CommitOperation(store, Stream, clientSequence: 1, "old");
        var current = CommitOperation(store, Stream, SecondClientSequence, CurrentCompactionPayloadText);
        SetOperationStateAt(database.Path, old.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        SetOperationStateAt(database.Path, current.OperationId, SyncOperationState.Rejected, CompactionNow.AddDays(EligibleTerminalAgeDays));
        CreateCompactionIgnoreOutboxDeleteTrigger(database.Path);

        var action = () => store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        DropCompactionIgnoreOutboxDeleteTrigger(database.Path);
        await Assert.That(OutboxOperationExists(database.Path, old.OperationId)).IsTrue();
        await Assert.That(OutboxOperationExists(database.Path, current.OperationId)).IsTrue();
    }

    /// <summary>Verifies compaction reports a defensive error when an inbox candidate delete affects no rows.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenInboxCandidateDeleteAffectsNoRows_ThenCompactionThrows()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(CompactionNow);
        using var store = CreateInitializedStore(database.Path, clock);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        InsertInboxEvent(database.Path, remoteEvent);
        SetInboxCommittedAt(database.Path, remoteEvent.EventId, CompactionNow.AddDays(ExpiredInboxAgeDays));
        CreateCompactionIgnoreInboxDeleteTrigger(database.Path);

        var action = () => store.Compact(
            new(Stream, CompactionNow.AddDays(-1), TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        DropCompactionIgnoreInboxDeleteTrigger(database.Path);
        await Assert.That(InboxEventExists(database.Path, remoteEvent.EventId)).IsTrue();
    }

    /// <summary>Verifies retention cutoff underflow creates a no-delete lower bound.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRetentionCutoffUnderflows_ThenCompactionUsesNoDeleteCutoff()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.MinValue);
        using var store = CreateInitializedStore(database.Path, clock);

        var result = store.Compact(
            new(Stream, DateTimeOffset.MinValue, TargetBytes: 0),
            CompactionRetention,
            CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(result.BytesReclaimed).IsEqualTo(0);
    }

    /// <summary>Sets operation state and its state-change timestamp.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The operation state.</param>
    /// <param name="changedAtUtc">The state-change timestamp.</param>
    private static void SetOperationStateAt(string path, OperationId operationId, SyncOperationState state, DateTimeOffset changedAtUtc)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $state,
                changed_at_utc = $changedAtUtc
            WHERE operation_id = $operationId;
        """;
        _ = command.Parameters.AddWithValue("$state", (int)state);
        _ = command.Parameters.AddWithValue("$changedAtUtc", SqliteLocalCommitSql.FormatDateTimeOffset(changedAtUtc));
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets an inbox row local committed timestamp.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="committedAtUtc">The local committed timestamp.</param>
    private static void SetInboxCommittedAt(string path, Guid eventId, DateTimeOffset committedAtUtc)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_inbox
            SET committed_at_utc = $committedAtUtc
            WHERE event_id = $eventId;
            """;
        _ = command.Parameters.AddWithValue("$committedAtUtc", SqliteLocalCommitSql.FormatDateTimeOffset(committedAtUtc));
        _ = command.Parameters.AddWithValue("$eventId", eventId.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Returns whether an outbox row exists.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>Whether the row exists.</returns>
    private static bool OutboxOperationExists(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_outbox WHERE operation_id = $operationId;";
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        return command.ExecuteScalar() is long count && count == 1;
    }

    /// <summary>Reads outbox payload and metadata bytes for one stream.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The encoded byte count.</returns>
    /// <exception cref="InvalidOperationException">The byte count could not be read.</exception>
    private static long ReadOutboxEncodedBytes(string path, StreamId streamId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COALESCE(SUM(
                length(outbox.payload) + COALESCE((
                    SELECT SUM(length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB)))
                    FROM oc_outbox_metadata AS metadata
                    WHERE metadata.store_identity = outbox.store_identity
                        AND metadata.operation_id = outbox.operation_id), 0)), 0)
            FROM oc_outbox AS outbox
            WHERE outbox.store_identity = $storeIdentity AND outbox.stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        return command.ExecuteScalar() is long bytes ? bytes : throw new InvalidOperationException("The outbox byte count could not be read.");
    }

    /// <summary>Reads outbox payload and metadata bytes for one operation.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The encoded byte count.</returns>
    /// <exception cref="InvalidOperationException">The byte count could not be read.</exception>
    private static long ReadOutboxEncodedBytes(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT length(outbox.payload) + COALESCE((
                SELECT SUM(length(CAST(metadata.key AS BLOB)) + length(CAST(metadata.value AS BLOB)))
                FROM oc_outbox_metadata AS metadata
                WHERE metadata.store_identity = outbox.store_identity
                    AND metadata.operation_id = outbox.operation_id), 0)
            FROM oc_outbox AS outbox
            WHERE outbox.store_identity = $storeIdentity AND outbox.operation_id = $operationId;
        """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        return command.ExecuteScalar() is long bytes ? bytes : throw new InvalidOperationException("The operation byte count could not be read.");
    }

    /// <summary>Returns whether an inbox row exists.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <returns>Whether the row exists.</returns>
    private static bool InboxEventExists(string path, Guid eventId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_inbox WHERE event_id = $eventId;";
        _ = command.Parameters.AddWithValue("$eventId", eventId.ToString("D"));
        return command.ExecuteScalar() is long count && count == 1;
    }

    /// <summary>Creates a trigger that ignores outbox compaction deletes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateCompactionIgnoreOutboxDeleteTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_outbox_compaction_ignore
            BEFORE DELETE ON oc_outbox
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the outbox compaction ignore trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropCompactionIgnoreOutboxDeleteTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_outbox_compaction_ignore;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that ignores inbox compaction deletes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateCompactionIgnoreInboxDeleteTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_inbox_compaction_ignore
            BEFORE DELETE ON oc_inbox
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the inbox compaction ignore trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropCompactionIgnoreInboxDeleteTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_inbox_compaction_ignore;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that aborts compaction deletes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateCompactionRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_outbox_compaction_abort
            BEFORE DELETE ON oc_outbox
            BEGIN
                SELECT RAISE(ABORT, 'rollback compaction delete');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the compaction rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropCompactionRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_outbox_compaction_abort;";
        _ = command.ExecuteNonQuery();
    }
}
