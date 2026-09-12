// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Remote inbox application tests for <see cref="SqliteLocalCommitStore"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The first remote cursor used by apply tests.</summary>
    private const string FirstRemoteCursor = "remote-cursor-1";

    /// <summary>The second remote cursor used by apply tests.</summary>
    private const string SecondRemoteCursor = "remote-cursor-2";

    /// <summary>The third remote cursor used by apply tests.</summary>
    private const string ThirdRemoteCursor = "remote-cursor-3";

    /// <summary>The number of events in the main remote batch.</summary>
    private const int TwoRemoteEvents = 2;

    /// <summary>The revision after one local commit and one remote apply.</summary>
    private const int RemoteSnapshotAfterLocalRevision = 2;

    /// <summary>The number of candidate identifiers remaining after one applied event is removed.</summary>
    private const int TwoUnappliedCandidateIds = 2;

    /// <summary>Verifies remote inbox identifiers, snapshot, and cursor persist atomically across reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteBatchAppliesAndStoreReopens_ThenInboxSnapshotCursorAndSequenceRecover()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = CreateRemoteEvent(FirstRemoteCursor);
        var second = CreateRemoteEvent(SecondRemoteCursor);
        var batch = CreateRemoteBatch(null, SecondRemoteCursor, [first, second]);
        var snapshot = new SnapshotMutation(Stream, CreatePayload("remote-snapshot"), FormatVersion: 1, ExpectedRevision: 0);

        var result = store.ApplyRemoteBatch(batch, snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var unapplied = reopened.GetUnappliedEventIds(Stream, [first.EventId, second.EventId, Guid.NewGuid()], CancellationToken.None);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.NextCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(result.AppliedCount).IsEqualTo(TwoRemoteEvents);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(result.SnapshotRevision).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsNotEqualTo(first.EventId);
        await Assert.That(recovery.ServerCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.ServerCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(recovery.Snapshot?.State.Payload.ToArray().SequenceEqual(snapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies remote apply persists the batch cursor even when the final event carries an earlier cursor.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteBatchNextCursorDiffersFromLastEventCursor_ThenBatchCursorIsPersisted()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        var batch = CreateRemoteBatch(null, SecondRemoteCursor, [remoteEvent]);
        var snapshot = new SnapshotMutation(Stream, CreatePayload("remote-snapshot"), FormatVersion: 1, ExpectedRevision: 0);

        var result = store.ApplyRemoteBatch(batch, snapshot, CancellationToken.None);

        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(result.NextCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(recovery.ServerCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(recovery.Snapshot?.ServerCursor).IsEqualTo(SecondRemoteCursor);
    }

    /// <summary>Verifies schema version two databases migrate to schema version three without losing committed data.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLegacyLocalCommitSchemaMigratesToCurrent_ThenCommittedRowsArePreserved()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        var operation = CreateOperation(clientSequence: 1);
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var transaction = connection.BeginTransaction();
            SqliteStoreSchemaTests.CreateLegacyLocalCommitSchema(connection, transaction);
            InsertLegacyLocalCommitRows(connection, transaction, subscriptionId, operation, CreateSnapshotMutation(expectedRevision: 0));
            transaction.Commit();
        }

        using var store = CreateInitializedStore(database.Path);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [Guid.NewGuid()], CancellationToken.None);

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies stale expected revision and cursor checks reject remote batches without durable side effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplyExpectedRevisionOrCursorIsStale_ThenBatchRollsBack()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(FirstRemoteCursor)]),
            new(Stream, CreatePayload("first-remote"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        var staleRevisionEvent = CreateRemoteEvent(SecondRemoteCursor);
        var staleCursorEvent = CreateRemoteEvent(ThirdRemoteCursor);

        var staleRevision = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(FirstRemoteCursor, SecondRemoteCursor, [staleRevisionEvent]),
            new(Stream, CreatePayload("stale-revision"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        var staleCursor = () => store.ApplyRemoteBatch(
            CreateRemoteBatch("wrong-cursor", ThirdRemoteCursor, [staleCursorEvent]),
            new(Stream, CreatePayload("stale-cursor"), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);

        await Assert.That(staleRevision).ThrowsExactly<InvalidOperationException>();
        await Assert.That(staleCursor).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [staleRevisionEvent.EventId, staleCursorEvent.EventId], CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(TwoRemoteEvents);
    }

    /// <summary>Verifies remote apply validates batch and event identity input before persistence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplyInputIsInvalid_ThenValidationFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var otherStream = ReopenedStream;
        var eventId = Guid.NewGuid();
        var causedByOperationId = OperationId.New();

        Action emptyCandidate = () => store.GetUnappliedEventIds(Stream, [Guid.Empty], CancellationToken.None);
        Action emptyBatchId = () => store.ApplyRemoteBatch(
            new(Guid.Empty, Stream, null, FirstRemoteCursor, []),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action blankPreviousCursor = () => store.ApplyRemoteBatch(
            new(Guid.NewGuid(), Stream, " ", FirstRemoteCursor, []),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action mismatchedMutationStream = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, []),
            new(otherStream, CreatePayload("other"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        Action emptyEventId = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.Empty, Stream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action duplicateEventId = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(
                null,
                FirstRemoteCursor,
                [CreateRemoteEvent(eventId, Stream, FirstRemoteCursor, null), CreateRemoteEvent(eventId, Stream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action wrongEventStream = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.NewGuid(), otherStream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action emptyCausalOperation = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.NewGuid(), Stream, FirstRemoteCursor, default(OperationId))]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        var result = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.NewGuid(), Stream, FirstRemoteCursor, causedByOperationId)]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);

        await Assert.That(emptyCandidate).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyBatchId).ThrowsExactly<ArgumentException>();
        await Assert.That(blankPreviousCursor).ThrowsExactly<ArgumentException>();
        await Assert.That(mismatchedMutationStream).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyEventId).ThrowsExactly<ArgumentException>();
        await Assert.That(duplicateEventId).ThrowsExactly<ArgumentException>();
        await Assert.That(wrongEventStream).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyCausalOperation).ThrowsExactly<ArgumentException>();
        await Assert.That(result.NextCursor).IsEqualTo(FirstRemoteCursor);
    }

    /// <summary>Verifies SQLite-specific remote apply validation guards reject invalid DTO shapes directly.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplyInputIsValidatedDirectly_ThenSqliteGuardsFailClosed()
    {
        var otherStream = ReopenedStream;
        var eventId = Guid.NewGuid();
        Action emptyBatchId = static () => SqliteLocalCommitValidation.ValidateRemoteApplyInput(
            new(Guid.Empty, Stream, null, FirstRemoteCursor, []),
            CreateSnapshotMutation(expectedRevision: 0));
        Action emptyEventId = static () => SqliteLocalCommitValidation.ValidateRemoteApplyInput(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.Empty, Stream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0));
        Action duplicateEventId = () => SqliteLocalCommitValidation.ValidateRemoteApplyInput(
            CreateRemoteBatch(
                null,
                FirstRemoteCursor,
                [CreateRemoteEvent(eventId, Stream, FirstRemoteCursor, null), CreateRemoteEvent(eventId, Stream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0));
        Action wrongEventStream = () => SqliteLocalCommitValidation.ValidateRemoteApplyInput(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(Guid.NewGuid(), otherStream, FirstRemoteCursor, null)]),
            CreateSnapshotMutation(expectedRevision: 0));

        await Assert.That(emptyBatchId).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyEventId).ThrowsExactly<ArgumentException>();
        await Assert.That(duplicateEventId).ThrowsExactly<ArgumentException>();
        await Assert.That(wrongEventStream).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies the cursor update compare-and-swap rejects stale expected cursors.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCursorCompareAndSwapIsStale_ThenUpdateFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = connection.BeginTransaction();

        Action action = () => SqliteLocalCommitSql.UpdateServerCursor(
            connection,
            transaction,
            StoreIdentity,
            Stream,
            FirstRemoteCursor,
            SecondRemoteCursor);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an inbox race between lookup and apply is reported as a duplicate while committing the cursor fence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEventBecomesInboxedAfterLookup_ThenRemoteApplyCountsDuplicateAndCommitsSnapshot()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        var unapplied = store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);
        InsertInboxEvent(database.Path, remoteEvent);

        var result = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload("race-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);

        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(result.AppliedCount).IsEqualTo(0);
        await Assert.That(result.DuplicateCount).IsEqualTo(1);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies a failure after inbox insertion rolls back inbox, snapshot, and cursor together.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplyFailsAfterInboxInsert_ThenInboxSnapshotAndCursorRollBack()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        CreateRemoteApplyRollbackTrigger(database.Path);

        var action = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload("rollback-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<SqliteException>();
        DropRemoteApplyRollbackTrigger(database.Path);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies inbox identifiers are isolated by store identity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSameRemoteEventIdExistsInAnotherPartition_ThenInboxLookupRemainsUnapplied()
    {
        using var database = TempDatabase.Create();
        using var alpha = CreateInitializedStore(database.Path, StoreIdentity);
        using var beta = CreateInitializedStore(database.Path, SecondaryStoreIdentity);
        _ = alpha.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = beta.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        _ = beta.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload("beta-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);

        var alphaUnapplied = alpha.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(alphaUnapplied.Count).IsEqualTo(1);
        await Assert.That(alphaUnapplied[0]).IsEqualTo(remoteEvent.EventId);
    }

    /// <summary>Verifies malformed inbox rows fail closed during lookup.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInboxRowsAreMalformed_ThenLookupFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        InsertMalformedInboxEvent(database.Path, remoteEvent.EventId);

        var action = () => store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies inbox lookup probes only candidate identifiers, not retained history.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenUnrelatedInboxRowsAreMalformed_ThenLookupStillReturnsCandidateOrder()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var unrelated = CreateRemoteEvent(FirstRemoteCursor);
        var applied = CreateRemoteEvent(SecondRemoteCursor);
        var firstNew = Guid.NewGuid();
        var secondNew = Guid.NewGuid();
        InsertInboxEvent(database.Path, applied);
        InsertMalformedInboxEvent(database.Path, unrelated.EventId);

        var unapplied = store.GetUnappliedEventIds(Stream, [firstNew, applied.EventId, secondNew], CancellationToken.None);

        await Assert.That(unapplied.Count).IsEqualTo(TwoUnappliedCandidateIds);
        await Assert.That(unapplied[0]).IsEqualTo(firstNew);
        await Assert.That(unapplied[1]).IsEqualTo(secondNew);
    }

    /// <summary>Verifies cancellation before remote apply starts leaves durable state unchanged.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplyIsCancelledBeforeCommit_ThenNothingIsWritten()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);

        var action = () => store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload("cancelled-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            cancellation.Token);

        await Assert.That(action).ThrowsExactly<OperationCanceledException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies replaying an exact old local operation still returns the original result after remote snapshots.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEarlierLocalOperationIsReplayedAfterRemoteSnapshot_ThenOriginalReceiptSurvives()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        var first = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(FirstRemoteCursor)]),
            new(Stream, CreatePayload("remote-after-local"), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);

        var replay = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(RemoteSnapshotAfterLocalRevision);
        await Assert.That(recovery.ServerCursor).IsEqualTo(FirstRemoteCursor);
    }

    /// <summary>Creates a representative remote batch.</summary>
    /// <param name="previousCursor">The cursor before the batch.</param>
    /// <param name="nextCursor">The cursor after the batch.</param>
    /// <param name="events">The batch events.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(string? previousCursor, string nextCursor, IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, events);

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="serverCursor">The event server cursor.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(string serverCursor) =>
        new(Guid.NewGuid(), Stream, serverCursor, new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), null, CreatePayload("remote"), new Dictionary<string, string>());

    /// <summary>Creates a representative remote event with explicit identity values.</summary>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="causedByOperationId">The optional causal operation identifier.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(Guid eventId, StreamId streamId, string serverCursor, OperationId? causedByOperationId) =>
        new(eventId, streamId, serverCursor, new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), causedByOperationId, CreatePayload("remote"), new Dictionary<string, string>());
}
