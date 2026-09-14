// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests durable subscription start positions for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The third server event sequence.</summary>
    private const long ThirdEventSequence = 3;

    /// <summary>Verifies Latest anchors the existing frontier and preserves a null client previous cursor after reopen.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionLatestEmptyPollReopenThenPublishStartsAfterStoredFrontier()
    {
        using var database = new TemporaryDatabase();
        var identity = SubscriptionIdentity(FirstSubscription);
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));
            var empty = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

            await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
            await Assert.That(empty.Batch).IsNull();
        }

        using var reopened = CreateSubscriptionJournal(database.Path);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));
        var second = OperationKey(SecondOperationSeed);
        _ = reopened.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));

        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
        await Assert.That(batch.Events).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(batch.Events[0].ServerCursor).IsEqualTo(SecondCursor);
    }

    /// <summary>Verifies repeated Latest registration keeps the original anchor instead of skipping intervening events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionLatestReregisterDoesNotMoveStoredAnchor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));

        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
        await Assert.That(batch.PreviousCursor).IsNull();
    }

    /// <summary>Verifies the identity overload keeps the historical beginning behavior.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionIdentityRegistrationStartsAtBeginning()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.RegisterSubscription(identity);

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
        await Assert.That(batch.PreviousCursor).IsNull();
    }

    /// <summary>Verifies FromSequence uses event sequence and includes the containing operation group whole.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromSequenceInsideLaterGroupIncludesWholeGroup()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var secondEntry = Entry(
            second,
            OperationResultKind.Accepted,
            SecondOperationSeed,
            events:
            [
                Event(second.OperationId, SecondCursor, EventPayload),
                Event(second.OperationId, ThirdCursor, EventPayload),
            ]);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), secondEntry));
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(ThirdEventSequence)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(ThirdCursor);
        await Assert.That(batch.Events).Count().IsEqualTo(DoubleEntryCount);
        await Assert.That(batch.Events[0].ServerCursor).IsEqualTo(SecondCursor);
        await Assert.That(batch.Events[1].ServerCursor).IsEqualTo(ThirdCursor);
    }

    /// <summary>Verifies future timestamp positions wait and later resolve inclusively.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromTimestampFutureWaitsThenIncludesMatchingGroup()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateSubscriptionJournal(database.Path, clock);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var identity = SubscriptionIdentity(FirstSubscription);
        var future = Start.AddTicks(DoubleEntryCount);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromTimestamp(future)));

        var empty = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        clock.SetUtcNow(future);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
        await Assert.That(batch.PreviousCursor).IsNull();
    }

    /// <summary>Verifies changing the start position for an existing binding fails closed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionReregisterWithIncompatibleStartPositionFailsClosed()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));

        await Assert.That(() => journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(0))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies future SQLite sequence positions wait, resolve, and preserve the client previous cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromSequenceFutureWaitsThenResolvesSqliteAnchor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(DoubleEntryCount)));

        var empty = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
    }

    /// <summary>Verifies SQLite sequence positions fail closed when the durable event row is missing behind a gap.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromSequenceMissingDurableEventRowReturnsRetentionGap()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        DeleteEventsAndMarkReceiveGap(database.Path);
        using var reopened = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));

        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
    }

    /// <summary>Verifies cursor start positions are read back from SQLite and continue after the stored cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromCursorSurvivesReopenAndContinuesAfterStoredCursor()
    {
        using var database = new TemporaryDatabase();
        var identity = SubscriptionIdentity(FirstSubscription);
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromCursor(FirstCursor)));
        }

        using var reopened = CreateSubscriptionJournal(database.Path);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromCursor(FirstCursor)));
        var second = OperationKey(SecondOperationSeed);
        _ = reopened.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
    }

    /// <summary>Verifies SQLite subscription identity mismatch is rejected on page selection.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionOfferRejectsForeignSqliteIdentityBinding()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        var foreign = new ServerSubscriptionIdentity(new(Tenant, OtherStream), Client, FirstSubscription);
        _ = journal.RegisterSubscription(identity);

        await Assert.That(() => journal.OfferReceivePage(new(foreign, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the SQLite acknowledgement interface dispatches the start-position registration overload.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionStartPositionInterfaceOverloadRegistersSqliteBinding()
    {
        using var database = new TemporaryDatabase();
        IServerSubscriptionAcknowledgementJournal journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);

        var state = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));

        await Assert.That(state.Identity.SubscriptionId).IsEqualTo(FirstSubscription);
    }

    /// <summary>Verifies a disappearing SQLite subscription row fails closed during deferred anchor persistence.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionDeferredInitialAnchorMissingRowFailsClosed()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(DoubleEntryCount)));
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        CreateDeleteInitialAnchorUpdateTrigger(database.Path);

        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema-three databases migrate through the direct schema-three branch.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaThreeDirectMigrationAddsStartPositionColumns()
    {
        using var database = new TemporaryDatabase();
        using (var created = CreateSubscriptionJournal(database.Path))
        {
            await Assert.That(created.SubscriptionCount).IsEqualTo(0);
        }

        RewriteSubscriptionTablesAsSchemaThree(database.Path);
        using var migrated = CreateSubscriptionJournal(database.Path);

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(MigratedSchemaVersion);
        await Assert.That(migrated.SubscriptionCount).IsEqualTo(0);
    }

    /// <summary>Verifies schema-three migration rejects unsupported metadata before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaThreeMigrationRejectsUnsupportedMetadataVersion()
    {
        using var database = new TemporaryDatabase();
        using (var created = CreateSubscriptionJournal(database.Path))
        {
            await Assert.That(created.SubscriptionCount).IsEqualTo(0);
        }

        WriteSchemaThreeUnsupportedMetadataVersion(database.Path);

        await Assert.That(() => CreateSubscriptionJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema-three migration wraps malformed metadata storage.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaThreeMigrationRejectsMalformedMetadataTable()
    {
        using var database = new TemporaryDatabase();
        CreateMalformedSchemaThreeMetadataDatabase(database.Path);

        await Assert.That(() => CreateSubscriptionJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies future SQLite sequence positions on missing streams report end without a retained frontier.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromSequenceMissingStreamWaitsAtZeroFrontier()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(page.LastGroupSequence).IsEqualTo(0);
    }

    /// <summary>Verifies a future first event sequence persists a null deferred anchor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromSequenceFutureFirstGroupPersistsNullDeferredAnchor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));
        var empty = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies SQLite sequence anchors can resolve before the first retained group.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromSequenceFirstGroupKeepsNullInternalAnchor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies SQLite sequence lookup can resolve a later durable row when no gap is marked.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromSequenceLaterDurableRowWithoutGapResolves()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        MoveFirstEventSequenceForwardWithoutGap(database.Path);
        using var reopened = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));
        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies SQLite sequence lookup does not leap to a later durable row across a retained-history gap.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromSequenceLaterDurableRowWithGapReturnsRetentionGap()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        MoveFirstEventSequenceForwardWithGap(database.Path);
        using var reopened = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));
        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
    }

    /// <summary>Verifies timestamp lookup does not leap over a missing interior group that could match the threshold.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromTimestampInteriorReceiveGapReturnsRetentionGap()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        var threshold = Start.AddTicks(SingleEntryCount);
        using (var journal = CreateSubscriptionJournal(database.Path, clock))
        {
            var first = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
            clock.SetUtcNow(threshold);
            var second = OperationKey(SecondOperationSeed);
            _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed, events: [])));
            var third = OperationKey(ThirdOperationSeed);
            _ = journal.TryCommit(Plan(DoubleEntryCount, null, Stamp(third), Entry(third, OperationResultKind.Accepted, ThirdOperationSeed, events: [])));
        }

        DeleteMiddleGroupAndMarkReceiveGap(database.Path);
        using var reopened = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = reopened.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromTimestamp(threshold)));
        var page = reopened.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
    }

    /// <summary>Verifies corrupted SQLite initial position kind fails closed while reading the subscription row.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionCorruptInitialPositionKindFailsClosed()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateSubscriptionJournal(database.Path))
        {
            _ = journal.RegisterSubscription(SubscriptionIdentity(FirstSubscription));
        }

        CorruptInitialPositionKind(database.Path);

        await Assert.That(() => CreateSubscriptionJournal(database.Path).RegisterSubscription(SubscriptionIdentity(FirstSubscription)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Moves the first durable event sequence forward without setting the gap marker.</summary>
    /// <param name="path">The database path.</param>
    private static void MoveFirstEventSequenceForwardWithoutGap(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_server_journal_event_metadata;
            UPDATE oc_server_journal_events SET event_sequence = 2;
            UPDATE oc_server_journal_streams SET last_event_sequence = 2, receive_history_incomplete = 0;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Moves the first durable event sequence forward and sets the gap marker.</summary>
    /// <param name="path">The database path.</param>
    private static void MoveFirstEventSequenceForwardWithGap(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_server_journal_event_metadata;
            UPDATE oc_server_journal_events SET event_sequence = 2;
            UPDATE oc_server_journal_streams SET last_event_sequence = 2, receive_history_incomplete = 1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes an interior durable group while marking receive history incomplete.</summary>
    /// <param name="path">The database path.</param>
    private static void DeleteMiddleGroupAndMarkReceiveGap(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_server_journal_ledger WHERE group_sequence = 2;
            UPDATE oc_server_journal_streams SET receive_history_incomplete = 1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted initial start-position discriminator.</summary>
    /// <param name="path">The database path.</param>
    private static void CorruptInitialPositionKind(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_subscriptions SET initial_position_kind = 99;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes durable event rows while marking receive history incomplete.</summary>
    /// <param name="path">The database path.</param>
    private static void DeleteEventsAndMarkReceiveGap(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_server_journal_event_metadata;
            DELETE FROM oc_server_journal_events;
            UPDATE oc_server_journal_streams SET receive_history_incomplete = 1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes a subscription before its deferred anchor update applies.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteInitialAnchorUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER delete_initial_anchor_update
            BEFORE UPDATE OF initial_anchor_resolved ON oc_server_journal_subscriptions
            BEGIN
                DELETE FROM oc_server_journal_subscriptions WHERE subscription_id = OLD.subscription_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Rewrites the empty subscription tables to their schema-three shape.</summary>
    /// <param name="path">The database path.</param>
    private static void RewriteSubscriptionTablesAsSchemaThree(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TABLE oc_server_journal_subscription_offers;
            DROP TABLE oc_server_journal_subscriptions;
            CREATE TABLE oc_server_journal_subscriptions (
                subscription_id TEXT NOT NULL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                acknowledged_cursor TEXT NULL,
                acknowledged_group_sequence INTEGER NOT NULL,
                latest_offered_cursor TEXT NULL,
                latest_offered_group_sequence INTEGER NOT NULL,
                acknowledged_at_utc TEXT NULL,
                updated_at_utc TEXT NOT NULL,
                last_touched_utc TEXT NOT NULL,
                logical_bytes INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_subscription_offers (
                subscription_id TEXT NOT NULL,
                cursor TEXT NOT NULL,
                group_sequence INTEGER NOT NULL,
                offered_at_utc TEXT NOT NULL,
                logical_bytes INTEGER NOT NULL,
                PRIMARY KEY (subscription_id, cursor),
                FOREIGN KEY (subscription_id)
                    REFERENCES oc_server_journal_subscriptions (subscription_id)
                    ON DELETE CASCADE);
            DELETE FROM oc_server_journal_metadata WHERE key = 'subscription_generation_high_water';
            UPDATE oc_server_journal_metadata SET value = '3' WHERE key = 'schema_version';
            PRAGMA user_version = 3;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Marks a database as schema three with unsupported metadata.</summary>
    /// <param name="path">The database path.</param>
    private static void WriteSchemaThreeUnsupportedMetadataVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_server_journal_metadata WHERE key = 'subscription_generation_high_water';
            UPDATE oc_server_journal_metadata SET value = '2' WHERE key = 'schema_version';
            PRAGMA user_version = 3;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a schema-three database whose metadata table cannot satisfy migration reads.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateMalformedSchemaThreeMetadataDatabase(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA user_version = 3;
            CREATE TABLE oc_server_journal_conflicts (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_event_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_events (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_ledger (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_streams (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_subscription_offers (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_subscriptions (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }
}
