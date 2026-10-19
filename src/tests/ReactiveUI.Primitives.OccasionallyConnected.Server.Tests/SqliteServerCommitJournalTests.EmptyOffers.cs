// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Proves unchanged polling does not write or reserve the writer lock.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The SQLite observer data-version query.</summary>
    private const string DataVersionSql = "PRAGMA data_version;";

    /// <summary>The repeated idle poll count.</summary>
    private const int IdlePollCount = 8;

    /// <summary>The initial sequence beyond all retained history.</summary>
    private const int PendingInitialSequence = 100;

    /// <summary>Verifies repeated empty offers preserve durable state and the observer's SQLite data version.</summary>
    /// <param name="hasHistory">Whether the empty poll follows a retained event.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyOffersDoNotWriteOrReserveWriterLock(bool hasHistory)
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateSubscriptionJournal(database.Path, clock);
        var identity = SubscriptionIdentity(FirstSubscription);
        var initial = journal.RegisterSubscription(identity);
        string? cursor = null;
        if (hasHistory)
        {
            var key = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            cursor = journal.OfferReceivePage(new(identity, null, 1, DefaultMaximumEvents, DefaultMaximumLogicalBytes)).RequireBatch().NextCursor;
            initial = journal.RegisterSubscription(identity);
        }

        using var observer = OpenRawConnection(database.Path);
        using var command = observer.CreateStatement();
        command.SetSql(DataVersionSql);
        var version = command.Scalar();
        clock.SetUtcNow(Start.AddMinutes(1));
        using (var writerReservation = observer.BeginTransaction())
        {
            for (var index = 0; index < IdlePollCount; index++)
            {
                var retained = journal.RegisterSubscription(identity);
                var empty = journal.OfferReceivePage(new(identity, cursor, 1, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
                await Assert.That(retained.Revision).IsEqualTo(initial.Revision);
                await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
            }
        }

        command.SetSql(DataVersionSql);
        await Assert.That(command.Scalar()).IsEqualTo(version);
        command.SetSql("SELECT value FROM oc_server_journal_metadata WHERE key = 'latest_utc';");
        await Assert.That(command.Scalar()).IsEqualTo(Start.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Verifies a pending sequence position does not persist a poll heartbeat.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task PendingInitialAnchorEmptyOffersRemainReadOnly()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(PendingInitialSequence)));
        using var observer = OpenRawConnection(database.Path);
        using var command = observer.CreateStatement();
        command.SetSql(DataVersionSql);
        var version = command.Scalar();
        using (var writerReservation = observer.BeginTransaction())
        {
            for (var index = 0; index < IdlePollCount; index++)
            {
                var empty = journal.OfferReceivePage(new(identity, null, 1, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
                await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
            }
        }

        command.SetSql(DataVersionSql);
        await Assert.That(command.Scalar()).IsEqualTo(version);
    }

    /// <summary>Verifies a page cannot cross a replaced binding generation even when registration races an offer.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ReceiveOfferRejectsReplacedBindingGenerationWithoutWriting()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateSubscriptionJournal(database.Path, clock, subscriptionRetention: TimeSpan.FromTicks(SingleEntryCount));
        var identity = SubscriptionIdentity(FirstSubscription);
        var original = journal.RegisterSubscription(identity);
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();
        var replacement = journal.RegisterSubscription(identity);
        using var observer = OpenRawConnection(database.Path);
        using var command = observer.CreateStatement();
        command.SetSql(DataVersionSql);
        var version = command.Scalar();
        using (var reservation = observer.BeginTransaction())
        {
            var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)
            { ExpectedGeneration = original.Generation });
            await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        }

        command.SetSql(DataVersionSql);
        await Assert.That(command.Scalar()).IsEqualTo(version);
        await Assert.That(replacement.Generation).IsGreaterThan(original.Generation);
    }

    /// <summary>Verifies replay and receive reconstruction do not read unrelated payload rows.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task TargetedReplayAndReceiveIgnoreUnselectedPayloads()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, null, null, Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(1, null, null, Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("UPDATE oc_server_journal_events SET payload_schema_version = -1 WHERE event_sequence = 2;");
            _ = command.Execute();
        }

        await Assert.That(journal.Read(StreamKey(), [first]).Entries).Count().IsEqualTo(1);
        var page = journal.ReadReceivePage(new(StreamKey(), null, 1, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        await Assert.That(page.RequireBatch().Events[0].ServerCursor).IsEqualTo(FirstCursor);
        await Assert.That(() => journal.Read(StreamKey(), [second])).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies targeted replay reconstruction preserves the durable stream frontier.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task TargetedReplayPreservesLaterFrontierCursor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, null, null, Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(SingleEntryCount, null, null, Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));

        var snapshot = journal.Read(StreamKey(), [first]);
        await Assert.That(snapshot.LastCursor).IsEqualTo(SecondCursor);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(snapshot.LastGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies candidate event conflicts remain fail-closed without loading unrelated ledger payloads.</summary>
    /// <param name="duplicateId">Whether to collide on the event identity instead of its cursor.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TargetedCommitRejectsRetainedEventKeyConflicts(bool duplicateId)
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var entry = Entry(first, OperationResultKind.Accepted, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, null, null, entry));
        var remoteEvent = new RemoteEvent(
            duplicateId ? entry.Events[0].EventId : Guid.NewGuid(),
            Stream,
            duplicateId ? SecondCursor : FirstCursor,
            Start,
            second.OperationId,
            Payload(EventPayload),
            new Dictionary<string, string>())
        { Origin = new(Client, second.OperationId) };
        var result = journal.TryCommit(Plan(
            SingleEntryCount,
            null,
            null,
            Entry(second, OperationResultKind.Accepted, SecondOperationSeed, events: [remoteEvent])));

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.IntentMismatch);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(SingleEntryCount);
    }
}
