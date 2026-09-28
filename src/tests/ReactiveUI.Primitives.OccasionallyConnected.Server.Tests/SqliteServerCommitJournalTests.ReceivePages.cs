// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests receive paging for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Verifies SQLite receive paging survives reopen and keeps zero-event groups ordered.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesRoundTripCompleteGroupsAndZeroEventCompletions()
    {
        using var database = new TemporaryDatabase();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        string cursor;
        using (var journal = CreateJournal(database.Path))
        {
            var entries = new[]
            {
                Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
                Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: []),
            };
            _ = journal.TryCommit(new(StreamKey(), 0, State(FirstVersion), Stamp(firstKey), entries));
            var firstPage = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            var firstBatch = firstPage.Batch ?? throw new InvalidOperationException("The first page did not return a batch.");
            cursor = firstBatch.NextCursor;
        }

        using var reopened = CreateJournal(database.Path);
        var secondPage = reopened.ReadReceivePage(new(StreamKey(), cursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second page did not return a batch.");
        var end = reopened.ReadReceivePage(new(StreamKey(), secondBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(secondPage.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(secondPage.NextGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(secondPage.LastGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(secondBatch.PreviousCursor).IsEqualTo(cursor);
        await Assert.That(secondBatch.NextCursor).IsNotEqualTo(string.Empty);
        await Assert.That(secondBatch.Events).Count().IsEqualTo(0);
        await Assert.That(secondBatch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(secondBatch.CompletedOperations[0].Origin).IsEqualTo(new(Client, secondKey.OperationId));
        await Assert.That(secondBatch.CompletedOperations[0].EventIds).Count().IsEqualTo(0);
        await Assert.That(end.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
    }

    /// <summary>Verifies SQLite reports a retention gap after the requested group has expired.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesReportRetentionGapAfterCompaction()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();

        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.LastGroupSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies corrupt durable receive-history markers fail closed during receive paging.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectCorruptReceiveHistoryMarker()
    {
        using var database = new TemporaryDatabase();
        var firstKey = OperationKey(FirstOperationSeed);
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        WriteReceiveHistoryMarker(database.Path, DoubleEntryCount);
        using var reopened = CreateJournal(database.Path);

        await Assert.That(() => reopened.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies receive page dispatch through the durable journal interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesUseCommitJournalInterface()
    {
        using var database = new TemporaryDatabase();
        IServerCommitJournal journal = CreateJournal(database.Path);
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));

        var snapshot = journal.Read(StreamKey(), [firstKey]);
        var page = ((IServerReceiveJournal)journal).ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
    }

    /// <summary>Verifies changing retention cannot make a page silently skip an expired middle group.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected complete prefix page is absent.</exception>
    [Test]
    public async Task ReceivePagesStopBeforeAnExpiredMiddleGroup()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateJournal(database.Path, clock);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var third = OperationKey(Client, ThirdOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        using (var shorterRetention = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount)))
        {
            _ = shorterRetention.TryCommit(Plan(SingleEntryCount, null, null, Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        }

        _ = journal.TryCommit(Plan(DoubleEntryCount, null, null, Entry(third, OperationResultKind.Accepted, ThirdOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        await Assert.That(journal.Compact()).IsEqualTo(SingleEntryCount);

        var page = journal.ReadReceivePage(new(StreamKey(), null, DefaultMaximumLedgerEntries, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The retained complete prefix was not returned.");

        await Assert.That(batch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
        var gap = journal.ReadReceivePage(new(StreamKey(), batch.NextCursor, DefaultMaximumLedgerEntries, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        await Assert.That(gap.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
    }

    /// <summary>Writes a raw receive-history marker value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="value">The marker value.</param>
    private static void WriteReceiveHistoryMarker(string path, int value)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_streams SET receive_history_incomplete = $value;";
        _ = command.Parameters.AddWithValue("$value", value);
        _ = command.ExecuteNonQuery();
    }
}
