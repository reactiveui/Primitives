// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Verifies grouped transaction boundaries preserve independent operation admission.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The first group size exceeding the bounded journal transaction limit.</summary>
    private const int OversizedCommitGroupCount = 9;

    /// <summary>Verifies bounded groups retain only the admissible prefix when capacity rejects a plan.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BatchedCommitsStopDependentPlansAfterCapacityRejection()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path, maximumLedgerEntries: 1);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var third = OperationKey(Client, ThirdOperationSeed);
        var results = journal.TryCommitBatch(
        [
            Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)),
            Plan(1, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)),
            Plan(DoubleEntryCount, State("v3"), Stamp(third), Entry(third, OperationResultKind.Accepted, ThirdOperationSeed)),
        ]);
        await Assert.That(results[0].Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(results[1].Status).IsEqualTo(ServerCommitStatus.CapacityExceeded);
        await Assert.That(results[2].Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(1);
        await Assert.That(journal.Read(StreamKey(), [first, second, third]).State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(1);
    }

    /// <summary>Verifies a fault before the grouped commit rolls back all operations and retained counters.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BatchedCommitFaultRollsBackWholePhysicalTransaction()
    {
        using var database = new TemporaryDatabase();
        using var journal = new SqliteServerCommitJournal(database.Path, new() { TimeProvider = new ManualTimeProvider(Start) }, new ThrowingBatchCommitFaultPoint());
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        await Assert.That(() => journal.TryCommitBatch(
        [
            Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)),
            Plan(1, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)),
        ])).ThrowsExactly<InvalidOperationException>();

        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(0);
        await Assert.That(journal.Read(StreamKey(), [first, second]).Entries).IsEmpty();
        await Assert.That(journal.StreamCount).IsEqualTo(0);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(0);
        await Assert.That(journal.EventCount).IsEqualTo(0);
        await Assert.That(journal.LogicalBytes).IsEqualTo(0);
    }

    /// <summary>Verifies empty and oversized groups are rejected before admission.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BatchedCommitsRejectInvalidGroupCounts()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        await Assert.That(() => journal.TryCommitBatch([])).ThrowsExactly<ArgumentOutOfRangeException>();
        var key = OperationKey(FirstOperationSeed);
        var plan = Plan(0, null, null, Entry(key, OperationResultKind.Accepted, FirstOperationSeed));
        await Assert.That(() => journal.TryCommitBatch(Enumerable.Repeat(plan, OversizedCommitGroupCount).ToArray())).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(0);
    }

    /// <summary>Injects a failure before the SQLite commit can become durable.</summary>
    private sealed class ThrowingBatchCommitFaultPoint : ISqliteServerCommitFaultPoint
    {
        /// <inheritdoc/>
        public void Reached(SqliteServerCommitCheckpoint checkpoint)
        {
            if (checkpoint == SqliteServerCommitCheckpoint.TryCommitBeforeCommit)
            {
                throw new InvalidOperationException("Injected grouped commit failure.");
            }
        }
    }
}
