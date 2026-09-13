// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Verifies retained data is bounded before reconstruction after configuration changes.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Verifies a smaller journal cannot reconstruct history exceeding its configured ledger bound.</summary>
    /// <param name="readOnly">Whether to request a replay instead of preparing another commit.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task SmallerReopenedJournalRejectsRetainedHistoryBeforeReconstruction(bool readOnly)
    {
        using var database = new TemporaryDatabase();
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        using var writer = CreateJournal(database.Path);
        await Assert.That(writer.TryCommit(Plan(0, null, null, Entry(first, OperationResultKind.Accepted, FirstOperationSeed))).Status)
            .IsEqualTo(ServerCommitStatus.Committed);
        using var bounded = CreateJournal(database.Path, maximumLedgerEntries: SingleEntryCount);
        await Assert.That(writer.TryCommit(Plan(SingleEntryCount, null, null, Entry(second, OperationResultKind.Accepted, SecondOperationSeed))).Status)
            .IsEqualTo(ServerCommitStatus.Committed);

        if (readOnly)
        {
            await Assert.That(() => bounded.Read(StreamKey(), [first])).ThrowsExactly<InvalidOperationException>();
        }
        else
        {
            var third = OperationKey(Client, ThirdOperationSeed);
            var plan = Plan(DoubleEntryCount, null, null, Entry(third, OperationResultKind.Accepted, ThirdOperationSeed));
            await Assert.That(() => bounded.TryCommit(plan)).ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(writer.LedgerEntryCount).IsEqualTo(DoubleEntryCount);
        await Assert.That(writer.Read(StreamKey(), [first, second]).Revision).IsEqualTo(DoubleEntryCount);
    }
}
