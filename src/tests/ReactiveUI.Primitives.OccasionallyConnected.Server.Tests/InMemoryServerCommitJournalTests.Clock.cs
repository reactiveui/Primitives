// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests clock callbacks and concurrent retention advancement.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Verifies a blocked clock does not hold the journal gate or overwrite a newer retention watermark.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BlockedClockAllowsCompactionAndCommitUsesLatestRetentionWatermark()
    {
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new BlockingCommitClock(entered, release);
        var retention = TimeSpan.FromMinutes(DefaultRetentionMinutes);
        var journal = new InMemoryServerCommitJournal(new() { TimeProvider = clock, OperationRetention = retention });
        var key = OperationKey(FirstOperationSeed);
        var plan = Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed));
        var pendingCommit = Task.Run(() => journal.TryCommit(plan));
        var later = Start + retention;
        try
        {
            await entered.Task.WaitAsync(GuardTimeout);
            var compacted = await Task.Run(() => journal.Compact(later)).WaitAsync(GuardTimeout);
            await Assert.That(compacted).IsEqualTo(0);
        }
        finally
        {
            release.Set();
        }

        var committed = await pendingCommit.WaitAsync(GuardTimeout);
        await Assert.That(committed.Status).IsEqualTo(ServerCommitStatus.Committed);
        var snapshot = journal.Read(StreamKey(), [key]);
        await Assert.That(snapshot.Entries.Count).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].CommittedAtUtc).IsEqualTo(later);
        await Assert.That(snapshot.Entries[0].ExpiresAtUtc).IsEqualTo(later + retention);
        await Assert.That(journal.Compact(later + retention)).IsEqualTo(0);
        await Assert.That(journal.Compact((later + retention).AddTicks(1))).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Blocks the sampled commit timestamp until compaction has advanced the journal.</summary>
    private sealed class BlockingCommitClock : TimeProvider
    {
        /// <summary>Signals entry to the caller-provided clock.</summary>
        private readonly TaskCompletionSource _entered;

        /// <summary>Allows the clock callback to finish.</summary>
        private readonly ManualResetEventSlim _release;

        /// <summary>Initializes a new instance of the <see cref="BlockingCommitClock"/> class.</summary>
        /// <param name="entered">The clock-entry signal.</param>
        /// <param name="release">The clock-release signal.</param>
        internal BlockingCommitClock(TaskCompletionSource entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        /// <inheritdoc/>
        /// <exception cref="TimeoutException">The test did not release its clock callback.</exception>
        public override DateTimeOffset GetUtcNow()
        {
            _entered.SetResult();
            if (!_release.Wait(GuardTimeout))
            {
                throw new TimeoutException("The commit clock was not released by the test.");
            }

            return Start;
        }
    }
}
