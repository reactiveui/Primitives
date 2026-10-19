// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Verifies worker bounds, independent acknowledgement capacity and deterministic draining.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The queued command's rejection value.</summary>
    private const int RejectedCommandValue = 3;

    /// <summary>The replacement command's result value.</summary>
    private const int ReplacementCommandValue = 4;

    /// <summary>The independently admitted acknowledgement result value.</summary>
    private const int AcknowledgementCommandValue = 5;

    /// <summary>Verifies cancellation of dispatched native work preserves its durable commit result.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduledNativeCommitPreservesResultAfterDispatchCancellation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        using var cancellation = new CancellationTokenSource();
        using var writer = OpenRawConnection(database.Path);
        using var reservation = writer.BeginTransaction();
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var key = OperationKey(FirstOperationSeed);
        var pending = journal.ExecuteAsync(
            () =>
            {
                _ = started.TrySetResult(!Thread.CurrentThread.IsThreadPoolThread);
                return journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            }, cancellation.Token);
        await Assert.That(await started.Task).IsTrue();
        await cancellation.CancelAsync();
        reservation.Commit();

        var committed = await pending;
        await Assert.That(committed.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(journal.Read(StreamKey(), [key]).Entries).Count().IsEqualTo(1);
    }

    /// <summary>Verifies dedicated execution and queued cancellation release a bounded slot.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduledCommandsBoundAdmissionAndReleaseCanceledSlot()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        journal.ConfigureWorkerCapacity(DoubleEntryCount);
        using var release = new ManualResetEventSlim();
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = journal.ExecuteAsync(
            () =>
            {
                _ = started.TrySetResult(!Thread.CurrentThread.IsThreadPoolThread);
                release.Wait();
                return 1;
            }, CancellationToken.None);
        try
        {
            await Assert.That(await started.Task).IsTrue();
            using var cancellation = new CancellationTokenSource();
            var canceled = journal.ExecuteAsync(static () => DoubleEntryCount, cancellation.Token);
            await Assert.That(() => journal.ExecuteAsync(static () => RejectedCommandValue, CancellationToken.None)).ThrowsExactly<QueueCapacityExceededException>();
            await cancellation.CancelAsync();
            _ = await Assert.ThrowsAsync<OperationCanceledException>(() => canceled);
            var replacement = journal.ExecuteAsync(static () => ReplacementCommandValue, CancellationToken.None);
            var acknowledgement = await journal.ExecuteAsync(static () => AcknowledgementCommandValue, CancellationToken.None, acknowledgement: true);
            await Assert.That(acknowledgement).IsEqualTo(AcknowledgementCommandValue);
            release.Set();
            await Assert.That(await active).IsEqualTo(1);
            await Assert.That(await replacement).IsEqualTo(ReplacementCommandValue);
        }
        finally
        {
            release.Set();
            _ = await active;
        }
    }

    /// <summary>Verifies disposal rejects queued work and joins a dispatched command without losing its result.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduledCommandDisposalCancelsQueueAndDrainsActiveCommand()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        journal.ConfigureWorkerCapacity(DoubleEntryCount);
        using var release = new ManualResetEventSlim();
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = journal.ExecuteAsync(
            () =>
            {
                _ = started.TrySetResult(true);
                release.Wait();
                return 1;
            }, CancellationToken.None);
        try
        {
            _ = await started.Task;
            var queued = journal.ExecuteAsync<int>(static () => throw new InvalidOperationException("Queued work ran during disposal."), CancellationToken.None);
            var disposal = journal.DisposeWorkersAsync().AsTask();
            _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => queued);
            await Assert.That(disposal.IsCompleted).IsFalse();
            await Assert.That(() => journal.ExecuteAsync(static () => RejectedCommandValue, CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
            release.Set();
            await Assert.That(await active).IsEqualTo(1);
            await disposal;
            await journal.DisposeWorkersAsync();
        }
        finally
        {
            release.Set();
            _ = await active;
        }
    }
}
