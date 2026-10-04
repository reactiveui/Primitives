// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Verifies owned connection reuse, transaction cleanup and disposal.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The finite deadline for observing connection ownership on another thread.</summary>
    private static readonly TimeSpan ConnectionOwnershipDeadline = TimeSpan.FromSeconds(5);

    /// <summary>Verifies repeated operations reuse native preparations and release ownership on disposal.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OwnedConnectionReusesWarmStatementsAndClosesOnDisposal()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var committed = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        await Assert.That(committed.Status).IsEqualTo(ServerCommitStatus.Committed);
        _ = journal.Read(StreamKey(), [key]);
        _ = journal.StreamCount;
        var warmed = journal.PreparationCount;
        await Assert.That(warmed).IsGreaterThan(0);
        await Assert.That(journal.Read(StreamKey(), [key]).Revision).IsEqualTo(1);
        await Assert.That(journal.StreamCount).IsEqualTo(1);
        await Assert.That(journal.PreparationCount).IsEqualTo(warmed);
        await Assert.That(journal.IsConnectionDisposed).IsFalse();
        await journal.DisposeWorkersAsync();
        await Assert.That(journal.IsConnectionDisposed).IsTrue();
        await Assert.That(() => journal.Read(StreamKey(), [key])).ThrowsExactly<ObjectDisposedException>();
        await journal.DisposeWorkersAsync();
        using var reopened = CreateJournal(database.Path);
        await Assert.That(reopened.Read(StreamKey(), [key]).Revision).IsEqualTo(1);
    }

    /// <summary>Verifies rollback and rejected cancellation leave the owned connection ready for later commands.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OwnedConnectionRecoversAfterCommitFaultAndCanceledAdmission()
    {
        using var database = new TemporaryDatabase();
        using var journal = new SqliteServerCommitJournal(database.Path, null, new FirstCommitFaultPoint());
        var key = OperationKey(FirstOperationSeed);
        var plan = Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed));
        await Assert.That(() => journal.TryCommit(plan)).ThrowsExactly<InvalidOperationException>();
        var recovered = await Task.Run(() => journal.Read(StreamKey(), [key])).WaitAsync(ConnectionOwnershipDeadline);
        await Assert.That(recovered.Revision).IsEqualTo(0);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => journal.ExecuteAsync(() => journal.TryCommit(plan), cancellation.Token));
        var result = await journal.ExecuteAsync(() => journal.TryCommit(plan), CancellationToken.None);
        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(journal.Read(StreamKey(), [key]).Entries).Count().IsEqualTo(1);
    }

    /// <summary>Verifies rejected ownership after disposal does not hold the gate against another thread.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OwnedConnectionReleasesGateAfterDisposedAccess()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        await journal.DisposeWorkersAsync();
        await Assert.That(() => journal.PreparationCount).ThrowsExactly<ObjectDisposedException>();
        var disposed = await Task.Run(() => journal.IsConnectionDisposed).WaitAsync(ConnectionOwnershipDeadline);
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies independent workers cannot overlap transactions on their shared owned connection.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OwnedConnectionSerializesEffectAndAcknowledgementTransactions()
    {
        using var database = new TemporaryDatabase();
        using var release = new ManualResetEventSlim();
        var committing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var journal = new SqliteServerCommitJournal(database.Path, null, new ConnectionBarrierFaultPoint(committing, release));
        var key = OperationKey(FirstOperationSeed);
        var commit = journal.ExecuteAsync(
            () => journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed))),
            CancellationToken.None);
        try
        {
            await committing.Task;
            var reading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var read = journal.ExecuteAsync(
                () =>
                {
                    reading.SetResult();
                    return journal.Read(StreamKey(), [key]);
                },
                CancellationToken.None,
                acknowledgement: true);
            await reading.Task;
            await Assert.That(read.IsCompleted).IsFalse();
            release.Set();
            await Assert.That((await commit).Status).IsEqualTo(ServerCommitStatus.Committed);
            await Assert.That((await read).Revision).IsEqualTo(1);
        }
        finally
        {
            release.Set();
            _ = await commit;
        }
    }

    /// <summary>Blocks the effect transaction while a separately admitted read reaches the ownership gate.</summary>
    /// <param name="committing">The effect readiness signal.</param>
    /// <param name="release">The transaction release signal.</param>
    private sealed class ConnectionBarrierFaultPoint(TaskCompletionSource committing, ManualResetEventSlim release) : ISqliteServerCommitFaultPoint
    {
        /// <inheritdoc/>
        public void Reached(SqliteServerCommitCheckpoint checkpoint)
        {
            if (checkpoint != SqliteServerCommitCheckpoint.TryCommitBeforeCommit)
            {
                return;
            }

            committing.SetResult();
            release.Wait();
        }
    }

    /// <summary>Throws once after native row writes and before their first commit.</summary>
    private sealed class FirstCommitFaultPoint : ISqliteServerCommitFaultPoint
    {
        /// <summary>Whether the first transaction has reached its failure boundary.</summary>
        private bool _failed;

        /// <inheritdoc/>
        public void Reached(SqliteServerCommitCheckpoint checkpoint)
        {
            if (checkpoint != SqliteServerCommitCheckpoint.TryCommitBeforeCommit || _failed)
            {
                return;
            }

            _failed = true;
            throw new InvalidOperationException("The first native commit failed before its durable boundary.");
        }
    }
}
