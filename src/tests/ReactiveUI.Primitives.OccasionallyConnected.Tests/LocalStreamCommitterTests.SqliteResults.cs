// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests reconciliation with pending noninvertible application mutations.</summary>
/// <content>Tests the committer against durable SQLite transactions across separate adapter lifetimes.</content>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>Verifies a replacement edit rejection survives reopening before and after the upload decision.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRestoresReplacementEditAcrossSqliteReopens()
    {
        var directory = SqliteTestDirectory.Create("oc-result-application-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var batch = await SeedSqliteResultAsync(databasePath);
            await ReconcileSqliteResultAsync(databasePath, batch);
            await using var reopened = new SqliteLocalStoreAdapter(databasePath);
            var subscription = await InitializeResultStoreAsync(reopened);
            var committer = CreateLocalCommitter(CreateResultOptions(reopened, subscription, new ReplacementProjection()));
            var state = await committer.RecoverAsync(CancellationToken.None);
            var recovery = await reopened.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

            await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
            await Assert.That(state.Revision).IsEqualTo(ReconciledResultRevision);
            await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
            await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
            await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(batch.Operations[0].OperationId);
            var rejected = await reopened.GetOperationStatusAsync(batch.Operations[1].OperationId, CancellationToken.None);
            await Assert.That(rejected?.State).IsEqualTo(SyncOperationState.Rejected);
            await Assert.That(state.ServerCursor).IsNull();
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Commits two durable replacement edits and leaves their upload lease available after shutdown.</summary>
    /// <param name="databasePath">The temporary application database.</param>
    /// <returns>The original upload batch.</returns>
    private static async Task<SyncBatch> SeedSqliteResultAsync(string databasePath)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        var subscription = await InitializeResultStoreAsync(store);
        var committer = CreateLocalCommitter(CreateResultOptions(store, subscription, new ReplacementProjection()));
        _ = await committer.RecoverAsync(CancellationToken.None);
        _ = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        _ = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);
        return new(lease.LeaseId, lease.Operations);
    }

    /// <summary>Reopens the application and atomically applies the mixed server decision.</summary>
    /// <param name="databasePath">The application database.</param>
    /// <param name="batch">The persisted lease and its original operations.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task ReconcileSqliteResultAsync(string databasePath, SyncBatch batch)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        var subscription = await InitializeResultStoreAsync(store);
        var committer = CreateLocalCommitter(CreateResultOptions(store, subscription, new ReplacementProjection()));
        var recovered = await committer.RecoverAsync(CancellationToken.None);
        await Assert.That(recovered.State.Sum).IsEqualTo(SecondReadingValue);
        var result = CreateRejectedSecondResult(batch.BatchId, batch.Operations[0].OperationId, batch.Operations[1].OperationId);
        var committed = await committer.ApplySyncResultAsync(batch, result, CancellationToken.None);
        await Assert.That(committed.State.Sum).IsEqualTo(FirstReadingValue);
    }
}
