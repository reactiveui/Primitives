// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for the durable local commit store.</summary>
/// <content>Bounded metadata query counts during recovery.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The pending, replay, and dead-letter operation sets each use one metadata query.</summary>
    private const int RecoveryMetadataQueryCount = 3;

    /// <summary>Checks bounded lease materialization batches metadata for actual selected members.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The commit connection or expected lease was unavailable.</exception>
    [Test]
    public async Task LeasedMetadataUsesOneQueryRegardlessOfSelectedOperationCount()
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, OutboxCapacityBytes, capture);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var connection = capture.Connection ?? throw new InvalidOperationException("The commit connection was not captured.");
        var lease = store.LeasePendingOperationBatch(new(Stream, 1, OutboxCapacityBytes, TimeSpan.FromMinutes(1)), CancellationToken.None)
            ?? throw new InvalidOperationException("Expected a lease.");
        var firstQueries = CountLeaseMetadataQueries(connection, lease.LeaseId);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        for (var sequence = SecondClientSequence; sequence <= AccountingHistoryCount; sequence++)
        {
            _ = store.CommitLocalOperation(CreateOperation(clientSequence: sequence), CreateSnapshotMutation(expectedRevision: sequence - 1), CancellationToken.None);
        }

        const long BatchBytes = (long)OutboxCapacityBytes * AccountingHistoryCount;
        var larger = store.LeasePendingOperationBatch(new(Stream, AccountingHistoryCount, BatchBytes, TimeSpan.FromMinutes(1)), CancellationToken.None)
            ?? throw new InvalidOperationException("Expected a larger lease.");
        await Assert.That(larger.Operations.Count).IsEqualTo(AccountingHistoryCount);
        await Assert.That(firstQueries).IsEqualTo(1);
        await Assert.That(CountLeaseMetadataQueries(connection, larger.LeaseId)).IsEqualTo(firstQueries);
        await Assert.That(larger.Operations[^1].Metadata[MetadataOriginKey]).IsEqualTo(UnitTestOrigin);
    }

    /// <summary>Checks actual native query executions stay fixed as recovered operation count grows.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The actual commit connection was not captured.</exception>
    [Test]
    public async Task RecoveryMetadataUsesFixedQueriesInsteadOfOneQueryPerOperation()
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, OutboxCapacityBytes, capture);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var connection = capture.Connection ?? throw new InvalidOperationException("The commit connection was not captured.");
        var firstQueries = CountRecoveryMetadataQueries(connection, store, subscription);
        for (var sequence = SecondClientSequence; sequence <= AccountingHistoryCount; sequence++)
        {
            _ = store.CommitLocalOperation(CreateOperation(clientSequence: sequence), CreateSnapshotMutation(expectedRevision: sequence - 1), CancellationToken.None);
        }

        await Assert.That(firstQueries).IsEqualTo(RecoveryMetadataQueryCount);
        var retainedHistoryQueries = CountRecoveryMetadataQueries(connection, store, subscription);
        await Assert.That(retainedHistoryQueries).IsEqualTo(firstQueries);
        await TestContext.Current!.OutputWriter.WriteLineAsync($"Executed metadata SELECTs: one operation = {firstQueries}; {AccountingHistoryCount} operations = {retainedHistoryQueries}.");
        var recovery = store.RecoverStream(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(AccountingHistoryCount);
        await Assert.That(recovery.PendingOperations[^1].Metadata[MetadataOriginKey]).IsEqualTo(UnitTestOrigin);
    }

    /// <summary>Counts executed native metadata SELECT statements, not preparations or cached statements.</summary>
    /// <param name="connection">The actual store-owned connection.</param>
    /// <param name="store">The local store.</param>
    /// <param name="subscription">The stream subscription.</param>
    /// <returns>The executed metadata query count.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static int CountRecoveryMetadataQueries(SqliteDatabase connection, SqliteLocalCommitStore store, SubscriptionId subscription) =>
        CountMetadataQueries(connection, () => _ = store.RecoverStream(Stream, subscription, CancellationToken.None));

    /// <summary>Counts metadata SELECTs while materializing a bounded persisted lease.</summary>
    /// <param name="connection">The store connection.</param>
    /// <param name="leaseId">The selected lease.</param>
    /// <returns>The native executed query count.</returns>
    private static int CountLeaseMetadataQueries(SqliteDatabase connection, Guid leaseId)
    {
        using var transaction = connection.BeginTransaction(deferred: true);
        return CountMetadataQueries(connection, () => _ = SqliteLocalCommitSql.ReadLeasedOperations(connection, transaction, StoreIdentity, leaseId, OutboxCapacityBytes));
    }

    /// <summary>Counts actual native metadata statement executions around one read action.</summary>
    /// <param name="connection">The actual store connection.</param>
    /// <param name="read">The read operation.</param>
    /// <returns>The native executed query count.</returns>
    private static int CountMetadataQueries(SqliteDatabase connection, Action read)
    {
        var queries = 0;
        raw.sqlite3_trace(
            connection.Handle,
            (strdelegate_trace)((_, sql) =>
        {
            if (sql.TrimStart().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("oc_outbox_metadata", StringComparison.Ordinal))
            {
                queries++;
            }
            }),
            null);
        try
        {
            read();
            return queries;
        }
        finally
        {
            raw.sqlite3_trace(connection.Handle, (strdelegate_trace)null!, null);
        }
    }
}
