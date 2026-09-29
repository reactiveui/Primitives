// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Bounded durable queue soak and release performance measurements.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Offline operation count.</summary>
    private const int SoakOperationCount = 512;

    /// <summary>Maximum reconnect batch size.</summary>
    private const int SoakBatchSize = 32;

    /// <summary>The expiry offset used when compacting acknowledged records.</summary>
    private const int SoakCompactionCutoffYears = 100;

    /// <summary>The start year offset for offline operations.</summary>
    private const int SoakStartYears = 56;

    /// <summary>Minimum durable commit throughput on a hosted CI runner.</summary>
    private const double MinimumCommitsPerSecond = 10;

    /// <summary>Maximum allocation per durable commit, including test input.</summary>
    private const long MaximumAllocatedBytesPerCommit = 512 * 1024;

    /// <summary>Maximum stream recovery duration.</summary>
    private static readonly TimeSpan MaximumRecoveryTime = TimeSpan.FromSeconds(15);

    /// <summary>Maximum compaction duration.</summary>
    private static readonly TimeSpan MaximumCompactionTime = TimeSpan.FromSeconds(15);

    /// <summary>Checks a large offline outbox survives restart and drains after reconnection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OfflineOutboxSoakRecoversDrainsAndCompacts() => _ = await RunDurableSoakAsync();

    /// <summary>Checks throughput, allocation, recovery, and compaction release budgets.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [Property("Category", "Performance")]
    [NotInParallel]
    public async Task DurableOutboxPerformanceStaysWithinReleaseBudgets()
    {
        var (commitsPerSecond, bytesPerCommit, recoveryTime, compactionTime) = await RunDurableSoakAsync();
        await Assert.That(commitsPerSecond).IsGreaterThanOrEqualTo(MinimumCommitsPerSecond);
        await Assert.That(bytesPerCommit).IsLessThanOrEqualTo(MaximumAllocatedBytesPerCommit);
        await Assert.That(recoveryTime).IsLessThanOrEqualTo(MaximumRecoveryTime);
        await Assert.That(compactionTime).IsLessThanOrEqualTo(MaximumCompactionTime);
    }

    /// <summary>Runs the durable queue cycle and returns its four performance measures.</summary>
    /// <returns>Commit throughput, allocation, recovery time, and compaction time.</returns>
    private static async Task<(double CommitsPerSecond, long BytesPerCommit, TimeSpan RecoveryTime, TimeSpan CompactionTime)> RunDurableSoakAsync()
    {
        using var database = TempDatabase.Create();
        var clock = new SoakTimeProvider(DateTimeOffset.UnixEpoch.AddYears(SoakStartYears));
        var (subscriptionId, commitsPerSecond, bytesPerCommit) = await FillOfflineOutboxAsync(database.Path, clock);
        await using var reconnected = new SqliteLocalStoreAdapter(database.Path, new() { TimeProvider = clock });
        await reconnected.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);

        var recoveryStart = Stopwatch.GetTimestamp();
        var recovered = await reconnected.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var recoveryTime = Stopwatch.GetElapsedTime(recoveryStart);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(SoakOperationCount);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SoakOperationCount + 1);

        await DrainReconnectedOutboxAsync(reconnected);
        var drained = await reconnected.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        clock.AdvanceTo(DateTimeOffset.UnixEpoch.AddYears(SoakCompactionCutoffYears));
        var compactionStart = Stopwatch.GetTimestamp();
        var compacted = await reconnected.CompactAsync(
            new(Stream, DateTimeOffset.UnixEpoch.AddYears(SoakCompactionCutoffYears), TargetBytes: 0),
            CancellationToken.None);
        var compactionTime = Stopwatch.GetElapsedTime(compactionStart);

        TestContext.Current?.Output.WriteLine($"soak.sqlite operations={SoakOperationCount} commits_per_second={commitsPerSecond:F1} "
            + $"allocated_bytes_per_commit={bytesPerCommit} recovery_ms={recoveryTime.TotalMilliseconds:F1} "
            + $"compaction_ms={compactionTime.TotalMilliseconds:F1} records_removed={compacted.RecordsRemoved}");
        await Assert.That(drained.PendingOperations).IsEmpty();
        await Assert.That(compacted.RecordsRemoved).IsGreaterThan(0);
        return (commitsPerSecond, bytesPerCommit, recoveryTime, compactionTime);
    }

    /// <summary>Commits a bounded offline queue and measures its cost.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="clock">The controlled store clock.</param>
    /// <returns>The subscription and measured throughput and allocation.</returns>
    private static async Task<(SubscriptionId SubscriptionId, double CommitsPerSecond, long BytesPerCommit)> FillOfflineOutboxAsync(
        string path,
        SoakTimeProvider clock)
    {
        await using var offline = new SqliteLocalStoreAdapter(path, new() { TimeProvider = clock });
        await offline.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await offline.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var allocationStart = GC.GetTotalAllocatedBytes(precise: true);
        var commitStart = Stopwatch.GetTimestamp();
        for (var index = 1; index <= SoakOperationCount; index++)
        {
            var result = await offline.CommitLocalOperationAsync(
                CreateOperation(index),
                CreateSnapshotMutation(index - 1),
                CancellationToken.None);
            await Assert.That(result.ClientSequence).IsEqualTo(index);
        }

        var duration = Stopwatch.GetElapsedTime(commitStart);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocationStart;
        return (subscriptionId, SoakOperationCount / duration.TotalSeconds, allocated / SoakOperationCount);
    }

    /// <summary>Drains every offline operation in bounded reconnect batches.</summary>
    /// <param name="store">The reopened store.</param>
    /// <returns>The drain task.</returns>
    private static async Task DrainReconnectedOutboxAsync(SqliteLocalStoreAdapter store)
    {
        string? previousCursor = null;
        var revision = SoakOperationCount;
        for (var acknowledged = 0; acknowledged < SoakOperationCount;)
        {
            var lease = await ReadSingleLeaseAsync(
                store,
                new(Stream, SoakBatchSize, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
            var results = lease.Operations
                .Select(static operation => new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion))
                .ToArray();
            await store.ApplySyncResultAsync(
                lease.LeaseId,
                new(lease.LeaseId, results, null, null),
                CancellationToken.None);
            var nextCursor = $"soak-cursor-{acknowledged + results.Length}";
            var completions = lease.Operations
                .Select(static operation => new RemoteOperationCompletion(new(ClientId, operation.OperationId), []))
                .ToArray();
            var batch = CreateRemoteBatch(previousCursor, nextCursor, []) with { CompletedOperations = completions };
            _ = await store.ApplyRemoteBatchAsync(
                batch,
                CreateSnapshotMutation(revision) with { AuthoritativeState = CreatePayload("authoritative") },
                CancellationToken.None);
            revision++;
            previousCursor = nextCursor;
            acknowledged += results.Length;
        }
    }

    /// <summary>A controlled clock for retention aging between reconnect and compaction.</summary>
    /// <param name="utcNow">The initial timestamp.</param>
    private sealed class SoakTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC timestamp.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Moves the clock forward after the reconnect drain.</summary>
        /// <param name="timestamp">The later timestamp.</param>
        public void AdvanceTo(DateTimeOffset timestamp) => _utcNow = timestamp;
    }
}
