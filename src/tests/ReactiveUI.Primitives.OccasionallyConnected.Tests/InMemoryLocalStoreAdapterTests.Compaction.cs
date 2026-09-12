// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies compaction preserves required local intent and honors the retained budget.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies compaction orders multiple terminal records while preserving the last snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionRemovesMultipleEligibleOperations()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var first = await CommitOperationAsync(store, Stream, 1, "first");
        await SetServerResultAsync(store, first, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromTicks(1));
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        await SetServerResultAsync(store, second, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None)).IsNull();
        await Assert.That(await store.GetOperationStatusAsync(second.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies an already satisfied byte target does not delete retained terminal history.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionDoesNotDeleteWhenAlreadyBelowTarget()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        await SetServerResultAsync(store, operation, OperationResultKind.Rejected);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), long.MaxValue), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNotNull();
    }

    /// <summary>Verifies conflicts protect the stream's retained operation history.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionPreservesHistoryWhileStreamHasUnresolvedConflict()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var first = await CommitOperationAsync(store, Stream, 1, "first");
        await SetServerResultAsync(store, first, OperationResultKind.Accepted);
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        await SetServerResultAsync(store, second, OperationResultKind.Conflict);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 1), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        await Assert.That(await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None)).IsNotNull();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies a zero target reclaims eligible records while preserving the current snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ZeroCompactionTargetPreservesSnapshotAndSequence()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        await SetServerResultAsync(store, operation, OperationResultKind.Rejected);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        var after = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(after.Snapshot).IsEqualTo(before.Snapshot);
        await Assert.That(after.NextClientSequence).IsEqualTo(before.NextClientSequence);
        var again = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);
        await Assert.That(again.RecordsRemoved).IsEqualTo(0);
    }

    /// <summary>Applies a correlated server outcome to one pending operation.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="kind">The outcome.</param>
    /// <returns>The asynchronous test setup.</returns>
    private static async Task SetServerResultAsync(InMemoryLocalStoreAdapter store, SyncOperation operation, OperationResultKind kind)
    {
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(operation.StreamId, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(lease.LeaseId, new(lease.LeaseId, [new(operation.OperationId, kind, null, null)], null, null), CancellationToken.None);
    }
}
