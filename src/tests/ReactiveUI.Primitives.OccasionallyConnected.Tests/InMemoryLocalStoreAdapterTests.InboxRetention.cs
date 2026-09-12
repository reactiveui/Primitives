// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies inbox retention independently of terminal operation compaction.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The finite record capacity for one inbox row and its stream snapshot.</summary>
    private const int SingleInboxRecordCapacity = 4;

    /// <summary>The finite byte capacity for inbox retention tests.</summary>
    private const int InboxRetentionByteCapacity = 4096;

    /// <summary>Verifies unresolved intent protects its inbox while independent streams can reclaim entries.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InboxCompactionPreservesUnresolvedStreamAndHonorsStreamSelection()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(OtherStream, null, CancellationToken.None);
        var firstEvent = CreateRemoteEvent(RemoteCursor);
        var secondEvent = new RemoteEvent(Guid.NewGuid(), OtherStream, RemoteCursor, DateTimeOffset.MaxValue, null, CreatePayload(RemotePayloadText), new Dictionary<string, string>());
        _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(null, RemoteCursor, [firstEvent]), CreateSnapshotMutation(0), CancellationToken.None);
        var otherBatch = new RemoteEventBatch(Guid.NewGuid(), OtherStream, null, RemoteCursor, [secondEvent]);
        var otherMutation = CreateSnapshotMutation(0) with { StreamId = OtherStream };
        _ = await store.ApplyRemoteBatchAsync(otherBatch, otherMutation, CancellationToken.None);
        var pending = CreateOperation(1);
        _ = await store.CommitLocalOperationAsync(pending, CreateSnapshotMutation(1), CancellationToken.None);
        clock.Advance(new RetentionOptions().InboxDeduplicationRetention + TimeSpan.FromTicks(1));
        var selected = await store.CompactAsync(new(OtherStream, DateTimeOffset.MinValue, long.MaxValue), CancellationToken.None);
        await Assert.That(selected.RecordsRemoved).IsEqualTo(1);
        var protectedResult = await store.CompactAsync(new(null, DateTimeOffset.MaxValue, long.MaxValue), CancellationToken.None);
        await Assert.That(protectedResult.RecordsRemoved).IsEqualTo(0);
        await Assert.That((await store.GetUnappliedEventIdsAsync(Stream, [firstEvent.EventId], CancellationToken.None)).Count).IsEqualTo(0);
        await Assert.That((await store.GetUnappliedEventIdsAsync(OtherStream, [secondEvent.EventId], CancellationToken.None)).Count).IsEqualTo(1);
        await SetServerResultAsync(store, pending, OperationResultKind.Accepted);
        var settled = await store.CompactAsync(new(null, DateTimeOffset.MinValue, long.MaxValue), CancellationToken.None);
        await Assert.That(settled.RecordsRemoved).IsEqualTo(1);
    }

    /// <summary>Verifies inbox retention uses receipt time and preserves the current snapshot and cursor.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InboxRetentionUsesLocalReceiptTimeAndReclaimsCapacity()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var retention = new RetentionOptions { InboxDeduplicationRetention = TimeSpan.FromMinutes(1) };
        await using var store = new InMemoryLocalStoreAdapter(clock, SingleInboxRecordCapacity, InboxRetentionByteCapacity, retention);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var firstEvent = CreateRemoteEvent(RemoteCursor);
        _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(null, RemoteCursor, [firstEvent]), CreateSnapshotMutation(0), CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var secondEvent = CreateRemoteEvent("cursor-2");
        var secondBatch = CreateRemoteBatch(RemoteCursor, "cursor-2", [secondEvent]);
        Func<Task> full = async () => _ = await store.ApplyRemoteBatchAsync(secondBatch, CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That(full).ThrowsExactly<QueueCapacityExceededException>();
        clock.Advance(TimeSpan.FromMinutes(1));
        var boundary = await store.CompactAsync(new(Stream, DateTimeOffset.MinValue, long.MaxValue), CancellationToken.None);
        await Assert.That(boundary.RecordsRemoved).IsEqualTo(0);
        clock.Advance(TimeSpan.FromTicks(1));
        var compacted = await store.CompactAsync(new(Stream, DateTimeOffset.MinValue, long.MaxValue), CancellationToken.None);
        await Assert.That(compacted.RecordsRemoved).IsEqualTo(1);
        await Assert.That(compacted.BytesReclaimed).IsGreaterThan(0);
        var after = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(after.Snapshot).IsEqualTo(before.Snapshot);
        await Assert.That(after.ServerCursor).IsEqualTo(before.ServerCursor);
        await Assert.That(after.NextClientSequence).IsEqualTo(before.NextClientSequence);
        _ = await store.ApplyRemoteBatchAsync(secondBatch, CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That((await store.GetUnappliedEventIdsAsync(Stream, [firstEvent.EventId], CancellationToken.None)).Count).IsEqualTo(1);
        await Assert.That((await store.GetUnappliedEventIdsAsync(Stream, [secondEvent.EventId], CancellationToken.None)).Count).IsEqualTo(0);
        await Assert.That((await store.CompactAsync(new(Stream, DateTimeOffset.MaxValue, 0), CancellationToken.None)).RecordsRemoved).IsEqualTo(0);
    }
}
