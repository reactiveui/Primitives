// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies transactional state and retention in the in-memory store.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies an application clock cannot block independent state reads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task BlockedApplicationClockDoesNotHoldStoreGate()
    {
        const int TimeoutSeconds = 5;
        using var clock = new BlockingTimeProvider();
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "first");
        clock.Block = true;
        var commit = Task.Run(async () => await CommitOperationAsync(store, Stream, SecondClientSequence, "second"));
        try
        {
            await Assert.That(clock.Entered.Wait(TimeSpan.FromSeconds(TimeoutSeconds))).IsTrue();
            var status = await Task.Run(async () => await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None))
                .WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
            await Assert.That(status?.OperationId).IsEqualTo(operation.OperationId);
        }
        finally
        {
            clock.Release();
            _ = await commit;
        }
    }

    /// <summary>Verifies upload acknowledgements cannot skip unapplied remote events.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UploadAcknowledgementPreservesReceiveCursorUntilRemoteApply()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], RemoteCursor, null),
            CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.ServerCursor).IsNull();

        var remoteEvent = CreateRemoteEvent(RemoteCursor);
        _ = await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [remoteEvent]),
            CreateSnapshotMutation(1),
            CancellationToken.None);
        recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(recovered.Snapshot?.ServerCursor).IsEqualTo(RemoteCursor);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Verifies ephemeral storage never advertises a durable inbox.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task EphemeralStorageDoesNotAdvertiseDurableInbox()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await Assert.That((store.Capabilities & LocalStoreCapabilities.DurableInbox) != 0).IsFalse();
    }

    /// <summary>Verifies a caller cutoff cannot bypass configured minimum retention.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionHonorsConfiguredTerminalRetention()
    {
        const int RetentionDays = 30;
        const int RecordCapacity = 100;
        const long ByteCapacity = 4096;
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new ManualTimeProvider(now);
        await using var store = new InMemoryLocalStoreAdapter(
            clock,
            RecordCapacity,
            ByteCapacity,
            new RetentionOptions { OutboxTerminalRetention = TimeSpan.FromDays(RetentionDays) });
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));
        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 1), CancellationToken.None);
        await Assert.That(result.RecordsRemoved).IsEqualTo(0);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>A clock whose callback can be held while another thread accesses the store.</summary>
    private sealed class BlockingTimeProvider : TimeProvider, IDisposable
    {
        /// <summary>The callback release signal.</summary>
        private readonly ManualResetEventSlim _release = new();

        /// <summary>Gets the callback entry signal.</summary>
        public ManualResetEventSlim Entered { get; } = new();

        /// <summary>Gets or sets whether the callback waits.</summary>
        public bool Block { get; set; }

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            if (Block)
            {
                Entered.Set();
                _release.Wait();
            }

            return new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        }

        /// <summary>Releases the callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release() => _release.Set();

        /// <inheritdoc/>
        public void Dispose()
        {
            Entered.Dispose();
            _release.Dispose();
        }
    }
}
