// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
/// <content>Bounded result transaction admission tests.</content>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies oversized remote result sets are rejected before internal result lookup allocation.</summary>
    /// <param name="reconcile">Whether the snapshot reconciliation overload is called.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenRemoteResultCountExceedsCapacity_ThenLeaseAndStateRemainAvailable(bool reconcile)
    {
        const int RecordLimit = 32;
        const long ByteLimit = 16_384;
        await using var store = new InMemoryLocalStoreAdapter(RecordLimit, ByteLimit);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var accepted = new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion);
        var oversized = new RemoteSyncResult(lease.LeaseId, Enumerable.Repeat(accepted, RecordLimit + 1).ToArray(), null, null);
        Func<Task> apply = async () =>
        {
            if (reconcile)
            {
                _ = await store.ApplySyncResultAsync(lease.LeaseId, oversized, [], CancellationToken.None);
            }
            else
            {
                await store.ApplySyncResultAsync(lease.LeaseId, oversized, CancellationToken.None);
            }
        };

        await Assert.That(apply).ThrowsExactly<QueueCapacityExceededException>();

        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await store.ApplySyncResultAsync(lease.LeaseId, new(lease.LeaseId, [accepted], null, null), CancellationToken.None);
        status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies caller counts cannot allocate beyond the configured transient result budget.</summary>
    /// <param name="count">The caller-advertised count.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(-1)]
    [Arguments(int.MaxValue)]
    public async Task WhenResultCountExceedsBounds_ThenInputIsNotIndexed(int count)
    {
        await using var store = await CreateInitializedStoreAsync();
        var mutations = new ResultMutationList(count, static () => CreateSnapshotMutation(0));
        var leaseId = Guid.NewGuid();
        var result = new RemoteSyncResult(leaseId, [], null, null);
        Func<Task> apply = async () => await store.ApplySyncResultAsync(leaseId, result, mutations, CancellationToken.None);

        await Assert.That(apply).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(mutations.ReadCount).IsEqualTo(0);
    }

    /// <summary>Verifies oversized mutation payloads fail before changing any durable state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultPayloadExceedsBounds_ThenTheLeaseRemainsUsable()
    {
        const int RecordLimit = 100;
        const long ByteLimit = 4096;
        const int OversizedPayloadLength = 8192;
        await using var store = new InMemoryLocalStoreAdapter(RecordLimit, ByteLimit);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);
        var oversized = CreateSnapshotMutation(1, new('x', OversizedPayloadLength));
        Func<Task> apply = async () => await store.ApplySyncResultAsync(lease.LeaseId, result, [oversized], CancellationToken.None);

        await Assert.That(apply).ThrowsExactly<QueueCapacityExceededException>();

        var snapshots = await store.ApplySyncResultAsync(lease.LeaseId, result, [], CancellationToken.None);
        await Assert.That(snapshots.Count).IsEqualTo(0);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies blocked caller capture reserves capacity without preventing independent store reads.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenResultCaptureBlocks_ThenOtherCapturesAreBoundedAndReadsRemainAvailable()
    {
        const int TimeoutSeconds = 5;
        await using var store = await CreateInitializedStoreAsync();
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload(AuthoritativeInitialText) },
            CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var result = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Rejected, ResultRejectedReason, null)], null, null);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var mutations = new ResultMutationList(1, () =>
        {
            entered.Set();
            release.Wait();
            return CreateSnapshotMutation(1, AuthoritativeInitialText);
        });
        var commit = Task.Run(async () => await store.ApplySyncResultAsync(lease.LeaseId, result, mutations, CancellationToken.None));
        try
        {
            await Assert.That(entered.Wait(TimeSpan.FromSeconds(TimeoutSeconds))).IsTrue();
            var status = await Task.Run(async () => await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None))
                .WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
            var competing = new ResultMutationList(1, static () => CreateSnapshotMutation(1));
            Func<Task> apply = async () => await store.ApplySyncResultAsync(lease.LeaseId, result, competing, CancellationToken.None);
            await Assert.That(apply).ThrowsExactly<QueueCapacityExceededException>();
            await Assert.That(competing.ReadCount).IsEqualTo(0);
        }
        finally
        {
            release.Set();
            _ = await commit;
        }
    }

    /// <summary>A caller-owned list with observable indexing callbacks.</summary>
    /// <param name="count">The advertised count.</param>
    /// <param name="read">The callback supplying each mutation.</param>
    private sealed class ResultMutationList(int count, Func<SnapshotMutation> read) : IReadOnlyList<SnapshotMutation>
    {
        /// <inheritdoc/>
        public int Count => count;

        /// <summary>Gets the number of indexer calls.</summary>
        public int ReadCount { get; private set; }

        /// <inheritdoc/>
        public SnapshotMutation this[int index]
        {
            get
            {
                ReadCount++;
                return read();
            }
        }

        /// <inheritdoc/>
        public IEnumerator<SnapshotMutation> GetEnumerator()
        {
            for (var index = 0; index < Count; index++)
            {
                yield return this[index];
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
