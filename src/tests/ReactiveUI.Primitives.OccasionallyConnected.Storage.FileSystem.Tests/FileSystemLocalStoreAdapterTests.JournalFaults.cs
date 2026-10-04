// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests durable append failure and complete-record replay boundaries.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>Verifies failed rollback closes the adapter instead of exposing uncertain state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedAppendRollbackFailsClosedAndRecoversCommittedPrefix()
    {
        var directory = CreateJournalTestDirectory();
        var armed = false;
        try
        {
            var stream = new StreamId("rollback-failure");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, checkpoint =>
            {
                if (armed && checkpoint is FileSystemJournalCheckpoint.AfterAppendHeader or FileSystemJournalCheckpoint.BeforeAppendRollback)
                {
                    throw new IOException("Injected append or rollback failure.");
                }
            }))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, 1);
                armed = true;
                await Assert.That(() => CommitJournalRangeAsync(adapter, stream, 1, TwoPendingOperations)).Throws<AggregateException>();
                await Assert.That(() => IgnoreResultAsync(adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None)))
                    .Throws<InvalidOperationException>();
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(recovered.PendingOperations).HasSingleItem();
            await Assert.That(recovered.Snapshot!.Revision).IsEqualTo(1);
            await CommitJournalRangeAsync(reopened, stream, 1, TwoPendingOperations);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies both authoritative and projected payloads survive incremental snapshot replay.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AuthoritativeSnapshotSurvivesIncrementalReplay()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("authoritative-replay");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            var authoritative = CreatePayload("authoritative");
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    CreateOperation(stream, 0),
                    new(stream, CreatePayload(SnapshotPayload), 1) { AuthoritativeState = authoritative },
                    CancellationToken.None);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(recovered.Snapshot!.AuthoritativeState!.PayloadHash).IsEqualTo(authoritative.PayloadHash);
            await Assert.That(recovered.Snapshot.State.PayloadHash).IsEqualTo(CreatePayload(SnapshotPayload).PayloadHash);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies invalid remote decisions cannot consume a lease or partially update operation status.</summary>
    /// <param name="shape">The invalid result shape.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("count")]
    [Arguments("duplicate")]
    [Arguments("unknown")]
    public async Task InvalidSyncResultKeepsLeaseAndCommittedOperations(string shape)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("invalid-sync-result");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var first = CreateOperation(stream, 0);
            var second = CreateOperation(stream, 1);
            await adapter.CommitLocalOperationAsync(first, new(stream, CreatePayload(SnapshotPayload), 1), CancellationToken.None);
            await adapter.CommitLocalOperationAsync(second, new(stream, CreatePayload(SnapshotPayload), 1, 1), CancellationToken.None);
            await using var leases = adapter.LeasePendingOperationsAsync(
                new(stream, TwoPendingOperations, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await leases.MoveNextAsync()).IsTrue();
            var decisions = new List<OperationSyncResult> { new(first.OperationId, OperationResultKind.Accepted, null, null) };
            if (shape != "count")
            {
                decisions.Add(new(shape == "duplicate" ? first.OperationId : OperationId.New(), OperationResultKind.Accepted, null, null));
            }

            var length = new FileInfo(Path.Combine(directory, JournalFileName)).Length;
            await Assert.That(() => adapter.ApplySyncResultAsync(
                leases.Current.LeaseId,
                new(Guid.NewGuid(), decisions, null, null),
                CancellationToken.None).AsTask()).Throws<InvalidOperationException>();
            await Assert.That(new FileInfo(Path.Combine(directory, JournalFileName)).Length).IsEqualTo(length);
            await Assert.That((await adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None)).Snapshot!.Revision)
                .IsEqualTo(SecondSnapshotRevision);
            await AssertOperationStateAsync(adapter, first.OperationId, SyncOperationState.Uploading);
            var accepted = new RemoteSyncResult(
                Guid.NewGuid(),
                [
                    new(first.OperationId, OperationResultKind.Accepted, null, null),
                    new(second.OperationId, OperationResultKind.Accepted, null, null),
                ],
                null,
                null);
            await adapter.ApplySyncResultAsync(leases.Current.LeaseId, accepted, CancellationToken.None);
            await Assert.That((await adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None)).PendingOperations).IsEmpty();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
