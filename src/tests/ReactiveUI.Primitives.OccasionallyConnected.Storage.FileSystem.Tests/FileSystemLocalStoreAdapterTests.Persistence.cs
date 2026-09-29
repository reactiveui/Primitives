// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Persistence and lease tests for the FileSystem store.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>Verifies a complete record with a bad checksum is treated as corruption.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CorruptCompleteRecordFailsRecovery()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("sensor/corrupt");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                _ = await adapter.CommitLocalOperationAsync(
                    CreateOperation(stream, FirstOperationSequence),
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);
            }

            var journalPath = Path.Combine(directory, JournalFileName);
            var bytes = await File.ReadAllBytesAsync(journalPath);
            bytes[^1] ^= 0xFF;
            await File.WriteAllBytesAsync(journalPath, bytes);

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await Assert.That(() => reopened.InitializeAsync(initialization, CancellationToken.None).AsTask())
                .Throws<InvalidDataException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies a lease never exceeds the requested payload byte limit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LeaseSkipsOperationThatExceedsMaximumBytes()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("sensor/lease-size");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(
                CreateOperation(stream, FirstOperationSequence),
                new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                CancellationToken.None);

            await using var batches = adapter.LeasePendingOperationsAsync(
                new(stream, NextOperationSequence, UndersizedLeaseByteLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await batches.MoveNextAsync()).IsFalse();

            var recovered = await adapter.RecoverStreamAsync(
                stream,
                subscriptionId,
                CancellationToken.None);
            await Assert.That(recovered.PendingOperations).HasSingleItem();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies lease renewal prevents a batch from being leased again after its original expiry.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RenewedLeaseRemainsExclusivePastOriginalExpiry()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("sensor/lease-renewal");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            var leaseId = Guid.Empty;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                _ = await adapter.CommitLocalOperationAsync(
                    CreateOperation(stream, FirstOperationSequence),
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);

                await using var leaseEnumerator = adapter.LeasePendingOperationsAsync(
                    new(stream, NextOperationSequence, StandardLeaseByteLimit, OriginalLeaseDuration),
                    CancellationToken.None).GetAsyncEnumerator();
                if (await leaseEnumerator.MoveNextAsync())
                {
                    leaseId = leaseEnumerator.Current.LeaseId;
                }

                await Assert.That(leaseId).IsNotEqualTo(Guid.Empty);
                await adapter.RenewLeaseAsync(leaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
            }

            await Task.Delay(OriginalLeaseExpiryDelay);

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            await using var batches = reopened.LeasePendingOperationsAsync(
                new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await batches.MoveNextAsync()).IsFalse();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies unsupported encryption is rejected explicitly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncryptionRequirementIsRejectedRatherThanAdvertised()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await Assert.That(() => adapter.InitializeAsync(
                new(TestClientId, 1, true),
                CancellationToken.None).AsTask()).Throws<NotSupportedException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies subscription identity, lease ownership, and accepted-result transitions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionLeaseAndResultWorkflow()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("workflow");
            var operation = CreateOperation(stream, FirstOperationSequence);
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscription = new SubscriptionId(Guid.NewGuid());
            await Assert.That(await adapter.GetOrCreateSubscriptionIdAsync(stream, subscription, CancellationToken.None))
                .IsEqualTo(subscription);
            await adapter.CommitLocalOperationAsync(
                operation,
                new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                CancellationToken.None);

            await foreach (var lease in adapter.LeasePendingOperationsAsync(
                new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None))
            {
                await adapter.ApplySyncResultAsync(
                    lease.LeaseId,
                    new(
                        Guid.NewGuid(),
                        [new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, null)],
                        null,
                        null),
                    CancellationToken.None);
            }

            var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status).IsNotNull();
            await Assert.That(status!.State).IsEqualTo(SyncOperationState.Synchronized);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies an accepted terminal operation state survives reopening the store.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AcceptedOperationResultSurvivesReopen()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("accepted-restart");
            var operation = CreateOperation(stream, FirstOperationSequence);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);

                await foreach (var lease in adapter.LeasePendingOperationsAsync(
                    new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                    CancellationToken.None))
                {
                    await adapter.ApplySyncResultAsync(
                        lease.LeaseId,
                        new(
                            Guid.NewGuid(),
                            [new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, null)],
                            null,
                            null),
                        CancellationToken.None);
                }
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var status = await reopened.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
            var recovered = await reopened.RecoverStreamAsync(
                stream,
                await reopened.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None),
                CancellationToken.None);
            await Assert.That(recovered.PendingOperations).IsEmpty();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies releasing a lease allows the operation to be leased after store recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReleasedLeaseAllowsRetryAfterReopen()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("released-restart");
            var operation = CreateOperation(stream, FirstOperationSequence);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            var leaseId = Guid.Empty;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);
                await using var leaseEnumerator = adapter.LeasePendingOperationsAsync(
                    new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                    CancellationToken.None).GetAsyncEnumerator();
                if (await leaseEnumerator.MoveNextAsync())
                {
                    leaseId = leaseEnumerator.Current.LeaseId;
                }

                await Assert.That(leaseId).IsNotEqualTo(Guid.Empty);
                await adapter.ReleaseLeaseAsync(leaseId, CancellationToken.None);
                await using var available = adapter.LeasePendingOperationsAsync(
                    new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                    CancellationToken.None).GetAsyncEnumerator();
                await Assert.That(await available.MoveNextAsync()).IsTrue();
                await adapter.ReleaseLeaseAsync(available.Current.LeaseId, CancellationToken.None);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var subscriptionId = await reopened.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
            await Assert.That(recovered.PendingOperations).HasSingleItem();
            await Assert.That(recovered.PendingOperations[0].StreamId).IsEqualTo(stream);
            var status = await reopened.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
            await using var batches = reopened.LeasePendingOperationsAsync(
                new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await batches.MoveNextAsync()).IsTrue();
            await Assert.That(batches.Current.Operations).HasSingleItem();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies cancellation before a durable commit leaves no partial operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledCommitDoesNotChangeDurableState()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("canceled-commit");
            var operation = CreateOperation(stream, FirstOperationSequence);
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            using var cancellation = new CancellationTokenSource();
            await cancellation.CancelAsync();

            await Assert.That(async () =>
            {
                _ = await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    cancellation.Token);
            }).Throws<OperationCanceledException>();

            var recovered = await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
            await Assert.That(recovered.PendingOperations).IsEmpty();
            await Assert.That(recovered.Snapshot).IsNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies cancellation after a journal header write rolls back the partial record.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledCommitDuringAppendDoesNotChangeDurableState()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        var cancelNextAppend = false;
        using var cancellation = new CancellationTokenSource();
        Action<FileSystemJournalCheckpoint> cancelAfterHeader = checkpoint =>
        {
            if (cancelNextAppend && checkpoint == FileSystemJournalCheckpoint.AfterAppendHeader)
            {
                cancelNextAppend = false;
                cancellation.Cancel();
            }
        };

        try
        {
            var stream = new StreamId("canceled-during-append");
            var operation = CreateOperation(stream, FirstOperationSequence);
            await using var adapter = new FileSystemLocalStoreAdapter(directory, cancelAfterHeader);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            cancelNextAppend = true;

            await Assert.That(async () =>
            {
                _ = await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    cancellation.Token);
            }).Throws<OperationCanceledException>();

            var recovered = await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
            await Assert.That(recovered.PendingOperations).IsEmpty();
            await Assert.That(recovered.Snapshot).IsNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies compaction preserves pending replay until authoritative receive inclusion.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompactionRewritesJournalAndRetainsUnincludedReplay()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("compaction");
            var operation = CreateOperation(stream, FirstOperationSequence);
            var initialization = new LocalStoreInitialization("store", 1, false) { ClientId = TestClientId };
            var subscriptionId = await PrepareCompactionStateAsync(directory, stream, operation, initialization);

            await using (var reopened = new FileSystemLocalStoreAdapter(directory))
            {
                await reopened.InitializeAsync(initialization, CancellationToken.None);
                var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
                await Assert.That(recovered.PendingOperations).IsEmpty();
                await Assert.That(recovered.ReplayOperations).HasSingleItem();
                await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
                await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
                await Assert.That(recovered.ServerCursor).IsNull();

                var batch = new RemoteEventBatch(Guid.NewGuid(), stream, null, TestCursor, []) { CompletedOperations = [new(new RemoteEventOrigin(TestClientId, operation.OperationId), [])], };
                await reopened.ApplyRemoteBatchAsync(
                    batch,
                    new(stream, CreatePayload("remote"), 1, 1),
                    CancellationToken.None);
                var included = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
                await Assert.That(included.ReplayOperations).IsEmpty();

                var secondCompaction = await reopened.CompactAsync(
                    new(stream, DateTimeOffset.MaxValue, NoCompactionRecords),
                    CancellationToken.None);
                await Assert.That(secondCompaction.RecordsRemoved).IsEqualTo(OneCompactedRecord);
                await Assert.That(secondCompaction.BytesReclaimed).IsGreaterThan(0);
            }

            await using var compacted = new FileSystemLocalStoreAdapter(directory);
            await compacted.InitializeAsync(initialization, CancellationToken.None);
            var afterCompaction = await compacted.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
            await Assert.That(afterCompaction.PendingOperations).IsEmpty();
            await Assert.That(afterCompaction.ReplayOperations).IsEmpty();
            await Assert.That(await compacted.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies leases, retry state, and inbox deduplication survive reopening.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LeaseRetryAndInboxStateSurviveReopen()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("restart");
            var operation = CreateOperation(stream, FirstOperationSequence);
            var eventId = Guid.NewGuid();
            var initialization = new LocalStoreInitialization("store", 1, false) { ClientId = TestClientId };
            var (subscriptionId, retryState) = await SeedRetryInboxStateAsync(
                directory,
                stream,
                operation,
                eventId,
                initialization);

            await using (var reopened = new FileSystemLocalStoreAdapter(directory))
            {
                await reopened.InitializeAsync(initialization, CancellationToken.None);
                var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
                await Assert.That(recovered.ServerCursor).IsEqualTo(TestCursor);
                await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondSnapshotRevision);
                await Assert.That(await reopened.GetRetryStateAsync(operation.OperationId, CancellationToken.None))
                    .IsEqualTo(retryState);

                var unapplied = await reopened.GetUnappliedEventIdsAsync(stream, [eventId], CancellationToken.None);
                await Assert.That(unapplied).IsEmpty();
                await using var blocked = reopened.LeasePendingOperationsAsync(
                    new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
                    CancellationToken.None).GetAsyncEnumerator();
                await Assert.That(await blocked.MoveNextAsync()).IsFalse();
            }

            await using var recoveredAdapter = new FileSystemLocalStoreAdapter(directory);
            await recoveredAdapter.InitializeAsync(initialization, CancellationToken.None);
            var status = await recoveredAdapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.Uploading);
            await Assert.That(await recoveredAdapter.GetRetryStateAsync(operation.OperationId, CancellationToken.None))
                .IsEqualTo(retryState);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Seeds the store with an accepted operation awaiting authoritative replay inclusion.</summary>
    /// <param name="directory">The temporary store directory.</param>
    /// <param name="stream">The operation stream.</param>
    /// <param name="operation">The operation to persist.</param>
    /// <param name="initialization">The store initialization settings.</param>
    /// <returns>The durable subscription identifier.</returns>
    private static async Task<SubscriptionId> PrepareCompactionStateAsync(
        string directory,
        StreamId stream,
        SyncOperation operation,
        LocalStoreInitialization initialization)
    {
        await using var adapter = new FileSystemLocalStoreAdapter(directory);
        await adapter.InitializeAsync(initialization, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        await adapter.CommitLocalOperationAsync(
            operation,
            new(stream, CreatePayload("local"), InitialRevision),
            CancellationToken.None);
        await foreach (var lease in adapter.LeasePendingOperationsAsync(
            new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
            CancellationToken.None))
        {
            await adapter.ApplySyncResultAsync(
                lease.LeaseId,
                new(
                    Guid.NewGuid(),
                    [new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, null)],
                    null,
                    null),
                CancellationToken.None);
        }

        var firstCompaction = await adapter.CompactAsync(
            new(stream, DateTimeOffset.MaxValue, NoCompactionRecords),
            CancellationToken.None);
        await Assert.That(firstCompaction.RecordsRemoved).IsEqualTo(NoCompactionRecords);
        await Assert.That(firstCompaction.BytesReclaimed).IsGreaterThan(0);
        return subscriptionId;
    }

    /// <summary>Seeds the store with a retry state and an applied remote event.</summary>
    /// <param name="directory">The temporary store directory.</param>
    /// <param name="stream">The operation stream.</param>
    /// <param name="operation">The operation to persist.</param>
    /// <param name="eventId">The remote event identifier.</param>
    /// <param name="initialization">The store initialization settings.</param>
    /// <returns>The subscription identifier and persisted retry state.</returns>
    private static async Task<(SubscriptionId SubscriptionId, RetryState RetryState)> SeedRetryInboxStateAsync(
        string directory,
        StreamId stream,
        SyncOperation operation,
        Guid eventId,
        LocalStoreInitialization initialization)
    {
        var retryState = new RetryState(
            TestTimeProvider.GetUtcNow(),
            TestTimeProvider.GetUtcNow().AddMinutes(1),
            TimeSpan.FromSeconds(1),
            1,
            RetryAuthenticationState.None,
            null);
        await using var adapter = new FileSystemLocalStoreAdapter(directory);
        await adapter.InitializeAsync(initialization, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        await adapter.CommitLocalOperationAsync(
            operation,
            new(stream, CreatePayload("local"), InitialSnapshotRevision),
            CancellationToken.None);
        await adapter.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None);

        await using var leases = adapter.LeasePendingOperationsAsync(
            new(stream, NextOperationSequence, StandardLeaseByteLimit, TimeSpan.FromMinutes(1)),
            CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await leases.MoveNextAsync()).IsTrue();
        var remoteEvent = new RemoteEvent(
            eventId,
            stream,
            TestCursor,
            TestTimeProvider.GetUtcNow(),
            null,
            CreatePayload("remote"),
            new Dictionary<string, string>());
        var batch = new RemoteEventBatch(Guid.NewGuid(), stream, null, TestCursor, [remoteEvent]);
        await adapter.ApplyRemoteBatchAsync(
            batch,
            new(stream, CreatePayload("merged"), SecondSnapshotRevision, NextOperationSequence),
            CancellationToken.None);
        return (subscriptionId, retryState);
    }

    /// <summary>Creates an operation at the supplied sequence using the deterministic test clock.</summary>
    /// <param name="stream">The operation stream.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The new operation.</returns>
    private static SyncOperation CreateOperation(StreamId stream, long sequence) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = stream,
            ClientSequence = sequence,
            TimestampUtc = TestTimeProvider.GetUtcNow(),
            Type = SyncOperationType.Update,
            Payload = CreatePayload("operation"),
        };

    /// <summary>Creates a payload envelope for a text value.</summary>
    /// <param name="value">The payload text.</param>
    /// <returns>The hashed payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        return new(
            "test",
            1,
            "text/plain",
            bytes,
            Convert.ToHexString(SHA256.HashData(bytes)));
    }

    /// <summary>Provides a fixed UTC time to deterministic tests.</summary>
    /// <param name="now">The time returned by this provider.</param>
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
