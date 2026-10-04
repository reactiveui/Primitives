// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers.Binary;
using System.Text;
using System.Text.Json.Nodes;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests incremental transactions and journal recovery.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>The operation count in the first growth block.</summary>
    private const int GrowthBlockSize = 32;

    /// <summary>The denominator used by journal growth bounds.</summary>
    private const int GrowthBoundDenominator = 10;

    /// <summary>The numerator allowing small digit-width changes in records.</summary>
    private const int GrowthBoundNumerator = 12;

    /// <summary>The expected growth factor when the operation count doubles.</summary>
    private const int DoubleGrowthFactor = 2;

    /// <summary>The large unchanged snapshot payload size.</summary>
    private const int LargeSnapshotBytes = 64 * 1024;

    /// <summary>The maximum size of the metadata-only test transaction.</summary>
    private const long MaximumMetadataFrameBytes = 4096;

    /// <summary>An invalid checksum length.</summary>
    private const int InvalidChecksumBytes = 31;

    /// <summary>The mask used to corrupt a checksum byte.</summary>
    private const byte ChecksumCorruptionMask = 0xFF;

    /// <summary>Verifies fixed-size commits grow the journal linearly rather than copying history.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FixedSizeCommitsHaveBoundedLinearJournalGrowth()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("linear-growth");
            await using var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider);
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var baseline = new FileInfo(Path.Combine(directory, JournalFileName)).Length;
            await CommitJournalRangeAsync(adapter, stream, 0, GrowthBlockSize);
            var first = new FileInfo(Path.Combine(directory, JournalFileName)).Length - baseline;
            await CommitJournalRangeAsync(adapter, stream, GrowthBlockSize, GrowthBlockSize * DoubleGrowthFactor);
            var second = new FileInfo(Path.Combine(directory, JournalFileName)).Length - baseline - first;
            await CommitJournalRangeAsync(
                adapter,
                stream,
                GrowthBlockSize * DoubleGrowthFactor,
                GrowthBlockSize * DoubleGrowthFactor * DoubleGrowthFactor);
            var third = new FileInfo(Path.Combine(directory, JournalFileName)).Length - baseline - first - second;
            await Assert.That(second).IsLessThan(first * GrowthBoundNumerator / GrowthBoundDenominator);
            await Assert.That(third).IsLessThan(first * GrowthBoundNumerator * DoubleGrowthFactor / GrowthBoundDenominator);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies every incomplete final-frame byte boundary preserves the committed prefix.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EveryFinalFrameTruncationRecoversPrefixAndAllowsNewAppend()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("truncate");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, 1);
            }

            var path = Path.Combine(directory, JournalFileName);
            var prefix = await File.ReadAllBytesAsync(path);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 1, TwoPendingOperations);
            }

            var complete = await File.ReadAllBytesAsync(path);
            for (var length = prefix.Length; length < complete.Length; length++)
            {
                await File.WriteAllBytesAsync(path, complete.AsMemory(0, length).ToArray());
                await VerifyTruncatedJournalAndAppendAsync(directory, stream, subscription, initialization, prefix.LongLength);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies complete malformed transactions cannot be mistaken for crash tails.</summary>
    /// <param name="payload">The checksummed invalid record contents.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("{}")]
    [Arguments("null")]
    [Arguments("{\"State\":null}")]
    [Arguments("{\"State\":{}}")]
    [Arguments("{\"Version\":99,\"State\":null,\"Delta\":{}}")]
    [Arguments("{\"Version\":2,\"State\":null,\"Delta\":{}}")]
    [Arguments("{\"Version\":2,\"State\":null,\"Delta\":{\"ClientId\":null,\"Streams\":null,\"Leases\":{},\"Inbox\":[],\"IncludedOperations\":[],\"RemovedLeases\":[]}}")]
    [Arguments("{\"Version\":2,\"Delta\":{\"ClientId\":null,\"Streams\":{},\"Leases\":{},\"Inbox\":[null],\"IncludedOperations\":[],\"RemovedLeases\":[]}}")]
    [Arguments("{")]
    public async Task CompleteInvalidRecordFailsClosedWithoutTruncating(string payload)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var prefix = await File.ReadAllBytesAsync(path);
            var invalid = CreateJournalFrame(Encoding.UTF8.GetBytes(payload));
            var bytes = prefix.Concat(invalid).ToArray();
            await File.WriteAllBytesAsync(path, bytes);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies legacy snapshots can be extended, reopened, and compacted without losing identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LegacySnapshotAcceptsIncrementalAppendAndCompaction()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("legacy");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, 1);
                await adapter.CompactAsync(new(stream, DateTimeOffset.MaxValue, 0), CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var bytes = await File.ReadAllBytesAsync(path);
            var size = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            var legacy = JsonNode.Parse(bytes.AsSpan(FileSystemLocalStoreAdapter.JournalHeaderBytes, size))!;
            _ = legacy.AsObject().Remove(nameof(FileSystemLocalStoreAdapter.JournalRecord.Version));
            _ = legacy.AsObject().Remove(nameof(FileSystemLocalStoreAdapter.JournalRecord.Delta));
            var frame = CreateJournalFrame(Encoding.UTF8.GetBytes(legacy.ToJsonString()));
            await File.WriteAllBytesAsync(path, frame);
            await using (var reopened = new FileSystemLocalStoreAdapter(directory))
            {
                await reopened.InitializeAsync(initialization, CancellationToken.None);
                await Assert.That(new FileInfo(path).Length).IsEqualTo(frame.LongLength);
                await CommitJournalRangeAsync(reopened, stream, 1, TwoPendingOperations);
            }

            await using var recovered = new FileSystemLocalStoreAdapter(directory);
            await recovered.InitializeAsync(initialization, CancellationToken.None);
            var state = await recovered.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(state.PendingOperations).Count().IsEqualTo(TwoPendingOperations);
            await Assert.That(state.Snapshot!.Revision).IsEqualTo(SecondSnapshotRevision);
            await recovered.CompactAsync(new(stream, DateTimeOffset.MaxValue, 0), CancellationToken.None);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies metadata changes do not append unchanged large snapshots.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryMutationOmitsUnchangedSnapshotAndReopenDoesNotGrowJournal()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("large-snapshot");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false) { ClientId = TestClientId };
            var operation = CreateOperation(stream, 0);
            var retry = RetryState.Start(TestTimeProvider.GetUtcNow());
            var path = Path.Combine(directory, JournalFileName);
            SubscriptionId subscription;
            long committedLength;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(new('x', LargeSnapshotBytes)), 1),
                    CancellationToken.None);
                var snapshotLength = new FileInfo(path).Length;
                await adapter.SaveRetryStateAsync(operation.OperationId, retry, CancellationToken.None);
                committedLength = new FileInfo(path).Length;
                await Assert.That(committedLength - snapshotLength).IsLessThan(MaximumMetadataFrameBytes);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            await Assert.That(new FileInfo(path).Length).IsEqualTo(committedLength);
            await Assert.That(await reopened.GetRetryStateAsync(operation.OperationId, CancellationToken.None)).IsEqualTo(retry);
            var recovered = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(recovered.Snapshot!.State.Payload.Length).IsEqualTo(LargeSnapshotBytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies complete invalid headers are not discarded.</summary>
    /// <param name="payloadLength">The invalid payload length.</param>
    /// <param name="checksumLength">The checksum length.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(0, FileSystemLocalStoreAdapter.JournalChecksumBytes)]
    [Arguments(-1, FileSystemLocalStoreAdapter.JournalChecksumBytes)]
    [Arguments(FileSystemLocalStoreAdapter.MaximumRecordBytes + 1, FileSystemLocalStoreAdapter.JournalChecksumBytes)]
    [Arguments(1, InvalidChecksumBytes)]
    public async Task CompleteInvalidHeaderIsNotDiscarded(int payloadLength, int checksumLength)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var prefix = await File.ReadAllBytesAsync(path);
            var header = new byte[FileSystemLocalStoreAdapter.JournalHeaderBytes];
            BinaryPrimitives.WriteInt32LittleEndian(header, payloadLength);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(sizeof(int)), checksumLength);
            var corrupt = prefix.Concat(header).ToArray();
            await File.WriteAllBytesAsync(path, corrupt);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, corrupt);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies checksum corruption fails closed even when another complete record follows it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CorruptInteriorChecksumIsNotDiscarded()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("interior");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            var path = Path.Combine(directory, JournalFileName);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, 1);
            }

            var bytes = await File.ReadAllBytesAsync(path);
            var firstLength = BinaryPrimitives.ReadInt32LittleEndian(bytes)
                + FileSystemLocalStoreAdapter.JournalHeaderBytes + FileSystemLocalStoreAdapter.JournalChecksumBytes;
            bytes[firstLength - 1] ^= ChecksumCorruptionMask;
            await File.WriteAllBytesAsync(path, bytes);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies a checksummed snapshot cannot rebind either durable identity.</summary>
    /// <param name="field">The identity field to corrupt.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.StoreIdentity))]
    [Arguments(nameof(FileSystemLocalStoreAdapter.StoreState.ClientId))]
    public async Task ChecksummedSnapshotCannotChangeBoundIdentity(string field)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var initialization = new LocalStoreInitialization(TestClientId, 1, false) { ClientId = TestClientId };
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var prefix = await File.ReadAllBytesAsync(path);
            var size = BinaryPrimitives.ReadInt32LittleEndian(prefix);
            var record = JsonNode.Parse(prefix.AsSpan(FileSystemLocalStoreAdapter.JournalHeaderBytes, size))!;
            record[nameof(FileSystemLocalStoreAdapter.JournalRecord.State)]![field] = "another-identity";
            var invalid = CreateJournalFrame(Encoding.UTF8.GetBytes(record.ToJsonString()));
            var bytes = prefix.Concat(invalid).ToArray();
            await File.WriteAllBytesAsync(path, bytes);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies a transaction cannot initialize a store without a committed snapshot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IncrementalRecordRequiresCommittedSnapshot()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var payload = "{\"Version\":2,\"Delta\":{\"ClientId\":null,\"Streams\":{},\"Leases\":{},\"Inbox\":[],\"IncludedOperations\":[],\"RemovedLeases\":[]}}"u8.ToArray();
            var bytes = CreateJournalFrame(payload);
            await File.WriteAllBytesAsync(Path.Combine(directory, JournalFileName), bytes);
            await AssertJournalRejectedAndUnchangedAsync(directory, new(TestClientId, 1, false), bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies binding a previously unbound client persists as an incremental transaction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ClientBindingAddedAfterInitialSnapshotSurvivesRestart()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var unbound = new LocalStoreInitialization(TestClientId, 1, false);
            var bound = unbound with { ClientId = TestClientId };
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(unbound, CancellationToken.None);
            }

            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(bound, CancellationToken.None);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await Assert.That(() => reopened.InitializeAsync(
                bound with { ClientId = "other" },
                CancellationToken.None).AsTask()).Throws<InvalidOperationException>();
            await reopened.InitializeAsync(bound, CancellationToken.None);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies a fresh stream accepts revision zero as the first remote apply fence.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FreshStreamAcceptsFirstRemoteSnapshotAndRecoversIt()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("first-remote");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                var result = await adapter.ApplyRemoteBatchAsync(
                    new(Guid.NewGuid(), stream, null, TestCursor, []),
                    new(stream, CreatePayload(SnapshotPayload), 1),
                    CancellationToken.None);
                await Assert.That(result.SnapshotRevision).IsEqualTo(1);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var state = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(state.ServerCursor).IsEqualTo(TestCursor);
            await Assert.That(state.Snapshot!.Revision).IsEqualTo(1);
            await Assert.That(state.NextClientSequence).IsEqualTo(0);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies repeated initialization still enforces both bound identities.</summary>
    /// <param name="changeClient">Whether the client identity, rather than store identity, changes.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RepeatedInitializationRejectsChangedIdentity(bool changeClient)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var initialization = new LocalStoreInitialization(TestClientId, 1, false) { ClientId = TestClientId };
            var changed = changeClient
                ? initialization with { ClientId = "another" }
                : initialization with { StoreIdentity = "another" };
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(initialization, CancellationToken.None);
            await Assert.That(() => adapter.InitializeAsync(changed, CancellationToken.None).AsTask()).Throws<InvalidOperationException>();
            await adapter.InitializeAsync(initialization, CancellationToken.None);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies cancellation rolls back inbox, cursor, snapshot, and completion as one transaction.</summary>
    /// <param name="checkpoint">The append boundary that cancels the remote apply.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendHeader))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendPayload))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendChecksum))]
    public async Task CanceledRemoteApplyPreservesAtomicPrefixAcrossRestart(string checkpoint)
    {
        var directory = CreateJournalTestDirectory();
        using var cancellation = new CancellationTokenSource();
        var armed = false;
        try
        {
            var stream = new StreamId("apply-cancel");
            var operation = CreateOperation(stream, 0);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false) { ClientId = TestClientId };
            var remoteEvent = new RemoteEvent(
                Guid.NewGuid(),
                stream,
                TestCursor,
                TestTimeProvider.GetUtcNow(),
                null,
                CreatePayload("event"),
                new Dictionary<string, string>());
            var batch = new RemoteEventBatch(Guid.NewGuid(), stream, null, TestCursor, [remoteEvent])
            { CompletedOperations = [new(new RemoteEventOrigin(TestClientId, operation.OperationId), [])], };
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, current =>
            {
                if (armed && current.ToString() == checkpoint)
                {
                    cancellation.Cancel();
                }
            }))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(operation, new(stream, CreatePayload(SnapshotPayload), 1), CancellationToken.None);
                var length = new FileInfo(Path.Combine(directory, JournalFileName)).Length;
                armed = true;
                await Assert.That(() => IgnoreResultAsync(adapter.ApplyRemoteBatchAsync(
                    batch,
                    new(stream, CreatePayload("remote"), 1, 1),
                    cancellation.Token))).Throws<OperationCanceledException>();
                await Assert.That(new FileInfo(Path.Combine(directory, JournalFileName)).Length).IsEqualTo(length);
                await AssertRemoteApplyPrefixAsync(adapter, stream, subscription, remoteEvent.EventId);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            await AssertRemoteApplyPrefixAsync(reopened, stream, subscription, remoteEvent.EventId);
            await reopened.ApplyRemoteBatchAsync(batch, new(stream, CreatePayload("remote"), 1, 1), CancellationToken.None);
            await Assert.That(await reopened.GetUnappliedEventIdsAsync(stream, [remoteEvent.EventId], CancellationToken.None)).IsEmpty();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies cancellation after the durable flush cannot undo an acknowledged commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationAfterDurableFlushKeepsCommittedOperation()
    {
        var directory = CreateJournalTestDirectory();
        using var cancellation = new CancellationTokenSource();
        var armed = false;
        try
        {
            var stream = new StreamId("cancel-after-flush");
            var operation = CreateOperation(stream, 0);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, checkpoint =>
            {
                if (armed && checkpoint == FileSystemJournalCheckpoint.AfterAppendFlush)
                {
                    cancellation.Cancel();
                }
            }))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                armed = true;
                var result = await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), 1),
                    cancellation.Token);
                await Assert.That(result.OperationId).IsEqualTo(operation.OperationId);
                await Assert.That(cancellation.IsCancellationRequested).IsTrue();
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var state = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(state.PendingOperations).HasSingleItem();
            await Assert.That(state.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies process crashes preserve an atomic prefix and permit further commits.</summary>
    /// <param name="checkpoint">The append checkpoint at which the writer dies.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [NotInParallel("filesystem-compaction-process-crash")]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendHeader))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendPayload))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendChecksum))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterAppendFlush))]
    public async Task AppendCrashRecoversAtomicPrefixAndAllowsNewCommit(string checkpoint)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var signal = Path.Combine(directory, "append.signal");
            var operationId = Guid.NewGuid();
            await RunCompactionCrashChildAsync(string.Join('\n', directory, signal, operationId.ToString("D"), checkpoint), signal);
            var stream = new StreamId("compaction-process-crash");
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var recovered = new FileSystemLocalStoreAdapter(directory))
            {
                await recovered.InitializeAsync(initialization, CancellationToken.None);
                var subscription = await recovered.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                var state = await recovered.RecoverStreamAsync(stream, subscription, CancellationToken.None);
                await Assert.That(state.PendingOperations.Count).IsGreaterThanOrEqualTo(1);
                await Assert.That(state.PendingOperations.Count).IsLessThanOrEqualTo(TwoPendingOperations);
                await Assert.That(state.PendingOperations[0].OperationId.Value).IsEqualTo(operationId);
                await Assert.That(state.Snapshot!.Revision).IsEqualTo((long)state.PendingOperations.Count);
                if (checkpoint == nameof(FileSystemJournalCheckpoint.AfterAppendFlush))
                {
                    await Assert.That(state.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
                }
                else if (checkpoint is nameof(FileSystemJournalCheckpoint.AfterAppendHeader)
                    or nameof(FileSystemJournalCheckpoint.AfterAppendPayload))
                {
                    await Assert.That(state.PendingOperations).HasSingleItem();
                }

                var sequence = (int)state.NextClientSequence;
                await CommitJournalRangeAsync(recovered, stream, sequence, sequence + 1);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var identity = await reopened.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var appended = await reopened.RecoverStreamAsync(stream, identity, CancellationToken.None);
            await Assert.That(appended.PendingOperations.Count).IsGreaterThanOrEqualTo(TwoPendingOperations);
            await Assert.That(appended.Snapshot!.Revision).IsEqualTo((long)appended.PendingOperations.Count);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Checks recovery and a second restart for one truncation boundary.</summary>
    /// <param name="directory">The store directory.</param>
    /// <param name="stream">The stored stream.</param>
    /// <param name="subscription">The durable subscription identity.</param>
    /// <param name="initialization">The bound store identities.</param>
    /// <param name="prefixLength">The complete committed prefix length.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifyTruncatedJournalAndAppendAsync(
        string directory,
        StreamId stream,
        SubscriptionId subscription,
        LocalStoreInitialization initialization,
        long prefixLength)
    {
        await using (var recovered = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
        {
            await recovered.InitializeAsync(initialization, CancellationToken.None);
            var state = await recovered.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(state.PendingOperations).HasSingleItem();
            await Assert.That(state.NextClientSequence).IsEqualTo(1);
            await Assert.That(state.Snapshot!.Revision).IsEqualTo(1);
            await Assert.That(new FileInfo(Path.Combine(directory, JournalFileName)).Length).IsEqualTo(prefixLength);
            await CommitJournalRangeAsync(recovered, stream, 1, TwoPendingOperations);
        }

        await using var reopened = new FileSystemLocalStoreAdapter(directory, TestTimeProvider);
        await reopened.InitializeAsync(initialization, CancellationToken.None);
        var appended = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
        await Assert.That(appended.PendingOperations).Count().IsEqualTo(TwoPendingOperations);
        await Assert.That(appended.Snapshot!.Revision).IsEqualTo(SecondSnapshotRevision);
    }

    /// <summary>Checks that initialization rejects a journal without modifying its bytes.</summary>
    /// <param name="directory">The store directory.</param>
    /// <param name="initialization">The requested bound identities.</param>
    /// <param name="bytes">The expected unchanged journal bytes.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertJournalRejectedAndUnchangedAsync(
        string directory,
        LocalStoreInitialization initialization,
        byte[] bytes)
    {
        await using var reopened = new FileSystemLocalStoreAdapter(directory);
        await Assert.That(() => reopened.InitializeAsync(initialization, CancellationToken.None).AsTask())
            .Throws<InvalidDataException>();
        await Assert.That((await File.ReadAllBytesAsync(Path.Combine(directory, JournalFileName))).SequenceEqual(bytes)).IsTrue();
    }

    /// <summary>Checks that a failed apply left every part of the prefix unchanged.</summary>
    /// <param name="adapter">The open recovered adapter.</param>
    /// <param name="stream">The stored stream.</param>
    /// <param name="subscription">The durable subscription identity.</param>
    /// <param name="eventId">The unapplied event identity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRemoteApplyPrefixAsync(
        FileSystemLocalStoreAdapter adapter,
        StreamId stream,
        SubscriptionId subscription,
        Guid eventId)
    {
        var recovered = await adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).HasSingleItem();
        await Assert.That(recovered.Snapshot!.Revision).IsEqualTo(1);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(await adapter.GetUnappliedEventIdsAsync(stream, [eventId], CancellationToken.None)).HasSingleItem();
    }

    /// <summary>Creates an isolated directory under the current working directory.</summary>
    /// <returns>The absolute store directory.</returns>
    private static string CreateJournalTestDirectory()
    {
        var directory = Path.GetFullPath(Path.Combine("artifacts", "filesystem-journal-tests", Guid.NewGuid().ToString("N")));
        _ = Directory.CreateDirectory(directory);
        return directory;
    }

    /// <summary>Commits fixed-size operations and snapshots for a sequence range.</summary>
    /// <param name="adapter">The open adapter.</param>
    /// <param name="stream">The stream receiving the operations.</param>
    /// <param name="start">The first sequence.</param>
    /// <param name="end">The exclusive last sequence.</param>
    /// <returns>The commit task.</returns>
    private static async Task CommitJournalRangeAsync(FileSystemLocalStoreAdapter adapter, StreamId stream, int start, int end)
    {
        for (var sequence = start; sequence < end; sequence++)
        {
            await adapter.CommitLocalOperationAsync(
                CreateOperation(stream, sequence),
                new(stream, CreatePayload(SnapshotPayload), 1, sequence),
                CancellationToken.None);
        }
    }

    /// <summary>Encodes a checksummed record using the existing binary frame.</summary>
    /// <param name="payload">The serialized record.</param>
    /// <returns>The complete journal frame.</returns>
    private static byte[] CreateJournalFrame(byte[] payload)
    {
        var checksum = FileSystemJournalHelpers.ComputeHash(payload);
        var frame = new byte[FileSystemLocalStoreAdapter.JournalHeaderBytes + payload.Length + checksum.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(sizeof(int)), checksum.Length);
        payload.CopyTo(frame, FileSystemLocalStoreAdapter.JournalHeaderBytes);
        checksum.CopyTo(frame, FileSystemLocalStoreAdapter.JournalHeaderBytes + payload.Length);
        return frame;
    }
}
