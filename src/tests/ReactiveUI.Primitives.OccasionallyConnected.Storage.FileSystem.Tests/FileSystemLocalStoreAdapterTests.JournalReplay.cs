// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests checksummed replay records against the committed prefix.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>The stream used by journal replay fence tests.</summary>
    private const string ReplayFenceStreamName = "replay-fences";

    /// <summary>Verifies copying an old complete retry frame cannot roll back the next client sequence.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CopiedEarlierRetryFrameCannotReuseCommittedSequence()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(ReplayFenceStreamName);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                var operation = CreateOperation(stream, 0);
                await adapter.CommitLocalOperationAsync(operation, new(stream, CreatePayload(SnapshotPayload), 1), CancellationToken.None);
                await adapter.SaveRetryStateAsync(operation.OperationId, RetryState.Start(TestTimeProvider.GetUtcNow()), CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var earlier = await File.ReadAllBytesAsync(path);
            var (frameStart, _) = GetFinalJournalRecord(earlier);
            var retryFrame = earlier.AsSpan(frameStart).ToArray();
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 1, TwoPendingOperations);
            }

            var committed = await File.ReadAllBytesAsync(path);
            var copied = committed.Concat(retryFrame).ToArray();
            await File.WriteAllBytesAsync(path, copied);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, copied);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies valid checksums do not permit an operation outside the committed sequence range.</summary>
    /// <param name="snapshotRecord">Whether to corrupt an explicit legacy snapshot instead of a retry transaction.</param>
    /// <param name="negativeSequence">Whether the corrupt operation has a negative sequence.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ChecksummedOperationOutsideCommittedSequenceRangeFailsRecovery(bool snapshotRecord, bool negativeSequence)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await PrepareSequenceCorruptionJournalAsync(directory, initialization, snapshotRecord);

            var bytes = await CorruptFinalJournalRecordAsync(directory, record =>
            {
                var root = record[snapshotRecord
                    ? nameof(FileSystemLocalStoreAdapter.JournalRecord.State)
                    : nameof(FileSystemLocalStoreAdapter.JournalRecord.Delta)]!;
                var persistedStream = root[nameof(FileSystemLocalStoreAdapter.StoreState.Streams)]![ReplayFenceStreamName]!;
                var sequence = persistedStream[nameof(FileSystemLocalStoreAdapter.StreamState.NextSequence)]!.GetValue<long>();
                var operation = GetFirstJournalOperation(persistedStream);
                operation[nameof(FileSystemLocalStoreAdapter.OperationState.Operation)]![nameof(SyncOperation.ClientSequence)]
                    = negativeSequence ? -1 : sequence;
            });
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies missing and null replacement snapshots cannot erase a committed checkpoint.</summary>
    /// <param name="omitProperty">Whether to omit the required snapshot property instead of writing null.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ChecksummedReplacementCannotEraseCommittedCheckpoint(bool omitProperty)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(ReplayFenceStreamName);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, TwoPendingOperations);
            }

            var bytes = await CorruptFinalJournalRecordAsync(directory, record =>
            {
                var change = GetReplayStreamDelta(record);
                if (omitProperty)
                {
                    _ = change.Remove(nameof(FileSystemLocalStoreAdapter.StreamDelta.Snapshot));
                }
                else
                {
                    change[nameof(FileSystemLocalStoreAdapter.StreamDelta.Snapshot)] = null;
                }
            });
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, bytes);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies an explicit null checkpoint is legal for a newly created empty stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExplicitNullCheckpointOnNewEmptyStreamRecovers()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(ReplayFenceStreamName);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscription;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            }

            await CorruptFinalJournalRecordAsync(directory, static record =>
            {
                GetReplayStreamDelta(record)[nameof(FileSystemLocalStoreAdapter.StreamDelta.ReplaceSnapshot)] = true;
            });
            await using var reopened = new FileSystemLocalStoreAdapter(directory, TestTimeProvider);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(recovered.Snapshot).IsNull();
            await Assert.That(recovered.NextClientSequence).IsEqualTo(0);
            await Assert.That(recovered.PendingOperations).IsEmpty();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies old complete legacy snapshots cannot discard later committed streams or sequences.</summary>
    /// <param name="omitStream">Whether the old snapshot predates stream creation.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CopiedLegacySnapshotCannotDiscardCommittedPrefix(bool omitStream)
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(ReplayFenceStreamName);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
            }

            var path = Path.Combine(directory, JournalFileName);
            var initial = await File.ReadAllBytesAsync(path);
            var (_, legacy) = GetFinalJournalRecord(initial);
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 0, 1);
            }

            if (!omitStream)
            {
                var (_, committed) = GetFinalJournalRecord(await File.ReadAllBytesAsync(path));
                legacy[nameof(FileSystemLocalStoreAdapter.JournalRecord.State)]![
                    nameof(FileSystemLocalStoreAdapter.StoreState.Streams)]![ReplayFenceStreamName] = GetReplayStreamDelta(committed).DeepClone();
            }

            var frame = CreateJournalFrame(Encoding.UTF8.GetBytes(legacy.ToJsonString()));
            await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                await CommitJournalRangeAsync(adapter, stream, 1, TwoPendingOperations);
            }

            var copied = (await File.ReadAllBytesAsync(path)).Concat(frame).ToArray();
            await File.WriteAllBytesAsync(path, copied);
            await AssertJournalRejectedAndUnchangedAsync(directory, initialization, copied);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Creates committed operation frames and, when requested, an explicit legacy snapshot fixture.</summary>
    /// <param name="directory">The store directory.</param>
    /// <param name="initialization">The bound store identities.</param>
    /// <param name="snapshotRecord">Whether the final record should be a legacy snapshot.</param>
    /// <returns>The fixture preparation task.</returns>
    private static async Task PrepareSequenceCorruptionJournalAsync(
        string directory,
        LocalStoreInitialization initialization,
        bool snapshotRecord)
    {
        var stream = new StreamId(ReplayFenceStreamName);
        var operation = CreateOperation(stream, 0);
        await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
        {
            await adapter.InitializeAsync(initialization, CancellationToken.None);
            await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            await adapter.CommitLocalOperationAsync(operation, new(stream, CreatePayload(SnapshotPayload), 1), CancellationToken.None);
        }

        var path = Path.Combine(directory, JournalFileName);
        var firstJournal = await File.ReadAllBytesAsync(path);
        await using (var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider))
        {
            await adapter.InitializeAsync(initialization, CancellationToken.None);
            await CommitJournalRangeAsync(adapter, stream, 1, TwoPendingOperations);
            if (!snapshotRecord)
            {
                await adapter.SaveRetryStateAsync(operation.OperationId, RetryState.Start(TestTimeProvider.GetUtcNow()), CancellationToken.None);
            }
        }

        if (!snapshotRecord)
        {
            return;
        }

        var frame = CreateLegacySnapshotFixture(firstJournal, await File.ReadAllBytesAsync(path));
        await File.WriteAllBytesAsync(path, frame);
        await AssertValidLegacySnapshotFixtureAsync(directory, initialization, stream);
    }

    /// <summary>Builds a full legacy snapshot from the actual initialized store and two committed operation records.</summary>
    /// <param name="firstJournal">The adapter journal after the first commit.</param>
    /// <param name="finalJournal">The adapter journal after the second commit.</param>
    /// <returns>The complete checksummed snapshot frame containing both pending operations.</returns>
    private static byte[] CreateLegacySnapshotFixture(byte[] firstJournal, byte[] finalJournal)
    {
        var initialLength = FileSystemLocalStoreAdapter.JournalHeaderBytes
            + BinaryPrimitives.ReadInt32LittleEndian(firstJournal.AsSpan(0, sizeof(int)))
            + FileSystemLocalStoreAdapter.JournalChecksumBytes;
        var (_, snapshot) = GetFinalJournalRecord(firstJournal.AsSpan(0, initialLength).ToArray());
        var (_, firstCommit) = GetFinalJournalRecord(firstJournal);
        var (_, secondCommit) = GetFinalJournalRecord(finalJournal);
        var stream = GetReplayStreamDelta(secondCommit).DeepClone().AsObject();
        var operations = stream[nameof(FileSystemLocalStoreAdapter.StreamState.Operations)]!.AsObject();
        foreach (var operation in GetReplayStreamDelta(firstCommit)[nameof(FileSystemLocalStoreAdapter.StreamState.Operations)]!.AsObject())
        {
            operations.Add(operation.Key, operation.Value!.DeepClone());
        }

        _ = stream.Remove(nameof(FileSystemLocalStoreAdapter.StreamDelta.ReplaceSnapshot));
        snapshot[nameof(FileSystemLocalStoreAdapter.JournalRecord.State)]![
            nameof(FileSystemLocalStoreAdapter.StoreState.Streams)]![ReplayFenceStreamName] = stream;
        _ = snapshot.Remove(nameof(FileSystemLocalStoreAdapter.JournalRecord.Version));
        _ = snapshot.Remove(nameof(FileSystemLocalStoreAdapter.JournalRecord.Delta));
        return CreateJournalFrame(Encoding.UTF8.GetBytes(snapshot.ToJsonString()));
    }

    /// <summary>Checks the real snapshot decoder accepts the complete fixture before corrupting a sequence.</summary>
    /// <param name="directory">The fixture store directory.</param>
    /// <param name="initialization">The bound identities.</param>
    /// <param name="stream">The committed stream identity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertValidLegacySnapshotFixtureAsync(
        string directory,
        LocalStoreInitialization initialization,
        StreamId stream)
    {
        await using var adapter = new FileSystemLocalStoreAdapter(directory, TestTimeProvider);
        await adapter.InitializeAsync(initialization, CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).Count().IsEqualTo(TwoPendingOperations);
        await Assert.That(recovered.NextClientSequence).IsEqualTo((long)TwoPendingOperations);
        await Assert.That(recovered.Snapshot!.Revision).IsEqualTo(SecondSnapshotRevision);
    }

    /// <summary>Finds the single changed stream in a generated transaction.</summary>
    /// <param name="record">The complete transaction record.</param>
    /// <returns>The changed stream object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static JsonObject GetReplayStreamDelta(JsonObject record) =>
        record[nameof(FileSystemLocalStoreAdapter.JournalRecord.Delta)]![
            nameof(FileSystemLocalStoreAdapter.JournalDelta.Streams)]![ReplayFenceStreamName]!.AsObject();

    /// <summary>Reads the first retained or changed operation from a generated record.</summary>
    /// <param name="stream">The generated stream state.</param>
    /// <returns>The first operation state object.</returns>
    private static JsonNode GetFirstJournalOperation(JsonNode stream)
    {
        using var operations = stream[nameof(FileSystemLocalStoreAdapter.StreamState.Operations)]!.AsObject().GetEnumerator();
        _ = operations.MoveNext();
        return operations.Current.Value!;
    }

    /// <summary>Changes a complete final record and recomputes its genuine frame checksum.</summary>
    /// <param name="directory">The store directory.</param>
    /// <param name="corrupt">The record mutation to test.</param>
    /// <returns>The complete journal bytes after the mutation.</returns>
    private static async Task<byte[]> CorruptFinalJournalRecordAsync(string directory, Action<JsonObject> corrupt)
    {
        var path = Path.Combine(directory, JournalFileName);
        var journal = await File.ReadAllBytesAsync(path);
        var (frameStart, record) = GetFinalJournalRecord(journal);
        corrupt(record);
        var bytes = journal.AsSpan(0, frameStart).ToArray()
            .Concat(CreateJournalFrame(Encoding.UTF8.GetBytes(record.ToJsonString()))).ToArray();
        await File.WriteAllBytesAsync(path, bytes);
        return bytes;
    }

    /// <summary>Reads the final complete record using the existing journal frame layout.</summary>
    /// <param name="journal">The valid journal produced by the adapter.</param>
    /// <returns>The final frame start and decoded record.</returns>
    private static (int FrameStart, JsonObject Record) GetFinalJournalRecord(byte[] journal)
    {
        var offset = 0;
        var frameStart = 0;
        while (offset < journal.Length)
        {
            frameStart = offset;
            var length = BinaryPrimitives.ReadInt32LittleEndian(journal.AsSpan(offset, sizeof(int)));
            offset += FileSystemLocalStoreAdapter.JournalHeaderBytes + length + FileSystemLocalStoreAdapter.JournalChecksumBytes;
        }

        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(journal.AsSpan(frameStart, sizeof(int)));
        var record = JsonNode.Parse(journal.AsSpan(frameStart + FileSystemLocalStoreAdapter.JournalHeaderBytes, payloadLength))!.AsObject();
        return (frameStart, record);
    }
}
