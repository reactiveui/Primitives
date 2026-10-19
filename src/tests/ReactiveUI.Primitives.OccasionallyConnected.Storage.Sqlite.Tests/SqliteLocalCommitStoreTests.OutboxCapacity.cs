// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Outbox capacity tests for <see cref="SqliteLocalCommitStore"/>.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The metadata text length used by outbox byte capacity tests.</summary>
    private const int OutboxCapacityMetadataLength = 1024;

    /// <summary>The byte limit used when testing count admission.</summary>
    private const int OutboxCapacityBytes = 4096;

    /// <summary>The count limit used when testing byte admission.</summary>
    private const int OutboxCapacityOperationLimit = 10;

    /// <summary>The number of operations used for cumulative admission checks.</summary>
    private const int DoubleOperationCapacity = 2;

    /// <summary>The second operation payload text.</summary>
    private const string SecondPayloadText = "second";

    /// <summary>The first operation payload text.</summary>
    private const string FirstPayloadText = "first";

    /// <summary>The third operation payload text.</summary>
    private const string ThirdPayloadText = "third";

    /// <summary>The server version returned by accepted upload results.</summary>
    private const string ServerVersion = "server-v1";

    /// <summary>The failure when a test did not observe its expected capacity exception.</summary>
    private const string ExpectedCapacityExceptionMessage = "Expected an outbox capacity exception.";

    /// <summary>The maximum time for concurrent commit gates.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies same-instance outbox initialization rejects invalid options and incompatible reinitialization.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task OutboxCapacityConfigurationRejectsInvalidOptionsAndIncompatibleReinitialization()
    {
        using var invalidDatabase = TempDatabase.Create();
        using var omittedDatabase = TempDatabase.Create();
        using var boundedDatabase = TempDatabase.Create();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var invalid = new SqliteLocalCommitStore(invalidDatabase.Path);
        using var omitted = CreateInitializedStoreWithoutOutbox(omittedDatabase.Path);
        using var bounded = CreateInitializedStore(boundedDatabase.Path, outbox);
        bounded.Initialize(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);

        var invalidOptions = () => invalid.Initialize(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox with { MaxOperations = 0 } },
            CancellationToken.None);
        var omittedToBounded = () => omitted.Initialize(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox },
            CancellationToken.None);
        var boundedToOmitted = () => bounded.Initialize(
            new(StoreIdentity, SchemaVersion, false),
            CancellationToken.None);
        var boundedToDifferent = () => bounded.Initialize(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox with { MaxOperations = DoubleOperationCapacity } },
            CancellationToken.None);

        await Assert.That(invalidOptions).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(omittedToBounded).ThrowsExactly<InvalidOperationException>();
        await Assert.That(boundedToOmitted).ThrowsExactly<InvalidOperationException>();
        await Assert.That(boundedToDifferent).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies pending operation count is global and rejection rolls back sequence, snapshot, and outbox rows.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task PendingOutboxOperationCountIsGlobalAndCommitRollbackIsAtomic()
    {
        using var database = TempDatabase.Create();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var store = CreateInitializedStore(database.Path, outbox);
        var firstSubscription = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var secondSubscription = store.GetOrCreateSubscriptionId(ReopenedStream, SubscriptionId.New(), CancellationToken.None);
        var volatilePolicy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var firstSnapshot = CreateSnapshotMutation(Stream, expectedRevision: 0, "first-snapshot");
        var firstOperation = CreateOperation(Stream, FirstClientSequence, "volatile", volatilePolicy) with { BaseVersion = null };
        var firstReceipt = store.CommitLocalOperation(
            firstOperation,
            firstSnapshot,
            CancellationToken.None);
        var replayReceipt = store.CommitLocalOperation(firstOperation, firstSnapshot, CancellationToken.None);

        var sameStreamFullOutbox = () =>
        {
            _ = store.CommitLocalOperation(
                CreateOperation(Stream, SecondClientSequence, "same-stream"),
                CreateSnapshotMutation(Stream, expectedRevision: 1, "changed-snapshot"),
                CancellationToken.None);
        };
        var otherStreamFullOutbox = () =>
        {
            _ = store.CommitLocalOperation(
                CreateOperation(ReopenedStream, FirstClientSequence, "other-stream"),
                CreateSnapshotMutation(ReopenedStream, expectedRevision: 0, "other-snapshot"),
                CancellationToken.None);
        };

        var sameStreamException = await Assert.That(sameStreamFullOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var otherStreamException = await Assert.That(otherStreamFullOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var firstRecovery = store.RecoverStream(Stream, firstSubscription, CancellationToken.None);
        var secondRecovery = store.RecoverStream(ReopenedStream, secondSubscription, CancellationToken.None);

        await Assert.That(sameStreamException.CanFitWhenEmpty).IsTrue();
        await Assert.That(otherStreamException.CanFitWhenEmpty).IsTrue();
        await Assert.That(replayReceipt).IsEqualTo(firstReceipt);
        await Assert.That(firstRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(firstRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(firstRecovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(firstRecovery.Snapshot?.State.Payload.ToArray().SequenceEqual(firstSnapshot.State.Payload.ToArray())).IsTrue();
        await Assert.That(secondRecovery.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(secondRecovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(secondRecovery.Snapshot).IsNull();
    }

    /// <summary>Verifies byte capacity counts retained pending operation envelopes and metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task PendingOutboxByteCapacityCountsOperationEnvelopeAndMetadata()
    {
        using var singleDatabase = TempDatabase.Create();
        var firstOperation = CreateOperationWithMetadata(Stream, FirstClientSequence, string.Empty, OutboxCapacityMetadataLength);
        var encodedBytes = GetOutboxOperationBytes(firstOperation);
        var singleOperationBudget = new OutboxOptions { MaxOperations = OutboxCapacityOperationLimit, MaxBytes = encodedBytes - 1, MaximumBlockedPublishers = 1 };
        using var singleOperationStore = CreateInitializedStore(singleDatabase.Path, singleOperationBudget);
        var singleSubscription = singleOperationStore.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);

        var tooLargeForEmptyOutbox = () =>
        {
            _ = singleOperationStore.CommitLocalOperation(
                firstOperation,
                CreateSnapshotMutation(Stream, expectedRevision: 0, string.Empty),
                CancellationToken.None);
        };

        var emptyException = await Assert.That(tooLargeForEmptyOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var emptyRecovery = singleOperationStore.RecoverStream(Stream, singleSubscription, CancellationToken.None);

        using var cumulativeDatabase = TempDatabase.Create();
        var cumulativeBudget = singleOperationBudget with { MaxBytes = (encodedBytes * DoubleOperationCapacity) - 1 };
        using var cumulativeStore = CreateInitializedStore(cumulativeDatabase.Path, cumulativeBudget);
        var cumulativeSubscription = cumulativeStore.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var firstSnapshot = CreateSnapshotMutation(Stream, expectedRevision: 0, FirstPayloadText);
        _ = cumulativeStore.CommitLocalOperation(
            firstOperation,
            firstSnapshot,
            CancellationToken.None);

        var cumulativeOverflow = () =>
        {
            _ = cumulativeStore.CommitLocalOperation(
                CreateOperationWithMetadata(Stream, SecondClientSequence, string.Empty, OutboxCapacityMetadataLength),
                CreateSnapshotMutation(Stream, expectedRevision: 1, SecondPayloadText),
                CancellationToken.None);
        };

        var cumulativeException = await Assert.That(cumulativeOverflow).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var cumulativeRecovery = cumulativeStore.RecoverStream(Stream, cumulativeSubscription, CancellationToken.None);

        await Assert.That(emptyException.CanFitWhenEmpty).IsFalse();
        await Assert.That(emptyRecovery.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(emptyRecovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(emptyRecovery.Snapshot).IsNull();
        await Assert.That(cumulativeException.CanFitWhenEmpty).IsTrue();
        await Assert.That(cumulativeRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(cumulativeRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(cumulativeRecovery.Snapshot?.State.Payload.ToArray().SequenceEqual(firstSnapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies SQLite counts UTF-8 bytes for stream ids, payload headers, and metadata.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task PendingOutboxByteCapacityCountsUnicodeEnvelopeAndMetadata()
    {
        using var database = TempDatabase.Create();
        var streamId = new StreamId("stream/λ");
        var firstOperation = CreateOperation(streamId, FirstClientSequence, string.Empty) with
        {
            BaseVersion = "version/λ",
            Payload = CreatePayload(string.Empty) with { ContractId = "contract.λ", ContentType = "application/λ" },
            Metadata = new Dictionary<string, string> { ["κ"] = "λ" },
        };
        var encodedBytes = GetOutboxOperationBytes(firstOperation);
        var outbox = new OutboxOptions { MaxOperations = DoubleOperationCapacity, MaxBytes = encodedBytes, MaximumBlockedPublishers = 1 };
        using var store = CreateInitializedStore(database.Path, outbox);
        var subscription = store.GetOrCreateSubscriptionId(streamId, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            firstOperation,
            CreateSnapshotMutation(streamId, 0, string.Empty),
            CancellationToken.None);
        var secondOperation = firstOperation with { OperationId = OperationId.New(), ClientSequence = SecondClientSequence };
        var overflow = () =>
        {
            _ = store.CommitLocalOperation(
                secondOperation,
                CreateSnapshotMutation(streamId, 1, string.Empty),
                CancellationToken.None);
        };

        var exception = await Assert.That(overflow).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var recovery = store.RecoverStream(streamId, subscription, CancellationToken.None);
        await Assert.That(exception.CanFitWhenEmpty).IsTrue();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies a terminal upload result releases the outbox slot before a later commit.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">An expected outbox lease was unavailable.</exception>
    [Test]
    public async Task TerminalOperationReleasesPendingOutboxCapacity()
    {
        using var database = TempDatabase.Create();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var store = CreateInitializedStore(database.Path, outbox);
        var subscription = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = store.CommitLocalOperation(
            CreateOperation(Stream, FirstClientSequence, FirstPayloadText),
            CreateSnapshotMutation(Stream, 0, FirstPayloadText),
            CancellationToken.None);
        var lease = store.LeasePendingOperationBatch(new(Stream, 1, OutboxCapacityBytes, TimeSpan.FromMinutes(1)), CancellationToken.None)
            ?? throw new InvalidOperationException("Expected a lease.");
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);

        var second = store.CommitLocalOperation(
            CreateOperation(Stream, SecondClientSequence, SecondPayloadText),
            CreateSnapshotMutation(Stream, 1, SecondPayloadText),
            CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscription, CancellationToken.None);

        await Assert.That(second.ClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies concurrent writers cannot both enter a globally full outbox across streams.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ConcurrentCrossStreamCommitsRespectGlobalPendingOutboxCapacity()
    {
        using var database = TempDatabase.Create();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        using var first = CreateInitializedStore(database.Path, outbox);
        using var second = CreateInitializedStore(database.Path, outbox);
        using var ready = new CountdownEvent(DoubleOperationCapacity);
        using var start = new ManualResetEventSlim();
        var firstSubscription = first.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var secondSubscription = first.GetOrCreateSubscriptionId(ReopenedStream, SubscriptionId.New(), CancellationToken.None);

        var firstTask = Task.Run(() => TryCommitAfterGate(
            ready,
            start,
            first,
            CreateOperation(Stream, FirstClientSequence, FirstPayloadText),
            CreateSnapshotMutation(Stream, expectedRevision: 0, FirstPayloadText)));
        var secondTask = Task.Run(() => TryCommitAfterGate(
            ready,
            start,
            second,
            CreateOperation(ReopenedStream, FirstClientSequence, SecondPayloadText),
            CreateSnapshotMutation(ReopenedStream, expectedRevision: 0, SecondPayloadText)));

        await Assert.That(ready.Wait(GuardTimeout)).IsTrue();
        start.Set();
        var attempts = await Task.WhenAll(firstTask, secondTask).WaitAsync(GuardTimeout);
        var firstRecovery = first.RecoverStream(Stream, firstSubscription, CancellationToken.None);
        var secondRecovery = first.RecoverStream(ReopenedStream, secondSubscription, CancellationToken.None);

        await Assert.That(attempts.Count(static attempt => attempt.Result is not null)).IsEqualTo(1);
        await Assert.That(attempts.Count(static attempt => attempt.Exception is QueueCapacityExceededException)).IsEqualTo(1);
        await Assert.That(GetUnexpectedExceptionDiagnostics(attempts)).IsEqualTo(string.Empty);
        await Assert.That(firstRecovery.PendingOperations.Count + secondRecovery.PendingOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies reopening with smaller limits preserves existing work and rejects new work until pending rows drain.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">An expected capacity exception or outbox lease was unavailable.</exception>
    [Test]
    public async Task ReopenWithSmallerOutboxCapacityPreservesOverLimitWorkAndRejectsNewCommitsUntilDrained()
    {
        using var database = TempDatabase.Create();
        var larger = new OutboxOptions { MaxOperations = DoubleOperationCapacity, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        var smaller = larger with { MaxOperations = 1 };
        SubscriptionId subscription;
        using (var setup = CreateInitializedStore(database.Path, larger))
        {
            subscription = setup.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
            _ = setup.CommitLocalOperation(CreateOperation(Stream, FirstClientSequence, FirstPayloadText), CreateSnapshotMutation(Stream, 0, FirstPayloadText), CancellationToken.None);
            _ = setup.CommitLocalOperation(CreateOperation(Stream, SecondClientSequence, SecondPayloadText), CreateSnapshotMutation(Stream, 1, SecondPayloadText), CancellationToken.None);
        }

        using var reopened = CreateInitializedStore(database.Path, smaller);
        var fullOutbox = () =>
        {
            _ = reopened.CommitLocalOperation(
                CreateOperation(Stream, ThirdClientSequence, ThirdPayloadText),
                CreateSnapshotMutation(Stream, DoubleOperationCapacity, ThirdPayloadText),
                CancellationToken.None);
        };

        var exception = await Assert.That(fullOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var beforeDrain = reopened.RecoverStream(Stream, subscription, CancellationToken.None);
        var lease = reopened.LeasePendingOperationBatch(new(Stream, DoubleOperationCapacity, OutboxCapacityBytes, TimeSpan.FromMinutes(1)), CancellationToken.None)
            ?? throw new InvalidOperationException("Expected a lease.");
        await reopened.ApplySyncResultAsync(
            lease.LeaseId,
            new(
                lease.LeaseId,
                [
                    new(lease.Operations[0].OperationId, OperationResultKind.Accepted, null, ServerVersion),
                    new(lease.Operations[1].OperationId, OperationResultKind.Accepted, null, ServerVersion),
                ],
                null,
                null),
            CancellationToken.None);

        var receipt = reopened.CommitLocalOperation(
            CreateOperation(Stream, ThirdClientSequence, ThirdPayloadText),
            CreateSnapshotMutation(Stream, DoubleOperationCapacity, ThirdPayloadText),
            CancellationToken.None);
        var afterDrain = reopened.RecoverStream(Stream, subscription, CancellationToken.None);

        await Assert.That(exception.CanFitWhenEmpty).IsTrue();
        await Assert.That(beforeDrain.PendingOperations.Count).IsEqualTo(DoubleOperationCapacity);
        await Assert.That(receipt.ClientSequence).IsEqualTo(ThirdClientSequence);
        await Assert.That(afterDrain.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterDrain.PendingOperations[0].ClientSequence).IsEqualTo(ThirdClientSequence);
    }

    /// <summary>Creates an initialized local commit store without outbox capacity.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteLocalCommitStore CreateInitializedStoreWithoutOutbox(string path)
    {
        var store = new SqliteLocalCommitStore(path);
        try
        {
            store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }
    }

    /// <summary>Creates an initialized local commit store with outbox capacity.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="outbox">The outbox capacity.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteLocalCommitStore CreateInitializedStore(string path, OutboxOptions outbox)
    {
        var store = new SqliteLocalCommitStore(path);
        try
        {
            store.Initialize(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }
    }

    /// <summary>Creates a representative operation for a stream.</summary>
    /// <param name="streamId">The stream id.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        StreamId streamId,
        long clientSequence,
        string payloadText,
        OperationPolicy? policy = null)
    {
        var operation = CreateOperation(clientSequence);
        return operation with
        {
            StreamId = streamId,
            Payload = CreatePayload(payloadText),
            Policy = policy ?? operation.Policy,
        };
    }

    /// <summary>Creates a representative operation with fixed-size metadata.</summary>
    /// <param name="streamId">The stream id.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="metadataLength">The metadata text length.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperationWithMetadata(
        StreamId streamId,
        long clientSequence,
        string payloadText,
        int metadataLength) =>
        CreateOperation(streamId, clientSequence, payloadText) with
        {
            Metadata = new Dictionary<string, string> { ["padding"] = new('m', metadataLength) },
        };

    /// <summary>Calculates the encoded operation envelope and metadata bytes without snapshot or status state.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The encoded byte count.</returns>
    private static long GetOutboxOperationBytes(SyncOperation operation)
    {
        const int fixedEnvelopeBytes = 68;
        var payload = operation.Payload;
        var bytes = (long)fixedEnvelopeBytes
            + System.Text.Encoding.UTF8.GetByteCount(operation.StreamId.Value)
            + System.Text.Encoding.UTF8.GetByteCount(operation.BaseVersion ?? string.Empty)
            + System.Text.Encoding.UTF8.GetByteCount(payload.ContractId)
            + System.Text.Encoding.UTF8.GetByteCount(payload.ContentType)
            + payload.PayloadLength
            + System.Text.Encoding.UTF8.GetByteCount(payload.PayloadHash);
        foreach (var pair in operation.Metadata)
        {
            bytes += System.Text.Encoding.UTF8.GetByteCount(pair.Key)
                + System.Text.Encoding.UTF8.GetByteCount(pair.Value);
        }

        return bytes;
    }

    /// <summary>Creates a representative snapshot mutation for a stream.</summary>
    /// <param name="streamId">The stream id.</param>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(StreamId streamId, long expectedRevision, string payloadText) =>
        new(streamId, CreatePayload(payloadText), FormatVersion: 1, expectedRevision);

    /// <summary>Attempts to commit after both concurrent writers are ready.</summary>
    /// <param name="ready">The ready counter.</param>
    /// <param name="start">The start gate.</param>
    /// <param name="store">The store.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshot">The snapshot mutation.</param>
    /// <returns>The commit attempt.</returns>
    private static CommitAttempt TryCommitAfterGate(
        CountdownEvent ready,
        ManualResetEventSlim start,
        SqliteLocalCommitStore store,
        SyncOperation operation,
        SnapshotMutation snapshot)
    {
        _ = ready.Signal();
        _ = start.Wait(GuardTimeout);
        return TryCommit(store, operation, snapshot);
    }

    /// <summary>Formats unexpected concurrent commit exceptions for diagnosis.</summary>
    /// <param name="attempts">The attempts.</param>
    /// <returns>The unexpected exception diagnostics, or an empty string.</returns>
    private static string GetUnexpectedExceptionDiagnostics(IReadOnlyList<CommitAttempt> attempts)
    {
        List<string> diagnostics = [];
        for (var index = 0; index < attempts.Count; index++)
        {
            var exception = attempts[index].Exception;
            if (exception is null or QueueCapacityExceededException)
            {
                continue;
            }

            diagnostics.Add($"{exception.GetType().FullName}: {exception.Message}");
        }

        return string.Join(Environment.NewLine, diagnostics);
    }
}
