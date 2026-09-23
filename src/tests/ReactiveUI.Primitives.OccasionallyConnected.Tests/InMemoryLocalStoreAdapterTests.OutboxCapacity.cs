// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Outbox capacity tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The metadata text length used by outbox byte capacity tests.</summary>
    private const int OutboxCapacityMetadataLength = 1024;

    /// <summary>The byte capacity used when only the count should reject admission.</summary>
    private const int OutboxCapacityBytes = 4096;

    /// <summary>The operation count used in byte capacity tests.</summary>
    private const int OutboxCapacityOperationLimit = 10;

    /// <summary>The number of operations used at cumulative byte boundaries.</summary>
    private const int DoubleOperationCapacity = 2;

    /// <summary>The legacy store byte budget for the default outbox count test.</summary>
    private const long LargeStoreEncodedBytes = 67_108_864;

    /// <summary>The second operation payload text.</summary>
    private const string SecondPayloadText = "second";

    /// <summary>The failure when a test did not observe its expected capacity exception.</summary>
    private const string ExpectedCapacityExceptionMessage = "Expected an outbox capacity exception.";

    /// <summary>Verifies same-instance outbox initialization rejects invalid options and incompatible reinitialization.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task OutboxCapacityConfigurationRejectsInvalidOptionsAndIncompatibleReinitialization()
    {
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        await using var invalid = new InMemoryLocalStoreAdapter();
        await using var omitted = new InMemoryLocalStoreAdapter();
        await using var bounded = new InMemoryLocalStoreAdapter();
        await omitted.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await bounded.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);
        await bounded.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);

        var invalidOptions = async () => await invalid.InitializeAsync(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox with { MaxOperations = 0 } },
            CancellationToken.None);
        var omittedToBounded = async () => await omitted.InitializeAsync(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox },
            CancellationToken.None);
        var boundedToOmitted = async () => await bounded.InitializeAsync(
            new(StoreIdentity, SchemaVersion, false),
            CancellationToken.None);
        var boundedToDifferent = async () => await bounded.InitializeAsync(
            new(StoreIdentity, SchemaVersion, false) { Outbox = outbox with { MaxOperations = DoubleOperationCapacity } },
            CancellationToken.None);

        await Assert.That(invalidOptions).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(omittedToBounded).ThrowsExactly<InvalidOperationException>();
        await Assert.That(boundedToOmitted).ThrowsExactly<InvalidOperationException>();
        await Assert.That(boundedToDifferent).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies pending operation count is global and rejection leaves durable stream state unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task PendingOutboxOperationCountIsGlobalAndCommitRollbackIsAtomic()
    {
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        await using var store = await CreateInitializedOutboxBoundedStoreAsync(outbox);
        var firstSubscription = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var secondSubscription = await store.GetOrCreateSubscriptionIdAsync(OtherStream, SubscriptionId.New(), CancellationToken.None);
        var volatilePolicy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var firstSnapshot = CreateSnapshotMutation(expectedRevision: 0, "first-snapshot");
        var firstOperation = CreateOperation(FirstClientSequence, "volatile", volatilePolicy);
        var firstReceipt = await store.CommitLocalOperationAsync(
            firstOperation,
            firstSnapshot,
            CancellationToken.None);
        var replayReceipt = await store.CommitLocalOperationAsync(firstOperation, firstSnapshot, CancellationToken.None);

        var sameStreamFullOutbox = async () => await store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence, "same-stream"),
            CreateSnapshotMutation(expectedRevision: 1, "changed-snapshot"),
            CancellationToken.None);
        var otherStreamFullOutbox = async () => await store.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence, "other-stream") with { StreamId = OtherStream },
            new(OtherStream, CreatePayload("other-snapshot"), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);

        var sameStreamException = await Assert.That(sameStreamFullOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var otherStreamException = await Assert.That(otherStreamFullOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var firstRecovery = await store.RecoverStreamAsync(Stream, firstSubscription, CancellationToken.None);
        var secondRecovery = await store.RecoverStreamAsync(OtherStream, secondSubscription, CancellationToken.None);

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

    /// <summary>Verifies the default outbox accepts 10,000 operations and rejects operation 10,001 atomically.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task DefaultOutboxOperationLimitRejectsTenThousandAndFirstCommit()
    {
        const int operationLimit = 10_000;
        await using var store = new InMemoryLocalStoreAdapter(maximumRecordCount: 30_000, maximumEncodedBytes: LargeStoreEncodedBytes);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { Outbox = new() }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        for (var sequence = 1L; sequence <= operationLimit; sequence++)
        {
            _ = await store.CommitLocalOperationAsync(
                CreateOperation(sequence, "x"),
                CreateSnapshotMutation(sequence - 1, "x"),
                CancellationToken.None);
        }

        Func<Task> overflow = async () => await store.CommitLocalOperationAsync(
            CreateOperation(operationLimit + 1L, "x"),
            CreateSnapshotMutation(operationLimit, "changed"),
            CancellationToken.None);

        var exception = await Assert.That(overflow).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var recovery = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(exception.CanFitWhenEmpty).IsTrue();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(operationLimit);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(operationLimit + 1L);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo((long)operationLimit);
    }

    /// <summary>Verifies byte capacity counts retained pending operation envelopes and metadata.</summary>
    /// <returns>The asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected capacity exception was not observed.</exception>
    [Test]
    public async Task PendingOutboxByteCapacityCountsOperationEnvelopeAndMetadata()
    {
        var firstOperation = CreateOperationWithMetadata(FirstClientSequence, string.Empty, OutboxCapacityMetadataLength);
        var encodedBytes = GetOutboxOperationBytes(firstOperation);
        var singleOperationBudget = new OutboxOptions { MaxOperations = OutboxCapacityOperationLimit, MaxBytes = encodedBytes - 1, MaximumBlockedPublishers = 1 };
        await using var singleOperationStore = await CreateInitializedOutboxBoundedStoreAsync(singleOperationBudget);
        var singleSubscription = await singleOperationStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);

        Func<Task> tooLargeForEmptyOutbox = async () => await singleOperationStore.CommitLocalOperationAsync(
            firstOperation,
            CreateSnapshotMutation(expectedRevision: 0, string.Empty),
            CancellationToken.None);

        var emptyException = await Assert.That(tooLargeForEmptyOutbox).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var emptyRecovery = await singleOperationStore.RecoverStreamAsync(Stream, singleSubscription, CancellationToken.None);

        var cumulativeBudget = singleOperationBudget with { MaxBytes = (encodedBytes * DoubleOperationCapacity) - 1 };
        await using var cumulativeStore = await CreateInitializedOutboxBoundedStoreAsync(cumulativeBudget);
        var cumulativeSubscription = await cumulativeStore.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var firstSnapshot = CreateSnapshotMutation(expectedRevision: 0, "first");
        _ = await cumulativeStore.CommitLocalOperationAsync(
            firstOperation,
            firstSnapshot,
            CancellationToken.None);

        Func<Task> cumulativeOverflow = async () => await cumulativeStore.CommitLocalOperationAsync(
            CreateOperationWithMetadata(SecondClientSequence, string.Empty, OutboxCapacityMetadataLength),
            CreateSnapshotMutation(expectedRevision: 1, SecondPayloadText),
            CancellationToken.None);

        var cumulativeException = await Assert.That(cumulativeOverflow).ThrowsExactly<QueueCapacityExceededException>()
            ?? throw new InvalidOperationException(ExpectedCapacityExceptionMessage);
        var cumulativeRecovery = await cumulativeStore.RecoverStreamAsync(Stream, cumulativeSubscription, CancellationToken.None);

        await Assert.That(emptyException.CanFitWhenEmpty).IsFalse();
        await Assert.That(emptyRecovery.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(emptyRecovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(emptyRecovery.Snapshot).IsNull();
        await Assert.That(cumulativeException.CanFitWhenEmpty).IsTrue();
        await Assert.That(cumulativeRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(cumulativeRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(cumulativeRecovery.Snapshot?.State.Payload.ToArray().SequenceEqual(firstSnapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies terminal upload results release pending outbox capacity for later local commits.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task TerminalOperationReleasesPendingOutboxCapacity()
    {
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        await using var store = await CreateInitializedOutboxBoundedStoreAsync(outbox);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "first");
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);

        var second = await store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence, SecondPayloadText),
            CreateSnapshotMutation(expectedRevision: 1, SecondPayloadText),
            CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(second.ClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(ThirdClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Creates an initialized outbox-bounded store.</summary>
    /// <param name="outbox">The outbox capacity.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateInitializedOutboxBoundedStoreAsync(OutboxOptions outbox)
    {
        var store = new InMemoryLocalStoreAdapter(maximumRecordCount: 100, maximumEncodedBytes: OutboxCapacityBytes);
        try
        {
            await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);
            return store;
        }
        catch
        {
            await store.DisposeAsync();
            throw;
        }
    }

    /// <summary>Creates a representative operation with fixed-size metadata.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="metadataLength">The metadata text length.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperationWithMetadata(long clientSequence, string payloadText, int metadataLength) =>
        CreateOperation(clientSequence, payloadText) with
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
}
