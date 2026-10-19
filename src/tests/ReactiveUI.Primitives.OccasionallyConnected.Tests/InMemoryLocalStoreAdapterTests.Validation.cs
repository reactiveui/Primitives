// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies public input validation for the in-memory store.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies an undefined semantic operation type cannot enter recovered intent.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UndefinedOperationTypeCannotBeCommitted()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(1) with { Type = (SyncOperationType)(-1) };
        Func<Task> commit = async () => _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        await Assert.That(commit).ThrowsExactly<ArgumentException>();
        await Assert.That((await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None)).PendingOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies malformed remote envelopes are rejected before cursor or snapshot advancement.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task MalformedRemoteEnvelopeCannotAdvanceCursor()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var payload = CreatePayload(RemotePayloadText);
        var now = DateTimeOffset.UnixEpoch;
        RemoteEvent[] malformed =
        [
            new(Guid.Empty, Stream, RemoteCursor, now, null, payload, new Dictionary<string, string>()),
            new(Guid.NewGuid(), OtherStream, RemoteCursor, now, null, payload, new Dictionary<string, string>()),
            new(Guid.NewGuid(), Stream, " ", now, null, payload, new Dictionary<string, string>()),
            new(Guid.NewGuid(), Stream, RemoteCursor, now, default(OperationId), payload, new Dictionary<string, string>()),
        ];
        foreach (var remoteEvent in malformed)
        {
            Func<Task> receive = async () => _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(null, RemoteCursor, [remoteEvent]), CreateSnapshotMutation(0), CancellationToken.None);
            await Assert.That(receive).ThrowsExactly<ArgumentException>();
        }

        var validEvent = CreateRemoteEvent(RemoteCursor);
        RemoteEventBatch[] invalidBatches =
        [
            new(Guid.Empty, Stream, null, RemoteCursor, [validEvent]),
            new(Guid.NewGuid(), Stream, null, " ", [validEvent]),
            new(Guid.NewGuid(), OtherStream, null, RemoteCursor, []),
        ];
        foreach (var batch in invalidBatches)
        {
            Func<Task> receive = async () => _ = await store.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(0), CancellationToken.None);
            await Assert.That(receive).ThrowsExactly<ArgumentException>();
        }

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot).IsNull();
    }

    /// <summary>Verifies retained-storage capacities must be positive.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStorageCapacitiesAreNotPositive_ThenConstructionIsRejected()
    {
        Action zeroRecordCount = static () => _ = new InMemoryLocalStoreAdapter(0, 1);
        Action negativeRecordCount = static () => _ = new InMemoryLocalStoreAdapter(-1, 1);
        Action zeroByteCount = static () => _ = new InMemoryLocalStoreAdapter(1, 0);
        Action negativeByteCount = static () => _ = new InMemoryLocalStoreAdapter(1, -1);

        await Assert.That(zeroRecordCount).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(negativeRecordCount).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(zeroByteCount).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(negativeByteCount).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies runtime remote apply validation covers header and event identity short-circuit branches.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RemoteApplyValidationRejectsWhitespaceCursorAndEmptyEventIdentity()
    {
        var payload = CreatePayload(RemotePayloadText);
        var now = DateTimeOffset.UnixEpoch;
        var validEvent = CreateRemoteEvent(RemoteCursor);
        var emptyEvent = new RemoteEvent(Guid.Empty, Stream, RemoteCursor, now, null, payload, new Dictionary<string, string>());
        var duplicateEvent = new RemoteEvent(
            validEvent.EventId,
            validEvent.StreamId,
            "cursor-2",
            validEvent.CommittedAtUtc,
            validEvent.CausedByOperationId,
            validEvent.Payload,
            validEvent.Metadata);
        var whitespaceCursor = new RemoteEventBatch(Guid.NewGuid(), Stream, null, " ", []);
        var emptyEventBatch = CreateRemoteBatch(null, RemoteCursor, [emptyEvent]);
        var duplicateEventBatch = CreateRemoteBatch(null, RemoteCursor, [validEvent, duplicateEvent]);

        await Assert.That(() => InMemoryLocalStoreAdapterValidation.ValidateRemoteApplyInput(whitespaceCursor, CreateSnapshotMutation(0)))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => InMemoryLocalStoreAdapterValidation.ValidateRemoteApplyInput(emptyEventBatch, CreateSnapshotMutation(0)))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => InMemoryLocalStoreAdapterValidation.ValidateRemoteApplyInput(duplicateEventBatch, CreateSnapshotMutation(0)))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies standalone dead-letter reason validation rejects blank and oversized values.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DeadLetterReasonValidationRejectsBlankAndOversizedReasons()
    {
        var oversizedReason = new string('x', OversizedDeadLetterReasonLength);

        await Assert.That(static () => InMemoryLocalStoreAdapterValidation.ValidateDeadLetterReasonCode(" "))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => InMemoryLocalStoreAdapterValidation.ValidateDeadLetterReasonCode(oversizedReason))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies malformed stream and subscription identifiers are rejected before state changes.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenSubscriptionIdentifiersAreMalformed_ThenSubscriptionStateIsUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        Func<Task> emptyStream = async () => _ = await store.GetOrCreateSubscriptionIdAsync(default, null, CancellationToken.None);
        Func<Task> emptyPreferredSubscription = async () => _ = await store.GetOrCreateSubscriptionIdAsync(Stream, default(SubscriptionId), CancellationToken.None);
        Func<Task> emptyRecoverySubscription = async () => _ = await store.RecoverStreamAsync(Stream, default, CancellationToken.None);

        await Assert.That(emptyStream).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyPreferredSubscription).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyRecoverySubscription).ThrowsExactly<ArgumentException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.SubscriptionId).IsEqualTo(subscription);
    }

    /// <summary>Verifies malformed local commits are rejected without advancing the stream snapshot.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitInputIsMalformed_ThenExistingSnapshotAndOperationsAreUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var committed = await store.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var invalidOperation = CreateOperation(SecondClientSequence) with { ClientSequence = 0 };
        var invalidSnapshot = CreateSnapshotMutation(1) with { ExpectedRevision = -1 };
        Func<Task> malformedOperation = async () => _ = await store.CommitLocalOperationAsync(invalidOperation, CreateSnapshotMutation(1), CancellationToken.None);
        Func<Task> malformedSnapshot = async () => _ = await store.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), invalidSnapshot, CancellationToken.None);

        await Assert.That(malformedOperation).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(malformedSnapshot).ThrowsExactly<ArgumentOutOfRangeException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(committed.SnapshotRevision);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies invalid lease bounds are rejected without leasing pending work.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseBoundsAreInvalid_ThenPendingOperationRemainsUnleased()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        Func<Task> zeroOperations = async () => _ = await LeaseSingleBatchAsync(store, new(Stream, 0, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        Func<Task> zeroBytes = async () => _ = await LeaseSingleBatchAsync(store, new(Stream, 1, 0, TimeSpan.FromMinutes(1)));
        Func<Task> infiniteDuration = async () => _ = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.MaxValue));

        await Assert.That(zeroOperations).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(zeroBytes).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(infiniteDuration).ThrowsExactly<ArgumentOutOfRangeException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.SavedLocally);
    }

    /// <summary>Verifies malformed remote batches are rejected without recording inbox entries.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteBatchIsMalformed_ThenRemoteStateIsUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);
        var duplicateEvent = new RemoteEvent(
            remoteEvent.EventId,
            remoteEvent.StreamId,
            "cursor-2",
            remoteEvent.CommittedAtUtc,
            remoteEvent.CausedByOperationId,
            remoteEvent.Payload,
            remoteEvent.Metadata);
        var malformedBatch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent, duplicateEvent]);
        Func<Task> applyMalformedBatch = async () => _ = await store.ApplyRemoteBatchAsync(malformedBatch, CreateSnapshotMutation(0), CancellationToken.None);

        await Assert.That(applyMalformedBatch).ThrowsExactly<ArgumentException>();
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies a duplicate operation identifier cannot overwrite its original canonical intent.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationIdentifierHasDifferentIntent_ThenOriginalReceiptAndStateArePreserved()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(0);
        var original = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        var changedIntent = operation with { Metadata = new Dictionary<string, string> { ["origin"] = "other" } };
        Func<Task> duplicateWithChangedMetadata = async () => _ = await store.CommitLocalOperationAsync(changedIntent, snapshot, CancellationToken.None);

        await Assert.That(duplicateWithChangedMetadata).ThrowsExactly<InvalidOperationException>();
        var replay = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(replay).IsEqualTo(original);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].Metadata["origin"]).IsEqualTo("unit-test");
    }
}
