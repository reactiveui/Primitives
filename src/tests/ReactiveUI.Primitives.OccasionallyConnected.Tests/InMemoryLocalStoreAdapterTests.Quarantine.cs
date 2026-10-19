// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Quarantine tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The invalid enum value used by validation tests.</summary>
    private const int InvalidEnumValue = -1;

    /// <summary>Verifies quarantine stores bounded evidence and prevents upload leasing for the affected stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncStoresMarkerAndBlocksAffectedStreamLease()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);

        var result = await store.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        var stored = await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var leased = await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(result.Created).IsTrue();
        await Assert.That(stored?.StreamId).IsEqualTo(Stream);
        await Assert.That(stored?.Evidence.PayloadLength).IsEqualTo(operation.Payload.PayloadLength);
        await Assert.That(leased).IsNull();
    }

    /// <summary>Verifies quarantine is idempotent and marker lookup returns null for unknown streams.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncReturnsExistingMarkerAndMissingLookupReturnsNull()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = CreateQuarantineRequest(subscriptionId, operation);

        var first = await store.QuarantinePayloadAsync(request, CancellationToken.None);
        var second = await store.QuarantinePayloadAsync(request, CancellationToken.None);
        var missing = await store.GetPayloadQuarantineAsync(OtherStream, CancellationToken.None);

        await Assert.That(first.Created).IsTrue();
        await Assert.That(second.Created).IsFalse();
        await Assert.That(second.Record.QuarantineId).IsEqualTo(first.Record.QuarantineId);
        await Assert.That(missing).IsNull();
    }

    /// <summary>Verifies quarantined streams reject further mutations that would retain more local data.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenStreamIsQuarantined_ThenFurtherLocalMutationFailsClosed()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        _ = await store.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> commit = () => store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence),
            CreateSnapshotMutation(1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(commit);

        await Assert.That(exception?.Message).Contains("quarantined");
    }

    /// <summary>Verifies malformed quarantine requests are rejected before a marker is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsMalformedRequests()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var evidence = CreateRawEvidence();
        Func<Task> emptySubscription = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { SubscriptionId = new(Guid.Empty), Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> emptyOperation = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { OperationId = new(Guid.Empty), Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> emptyEvent = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { EventId = Guid.Empty, Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> invalidSource = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { Source = (LocalPayloadQuarantineSource)InvalidEnumValue, Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> invalidReason = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { Reason = (LocalPayloadQuarantineReason)InvalidEnumValue, Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> defaultObserved = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { ObservedAtUtc = default, Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> missingEvidence = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { Evidence = null, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> negativeEvidence = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(operation) with { Evidence = CreateRawEvidence(payloadLength: InvalidEnumValue), Envelope = null },
            CancellationToken.None).AsTask();

        await Assert.That(emptySubscription).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyOperation).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyEvent).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidSource).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidReason).ThrowsExactly<ArgumentException>();
        await Assert.That(defaultObserved).ThrowsExactly<ArgumentException>();
        await Assert.That(missingEvidence).ThrowsExactly<ArgumentException>();
        await Assert.That(negativeEvidence).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies valid quarantine source and reason variants are accepted with raw bounded evidence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncAcceptsValidSourcesReasonsAndNullableEvidenceSchema()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var snapshotStream = new StreamId("sensor/snapshot");
        var remoteStream = new StreamId("sensor/remote");
        var upcastStream = new StreamId("sensor/upcast");
        var persistedStream = new StreamId("sensor/persisted");
        _ = await store.GetOrCreateSubscriptionIdAsync(snapshotStream, null, CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(remoteStream, null, CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(upcastStream, null, CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(persistedStream, null, CancellationToken.None);

        var snapshot = await store.QuarantinePayloadAsync(
            CreateRawQuarantineRequest(snapshotStream, LocalPayloadQuarantineSource.Snapshot, LocalPayloadQuarantineReason.SchemaRejected, SchemaVersion),
            CancellationToken.None);
        var remote = await store.QuarantinePayloadAsync(
            CreateRawQuarantineRequest(remoteStream, LocalPayloadQuarantineSource.RemoteEvent, LocalPayloadQuarantineReason.UpcastFailed, SchemaVersion),
            CancellationToken.None);
        var persisted = await store.QuarantinePayloadAsync(
            CreateRawQuarantineRequest(persistedStream, LocalPayloadQuarantineSource.Snapshot, LocalPayloadQuarantineReason.PersistedRecordCorrupt, null),
            CancellationToken.None);
        var upcast = await store.QuarantinePayloadAsync(
            CreateRawQuarantineRequest(upcastStream, LocalPayloadQuarantineSource.OutboxOperation, LocalPayloadQuarantineReason.UpcastFailed, null),
            CancellationToken.None);

        await Assert.That(snapshot.Record.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(snapshot.Record.Reason).IsEqualTo(LocalPayloadQuarantineReason.SchemaRejected);
        await Assert.That(remote.Record.Source).IsEqualTo(LocalPayloadQuarantineSource.RemoteEvent);
        await Assert.That(remote.Record.Reason).IsEqualTo(LocalPayloadQuarantineReason.UpcastFailed);
        await Assert.That(persisted.Record.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(persisted.Record.Evidence.SchemaVersion).IsNull();
        await Assert.That(upcast.Record.Evidence.SchemaVersion).IsNull();
    }

    /// <summary>Creates a quarantine request for a committed operation.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The request.</returns>
    private static LocalPayloadQuarantineRequest CreateQuarantineRequest(SubscriptionId subscriptionId, SyncOperation operation) =>
        CreateQuarantineRequest(operation) with { SubscriptionId = subscriptionId };

    /// <summary>Creates a quarantine request for an operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The request.</returns>
    private static LocalPayloadQuarantineRequest CreateQuarantineRequest(SyncOperation operation) =>
        new()
        {
            StreamId = operation.StreamId,
            OperationId = operation.OperationId,
            Source = LocalPayloadQuarantineSource.OutboxOperation,
            Reason = LocalPayloadQuarantineReason.PayloadHashMismatch,
            ReasonCode = "PayloadHashMismatch",
            Envelope = operation.Payload,
            ObservedAtUtc = operation.TimestampUtc,
        };

    /// <summary>Creates a raw evidence quarantine request.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="source">The source.</param>
    /// <param name="reason">The reason.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <returns>The request.</returns>
    private static LocalPayloadQuarantineRequest CreateRawQuarantineRequest(
        StreamId streamId,
        LocalPayloadQuarantineSource source,
        LocalPayloadQuarantineReason reason,
        int? schemaVersion) =>
        new() { StreamId = streamId, Source = source, Reason = reason, ReasonCode = reason.ToString(), Evidence = CreateRawEvidence(schemaVersion), ObservedAtUtc = DateTimeOffset.UnixEpoch };

    /// <summary>Creates raw quarantine evidence.</summary>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="payloadLength">The payload length.</param>
    /// <returns>The evidence.</returns>
    private static LocalPayloadQuarantineEvidence CreateRawEvidence(int? schemaVersion = SchemaVersion, int payloadLength = FirstClientSequence) =>
        new("reading", schemaVersion, "application/json", payloadLength, "hash", "x"u8.ToArray());
}
