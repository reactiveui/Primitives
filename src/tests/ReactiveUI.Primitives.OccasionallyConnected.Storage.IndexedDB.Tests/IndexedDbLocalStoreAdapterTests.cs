// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB.Tests;

/// <summary>Tests for the IndexedDB local store adapter.</summary>
public sealed class IndexedDbLocalStoreAdapterTests
{
    /// <summary>The browser client identifier used by the test store.</summary>
    private const string ClientId = "browser-client";

    /// <summary>The payload contract identifier used by test payloads.</summary>
    private const string DefaultContractId = "test-contract";

    /// <summary>The content type used by test payloads.</summary>
    private const string DefaultContentType = "application/json";

    /// <summary>The payload format version used by snapshot mutations.</summary>
    private const int DefaultFormatVersion = 1;

    /// <summary>The first upload attempt number.</summary>
    private const int FirstAttempt = 1;

    /// <summary>The initial snapshot revision.</summary>
    private const int FirstRevision = 1;

    /// <summary>The first client sequence number.</summary>
    private const int FirstSequence = 0;

    /// <summary>The maximum operation count leased in one batch.</summary>
    private const int LeaseLimit = 8;

    /// <summary>The maximum leased payload bytes.</summary>
    private const long MaximumLeaseBytes = 4096;

    /// <summary>The second snapshot revision.</summary>
    private const int SecondRevision = 2;

    /// <summary>The second client sequence number.</summary>
    private const int SecondSequence = 1;

    /// <summary>The store identity used in tests.</summary>
    private const string StoreIdentity = "orders";

    /// <summary>The stream name used in tests.</summary>
    private const string StreamName = "orders/42";

    /// <summary>The first snapshot payload marker.</summary>
    private const string SnapshotOne = "snapshot-1";

    /// <summary>The first remote cursor marker.</summary>
    private const string CursorOne = "cursor-1";

    /// <summary>The first lease duration in seconds.</summary>
    private const int FirstLeaseSeconds = 30;

    /// <summary>The sync retry backoff duration in seconds.</summary>
    private const int RetryDelaySeconds = 5;

    /// <summary>The snapshot revision after a dead-letter replacement.</summary>
    private const int ThirdRevision = 3;

    /// <summary>The fixed timestamp used by deterministic tests.</summary>
    private static readonly DateTimeOffset FixedTimestamp = new(2035, 6, 7, 8, 9, 10, TimeSpan.Zero);

    /// <summary>Verifies the adapter imports its module, persists identity, and disposes the module.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InitializeRecoverAndDisposeWorkflow()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);

        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var streamId = new StreamId(StreamName);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None);

        await Assert.That(runtime.Imports).HasSingleItem();
        await Assert.That(runtime.Imports[0]).IsEqualTo(
            "./_content/ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB/indexedDbInterop.js");
        await Assert.That(recovered.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(recovered.PendingOperations).IsEmpty();

        await adapter.DisposeAsync();
        await Assert.That(runtime.Module.DisposeCalled).IsTrue();
    }

    /// <summary>Verifies initialization rejects unsupported encryption-at-rest requirements.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncryptionRequirementIsRejected()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);

        await Assert.That(() => adapter.InitializeAsync(
            new(StoreIdentity, 1, true) { ClientId = ClientId },
            CancellationToken.None).AsTask()).Throws<NotSupportedException>();
    }

    /// <summary>Verifies compare-exchange retries preserve the committed subscription id.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionCreationRetriesAfterCompareExchangeConflict()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);

        runtime.Module.FailNextCompareExchange = true;
        var preferred = new SubscriptionId(Guid.NewGuid());
        var actual = await adapter.GetOrCreateSubscriptionIdAsync(
            new(StreamName),
            preferred,
            CancellationToken.None);

        await Assert.That(actual).IsEqualTo(preferred);
    }

    /// <summary>Verifies local commit, leasing, retry state, and remote sync results.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CommitLeaseRetryAndSyncWorkflow()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var streamId = new StreamId(StreamName);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None);

        var accepted = CreateOperation(streamId, FirstSequence, "accepted");
        var retryable = CreateOperation(streamId, SecondSequence, "retryable");
        _ = await adapter.CommitLocalOperationAsync(
            accepted,
            CreateSnapshotMutation(streamId, SnapshotOne, expectedRevision: 0),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            retryable,
            CreateSnapshotMutation(streamId, "snapshot-2", expectedRevision: FirstRevision),
            CancellationToken.None);

        var lease = await LeaseBatchContainingOperationAsync(adapter, streamId, accepted.OperationId);

        var barrier = await adapter.TryBeginRemoteAttemptAsync(
            lease.LeaseId,
            accepted.OperationId,
            FirstAttempt,
            CancellationToken.None);
        await Assert.That(barrier.MaySend).IsTrue();

        var retryState = RetryState.Start(FixedTimestamp) with
        {
            DueUtc = FixedTimestamp.AddMinutes(1),
            PreviousDelay = TimeSpan.FromSeconds(FirstLeaseSeconds),
            TransientAttemptCount = 1,
        };
        await adapter.SaveRetryStateAsync(retryable.OperationId, retryState, CancellationToken.None);
        var changedSnapshots = await ApplySyncResultsAsync(adapter, streamId, lease, accepted.OperationId, retryable.OperationId);

        var acceptedStatus = await adapter.GetOperationStatusAsync(accepted.OperationId, CancellationToken.None);
        var retryableStatus = await adapter.GetOperationStatusAsync(retryable.OperationId, CancellationToken.None);
        var storedRetryState = await adapter.GetRetryStateAsync(retryable.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(
            streamId,
            await adapter.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None),
            CancellationToken.None);

        await Assert.That(changedSnapshots).IsEmpty();
        await Assert.That(acceptedStatus!.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(retryableStatus!.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(storedRetryState).IsEqualTo(retryState);
        await Assert.That(recovered.PendingOperations).HasSingleItem();
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(retryable.OperationId);
    }

    /// <summary>Verifies remote batches deduplicate inbox entries and complete local operations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RemoteBatchDeduplicatesEventsAndCompletesOperations()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var streamId = new StreamId(StreamName);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None);

        var operation = CreateOperation(streamId, FirstSequence, "local");
        _ = await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(streamId, SnapshotOne, expectedRevision: 0),
            CancellationToken.None);

        var eventId = Guid.NewGuid();
        var batch = new RemoteEventBatch(
            Guid.NewGuid(),
            streamId,
            previousCursor: null,
            nextCursor: CursorOne,
            [
                CreateRemoteEvent(streamId, eventId, CursorOne),
                CreateRemoteEvent(streamId, eventId, CursorOne),
            ]) { CompletedOperations = [new RemoteOperationCompletion(new RemoteEventOrigin(ClientId, operation.OperationId), [eventId])] };

        var result = await adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(streamId, "server-snapshot", expectedRevision: FirstRevision, authoritative: "authoritative"),
            CancellationToken.None);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(streamId, [eventId, Guid.NewGuid()], CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(1);
        await Assert.That(unapplied).HasSingleItem();
        await Assert.That(status!.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.ReplayOperations).IsEmpty();
        await Assert.That(recovered.Snapshot!.AuthoritativeState).IsNotNull();
    }

    /// <summary>Verifies dead-letter transitions, lease release, and compaction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeadLetterReleaseAndCompactionWorkflow()
    {
        var runtime = new FakeJsRuntime();
        await using var adapter = CreateAdapter(runtime);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var streamId = new StreamId(StreamName);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(streamId, null, CancellationToken.None);

        var deadLetter = CreateOperation(streamId, FirstSequence, "dead-letter");
        var releasable = CreateOperation(streamId, SecondSequence, "release");
        _ = await adapter.CommitLocalOperationAsync(
            deadLetter,
            CreateSnapshotMutation(streamId, SnapshotOne, expectedRevision: 0),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            releasable,
            CreateSnapshotMutation(streamId, "snapshot-2", expectedRevision: FirstRevision),
            CancellationToken.None);

        var lease = await LeaseBatchContainingOperationAsync(adapter, streamId, deadLetter.OperationId);

        var deadLetterSnapshot = await adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            deadLetter.OperationId,
            "permanent",
            CreateSnapshotMutation(streamId, "snapshot-3", expectedRevision: SecondRevision),
            CancellationToken.None);
        await adapter.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromSeconds(FirstLeaseSeconds), CancellationToken.None);
        await adapter.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var compacted = await adapter.CompactAsync(
            new(streamId, DateTimeOffset.MaxValue, 0),
            CancellationToken.None);

        var deadLetterStatus = await adapter.GetOperationStatusAsync(deadLetter.OperationId, CancellationToken.None);
        var releaseStatus = await adapter.GetOperationStatusAsync(releasable.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(streamId, subscriptionId, CancellationToken.None);

        await Assert.That(deadLetterSnapshot.Revision).IsEqualTo(ThirdRevision);
        await Assert.That(compacted.RecordsRemoved).IsEqualTo(1);
        await Assert.That(deadLetterStatus).IsNull();
        await Assert.That(releaseStatus!.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(recovered.DeadLetters).HasSingleItem();
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(deadLetter.OperationId);
    }

    /// <summary>Creates one adapter instance backed by the fake JS runtime.</summary>
    /// <param name="runtime">The fake JS runtime.</param>
    /// <returns>The adapter under test.</returns>
    private static IndexedDbLocalStoreAdapter CreateAdapter(FakeJsRuntime runtime) =>
        new(runtime, new FixedTimeProvider(FixedTimestamp));

    /// <summary>Creates the default store initialization request.</summary>
    /// <returns>The default initialization request.</returns>
    private static LocalStoreInitialization CreateInitialization() => new(StoreIdentity, 1, false) { ClientId = ClientId };

    /// <summary>Leases the next pending operation batch for one stream.</summary>
    /// <param name="adapter">The adapter under test.</param>
    /// <param name="streamId">The target stream.</param>
    /// <returns>The leased operation batch.</returns>
    private static async Task<LeasedOperationBatch> LeaseNextBatchAsync(IndexedDbLocalStoreAdapter adapter, StreamId streamId)
    {
        await using var enumerator = adapter.LeasePendingOperationsAsync(
            new(streamId, LeaseLimit, MaximumLeaseBytes, TimeSpan.FromMinutes(1)),
            CancellationToken.None).GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        return enumerator.Current;
    }

    /// <summary>Leases batches until the requested operation is present.</summary>
    /// <param name="adapter">The adapter under test.</param>
    /// <param name="streamId">The target stream.</param>
    /// <param name="operationId">The operation to locate.</param>
    /// <returns>The batch that contains the requested operation.</returns>
    /// <exception cref="InvalidOperationException">The requested operation was not leased after repeated attempts.</exception>
    private static async Task<LeasedOperationBatch> LeaseBatchContainingOperationAsync(
        IndexedDbLocalStoreAdapter adapter,
        StreamId streamId,
        OperationId operationId)
    {
        for (var attempt = 0; attempt < LeaseLimit; attempt++)
        {
            var batch = await LeaseNextBatchAsync(adapter, streamId);
            if (LeaseContainsOperation(batch, operationId))
            {
                return batch;
            }
        }

        throw new InvalidOperationException("The requested operation was not leased.");
    }

    /// <summary>Applies accepted and retryable sync results for the requested operations.</summary>
    /// <param name="adapter">The adapter under test.</param>
    /// <param name="streamId">The target stream.</param>
    /// <param name="acceptedLease">The lease that contains the accepted operation.</param>
    /// <param name="acceptedOperationId">The accepted operation identifier.</param>
    /// <param name="retryableOperationId">The retryable operation identifier.</param>
    /// <returns>The combined changed snapshots.</returns>
    private static async Task<IReadOnlyList<LocalSnapshot>> ApplySyncResultsAsync(
        IndexedDbLocalStoreAdapter adapter,
        StreamId streamId,
        LeasedOperationBatch acceptedLease,
        OperationId acceptedOperationId,
        OperationId retryableOperationId)
    {
        if (LeaseContainsOperation(acceptedLease, retryableOperationId))
        {
            return await adapter.ApplySyncResultAsync(
                acceptedLease.LeaseId,
                new(
                    Guid.NewGuid(),
                    [
                        new OperationSyncResult(acceptedOperationId, OperationResultKind.Accepted, null, null),
                        new OperationSyncResult(retryableOperationId, OperationResultKind.Retryable, "retry", null),
                    ],
                    null,
                    TimeSpan.FromSeconds(RetryDelaySeconds)),
                [],
                CancellationToken.None);
        }

        var retryLease = await LeaseBatchContainingOperationAsync(adapter, streamId, retryableOperationId);
        var acceptedSnapshots = await adapter.ApplySyncResultAsync(
            acceptedLease.LeaseId,
            new(
                Guid.NewGuid(),
                [new OperationSyncResult(acceptedOperationId, OperationResultKind.Accepted, null, null)],
                null,
                TimeSpan.FromSeconds(RetryDelaySeconds)),
            [],
            CancellationToken.None);
        var retrySnapshots = await adapter.ApplySyncResultAsync(
            retryLease.LeaseId,
            new(
                Guid.NewGuid(),
                [new OperationSyncResult(retryableOperationId, OperationResultKind.Retryable, "retry", null)],
                null,
                TimeSpan.FromSeconds(RetryDelaySeconds)),
            [],
            CancellationToken.None);
        return [.. acceptedSnapshots, .. retrySnapshots];
    }

    /// <summary>Determines whether one leased batch contains the requested operation.</summary>
    /// <param name="batch">The leased operation batch.</param>
    /// <param name="operationId">The requested operation identifier.</param>
    /// <returns>True when the batch contains the operation; otherwise false.</returns>
    private static bool LeaseContainsOperation(LeasedOperationBatch batch, OperationId operationId)
    {
        foreach (var operation in batch.Operations)
        {
            if (operation.OperationId == operationId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Creates one local sync operation.</summary>
    /// <param name="streamId">The target stream.</param>
    /// <param name="sequence">The client sequence number.</param>
    /// <param name="payload">The payload marker.</param>
    /// <returns>The created sync operation.</returns>
    private static SyncOperation CreateOperation(StreamId streamId, long sequence, string payload) =>
        new() { ClientSequence = sequence, OperationId = OperationId.New(), Payload = CreatePayload(payload), StreamId = streamId, TimestampUtc = FixedTimestamp, Type = SyncOperationType.Update };

    /// <summary>Creates one snapshot mutation for the test stream.</summary>
    /// <param name="streamId">The target stream.</param>
    /// <param name="payload">The payload marker.</param>
    /// <param name="expectedRevision">The expected current revision.</param>
    /// <param name="authoritative">The optional authoritative payload marker.</param>
    /// <returns>The created snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(
        StreamId streamId,
        string payload,
        long expectedRevision,
        string? authoritative = null)
    {
        var mutation = new SnapshotMutation(streamId, CreatePayload(payload), DefaultFormatVersion, expectedRevision);
        return authoritative is null ? mutation : mutation with { AuthoritativeState = CreatePayload(authoritative) };
    }

    /// <summary>Creates one payload envelope from the supplied marker string.</summary>
    /// <param name="value">The payload marker.</param>
    /// <returns>The created payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        return new(
            DefaultContractId,
            schemaVersion: 1,
            DefaultContentType,
            bytes,
            Convert.ToHexString(SHA256.HashData(bytes)));
    }

    /// <summary>Creates one remote event for the supplied stream and cursor.</summary>
    /// <param name="streamId">The target stream.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="cursor">The cursor for the event.</param>
    /// <returns>The created remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(StreamId streamId, Guid eventId, string cursor) =>
        new(
            eventId,
            streamId,
            cursor,
            FixedTimestamp,
            causedByOperationId: null,
            CreatePayload($"event-{eventId:N}"),
            new Dictionary<string, string>());

    /// <summary>Provides a deterministic clock for tests.</summary>
    /// <param name="utcNow">The fixed UTC time returned by the provider.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
