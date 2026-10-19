// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive inclusion tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The second remote cursor used by inclusion tests.</summary>
    private const string SecondRemoteCursor = "cursor-2";

    /// <summary>The revision after the initial remote apply and a mixed remote apply.</summary>
    private const int MixedRemoteBatchRevision = 2;

    /// <summary>The small record count used for receive-bound tests.</summary>
    private const int BoundedReceiveRecordCount = 8;

    /// <summary>The receive completion count that exceeds the small record bound.</summary>
    private const int ExceededReceiveRecordCount = BoundedReceiveRecordCount + 1;

    /// <summary>The authoritative payload used by completion tests.</summary>
    private const string AuthoritativePayloadText = "authoritative";

    /// <summary>Accepted upload results without authoritative inclusion remain replay-visible after recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AcceptedOperationWithoutReceiveInclusionRemainsReplayVisible()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        await SetServerResultAsync(store, operation, OperationResultKind.Accepted);

        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Own authoritative completions retain the operation for correlation but remove it from replay.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task OwnCompletionRetainsPendingOperationButRemovesItFromReplay()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        var result = await store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);

        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var afterResult = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(afterResult.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(afterResult.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Accepted upload results stop replay after later authoritative receive inclusion.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AcceptedOperationWithLaterCompletionStopsReplay()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        await SetServerResultAsync(store, operation, OperationResultKind.Accepted);
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        var result = await store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Mixed duplicate and new remote batches deduplicate atomically while advancing snapshot and cursor.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task MixedDuplicateAndNewRemoteBatchAppliesOnlyNewEventsAndAdvancesCursor()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var duplicate = CreateRemoteEvent(RemoteCursor);
        _ = await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [duplicate]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        var fresh = CreateRemoteEvent(SecondRemoteCursor);

        var result = await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(RemoteCursor, SecondRemoteCursor, [duplicate, fresh]),
            new(Stream, CreatePayload("mixed-remote"), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [duplicate.EventId, fresh.EventId], CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(1);
        await Assert.That(result.SnapshotRevision).IsEqualTo(MixedRemoteBatchRevision);
        await Assert.That(recovery.ServerCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Stale receive inclusion batches roll back inclusion, snapshot, cursor, and inbox updates together.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task StaleReceiveInclusionBatchDoesNotMarkOperationIncluded()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        await SetServerResultAsync(store, operation, OperationResultKind.Accepted);
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        Func<Task> staleApply = () => store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None).AsTask();

        await Assert.That(staleApply).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Unknown own zero-event completions advance the receive cursor without creating replay state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UnknownOwnCompletionWithoutEventsAdvancesCursorWithoutReplaySideEffects()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var batch = CreateRemoteBatch(null, RemoteCursor, []) with
        {
            CompletedOperations = [new(new(ClientId, OperationId.New()), [])],
        };

        var result = await store.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(0);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Known own completions require authoritative state before mutating inclusion state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task KnownOwnCompletionWithoutAuthoritativeStateRejectsWithoutMutation()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        Func<Task> apply = () => store.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 1), CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Same-client completions for a local operation in another stream reject atomically.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task SameClientCompletionForDifferentStreamRejectsWithoutMutation()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var other = await CommitOperationAsync(store, OtherStream, FirstClientSequence, OperationPayloadText);
        var remoteEvent = CreateOriginEvent(RemoteCursor, other.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, other.OperationId), [remoteEvent.EventId])],
        };

        Func<Task> apply = () => store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Authoritatively included synchronized operations can be compacted and reclaim their inclusion marker.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CompactionRemovesIncludedSynchronizedOperation()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedBoundStoreAsync(timeProvider: clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };
        _ = await store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        await SetServerResultAsync(store, operation, OperationResultKind.Accepted);
        clock.Advance(TimeSpan.FromDays(CompactionAdvanceDays));

        var result = await store.CompactAsync(new(Stream, clock.GetUtcNow(), 0), CancellationToken.None);

        await Assert.That(result.RecordsRemoved).IsEqualTo(1);
        await Assert.That(await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Receive validation admits completion declarations at the configured record bound.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ReceiveCompletionCountAtConfiguredBoundApplies()
    {
        await using var store = await CreateInitializedBoundStoreAsync(BoundedReceiveRecordCount);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var batch = CreateCompletionOnlyBatch(BoundedReceiveRecordCount, RemoteCursor);

        var result = await store.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(0);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.ServerCursor).IsEqualTo(RemoteCursor);
    }

    /// <summary>Zero-event completion flooding over the configured record bound is rejected before mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ZeroEventCompletionFloodingOverConfiguredBoundRejectsWithoutMutation()
    {
        await using var store = await CreateInitializedBoundStoreAsync(BoundedReceiveRecordCount);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var batch = CreateCompletionOnlyBatch(ExceededReceiveRecordCount, RemoteCursor);

        Func<Task> apply = () => store.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<ArgumentException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Creates a store bound to the default client identity.</summary>
    /// <param name="maximumRecordCount">The maximum persisted record count.</param>
    /// <param name="timeProvider">The optional time provider.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateInitializedBoundStoreAsync(int maximumRecordCount = 100, TimeProvider? timeProvider = null)
    {
        var store = timeProvider is null
            ? new InMemoryLocalStoreAdapter(maximumRecordCount, maximumEncodedBytes: 4096)
            : new InMemoryLocalStoreAdapter(timeProvider, maximumRecordCount, maximumEncodedBytes: 4096, new RetentionOptions());
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        return store;
    }

    /// <summary>Creates a remote batch with only completion declarations.</summary>
    /// <param name="count">The completion count.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateCompletionOnlyBatch(int count, string nextCursor)
    {
        var completions = Enumerable.Range(0, count)
            .Select(static index => new RemoteOperationCompletion(new($"client-{index}", OperationId.New()), []))
            .ToArray();
        return CreateRemoteBatch(null, nextCursor, []) with { CompletedOperations = completions };
    }

    /// <summary>Creates a remote event with an authoritative origin.</summary>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="operationId">The local operation identifier.</param>
    /// <param name="clientId">The origin client identity.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateOriginEvent(string serverCursor, OperationId operationId, string clientId) =>
        new(Guid.NewGuid(), Stream, serverCursor, new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), operationId, CreatePayload(RemotePayloadText), new Dictionary<string, string>())
        {
            Origin = new(clientId, operationId),
        };
}
