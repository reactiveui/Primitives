// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Receive inclusion tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The second remote cursor used by inclusion tests.</summary>
    private const string SecondReceiveCursor = "cursor-2";

    /// <summary>The revision after the initial remote apply and a mixed remote apply.</summary>
    private const int MixedRemoteBatchRevision = 2;

    /// <summary>The SQLite receive batch count bound mirrored from the durable store.</summary>
    private const int SqliteReceiveBatchBound = 128;

    /// <summary>The receive completion count that exceeds the SQLite bound.</summary>
    private const int ExceededSqliteReceiveBatchBound = SqliteReceiveBatchBound + 1;

    /// <summary>The authoritative payload used by completion tests.</summary>
    private const string AuthoritativePayloadText = "authoritative";

    /// <summary>Accepted upload results without authoritative inclusion remain replay-visible after recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AcceptedOperationWithoutReceiveInclusionRemainsReplayVisible()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        await ApplyServerResultAsync(adapter, operation, OperationResultKind.Accepted);

        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Own authoritative completions retain unresolved upload correlation but remove the operation from replay.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task OwnCompletionRetainsPendingOperationButRemovesItFromReplay()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var remoteEvent = CreateReceiveOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        var result = await adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);

        await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var afterResult = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(afterResult.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(afterResult.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Accepted upload results stop replay after later authoritative receive inclusion.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AcceptedOperationWithLaterCompletionStopsReplay()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        await ApplyServerResultAsync(adapter, operation, OperationResultKind.Accepted);
        var remoteEvent = CreateReceiveOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        var result = await adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

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
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var duplicate = CreateRemoteEvent(RemoteCursor);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [duplicate]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        var fresh = CreateRemoteEvent(SecondReceiveCursor);

        var result = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(RemoteCursor, SecondReceiveCursor, [duplicate, fresh]),
            CreateSnapshotMutation(expectedRevision: 1),
            CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(Stream, [duplicate.EventId, fresh.EventId], CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(1);
        await Assert.That(result.SnapshotRevision).IsEqualTo(MixedRemoteBatchRevision);
        await Assert.That(recovery.ServerCursor).IsEqualTo(SecondReceiveCursor);
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Stale receive inclusion batches roll back inclusion, snapshot, cursor, and inbox updates together.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task StaleReceiveInclusionBatchDoesNotMarkOperationIncluded()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        await ApplyServerResultAsync(adapter, operation, OperationResultKind.Accepted);
        var remoteEvent = CreateReceiveOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };

        Func<Task> staleApply = () => adapter.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None).AsTask();

        await Assert.That(staleApply).ThrowsExactly<InvalidOperationException>();
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Unknown own completion declarations advance receive state without creating orphan inclusion rows.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UnknownOwnCompletionIsIgnoredWithoutAuthoritativeState()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var unknownOperationId = OperationId.New();
        var batch = CreateRemoteBatch(null, RemoteCursor, []) with
        {
            CompletedOperations = [new(new(ClientId, unknownOperationId), [])],
        };

        var result = await adapter.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(0);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Matching local completions for another stream fail before mutating receive state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task SameClientCompletionForDifferentStreamRejectsWithoutMutation()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var otherStream = new StreamId("sensor/other");
        _ = await adapter.GetOrCreateSubscriptionIdAsync(otherStream, null, CancellationToken.None);
        var batch = new RemoteEventBatch(Guid.NewGuid(), otherStream, null, RemoteCursor, []) { CompletedOperations = [new(new(ClientId, operation.OperationId), [])] };
        var mutation = new SnapshotMutation(otherStream, CreatePayload("other"), FormatVersion: 1, ExpectedRevision: 0) { AuthoritativeState = CreatePayload(AuthoritativePayloadText) };

        Func<Task> apply = () => adapter.ApplyRemoteBatchAsync(batch, mutation, CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Known same-stream local completions require authoritative state before inclusion is durable.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task SameClientCompletionWithoutAuthoritativeStateRejectsWithoutMutation()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var batch = CreateRemoteBatch(null, RemoteCursor, []) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [])],
        };

        Func<Task> apply = () => adapter.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 1), CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Receive validation admits completion declarations at the SQLite receive bound.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ReceiveCompletionCountAtConfiguredBoundApplies()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var batch = CreateCompletionOnlyBatch(SqliteReceiveBatchBound, RemoteCursor);

        var result = await adapter.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(0);
        await Assert.That(result.DuplicateCount).IsEqualTo(0);
        await Assert.That(recovery.ServerCursor).IsEqualTo(RemoteCursor);
    }

    /// <summary>Zero-event completion flooding over the SQLite receive bound is rejected before mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ZeroEventCompletionFloodingOverConfiguredBoundRejectsWithoutMutation()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var batch = CreateCompletionOnlyBatch(ExceededSqliteReceiveBatchBound, RemoteCursor);

        Func<Task> apply = () => adapter.ApplyRemoteBatchAsync(batch, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<ArgumentException>();
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsNull();
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Applies a server result to one operation through a real lease.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="kind">The result kind.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task ApplyServerResultAsync(SqliteLocalStoreAdapter adapter, SyncOperation operation, OperationResultKind kind)
    {
        var lease = await ReadSingleLeaseAsync(adapter, new(operation.StreamId, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, kind, null, ServerVersion)], null, null),
            CancellationToken.None);
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
    private static RemoteEvent CreateReceiveOriginEvent(string serverCursor, OperationId operationId, string clientId) =>
        new(Guid.NewGuid(), Stream, serverCursor, DateTimeOffset.UnixEpoch, operationId, CreatePayload("remote"), new Dictionary<string, string>()) { Origin = new(clientId, operationId) };
}
