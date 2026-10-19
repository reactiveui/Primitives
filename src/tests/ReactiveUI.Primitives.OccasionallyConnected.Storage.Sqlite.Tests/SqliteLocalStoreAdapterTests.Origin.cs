// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Origin-related tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>A bounded worker budget that admits the representative event without origin correlation.</summary>
    private const long BoundedRemoteApplyBytes = 1200;

    /// <summary>A worker budget that admits a remote event with an origin correlation.</summary>
    private const long OriginRemoteApplyBytes = 4096;

    /// <summary>The persisted cursor of the origin-correlated event.</summary>
    private const string RejectedOriginCursor = "origin-rejected";

    /// <summary>The persisted cursor of the admitted UTF-8 origin event.</summary>
    private const string AcceptedOriginCursor = "origin-accepted";

    /// <summary>A large non-ASCII authenticated client identity with a UTF-8 representation.</summary>
    private static readonly string Utf8OriginClientId = new('界', 256);

    /// <summary>Verifies origin retention participates in remote admission before SQLite mutates durable state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLargeOriginExceedsWorkerBudget_ThenRemoteApplyRejectsWithoutMutationAndLaterRequestSucceeds()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = BoundedRemoteApplyBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operationId = OperationId.New();
        var rejectedEvent = CreateOriginEvent(RejectedOriginCursor, operationId);
        var originlessEvent = rejectedEvent with { Origin = null };

        Func<Task<RemoteApplyResult>> rejected = () => adapter.ApplyRemoteBatchAsync(
            CreateOriginBatch(RejectedOriginCursor, operationId, rejectedEvent),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(rejected);
        var recoveryAfterReject = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var missingOriginEvent = await adapter.GetUnappliedEventIdsAsync(Stream, [rejectedEvent.EventId], CancellationToken.None);
        var later = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RejectedOriginCursor, [originlessEvent]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(recoveryAfterReject.ServerCursor).IsNull();
        await Assert.That(recoveryAfterReject.Snapshot).IsNull();
        await Assert.That(missingOriginEvent.Count).IsEqualTo(1);
        await Assert.That(missingOriginEvent[0]).IsEqualTo(rejectedEvent.EventId);
        await Assert.That(later.AppliedCount).IsEqualTo(1);
    }

    /// <summary>Verifies sufficient admission capacity accepts an origin with a UTF-8 client identity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOriginWithUtf8ClientIdentityFitsWorkerBudget_ThenRemoteApplyPersistsIt()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = OriginRemoteApplyBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operationId = OperationId.New();
        var originEvent = CreateOriginEvent(AcceptedOriginCursor, operationId);

        var result = await adapter.ApplyRemoteBatchAsync(
            CreateOriginBatch(AcceptedOriginCursor, operationId, originEvent),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(Stream, [originEvent.EventId], CancellationToken.None);

        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(recovery.ServerCursor).IsEqualTo(AcceptedOriginCursor);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(result.SnapshotRevision);
        await Assert.That(unapplied.Count).IsEqualTo(0);
    }

    /// <summary>Creates a remote batch with a complete origin declaration.</summary>
    /// <param name="serverCursor">The server cursor assigned to the event.</param>
    /// <param name="operationId">The operation that caused the event.</param>
    /// <param name="remoteEvent">The origin-correlated event.</param>
    /// <returns>The origin-correlated batch.</returns>
    private static RemoteEventBatch CreateOriginBatch(string serverCursor, OperationId operationId, RemoteEvent remoteEvent) =>
        CreateRemoteBatch(null, serverCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(Utf8OriginClientId, operationId), [remoteEvent.EventId])],
        };

    /// <summary>Creates a remote event with an authenticated origin correlation.</summary>
    /// <param name="serverCursor">The server cursor assigned to the event.</param>
    /// <param name="operationId">The operation that caused the event.</param>
    /// <returns>The origin-correlated event.</returns>
    private static RemoteEvent CreateOriginEvent(string serverCursor, OperationId operationId) =>
        new(Guid.NewGuid(), Stream, serverCursor, DateTimeOffset.UnixEpoch, operationId, CreatePayload("remote"), new Dictionary<string, string>()) { Origin = new(Utf8OriginClientId, operationId) };
}
