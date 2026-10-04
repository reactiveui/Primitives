// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using TUnit.Assertions;

namespace ReactiveUI.Primitives.OccasionallyConnected.Testing;

/// <summary>Reusable TUnit assertions for a fresh initialized local store.</summary>
internal static class LocalStoreConformance
{
    /// <summary>The identity shared by a store and its reopened instance.</summary>
    internal const string Identity = "local-store-conformance";

    /// <summary>The client bound to the initialized store.</summary>
    internal const string Client = "conformance-client";

    /// <summary>The second snapshot revision.</summary>
    internal const int SecondRevision = 2;

    /// <summary>The maximum leased operations.</summary>
    internal const int LeaseOperations = 8;

    /// <summary>The lease payload byte budget.</summary>
    internal const int LeaseBytes = 4096;

    /// <summary>The first snapshot marker.</summary>
    internal const string FirstSnapshot = "first";

    /// <summary>The persisted remote cursor.</summary>
    internal const string DurableCursor = "durable";

    /// <summary>The terminal server version.</summary>
    internal const string ServerVersion = "version";

    /// <summary>The stream isolated within each fresh fixture.</summary>
    internal static readonly StreamId Stream = new("conformance/store");

    /// <summary>Checks a failed revision cannot partially commit an operation, sequence or snapshot.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The assertions.</returns>
    internal static async Task AtomicLocalCommitAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var operation = Operation(store, before.NextClientSequence);
        var result = await store.CommitLocalOperationAsync(operation, Mutation(0, FirstSnapshot), CancellationToken.None);
        var rejected = Operation(store, before.NextClientSequence + 1);
        Func<Task> failedCommit = async () => _ = await store.CommitLocalOperationAsync(rejected, Mutation(0, "invalid"), CancellationToken.None);
        await Assert.That(failedCommit)
            .ThrowsExactly<InvalidOperationException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(result.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(before.NextClientSequence + 1);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovered.Snapshot?.State.Payload.ToArray().SequenceEqual(Payload(FirstSnapshot).Payload.ToArray())).IsTrue();
        await Assert.That(recovered.PendingOperations.Select(static item => item.OperationId).SequenceEqual([operation.OperationId])).IsTrue();
        await Assert.That(await store.GetOperationStatusAsync(rejected.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Checks cancellation before dispatch cannot mutate durable state.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The assertions.</returns>
    internal static async Task CanceledCommitAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();
        Func<Task> canceledCommit = async () =>
            _ = await store.CommitLocalOperationAsync(Operation(store, before.NextClientSequence), Mutation(0, "canceled"), cancellation.Token);
        await Assert.That(canceledCommit)
            .Throws<OperationCanceledException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.Snapshot).IsNull();
        await Assert.That(recovered.NextClientSequence).IsEqualTo(before.NextClientSequence);
    }

    /// <summary>Checks inbox, cursor and snapshot fencing, including mixed duplicate delivery.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The assertions.</returns>
    internal static async Task AtomicRemoteApplyAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = Event("one");
        var second = Event("two");
        _ = await store.ApplyRemoteBatchAsync(Batch(null, "one", [first]), Mutation(0, FirstSnapshot), CancellationToken.None);
        Func<Task> failedApply = async () =>
            _ = await store.ApplyRemoteBatchAsync(Batch(null, "two", [second]), Mutation(1, "invalid"), CancellationToken.None);
        await Assert.That(failedApply)
            .ThrowsExactly<InvalidOperationException>();
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [first.EventId, second.EventId], CancellationToken.None);
        await Assert.That(unapplied.SequenceEqual([second.EventId])).IsTrue();
        var unchanged = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(unchanged.ServerCursor).IsEqualTo("one");
        await Assert.That(unchanged.Snapshot?.Revision).IsEqualTo(1);
        var result = await store.ApplyRemoteBatchAsync(Batch("one", "two", [first, second]), Mutation(1, "second"), CancellationToken.None);
        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(1);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo("two");
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondRevision);
        await Assert.That(await store.GetUnappliedEventIdsAsync(Stream, [first.EventId, second.EventId], CancellationToken.None)).IsEmpty();
    }

    /// <summary>Checks exclusive leases, release, invalid result atomicity and accepted replay retention.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The assertions.</returns>
    internal static async Task LeasedOutboxAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var operation = Operation(store, before.NextClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, Mutation(0, "leased"), CancellationToken.None);
        var lease = await LeaseAsync(store);
        await Assert.That(lease).IsNotNull();
        await Assert.That(await LeaseAsync(store)).IsNull();
        await store.RenewLeaseAsync(lease!.LeaseId, TimeSpan.FromMinutes(SecondRevision), CancellationToken.None);
        var invalid = new RemoteSyncResult(lease.LeaseId, [], null, null);
        var failure = await Assert.ThrowsAsync<Exception>(() => store.ApplySyncResultAsync(lease.LeaseId, invalid, CancellationToken.None).AsTask());
        await Assert.That(failure is InvalidOperationException or SyncBatchValidationException).IsTrue();
        var unchanged = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(unchanged.PendingOperations.Count).IsEqualTo(1);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var reclaimed = await LeaseAsync(store);
        await Assert.That(reclaimed).IsNotNull();
        await Assert.That(reclaimed!.LeaseId).IsNotEqualTo(lease.LeaseId);
        await store.ApplySyncResultAsync(
            reclaimed.LeaseId,
            new(reclaimed.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.ReplayOperations.Select(static item => item.OperationId).SequenceEqual([operation.OperationId])).IsTrue();
        await Assert.That((await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None))?.State)
            .IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Checks a renewed lease excludes work until its controlled expiry, then permits reclaim.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <param name="advance">The fixture's clock advance callback.</param>
    /// <returns>The assertions.</returns>
    internal static async Task LeaseExpiryAsync(ILocalStoreAdapter store, Action<TimeSpan> advance)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var operation = Operation(store, before.NextClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, Mutation(0, "expiry"), CancellationToken.None);
        var lease = await LeaseAsync(store);
        await Assert.That(lease).IsNotNull();
        await store.RenewLeaseAsync(lease!.LeaseId, TimeSpan.FromMinutes(SecondRevision), CancellationToken.None);
        advance(TimeSpan.FromMinutes(1));
        await Assert.That(await LeaseAsync(store)).IsNull();
        advance(TimeSpan.FromMinutes(SecondRevision));
        var reclaimed = await LeaseAsync(store);
        await Assert.That(reclaimed).IsNotNull();
        await Assert.That(reclaimed!.LeaseId).IsNotEqualTo(lease.LeaseId);
        await Assert.That(reclaimed.Operations.Select(static item => item.OperationId).SequenceEqual([operation.OperationId])).IsTrue();
        var staleResult = new RemoteSyncResult(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null);
        _ = await Assert.ThrowsAsync<Exception>(() => store.ApplySyncResultAsync(lease.LeaseId, staleResult, CancellationToken.None).AsTask());
        var unchanged = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(unchanged.PendingOperations.Count).IsEqualTo(1);
        await store.ReleaseLeaseAsync(reclaimed.LeaseId, CancellationToken.None);
    }

    /// <summary>Checks initialization cannot silently rebind persisted state to another client.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The assertions.</returns>
    internal static async Task ClientIdentityAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(() => store.InitializeAsync(new(Identity, 1, false) { ClientId = "other-client" }, CancellationToken.None).AsTask())
            .Throws<InvalidOperationException>();
        await Assert.That(await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscription);
        await Assert.That(() => store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None).AsTask())
            .Throws<InvalidOperationException>();
    }

    /// <summary>Checks snapshot recovery fences and atomically replaces the authoritative checkpoint.</summary>
    /// <param name="store">The fresh initialized recovery-capable store.</param>
    /// <returns>The assertions.</returns>
    /// <exception cref="InvalidOperationException">The advertised recovery interface is missing.</exception>
    internal static async Task SnapshotRecoveryAsync(ILocalStoreAdapter store)
    {
        var recovery = store as ILocalSnapshotRecoveryStore
            ?? throw new InvalidOperationException("AtomicSnapshotRecovery requires ILocalSnapshotRecoveryStore.");
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        LocalSnapshotRecoveryMutation mutation = new()
        {
            StreamId = Stream,
            SubscriptionId = subscription,
            ExpectedRevision = 0,
            ExpectedPreviousCursor = null,
            Checkpoint = new()
            {
                StreamId = Stream,
                SubscriptionId = subscription,
                FrontierCursor = DurableCursor,
                ServerVersion = ServerVersion,
                SnapshotFormatVersion = 1,
                ClientState = Payload("authoritative"),
                ObservedAtUtc = DateTimeOffset.UnixEpoch,
            },
            OptimisticState = Payload("optimistic"),
            SnapshotFormatVersion = 1,
            OperationDispositions = [],
        };
        Func<Task> invalid = async () => _ = await recovery.ApplySnapshotRecoveryAsync(mutation with { ExpectedRevision = 1 }, CancellationToken.None);
        await Assert.That(invalid).Throws<InvalidOperationException>();
        var unchanged = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(unchanged.ServerCursor).IsNull();
        await Assert.That(unchanged.Snapshot).IsNull();
        var result = await recovery.ApplySnapshotRecoveryAsync(mutation, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(result.Snapshot.Revision).IsEqualTo(1);
        await Assert.That(recovered.ServerCursor).IsEqualTo(DurableCursor);
        await Assert.That(recovered.Snapshot?.State.Payload.ToArray().SequenceEqual(Payload("optimistic").Payload.ToArray())).IsTrue();
        await Assert.That(recovered.Snapshot?.AuthoritativeState?.Payload.ToArray().SequenceEqual(Payload("authoritative").Payload.ToArray())).IsTrue();
    }

    /// <summary>Writes local work and remote state for restart or process-kill verification.</summary>
    /// <param name="store">The fresh initialized store.</param>
    /// <returns>The operation and inbox identity that must survive.</returns>
    internal static async Task<(OperationId Operation, Guid Event)> SeedDurableAsync(ILocalStoreAdapter store)
    {
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var operation = Operation(store, before.NextClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, Mutation(0, "local"), CancellationToken.None);
        var remote = Event(DurableCursor);
        _ = await store.ApplyRemoteBatchAsync(Batch(null, DurableCursor, [remote]), Mutation(1, "remote"), CancellationToken.None);
        return (operation.OperationId, remote.EventId);
    }

    /// <summary>Checks restart preserves operation, subscription, inbox, cursor and snapshot together.</summary>
    /// <param name="store">The reopened initialized store.</param>
    /// <param name="subscription">The pre-close subscription.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="eventId">The committed inbox event.</param>
    /// <returns>The assertions.</returns>
    internal static async Task AssertDurableAsync(ILocalStoreAdapter store, SubscriptionId subscription, OperationId operation, Guid eventId)
    {
        await Assert.That(await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscription);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(DurableCursor);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondRevision);
        await Assert.That(recovered.Snapshot?.State.Payload.ToArray().SequenceEqual(Payload("remote").Payload.ToArray())).IsTrue();
        await Assert.That(recovered.PendingOperations.Select(static item => item.OperationId).SequenceEqual([operation])).IsTrue();
        await Assert.That(await store.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None)).IsEmpty();
        var status = await store.GetOperationStatusAsync(operation, CancellationToken.None);
        await Assert.That(status?.State is SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload).IsTrue();
    }

    /// <summary>Reads one lease without hiding multiple returned batches.</summary>
    /// <param name="store">The initialized store.</param>
    /// <returns>The lease or no eligible work.</returns>
    private static async Task<LeasedOperationBatch?> LeaseAsync(ILocalStoreAdapter store)
    {
        LeasedOperationBatch? result = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(new(Stream, LeaseOperations, LeaseBytes, TimeSpan.FromMinutes(1)), CancellationToken.None))
        {
            await Assert.That(result).IsNull();
            result = batch;
        }

        return result;
    }

    /// <summary>Creates a local operation using the fixture's advertised durability.</summary>
    /// <param name="store">The initialized store.</param>
    /// <param name="sequence">The recovered next sequence.</param>
    /// <returns>The input operation.</returns>
    private static SyncOperation Operation(ILocalStoreAdapter store, long sequence) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Update,
        Payload = Payload("operation"),
        Policy = OperationPolicy.Default with
        {
            Durability = (store.Capabilities & LocalStoreCapabilities.DurableLocalCommit) != 0
                ? OperationDurability.Durable
                : OperationDurability.Volatile,
        },
        Metadata = new Dictionary<string, string>(),
    };

    /// <summary>Creates a bounded snapshot.</summary>
    /// <param name="revision">The required prior revision.</param>
    /// <param name="value">The snapshot marker.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation Mutation(long revision, string value) => new(Stream, Payload(value), 1, revision);

    /// <summary>Creates an inbox event.</summary>
    /// <param name="cursor">The event cursor.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent Event(string cursor) =>
        new(Guid.NewGuid(), Stream, cursor, DateTimeOffset.UnixEpoch, null, Payload("event"), new Dictionary<string, string>());

    /// <summary>Creates a cursor-fenced remote batch.</summary>
    /// <param name="previous">The previous cursor.</param>
    /// <param name="next">The next cursor.</param>
    /// <param name="events">The delivered events.</param>
    /// <returns>The batch.</returns>
    private static RemoteEventBatch Batch(string? previous, string next, IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previous, next, events);

    /// <summary>Creates correctly hashed opaque payload bytes.</summary>
    /// <param name="value">The UTF-8 payload.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope Payload(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        return new("conformance", 1, "application/octet-stream", bytes, $"sha256:{Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant()}");
    }
}
