// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Outbox lease tests for <see cref="SqliteLocalCommitStore"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The default lease payload byte bound used by tests.</summary>
    private const long DefaultLeaseBytes = 128;

    /// <summary>The two-operation bound used by tests.</summary>
    private const int TwoOperations = 2;

    /// <summary>The three-operation bound used by tests.</summary>
    private const int ThreeOperations = 3;

    /// <summary>The byte bound that excludes the oversized payload.</summary>
    private const long OversizedByteBound = 4;

    /// <summary>The byte bound that includes only the first two short payloads.</summary>
    private const long PrefixByteBound = 5;

    /// <summary>The small byte bound used by membership tests.</summary>
    private const long MembershipByteBound = 16;

    /// <summary>The alternate stream byte bound used by tests.</summary>
    private const long AlternateStreamByteBound = 32;

    /// <summary>The minutes required to expire the first lease.</summary>
    private const int LeaseExpiryAdvanceMinutes = 2;

    /// <summary>Verifies releasing a persisted lease lets a reopened store lease the same operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseIsReleasedAfterReopen_ThenOperationCanBeLeasedAgain()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var batch = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        using var blocked = CreateInitializedStore(database.Path);
        var none = await LeaseSingleBatch(blocked, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await blocked.ReleaseLeaseAsync(batch.LeaseId, CancellationToken.None);
        using var reopened = CreateInitializedStore(database.Path);
        var released = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(none).IsNull();
        var releasedBatch = RequireBatch(released);
        await Assert.That(releasedBatch.Operations.Count).IsEqualTo(1);
        await Assert.That(releasedBatch.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies competing store instances do not lease the same pending operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenIndependentInstancesLeaseConcurrently_ThenOnlyOneReceivesTheBatch()
    {
        using var database = TempDatabase.Create();
        using var writer = CreateInitializedStore(database.Path);
        _ = CommitOperation(writer, Stream, clientSequence: 1, OperationPayloadText);
        var first = CreateInitializedStore(database.Path);
        var second = CreateInitializedStore(database.Path);
        try
        {
            var attempts = await Task.WhenAll(
                Task.Run(() => LeaseSingleBatch(first, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)))),
                Task.Run(() => LeaseSingleBatch(second, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)))));

            await Assert.That(attempts.Count(static batch => batch is not null)).IsEqualTo(1);
            await Assert.That(attempts.Count(static batch => batch is null)).IsEqualTo(1);
        }
        finally
        {
            first.Dispose();
            second.Dispose();
        }
    }

    /// <summary>Verifies expiry reclamation writes a new lease id and stale owners cannot act later.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExpiredLeaseIsReclaimed_ThenOldOwnerCannotRenewOrRelease()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var first = await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));

        var second = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var firstLease = RequireBatch(first);
        Func<Task> renewOld = async () => await store.RenewLeaseAsync(firstLease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        Func<Task> releaseOld = async () => await store.ReleaseLeaseAsync(firstLease.LeaseId, CancellationToken.None);

        await Assert.That(second.LeaseId).IsNotEqualTo(firstLease.LeaseId);
        await Assert.That(renewOld).ThrowsExactly<InvalidOperationException>();
        await Assert.That(releaseOld).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies stream filtering, ordering, operation count, and payload byte bounds.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseBoundsAreApplied_ThenOnlyAContiguousStreamPrefixIsMaterialized()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var other = new StreamId("sensor/humidity");
        var first = CommitOperation(store, Stream, clientSequence: 1, "aa");
        var second = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "bbb");
        _ = CommitOperation(store, Stream, clientSequence: ThirdClientSequence, "cccc");
        _ = CommitOperation(store, other, clientSequence: 1, "zz");

        var batch = RequireBatch(await LeaseSingleBatch(store, new(Stream, ThreeOperations, PrefixByteBound, TimeSpan.FromMinutes(1))));
        var otherBatch = RequireBatch(await LeaseSingleBatch(store, new(other, ThreeOperations, AlternateStreamByteBound, TimeSpan.FromMinutes(1))));

        await Assert.That(batch.Operations.Count).IsEqualTo(TwoOperations);
        await Assert.That(batch.Operations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(batch.Operations[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(otherBatch.Operations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies an oversized stream head blocks later operations in that stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamHeadExceedsByteBound_ThenLaterOperationsDoNotOvertakeIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, "oversized");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "x");

        var batch = await LeaseSingleBatch(store, new(Stream, TwoOperations, OversizedByteBound, TimeSpan.FromMinutes(1)));

        await Assert.That(batch).IsNull();
    }

    /// <summary>Verifies release validates the complete persisted membership before mutating rows.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseMembershipIsIncomplete_ThenReleaseFailsWithoutPartialMutation()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, "aa");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "bb");
        var batch = await LeaseSingleBatch(store, new(Stream, TwoOperations, MembershipByteBound, TimeSpan.FromMinutes(1)));
        var lease = RequireBatch(batch);
        DeleteOutboxOperation(database.Path, lease.Operations[0].OperationId);

        Func<Task> release = async () => await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);

        await Assert.That(release).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies partial expired reclaim makes the stale lease membership incomplete.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExpiredLeaseIsPartiallyReclaimed_ThenOldLeaseCannotReleaseSurvivingSubset()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var first = CommitOperation(store, Stream, clientSequence: 1, "aa");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "bb");
        var stale = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, MembershipByteBound, TimeSpan.FromMinutes(1))));
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));

        var reclaimed = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, MembershipByteBound, TimeSpan.FromMinutes(1))));
        Func<Task> releaseStale = async () => await store.ReleaseLeaseAsync(stale.LeaseId, CancellationToken.None);

        await Assert.That(reclaimed.Operations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(reclaimed.LeaseId).IsNotEqualTo(stale.LeaseId);
        await Assert.That(releaseStale).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, stale.LeaseId)).IsEqualTo(1);
        await Assert.That(CountLeaseRows(database.Path, reclaimed.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies malformed selected lease expiry fails closed during acquisition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSelectedLeaseExpiryIsMalformed_ThenAcquireFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        UpdateLeaseExpiryText(database.Path, lease.LeaseId, "not-a-timestamp");

        Func<Task> action = async () => await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation before lease commit leaves no lease rows.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseIsCancelledBeforeCommit_ThenNoLeaseRowsPersist()
    {
        using var database = TempDatabase.Create();
        var store = CreateInitializedStore(database.Path);
        using var cancellation = new CancellationTokenSource();
        try
        {
            _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
            await cancellation.CancelAsync();

            Func<Task> action = async () => await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)), cancellation.Token);

            await Assert.That(action).ThrowsExactly<OperationCanceledException>();
            await Assert.That(CountAllLeaseRows(database.Path)).IsEqualTo(0);
        }
        finally
        {
            store.Dispose();
        }
    }

    /// <summary>Verifies trigger failure during membership insertion rolls back the whole lease.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseInsertTriggerFails_ThenLeaseRollsBack()
    {
        using var database = TempDatabase.Create();
        var store = CreateInitializedStore(database.Path);
        try
        {
            _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
            CreateLeaseRollbackTrigger(database.Path);

            Func<Task> action = async () => await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

            await Assert.That(action).ThrowsExactly<SqliteException>();
            DropLeaseRollbackTrigger(database.Path);
            await Assert.That(CountAllLeaseRows(database.Path)).IsEqualTo(0);
        }
        finally
        {
            store.Dispose();
        }
    }

    /// <summary>Verifies frozen schema version three databases migrate to lease-capable schema version four.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplySchemaMigratesToCurrent_ThenPendingRowsCanBeLeased()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        var operation = CreateOperation(clientSequence: 1);
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var transaction = connection.BeginTransaction();
            SqliteStoreSchemaTests.CreateRemoteApplySchema(connection, transaction);
            InsertLegacyLocalCommitRows(connection, transaction, subscriptionId, operation, CreateSnapshotMutation(expectedRevision: 0));
            transaction.Commit();
        }

        using var store = CreateInitializedStore(database.Path);
        var batch = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
        await Assert.That(batch.Operations.Count).IsEqualTo(1);
        await Assert.That(batch.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Leases a single batch from the store.</summary>
    /// <param name="store">The store.</param>
    /// <param name="request">The lease request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, if one is available.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<LeasedOperationBatch?> LeaseSingleBatch(
        SqliteLocalCommitStore store,
        OutboxLeaseRequest request,
        CancellationToken cancellationToken = default) =>
        Task.FromResult(store.LeasePendingOperationBatch(request, cancellationToken));

    /// <summary>Requires a leased batch to be present.</summary>
    /// <param name="batch">The optional batch.</param>
    /// <returns>The leased batch.</returns>
    /// <exception cref="InvalidOperationException">No batch was leased.</exception>
    private static LeasedOperationBatch RequireBatch(LeasedOperationBatch? batch)
    {
        if (batch is not null)
        {
            return batch;
        }

        throw new InvalidOperationException("Expected a leased operation batch.");
    }

    /// <summary>Commits one operation for a lease test.</summary>
    /// <param name="store">The store.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The committed operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperation CommitOperation(SqliteLocalCommitStore store, StreamId streamId, long clientSequence, string payloadText) =>
        CommitOperation(store, streamId, clientSequence, payloadText, DeliveryGuarantee.AtLeastOnce);

    /// <summary>Commits one operation for a lease test.</summary>
    /// <param name="store">The store.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee.</param>
    /// <returns>The committed operation.</returns>
    private static SyncOperation CommitOperation(
        SqliteLocalCommitStore store,
        StreamId streamId,
        long clientSequence,
        string payloadText,
        DeliveryGuarantee deliveryGuarantee)
    {
        _ = store.GetOrCreateSubscriptionId(streamId, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence) with
        {
            StreamId = streamId,
            Payload = CreatePayload(payloadText),
            Policy = new(deliveryGuarantee, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        };
        _ = store.CommitLocalOperation(operation, new(streamId, CreatePayload($"snapshot-{payloadText}"), FormatVersion: 1, clientSequence - 1), CancellationToken.None);
        return operation;
    }

    /// <summary>Deletes an outbox operation directly through SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void DeleteOutboxOperation(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = ON;
            DELETE FROM oc_outbox WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Updates a lease expiry directly through SQLite.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="expiryText">The expiry text.</param>
    private static void UpdateLeaseExpiryText(string path, Guid leaseId, string expiryText)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_leases
            SET lease_expires_at_utc = $leaseExpiresAtUtc
            WHERE store_identity = $storeIdentity AND lease_id = $leaseId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        _ = command.Parameters.AddWithValue("$leaseExpiresAtUtc", expiryText);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Counts lease rows for one lease.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <returns>The row count.</returns>
    /// <exception cref="InvalidOperationException">The row count cannot be read.</exception>
    private static long CountLeaseRows(string path, Guid leaseId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_outbox_leases WHERE store_identity = $storeIdentity AND lease_id = $leaseId;";
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", leaseId.ToString("D"));
        return command.ExecuteScalar() is long count ? count : throw new InvalidOperationException("The lease row count could not be read.");
    }

    /// <summary>Counts all lease rows.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The row count.</returns>
    /// <exception cref="InvalidOperationException">The row count cannot be read.</exception>
    private static long CountAllLeaseRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_outbox_leases;";
        return command.ExecuteScalar() is long count ? count : throw new InvalidOperationException("The lease row count could not be read.");
    }

    /// <summary>Creates a trigger that aborts lease inserts.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateLeaseRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_outbox_lease_abort
            AFTER INSERT ON oc_outbox_leases
            BEGIN
                SELECT RAISE(ABORT, 'rollback lease insert');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the lease rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropLeaseRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_outbox_lease_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Manual time provider for lease expiry tests.</summary>
    /// <param name="timestamp">The initial timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>The current timestamp.</summary>
        private DateTimeOffset _timestamp = timestamp;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _timestamp;

        /// <summary>Advances the current timestamp.</summary>
        /// <param name="duration">The duration.</param>
        public void Advance(TimeSpan duration) => _timestamp = _timestamp.Add(duration);
    }
}
