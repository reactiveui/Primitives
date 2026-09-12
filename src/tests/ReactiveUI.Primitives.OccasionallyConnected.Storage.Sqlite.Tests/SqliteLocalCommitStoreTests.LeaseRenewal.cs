// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Tests durable lease renewal and expiry boundaries.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Verifies silent trigger rejection cannot be mistaken for a durable renewal or release.</summary>
    /// <param name="renew">Whether to reject renewal rather than release.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task WhenLeaseMutationIsIgnored_ThenCallerReceivesFailure(bool renew)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        if (renew)
        {
            command.CommandText = "CREATE TRIGGER reject_renew BEFORE UPDATE ON oc_outbox_leases BEGIN SELECT RAISE(IGNORE); END;";
        }
        else
        {
            command.CommandText = "CREATE TRIGGER reject_release BEFORE DELETE ON oc_outbox_leases BEGIN SELECT RAISE(IGNORE); END;";
        }

        _ = command.ExecuteNonQuery();

        await Assert.That(async () =>
        {
            if (renew)
            {
                await store.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
            }
            else
            {
                await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
            }
        }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies a malformed lease identifier fails closed instead of reclaiming its rows.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStoredLeaseIdentifierIsMalformed_ThenAcquisitionPreservesIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        _ = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox_leases SET lease_id = 'invalid';";
        _ = command.ExecuteNonQuery();

        await Assert.That(() => LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountAllLeaseRows(database.Path)).IsEqualTo(1);
    }

    /// <summary>Verifies inconsistent expiry timestamps cannot partially renew or release a batch.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenBatchExpiryIsInconsistent_ThenRenewalPreservesEveryMember()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, clientSequence: 1, "a");
        _ = CommitOperation(store, Stream, clientSequence: SecondClientSequence, "b");
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox_leases SET lease_expires_at_utc = $expiry WHERE operation_id = $operationId;";
        _ = command.Parameters.AddWithValue("$expiry", DateTimeOffset.UnixEpoch.ToString("O", CultureInfo.InvariantCulture));
        _ = command.Parameters.AddWithValue("$operationId", operation.OperationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();

        await Assert.That(async () => await store.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(TwoOperations);
    }

    /// <summary>Verifies one acquisition returns one committed batch without draining more work.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseBatchIsAcquired_ThenAnotherAcquisitionCannotBypassIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = CommitOperation(store, Stream, clientSequence: 1, "a");
        var batch = RequireBatch(store.LeasePendingOperationBatch(
            new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)),
            CancellationToken.None));

        var next = await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(next).IsNull();
        await Assert.That(CountLeaseRows(database.Path, batch.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies inconsistent historical metadata aborts migration before adding lease tables.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenHistoricalRemoteSchemaMetadataIsInvalid_ThenMigrationLeavesItUnchanged()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var transaction = connection.BeginTransaction();
            SqliteStoreSchemaTests.CreateRemoteApplySchema(connection, transaction);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE oc_metadata SET value = 'invalid' WHERE key = 'schema_version';";
            _ = command.ExecuteNonQuery();
            transaction.Commit();
        }

        await Assert.That(() => CreateInitializedStore(database.Path)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SqliteStoreSchema.RemoteApplySchemaVersion);
    }

    /// <summary>Verifies renewal at the expiry boundary cannot revive stale ownership.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseHasExpired_ThenRenewalCannotReviveOwnership()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var duration = TimeSpan.FromMinutes(1);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, duration)));
        clock.Advance(duration);

        await Assert.That(async () => await store.RenewLeaseAsync(lease.LeaseId, duration, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
    }

    /// <summary>Verifies an overflowing extension preserves the original durable lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenRenewalExpiryOverflows_ThenOriginalLeaseRemainsIntact()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue.AddMinutes(-1));
        using var store = CreateInitializedStore(database.Path, clock);
        _ = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var duration = TimeSpan.FromMinutes(1);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, duration)));

        await Assert.That(async () => await store.RenewLeaseAsync(lease.LeaseId, duration, CancellationToken.None))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(CountLeaseRows(database.Path, lease.LeaseId)).IsEqualTo(1);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
    }

    /// <summary>Verifies unfiltered acquisition skips blocked stream heads while retaining per-stream ordering.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenUnfilteredHeadsAreBlocked_ThenAnotherStreamCanProgress()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var oversized = new StreamId("a-oversized");
        var busy = new StreamId("b-busy");
        var ready = new StreamId("c-ready");
        _ = CommitOperation(store, oversized, clientSequence: 1, "oversized");
        _ = CommitOperation(store, busy, clientSequence: 1, "x");
        var expected = CommitOperation(store, ready, clientSequence: 1, "y");
        _ = RequireBatch(await LeaseSingleBatch(store, new(busy, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        var selected = RequireBatch(await LeaseSingleBatch(store, new(null, 1, OversizedByteBound, TimeSpan.FromMinutes(1))));
        var exhausted = await LeaseSingleBatch(store, new(null, 1, OversizedByteBound, TimeSpan.FromMinutes(1)));

        await Assert.That(selected.Operations[0].OperationId).IsEqualTo(expected.OperationId);
        await Assert.That(exhausted).IsNull();
    }

    /// <summary>Verifies invalid admission limits do not create lease rows.</summary>
    /// <param name="maximumOperations">The operation limit.</param>
    /// <param name="maximumBytes">The byte limit.</param>
    /// <param name="durationTicks">The duration in ticks.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(0, 1L, 1L)]
    [Arguments(1, 0L, 1L)]
    [Arguments(1, 1L, 0L)]
    public async Task WhenLeaseBoundsAreInvalid_ThenNoLeaseIsCreated(int maximumOperations, long maximumBytes, long durationTicks)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);

        await Assert.That(() => LeaseSingleBatch(store, new(Stream, maximumOperations, maximumBytes, TimeSpan.FromTicks(durationTicks))))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(CountAllLeaseRows(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies an empty lease identifier is rejected before store access.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseIdentifierIsEmpty_ThenRenewAndReleaseAreRejected()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);

        await Assert.That(async () => await store.RenewLeaseAsync(Guid.Empty, TimeSpan.FromMinutes(1), CancellationToken.None))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(async () => await store.ReleaseLeaseAsync(Guid.Empty, CancellationToken.None))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies early renewal extends the existing expiry instead of shortening the lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseIsRenewedEarly_ThenOriginalExpiryIsExtendedAcrossReopen()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        using var store = CreateInitializedStore(database.Path, clock);
        var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
        var duration = TimeSpan.FromMinutes(1);
        var extension = TimeSpan.FromSeconds(1);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, duration)));

        await store.RenewLeaseAsync(lease.LeaseId, extension, CancellationToken.None);
        clock.Advance(duration);
        using var reopened = CreateInitializedStore(database.Path, clock);
        var beforeExpiry = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, duration));

        await Assert.That(beforeExpiry).IsNull();
        clock.Advance(extension);
        var afterExpiry = RequireBatch(await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, duration)));
        await Assert.That(afterExpiry.LeaseId).IsNotEqualTo(lease.LeaseId);
        await Assert.That(afterExpiry.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }
}
