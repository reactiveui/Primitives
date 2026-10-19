// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for the durable local commit store.</summary>
/// <content>Outbox accounting shares the local commit rollback boundary.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Checks faults and cancellation after counter changes leave no charge, snapshot, or sequence change.</summary>
    /// <param name="cancel">Whether the checkpoint cancels instead of throwing an I/O fault.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OutboxAccountingRollsBackLocalCommitFaultAndCancellation(bool cancel)
    {
        using var database = TempDatabase.Create();
        using var cancellation = new CancellationTokenSource();
        var checkpoint = new AccountingRollbackFaultPoint(cancel, cancellation);
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, OutboxCapacityBytes, checkpoint);
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = OutboxCapacityBytes, MaximumBlockedPublishers = 1 };
        store.Initialize(new(StoreIdentity, SchemaVersion, false) { Outbox = outbox }, CancellationToken.None);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        Action commit = () => store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), cancellation.Token);
        if (cancel)
        {
            await Assert.That(commit).ThrowsExactly<OperationCanceledException>();
        }
        else
        {
            await Assert.That(commit).ThrowsExactly<IOException>();
        }

        await Assert.That(checkpoint.UsageBeforeRollback).IsEqualTo((1L, GetOutboxOperationBytes(operation)));
        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((0L, 0L));
        var recovery = store.RecoverStream(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1L);
        await Assert.That(recovery.Snapshot).IsNull();
        checkpoint.Armed = false;
        _ = store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        await Assert.That(ReadAccountingUsage(database.Path)).IsEqualTo((1L, GetOutboxOperationBytes(operation)));
    }

    /// <summary>Observes the changed charge at the exact local commit durability boundary.</summary>
    /// <param name="cancel">Whether to cancel instead of simulating an I/O failure.</param>
    /// <param name="cancellation">The caller cancellation source.</param>
    private sealed class AccountingRollbackFaultPoint(bool cancel, CancellationTokenSource cancellation) : ISqliteCommitFaultPoint
    {
        /// <summary>The observed store-owned connection.</summary>
        private SqliteDatabase? _connection;

        /// <summary>Gets or sets whether the checkpoint throws.</summary>
        internal bool Armed { get; set; } = true;

        /// <summary>Gets the uncommitted counter value before rollback.</summary>
        internal (long Count, long Bytes) UsageBeforeRollback { get; private set; }

        /// <inheritdoc/>
        public void BeforeLocalCommitTransaction(SqliteDatabase connection) => _connection = connection;

        /// <inheritdoc/>
        /// <exception cref="IOException">The checkpoint simulates an I/O fault.</exception>
        public void Reached(SqliteCommitCheckpoint checkpoint)
        {
            if (!Armed || checkpoint != SqliteCommitCheckpoint.LocalCommitBeforeCommit)
            {
                return;
            }

            UsageBeforeRollback = SqliteOutboxCapacitySql.ReadUsage(_connection!, _connection!.Transaction!, StoreIdentity);
            if (cancel)
            {
                cancellation.Cancel();
                cancellation.Token.ThrowIfCancellationRequested();
            }

            throw new IOException("Simulated accounting commit fault.");
        }
    }
}
