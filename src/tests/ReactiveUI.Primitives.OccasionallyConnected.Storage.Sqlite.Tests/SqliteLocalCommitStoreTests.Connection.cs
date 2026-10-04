// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for the durable local commit store.</summary>
/// <content>Connection gate ownership and failed borrower cleanup.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The bounded deadline for connection ownership assertions.</summary>
    private static readonly TimeSpan ConnectionBorrowDeadline = TimeSpan.FromSeconds(5);

    /// <summary>The cancellation deadline while a competing owner holds the monitor.</summary>
    private static readonly TimeSpan ConnectionBorrowCancellationDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>Checks a failed connection preparation releases the gate and retires its unusable connection.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The native connection was not captured.</exception>
    [Test]
    public async Task FailedConnectionPreparationReleasesGateAndReopensNativeConnection()
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, OutboxCapacityBytes, capture);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var connection = capture.Connection ?? throw new InvalidOperationException("The commit connection was not captured.");
        connection.Dispose();

        await Assert.That(() => store.GetOperationStatus(CreateOperation(clientSequence: 1).OperationId, CancellationToken.None))
            .Throws<ObjectDisposedException>();
        var recovery = await Task.Run(() => store.RecoverStream(Stream, subscription, CancellationToken.None))
            .WaitAsync(ConnectionBorrowDeadline);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
    }

    /// <summary>Checks a failing borrower retains monitor reentrancy without releasing an enclosing owner.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The native connection was not captured.</exception>
    [Test]
    public async Task ReentrantBorrowFailureLeavesEnclosingGateOwnerIntact()
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, OutboxCapacityBytes, capture);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        _ = store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var connection = capture.Connection ?? throw new InvalidOperationException("The commit connection was not captured.");
        var gate = GetStoreConnectionGate(store);
        var remainedOwned = RunReentrantBorrowFailure(store, gate, connection, operation.OperationId);
        await Assert.That(remainedOwned).IsTrue();
        await Assert.That(Monitor.IsEntered(gate)).IsFalse();
        var subscription = await Task.Run(() => store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None))
            .WaitAsync(ConnectionBorrowDeadline);
        await Assert.That(subscription.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Checks a competing connection borrower observes cancellation while the gate is owned.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentBorrowCanCancelWithoutReleasingAnotherOwner()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteLocalCommitStore(database.Path);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        _ = store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var gate = GetStoreConnectionGate(store);
        using var owned = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var holder = Task.Run(() => HoldStoreConnectionGate(gate, owned, release));
        try
        {
            await Assert.That(owned.Wait(ConnectionBorrowDeadline)).IsTrue();
            cancellation.CancelAfter(ConnectionBorrowCancellationDelay);
            var borrower = Task.Run(() => store.GetOperationStatus(operation.OperationId, cancellation.Token));
            await Assert.That(async () => await borrower.WaitAsync(ConnectionBorrowDeadline))
                .Throws<OperationCanceledException>();
            await Assert.That(holder.IsCompleted).IsFalse();
        }
        finally
        {
            release.Set();
            await holder.WaitAsync(ConnectionBorrowDeadline);
        }

        var status = store.GetOperationStatus(operation.OperationId, CancellationToken.None);
        await Assert.That(status).IsNotNull();
    }

    /// <summary>Accesses the ownership monitor without adding a production testing API.</summary>
    /// <param name="store">The store under test.</param>
    /// <returns>The store connection monitor.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_connectionGate")]
    private static extern ref object GetStoreConnectionGate(SqliteLocalCommitStore store);

    /// <summary>Runs a borrower with an active transaction under an enclosing monitor owner.</summary>
    /// <param name="store">The initialized store.</param>
    /// <param name="gate">The connection monitor.</param>
    /// <param name="connection">The actual native connection.</param>
    /// <param name="operationId">The committed operation identifier.</param>
    /// <returns>Whether the enclosing owner remained intact.</returns>
    private static bool RunReentrantBorrowFailure(
        SqliteLocalCommitStore store,
        object gate,
        SqliteDatabase connection,
        OperationId operationId)
    {
        lock (gate)
        {
            using var transaction = connection.BeginTransaction();
            try
            {
                _ = store.GetOperationStatus(operationId, CancellationToken.None);
            }
            catch (InvalidOperationException)
            {
                return Monitor.IsEntered(gate);
            }

            return false;
        }
    }

    /// <summary>Owns the connection monitor until the test releases its explicit barrier.</summary>
    /// <param name="gate">The connection monitor.</param>
    /// <param name="owned">The acquired ownership signal.</param>
    /// <param name="release">The release signal.</param>
    private static void HoldStoreConnectionGate(object gate, ManualResetEventSlim owned, ManualResetEventSlim release)
    {
        lock (gate)
        {
            owned.Set();
            release.Wait();
        }
    }
}
