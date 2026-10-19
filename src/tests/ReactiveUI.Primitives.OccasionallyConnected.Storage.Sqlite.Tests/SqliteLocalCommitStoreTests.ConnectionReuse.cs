// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Native resource reuse across durable local commits.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The provider key identifier for connection reuse tests.</summary>
    private const string ReuseKeyId = "reuse";

    /// <summary>The original owner and its replacement after retirement.</summary>
    private const int ConnectionOwnersAfterRetirement = 2;

    /// <summary>The first measured commit after the two warm-up writes.</summary>
    private const int FirstReuseClientSequence = 3;

    /// <summary>The final measured commit for native resource reuse.</summary>
    private const int LastReuseClientSequence = 18;

    /// <summary>Checks warm commits retain one owned connection, fixed native statements and derived key.</summary>
    /// <param name="protect">Whether the store encrypts records.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WarmCommitsReuseOwnedConnectionAndPreparedStatements(bool protect)
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        var protection = protect
            ? SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(LocalStoreKey.CreateRandom(ReuseKeyId)))
            : null;
        var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, DefaultLeaseBytes, capture, protection);
        using (store)
        {
            store.Initialize(new(StoreIdentity, SchemaVersion, protect), CancellationToken.None);
            _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
            _ = store.CommitLocalOperation(CreateOperation(1), CreateSnapshotMutation(0), CancellationToken.None);
            _ = store.CommitLocalOperation(CreateOperation(SecondClientSequence), CreateSnapshotMutation(1), CancellationToken.None);
            await Assert.That(capture.Connection).IsNotNull();
            var connection = capture.Connection!;
            var prepared = connection.PreparationCount;
            for (var sequence = FirstReuseClientSequence; sequence <= LastReuseClientSequence; sequence++)
            {
                _ = store.CommitLocalOperation(CreateOperation(sequence), CreateSnapshotMutation(sequence - 1), CancellationToken.None);
            }

            await Assert.That(capture.Connections.Count).IsEqualTo(1);
            await Assert.That(connection.PreparationCount).IsEqualTo(prepared);
            await Assert.That(capture.Checkpoints.Count).IsGreaterThan(0);
            if (protection is not null)
            {
                await Assert.That(protection.DerivationCount).IsEqualTo(1L);
            }
        }

        await Assert.That(capture.Connection!.IsDisposed).IsTrue();
    }

    /// <summary>Checks same-connection writes invalidate global trust even when SQLite data_version stays unchanged.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UntrackedOwnConnectionMutationInvalidatesGlobalProofVerification()
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        var protection = SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(LocalStoreKey.CreateRandom(ReuseKeyId)));
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, DefaultLeaseBytes, capture, protection);
        store.Initialize(new(StoreIdentity, SchemaVersion, true), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var first = CreateOperation(1);
        var second = CreateOperation(SecondClientSequence);
        _ = store.CommitLocalOperation(first, CreateSnapshotMutation(0), CancellationToken.None);
        _ = store.CommitLocalOperation(second, CreateSnapshotMutation(1), CancellationToken.None);
        _ = store.GetOperationStatus(first.OperationId, CancellationToken.None);
        await Assert.That(capture.Connection).IsNotNull();
        using (var tamper = capture.Connection!.CreateStatement())
        {
            tamper.SetSql("DELETE FROM oc_operation_state_proofs WHERE operation_id = $operationId;");
            _ = tamper.Bind("$operationId", second.OperationId.Value.ToString("D"));
            await Assert.That(tamper.Execute()).IsEqualTo(1);
        }

        await Assert.That(() => store.GetOperationStatus(first.OperationId, CancellationToken.None))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Checks failed cleanup retires a damaged native owner and the next operation reopens verified state.</summary>
    /// <param name="protect">Whether the store encrypts records.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailedNativeCleanupRetiresConnectionBeforeRetry(bool protect)
    {
        using var database = TempDatabase.Create();
        var capture = new CapturingConnectionFaultPoint();
        var protection = protect
            ? SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(LocalStoreKey.CreateRandom(ReuseKeyId)))
            : null;
        using var store = new SqliteLocalCommitStore(database.Path, TimeProvider.System, DefaultLeaseBytes, capture, protection);
        store.Initialize(new(StoreIdentity, SchemaVersion, protect), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(1), CreateSnapshotMutation(0), CancellationToken.None);
        await Assert.That(capture.Connection).IsNotNull();
        var previous = capture.Connection!;
        previous.Dispose();
        var next = CreateOperation(SecondClientSequence);
        await Assert.That(() => store.CommitLocalOperation(next, CreateSnapshotMutation(1), CancellationToken.None))
            .ThrowsExactly<ObjectDisposedException>();
        var committed = store.CommitLocalOperation(next, CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That(committed.ClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(capture.Connections.Count).IsEqualTo(ConnectionOwnersAfterRetirement);
        await Assert.That(previous.IsDisposed).IsTrue();
        await Assert.That(store.GetOperationStatus(next.OperationId, CancellationToken.None)?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Captures native owners without changing their connection state or checkpoint behavior.</summary>
    private sealed class CapturingConnectionFaultPoint : ISqliteCommitFaultPoint
    {
        /// <summary>Gets the native connections seen by local commits.</summary>
        internal HashSet<SqliteDatabase> Connections { get; } = [];

        /// <summary>Gets the named durability checkpoints observed.</summary>
        internal HashSet<SqliteCommitCheckpoint> Checkpoints { get; } = [];

        /// <summary>Gets the latest local commit connection.</summary>
        internal SqliteDatabase? Connection { get; private set; }

        /// <inheritdoc/>
        public void BeforeLocalCommitTransaction(SqliteDatabase connection)
        {
            Connection = connection;
            _ = Connections.Add(connection);
        }

        /// <inheritdoc/>
        public void Reached(SqliteCommitCheckpoint checkpoint) => _ = Checkpoints.Add(checkpoint);
    }
}
