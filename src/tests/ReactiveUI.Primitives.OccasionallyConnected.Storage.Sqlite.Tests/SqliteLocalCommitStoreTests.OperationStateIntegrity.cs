// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Operation state transaction and recovery integrity tests.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Verifies an ignored initial status write rolls back the entire local commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IgnoredInitialStateWriteRollsBackLocalCommit()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        IgnoreInitialOperationStateInsert(database.Path);

        await Assert.That(() => CommitOperation(store, Stream, 1, OperationPayloadText))
            .ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
    }

    /// <summary>Verifies a rejected barrier write cannot grant permission to send.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IgnoredAttemptWriteNeverGrantsSendPermission()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, 1, OperationPayloadText, DeliveryGuarantee.AtMostOnce);
        var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        CreateDeleteStateBeforeUpdateTrigger(database.Path);

        await Assert.That(() => store.TryBeginRemoteAttempt(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        var status = store.GetOperationStatus(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Verifies restart recovery retains unresolved intent even when it cannot currently be uploaded.</summary>
    /// <param name="state">The unresolved operation state.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncOperationState.Conflict)]
    [Arguments(SyncOperationState.GuaranteeExpired)]
    [Arguments(SyncOperationState.Ambiguous)]
    public async Task RecoveryPreservesUnresolvedIntent(SyncOperationState state)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var first = CommitOperation(store, Stream, 1, OperationPayloadText, DeliveryGuarantee.AtMostOnce);
        var second = CommitOperation(store, Stream, SecondClientSequence, OperationPayloadText);
        SetOperationState(database.Path, first.OperationId, state);
        using var reopened = CreateInitializedStore(database.Path);
        var subscription = reopened.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);

        var recovery = reopened.RecoverStream(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(TwoOperations);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovery.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(await LeaseSingleBatch(reopened, new(Stream, TwoOperations, DefaultLeaseBytes, TimeSpan.FromMinutes(1))))
            .IsNull();
    }

    /// <summary>Verifies missing or invalid state cannot silently remove committed intent during recovery.</summary>
    /// <param name="missing">Whether to delete the state row instead of corrupting its state value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task RecoveryRejectsMissingOrInvalidState(bool missing)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var operation = CommitOperation(store, Stream, 1, OperationPayloadText);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        if (missing)
        {
            DeleteOperationState(database.Path, operation.OperationId);
        }
        else
        {
            SetOperationStateValue(database.Path, operation.OperationId, UndefinedEnumValue);
        }

        await Assert.That(() => store.RecoverStream(Stream, subscription, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies replay-visible operation sequence corruption is rejected separately from pending recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReplaySequenceAtNextClientSequenceFailsRecovery()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscription = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var operation = CommitOperation(store, Stream, 1, OperationPayloadText);
        SetOperationState(database.Path, operation.OperationId, SyncOperationState.Synchronized);
        SetStreamNextClientSequence(database.Path, operation.ClientSequence);

        await Assert.That(() => store.RecoverStream(Stream, subscription, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Installs a real SQLite trigger that ignores initial operation-state insertion.</summary>
    /// <param name="path">The database path.</param>
    private static void IgnoreInitialOperationStateInsert(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER ignore_initial_operation_state
            BEFORE INSERT ON oc_outbox_operation_states
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }
}
