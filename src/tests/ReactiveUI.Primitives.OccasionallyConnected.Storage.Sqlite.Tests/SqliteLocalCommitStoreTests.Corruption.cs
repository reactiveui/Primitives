// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Corruption recovery tests for <see cref="SqliteLocalCommitStore"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Verifies null operation base versions remain durable and distinct from empty base versions.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenBaseVersionIsNull_ThenRecoveryPreservesNullAndDuplicateIntentRemainsExact()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1) with { BaseVersion = null };
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);

        var first = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        var replay = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        Action emptyBaseVersion = () => store.CommitLocalOperation(operation with { BaseVersion = string.Empty }, snapshot, CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(emptyBaseVersion).ThrowsExactly<InvalidOperationException>();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].BaseVersion).IsNull();
    }

    /// <summary>Verifies drift between duplicated stream and identity subscription ids is rejected during recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamSubscriptionIdDriftsFromIdentity_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        SetStreamSubscriptionId(database.Path, SubscriptionId.New());

        Action action = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Sets the duplicated stream subscription id directly.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="subscriptionId">The drifted subscription id.</param>
    private static void SetStreamSubscriptionId(string path, SubscriptionId subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_streams
            SET subscription_id = $subscriptionId
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.ExecuteNonQuery();
    }
}
