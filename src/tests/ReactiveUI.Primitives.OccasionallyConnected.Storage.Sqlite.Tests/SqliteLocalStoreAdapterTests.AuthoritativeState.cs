// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Authoritative snapshot persistence through the public adapter.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies a worker-admitted authoritative payload survives disposal and reopening.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeSnapshotPassesWorkerAdmission_ThenBothStatesSurviveReopening()
    {
        using var database = TempDatabase.Create();
        var authoritative = CreatePayload("confirmed");
        var snapshot = CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = authoritative };
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.Snapshot?.State.PayloadHash).IsEqualTo(snapshot.State.PayloadHash);
        await Assert.That(recovery.Snapshot?.AuthoritativeState?.PayloadHash).IsEqualTo(authoritative.PayloadHash);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
    }
}
