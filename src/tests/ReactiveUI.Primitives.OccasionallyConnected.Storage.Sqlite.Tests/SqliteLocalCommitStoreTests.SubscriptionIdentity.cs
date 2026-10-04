// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Subscription identity persistence tests for <see cref="SqliteLocalCommitStore"/>.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Verifies an empty preferred identity cannot create a durable stream mapping.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPreferredSubscriptionIdentityIsEmpty_ThenStoreRejectsItAndAcceptsRetry()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        Action invalid = () => store.GetOrCreateSubscriptionId(Stream, new SubscriptionId(Guid.Empty), CancellationToken.None);

        await Assert.That(invalid).ThrowsExactly<ArgumentException>();

        var accepted = new SubscriptionId(Guid.NewGuid());
        await Assert.That(store.GetOrCreateSubscriptionId(Stream, accepted, CancellationToken.None)).IsEqualTo(accepted);
    }

    /// <summary>Verifies a conflicting preferred identity cannot replace the durable mapping after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPreferredSubscriptionIdentityConflictsAfterReopen_ThenOriginalIdentitySurvives()
    {
        using var database = TempDatabase.Create();
        var original = new SubscriptionId(Guid.NewGuid());
        using (var first = CreateInitializedStore(database.Path))
        {
            await Assert.That(first.GetOrCreateSubscriptionId(Stream, original, CancellationToken.None)).IsEqualTo(original);
        }

        using var reopened = CreateInitializedStore(database.Path);
        Action conflict = () => reopened.GetOrCreateSubscriptionId(Stream, new SubscriptionId(Guid.NewGuid()), CancellationToken.None);
        await Assert.That(conflict).ThrowsExactly<InvalidOperationException>();
        await Assert.That(reopened.GetOrCreateSubscriptionId(Stream, original, CancellationToken.None)).IsEqualTo(original);
    }
}
