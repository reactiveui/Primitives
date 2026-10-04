// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb.Tests;

/// <content>Tests lifetime ownership of the cached durable store state.</content>
public sealed partial class LiteDbLocalStoreAdapterTests
{
    /// <summary>Rejects a competing owner and permits its retry only after ownership is released.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task CompetingOwnerCanRetryAfterOwnerDisposes()
    {
        using var directory = new TestDirectory();
        await using var owner = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await using var competing = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await owner.InitializeAsync(CreateInitialization(), CancellationToken.None);
        await Assert.ThrowsAsync<IOException>(
            () => competing.InitializeAsync(CreateInitialization(), CancellationToken.None).AsTask());
        await owner.DisposeAsync();
        await competing.InitializeAsync(CreateInitialization(), CancellationToken.None);
        await Assert.That(File.Exists($"{directory.DatabasePath}.owner.lock")).IsTrue();
    }

    /// <summary>Releases ownership when durable identity validation fails during initialization.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task FailedInitializationReleasesLifetimeOwnership()
    {
        using var directory = new TestDirectory();
        await using (var initial = new LiteDbLocalStoreAdapter(directory.DatabasePath))
        {
            await initial.InitializeAsync(CreateInitialization(), CancellationToken.None);
        }

        await using var invalid = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => invalid.InitializeAsync(
                new("another-store", 1, false) { ClientId = ClientIdentity },
                CancellationToken.None).AsTask());
        await using var valid = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await valid.InitializeAsync(CreateInitialization(), CancellationToken.None);
    }
}
