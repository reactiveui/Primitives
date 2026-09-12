// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests store guarantees at the durable commit boundary.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>Verifies a volatile adapter cannot return a successful durable publish receipt.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsDurablePublishAgainstInMemoryStore()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new("committer-capabilities", 1, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var options = CreateOptions(new(), new());
        var committer = CreateLocalCommitter(options with
        {
            SubscriptionId = subscription,
            Dependencies = options.Dependencies with { Store = store },
        });
        _ = await committer.RecoverAsync(CancellationToken.None);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.Snapshot).IsNull();
        await Assert.That(recovered.NextClientSequence).IsEqualTo(1);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies both atomicity and durability are required before the store receives a mutation.</summary>
    /// <param name="capabilities">The incomplete store guarantees.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(LocalStoreCapabilities.None)]
    [Arguments(LocalStoreCapabilities.AtomicLocalCommit)]
    [Arguments(LocalStoreCapabilities.DurableLocalCommit)]
    public async Task CommitAsyncRejectsIncompleteStoreCapabilities(LocalStoreCapabilities capabilities)
    {
        var store = new ScriptedLocalStore { Capabilities = capabilities };
        var committer = await CreateRecoveredCommitterAsync(store);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.Revision).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }
}
