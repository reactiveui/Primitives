// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies durable publish options and initialization cancellation through the stream facade.</summary>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies subscription identity and concurrency options survive a typed durable publish.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PublishRetainsBaseVersionAndSubscriptionOptions()
    {
        const string expectedBaseVersion = "server-version-42";
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateDefinition() with
        {
            Subscription = new RemoteSubscriptionOptions { StreamId = Stream, SubscriptionId = ExplicitSubscription },
        };
        await using var stream = CreateStream(store, definition);
        var options = new RemotePublishOptions { StreamId = Stream, BaseVersion = expectedBaseVersion, Durable = true };

        var receipt = await stream.PublishAsync(new(FirstValue), options, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await Assert.That(stream.SubscriptionId).IsEqualTo(ExplicitSubscription);
        await Assert.That(recovered.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(recovered.PendingOperations[0].BaseVersion).IsEqualTo(expectedBaseVersion);
        await Assert.That(recovered.PendingOperations[0].Policy.Durability).IsEqualTo(OperationDurability.Durable);
    }

    /// <summary>Verifies cancellation of first publish leaves initialization usable without committing canceled input.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task CanceledFirstPublishPreservesSharedInitialization()
    {
        const int expectedPendingCount = 2;
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { IdentityEntered = identityEntered, ReleaseIdentity = releaseIdentity };
        await using var stream = CreateStream(store, coordinator);
        using CancellationTokenSource canceled = new();
        var first = stream.PublishAsync(new(FirstValue), null, canceled.Token).AsTask();
        await identityEntered.Task;
        try
        {
            await canceled.CancelAsync();
            await Assert.That(first).Throws<OperationCanceledException>();
        }
        finally
        {
            releaseIdentity.SetResult();
        }

        using CancellationTokenSource active = new();
        var second = await stream.PublishAsync(new(SecondValue), null, active.Token);
        var third = await stream.PublishAsync(new(ThirdValue), null, active.Token);
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(second.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(third.ClientSequence).IsEqualTo(SecondSequence);
        await Assert.That(recovered.PendingOperations).Count().IsEqualTo(expectedPendingCount);
        await Assert.That(coordinator.IdentityCalls).IsEqualTo(1);
    }
}
