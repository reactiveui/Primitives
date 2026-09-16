// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the teardown of the combine-latest subscription lifecycle.</summary>
public sealed class SyncLatestLifecycleTests
{
    /// <summary>A failing source subscription disposal still cancels and releases the lifecycle before the failure surfaces.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FinishAsync_SubscriptionDisposalFails_CleansUpAndRethrows()
    {
        InvalidOperationException expected = new("subscription dispose failed");
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        SyncLatestLifecycle<int> lifecycle = new(observer, 1);
        lifecycle.Subscriptions[0] = new FailingAsyncDisposable(expected);

        var error = await Assert.That(async () => await lifecycle.FinishAsync(null)).ThrowsExactly<InvalidOperationException>();

        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(lifecycle.HasDisposed).IsTrue();
        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
    }
}
