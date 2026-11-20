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

    /// <summary>
    /// An observer that disposes the lifecycle from inside a delivery and then calls back in re-enters the gate,
    /// and the disposed lifecycle drops both the value and the resumed error.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EmitAndErrorResume_ReenteredAfterDisposeInsideDelivery_AreDropped()
    {
        const int FirstValue = 1;
        const int LateValue = 2;
        List<int> values = [];
        List<Exception> errors = [];
        SyncLatestLifecycle<int>? lifecycle = null;
        CallbackWitnessAsync<int> observer = new(
            async (value, _) =>
            {
                values.Add(value);
                await lifecycle!.DisposeAsync();
                await lifecycle.EmitDownstreamAsync(LateValue);
                await lifecycle.OnErrorResumeAsync(new InvalidOperationException("late"));
            },
            (error, _) =>
            {
                errors.Add(error);
                return default;
            });
        lifecycle = new(observer, 1);

        await lifecycle.EmitDownstreamAsync(FirstValue);

        await Assert.That(values).IsCollectionEqualTo([FirstValue]);
        await Assert.That(errors).IsEmpty();
        await Assert.That(lifecycle.HasDisposed).IsTrue();
    }
}
