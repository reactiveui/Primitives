// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests interval cancellation and notification ordering with a controlled clock.</summary>
public sealed class IntervalSubscriptionTests
{
    /// <summary>Cancellation while waiting prevents the pending tick from being delivered.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ExecuteAsync_WhenCanceledBeforeTheTimer_ThenDoesNotEmit()
    {
        ManualTimeProvider time = new();
        using CancellationTokenSource cancellation = new();
        List<long> ticks = [];
        List<Result> completions = [];
        CallbackWitnessAsync<long> observer = new(
            (tick, _) =>
        {
            ticks.Add(tick);
            return default;
        },
            null,
            result =>
        {
            completions.Add(result);
            return default;
        });
        await using IntervalSubscription subscription = new(observer, TimeSpan.FromSeconds(1), time);
        var running = subscription.ExecuteAsync(cancellation.Token).AsTask();
        var timer = await time.NextTimerAsync();
        await cancellation.CancelAsync();
        await running;
        timer.Fire();
        await Assert.That(ticks).IsEmpty();
        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsTypeOf<TaskCanceledException>();
    }

    /// <summary>The next delay starts only after the current notification finishes.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ExecuteAsync_WhenNotificationIsPending_ThenDoesNotScheduleAnotherTick()
    {
        ManualTimeProvider time = new();
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<long> ticks = [];
        CallbackWitnessAsync<long> observer = new(
            async (tick, _) =>
        {
            ticks.Add(tick);
            entered.SetResult();
            await resume.Task;
        },
            null,
            null);
        await using IntervalSubscription subscription = new(observer, TimeSpan.FromSeconds(1), time);
        var running = subscription.ExecuteAsync(cancellation.Token).AsTask();
        await time.FireNextAsync();
        await entered.Task;
        await Assert.That(time.PendingTimerCount).IsEqualTo(0);
        await cancellation.CancelAsync();
        resume.SetResult();
        await running;
        await Assert.That(ticks).IsCollectionEqualTo([1L]);
        await Assert.That(time.PendingTimerCount).IsEqualTo(0);
    }
}
