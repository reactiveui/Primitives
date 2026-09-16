// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests interval cancellation and notification ordering with a controlled clock.</summary>
public sealed class IntervalSubscriptionTests
{
    /// <summary>A zero period uses the system-clock path without waiting for wall-clock time.</summary>
    /// <param name="explicitSystemProvider">Whether the system provider is supplied explicitly.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ExecuteAsync_ZeroPeriod_StopsAfterObserverCancellation(bool explicitSystemProvider)
    {
        using CancellationTokenSource cancellation = new();
        List<long> ticks = [];
        CallbackWitnessAsync<long> observer = new(RecordThenCancel(ticks, cancellation));
        await using IntervalSubscription subscription = new(
            observer,
            TimeSpan.Zero,
            explicitSystemProvider ? TimeProvider.System : null);

        await TaskSignalState.ExecuteAsync(subscription, observer, cancellation.Token);

        await Assert.That(ticks).IsCollectionEqualTo([1L]);
    }

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
        var running = TaskSignalState.ExecuteAsync(subscription, observer, cancellation.Token).AsTask();
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
        var running = TaskSignalState.ExecuteAsync(subscription, observer, cancellation.Token).AsTask();
        await time.FireNextAsync();
        await entered.Task;
        await Assert.That(time.PendingTimerCount).IsEqualTo(0);
        await cancellation.CancelAsync();
        resume.SetResult();
        await running;
        await Assert.That(ticks).IsCollectionEqualTo([1L]);
        await Assert.That(time.PendingTimerCount).IsEqualTo(0);
    }

    /// <summary>Creates a tick callback that records the tick and cancels the source synchronously.</summary>
    /// <param name="ticks">Receives the delivered ticks.</param>
    /// <param name="cancellation">The source to cancel on the first tick.</param>
    /// <returns>The tick callback.</returns>
    private static Func<long, CancellationToken, ValueTask> RecordThenCancel(List<long> ticks, CancellationTokenSource cancellation) =>
        (tick, _) =>
        {
            ticks.Add(tick);
            CancelSynchronously(cancellation);
            return default;
        };

    /// <summary>Cancels the source on the calling thread, running its registrations before returning.</summary>
    /// <param name="source">The source to cancel.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CancelSynchronously(CancellationTokenSource source) => source.Cancel();
}
