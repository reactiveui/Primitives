// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>SyncTimerObservable</c> — covers the mid-array remove path of the
/// shared timer's observer set, the idempotent subscription dispose, and the empty-targets
/// fast-path inside the tick callback.</summary>
public class SyncTimerObservableTests
{
    /// <summary>Number of periods to advance for the idempotent-dispose assertion.</summary>
    private const int IdempotentAdvancePeriods = 3;

    /// <summary>Verifies that disposing the middle subscription of three keeps the other two ticking.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMiddleObserverDisposed_ThenOthersStillReceiveTicks()
    {
        var scheduler = new VirtualClock();
        var firstTicks = 0;
        var secondTicks = 0;
        var thirdTicks = 0;
        var period = TimeSpan.FromTicks(100);

        var timer = ReactiveExtensions.SyncTimer(period, scheduler);

        using var subFirst = timer.Subscribe(_ => firstTicks++);
        var subSecond = timer.Subscribe(_ => secondTicks++);
        using var subThird = timer.Subscribe(_ => thirdTicks++);

        scheduler.AdvanceBy(period.Ticks);
        var secondTicksBeforeDispose = secondTicks;
        subSecond.Dispose();
        scheduler.AdvanceBy(period.Ticks);

        await Assert.That(firstTicks).IsGreaterThanOrEqualTo(1);
        await Assert.That(thirdTicks).IsGreaterThanOrEqualTo(1);
        await Assert.That(secondTicks).IsEqualTo(secondTicksBeforeDispose);
    }

    /// <summary>Verifies that disposing a subscription twice is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposedTwice_ThenIdempotent()
    {
        var scheduler = new VirtualClock();
        var ticks = 0;
        var period = TimeSpan.FromTicks(100);

        var timer = ReactiveExtensions.SyncTimer(period, scheduler);
        var sub = timer.Subscribe(_ => ticks++);

        sub.Dispose();
        sub.Dispose();

        scheduler.AdvanceBy(period.Ticks * IdempotentAdvancePeriods);

        await Assert.That(ticks).IsEqualTo(0);
    }
}
