// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for periodic sequencer scheduling helpers.</summary>
public class SequencerPeriodicMixinsTests
{
    /// <summary>Period used by virtual-clock tests.</summary>
    private static readonly TimeSpan Period = TimeSpan.FromTicks(10);

    /// <summary>Verifies repeated disposal and a tick observed after disposal are both no-ops.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPeriodicSubscriptionDisposedTwiceAndTickRuns_ThenNoOp()
    {
        var scheduler = new VirtualClock();
        var ticks = 0;
        var subscription = scheduler.SchedulePeriodic(Period, Period, () => ticks++);

        const int PeriodsToAdvance = 3;
        subscription.Dispose();
        subscription.Dispose();
        InvokeTick(subscription);
        scheduler.AdvanceBy(TimeSpan.FromTicks(Period.Ticks * PeriodsToAdvance));

        await Assert.That(ticks).IsEqualTo(0);
    }

    /// <summary>Invokes the tick method to exercise the disposed tick guard.</summary>
    /// <param name="subscription">The subscription under test.</param>
    private static void InvokeTick(IDisposable subscription) =>
        ((SequencerPeriodicExtensions.PeriodicSubscription<Action>)subscription).Tick();
}
