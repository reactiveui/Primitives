// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
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

        subscription.Dispose();
        subscription.Dispose();
        InvokePrivateTick(subscription);
        scheduler.AdvanceBy(TimeSpan.FromTicks(Period.Ticks * 3));

        await Assert.That(ticks).IsEqualTo(0);
    }

    /// <summary>Invokes the private tick method to exercise the disposed tick guard.</summary>
    /// <param name="subscription">The subscription under test.</param>
    [SuppressMessage(
        "Major Code Smell",
        "S3011:Reflection should not be used to increase accessibility of classes, methods, or fields",
        Justification = "Coverage-only test exercises a disposed guard otherwise reachable only through a scheduler race.")]
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Coverage-only test invokes a private member on a concrete test subscription instance.")]
    private static void InvokePrivateTick(IDisposable subscription)
    {
        var method = subscription.GetType().GetMethod("Tick", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(subscription.GetType().FullName, "Tick");

        _ = method.Invoke(subscription, null);
    }
}
