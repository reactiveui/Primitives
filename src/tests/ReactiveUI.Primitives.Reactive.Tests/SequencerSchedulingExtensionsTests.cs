// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using Microsoft.Reactive.Testing;
using ReactiveUI.Primitives.Reactive.Signals;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Verifies the seam that gives <see cref="IScheduler"/> the shared-source sequencer scheduling shape.</summary>
public class SequencerSchedulingExtensionsTests
{
    /// <summary>The relative delay, in ticks, applied by the shift.</summary>
    private const long ShiftTicks = 10;

    /// <summary>Virtual ticks to advance past the shift delay.</summary>
    private const long AdvancePastShift = 100;

    /// <summary>The values pushed through the shifted pipeline.</summary>
    private static readonly int[] Values = [1, 2, 3];

    /// <summary>The <c>Shift</c> operator schedules each notification on the supplied System.Reactive scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Shift_SchedulesNotificationsOnSuppliedScheduler()
    {
        TestScheduler scheduler = new();
        List<int> received = [];

        using var sub = Signal.FromEnumerable(Values).Shift(TimeSpan.FromTicks(ShiftTicks), scheduler).Subscribe(received.Add);

        // The shift defers delivery onto the scheduler, so nothing arrives until virtual time advances.
        await Assert.That(received).IsEmpty();

        scheduler.AdvanceBy(AdvancePastShift);

        await Assert.That(received).IsEquivalentTo(Values, EqualityComparer<int>.Default);
    }

    /// <summary>The <c>Shift</c> operator delivers inline when given the immediate scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Shift_OnImmediateScheduler_DeliversAllValues()
    {
        List<int> received = [];

        using var sub = Signal.FromEnumerable(Values).Shift(TimeSpan.Zero, ImmediateScheduler.Instance).Subscribe(received.Add);

        await Assert.That(received).IsEquivalentTo(Values, EqualityComparer<int>.Default);
    }
}
