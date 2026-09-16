// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests monotonic duration conversion.</summary>
public partial class SequencerTests
{
    /// <summary>The largest timestamp duration saturates when it exceeds TimeSpan's range.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ToTimeSpanDelta_WhenTimestampIsMaximum_ThenClampsToTheDurationRange()
    {
        var ticks = decimal.Ceiling(((decimal)long.MaxValue * TimeSpan.TicksPerSecond) / System.Diagnostics.Stopwatch.Frequency);
        var expected = TimeSpan.FromTicks((long)Math.Min(ticks, TimeSpan.MaxValue.Ticks));

        await Assert.That(Sequencer.ToTimeSpanDelta(long.MaxValue)).IsEqualTo(expected);
    }

    /// <summary>The absolute action overload queues work until its virtual deadline.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Schedule_WhenAbsoluteDeadlineArrives_ThenRunsTheAction()
    {
        VirtualClock clock = new();
        ISequencer sequencer = clock;
        List<int> pending = [1];
        var due = clock.Now.AddTicks(1);
        using var subscription = sequencer.Schedule(due, pending.Clear);

        await Assert.That(pending.Count).IsEqualTo(1);
        clock.AdvanceTo(due);
        await Assert.That(pending.Count).IsEqualTo(0);
    }

    /// <summary>A delta past TimeSpan's range saturates at a coarse timestamp frequency.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ToTimeSpanDelta_BeyondRangeAtCoarseFrequency_Saturates()
    {
        const long CoarseFrequency = TimeSpan.TicksPerSecond;

        await Assert.That(Sequencer.ToTimeSpanDelta(long.MaxValue, CoarseFrequency)).IsEqualTo(TimeSpan.MaxValue);
    }
}
