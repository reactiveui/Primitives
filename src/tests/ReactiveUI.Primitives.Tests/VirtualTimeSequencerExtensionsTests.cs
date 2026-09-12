// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests action scheduling through the generic virtual-time interface.</summary>
public sealed class VirtualTimeSequencerExtensionsTests
{
    /// <summary>Both generic overloads deliver actions at the requested virtual time.</summary>
    /// <param name="absolute">Whether to use the absolute-time overload.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Schedule_WhenDeadlineArrives_ThenInvokesTheAction(bool absolute)
    {
        var sequencer = MinimalVirtualClock.Create();
        List<int> pending = [1];
        using var subscription = absolute
            ? sequencer.ScheduleAbsolute(1L, pending.Clear)
            : sequencer.ScheduleRelative(1L, pending.Clear);

        await Assert.That(pending.Count).IsEqualTo(1);
        sequencer.AdvanceBy(1L);
        await Assert.That(pending.Count).IsEqualTo(0);
    }
}
