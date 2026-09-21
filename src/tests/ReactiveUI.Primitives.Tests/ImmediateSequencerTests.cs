// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="ImmediateSequencer"/> driven by a <see cref="TimeProvider"/>.</summary>
public class ImmediateSequencerTests
{
    /// <summary>The delay the blocking tests wait for.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>The smallest step of the virtual clock.</summary>
    private static readonly TimeSpan OneTick = TimeSpan.FromTicks(1);

    /// <summary>A sequencer rejects a missing provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsANullProvider() =>
        await Assert.That(static () => new ImmediateSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        ImmediateSequencer sequencer = new(clock);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(OneSecond);

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + OneSecond);
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(OneSecond));
    }

    /// <summary>Work runs before the call returns, and cancelled work is skipped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ImmediateWorkRunsInlineUnlessCancelled()
    {
        ImmediateSequencer sequencer = new(new VirtualClock());
        RecordingWorkItem item = new();
        RecordingWorkItem cancelled = new();
        cancelled.Dispose();

        sequencer.Schedule(item);
        sequencer.Schedule(cancelled);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(cancelled.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Work already due runs without waiting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WorkAlreadyDueRunsWithoutCreatingATimer()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        ImmediateSequencer sequencer = new(provider);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(provider.TimerDueTimes).IsEmpty();
    }

    /// <summary>Delayed work blocks the caller until the provider's timer fires, then runs.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkBlocksUntilTheProviderIsAdvancedPastTheDueTime()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        ImmediateSequencer sequencer = new(provider);
        RecordingWorkItem item = new();
        var due = sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond);

        var blocked = Task.Run(() => sequencer.Schedule(item, due));
        await provider.TimerCreated;

        await Assert.That(provider.TimerDueTimes.SequenceEqual([OneSecond])).IsTrue();
        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(OneSecond - OneTick);

        await Assert.That(blocked.IsCompleted).IsFalse();

        clock.AdvanceBy(OneTick);
        await blocked;

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Delayed work cancelled while its caller is blocked does not run.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkCancelledWhileBlockedDoesNotRun()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        ImmediateSequencer sequencer = new(provider);
        RecordingWorkItem item = new();
        var due = sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond);

        var blocked = Task.Run(() => sequencer.Schedule(item, due));
        await provider.TimerCreated;
        item.Dispose();
        clock.AdvanceBy(OneSecond);
        await blocked;

        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }
}
