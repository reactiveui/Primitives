// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="ThreadPoolSequencer"/> driven by a <see cref="TimeProvider"/>.</summary>
public class ThreadPoolSequencerTests
{
    /// <summary>Timestamp ticks per second of a provider that counts milliseconds.</summary>
    private const long MillisecondFrequency = 1000;

    /// <summary>Name recorded by the work due after one second.</summary>
    private const string First = "first";

    /// <summary>Name recorded by the work due after two seconds.</summary>
    private const string Second = "second";

    /// <summary>A short delay used for scheduled work.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>A delay later than <see cref="OneSecond"/>.</summary>
    private static readonly TimeSpan TwoSeconds = TimeSpan.FromSeconds(2);

    /// <summary>The smallest step of the virtual clock.</summary>
    private static readonly TimeSpan OneTick = TimeSpan.FromTicks(1);

    /// <summary>A sequencer rejects a missing provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsANullProvider() =>
        await Assert.That(static () => new ThreadPoolSequencer((TimeProvider)null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        var sequencer = InlineThreadPool.Create(clock);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(TwoSeconds);

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + TwoSeconds);
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(TwoSeconds));
    }

    /// <summary>Work scheduled for a timestamp runs when the provider is advanced to it and not before.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ScheduledWorkRunsExactlyWhenTheProviderReachesItsTimestamp()
    {
        VirtualClock clock = new();
        var sequencer = InlineThreadPool.Create(clock);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        clock.AdvanceBy(OneSecond - OneTick);

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(OneTick);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);

        clock.AdvanceBy(TwoSeconds);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Work due at several timestamps runs in due order as the provider advances.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ScheduledWorkRunsInDueOrder()
    {
        VirtualClock clock = new();
        var sequencer = InlineThreadPool.Create(clock);
        List<string> order = [];

        _ = sequencer.Schedule(order, TwoSeconds, static names => names.Add(Second));
        _ = sequencer.Schedule(order, OneSecond, static names => names.Add(First));
        clock.AdvanceBy(OneSecond);

        await Assert.That(order.SequenceEqual([First])).IsTrue();

        clock.AdvanceBy(OneSecond);

        await Assert.That(order.SequenceEqual([First, Second])).IsTrue();
    }

    /// <summary>Work scheduled by due work re-arms the delay timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WorkScheduledByDueWorkRunsWhenItsOwnTimestampIsReached()
    {
        VirtualClock clock = new();
        var sequencer = InlineThreadPool.Create(clock);
        RecordingWorkItem later = new();

        _ = sequencer.Schedule(
            (Owner: sequencer, Later: later),
            OneSecond,
            static state => state.Owner.Schedule(state.Later, state.Owner.Timestamp + Sequencer.ToTimestampDelta(OneSecond)));
        clock.AdvanceBy(OneSecond);

        await Assert.That(later.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(OneSecond);

        await Assert.That(later.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Cancelled work never runs.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancelledWorkDoesNotRun()
    {
        VirtualClock clock = new();
        var sequencer = InlineThreadPool.Create(clock);
        RecordingWorkItem cancelled = new();
        RecordingWorkItem kept = new();

        sequencer.Schedule(cancelled, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        sequencer.Schedule(kept, sequencer.Timestamp + Sequencer.ToTimestampDelta(TwoSeconds));
        cancelled.Dispose();
        clock.AdvanceBy(TwoSeconds);

        await Assert.That(cancelled.ExecuteCount).IsEqualTo(0);
        await Assert.That(kept.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Immediate work runs through the immediate queue without touching the delay timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ImmediateWorkDoesNotCreateAnyTimer()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        var sequencer = InlineThreadPool.Create(provider);
        RecordingWorkItem now = new();
        RecordingWorkItem elapsed = new();

        sequencer.Schedule(now);
        sequencer.Schedule(elapsed, sequencer.Timestamp);

        await Assert.That(now.ExecuteCount).IsEqualTo(1);
        await Assert.That(elapsed.ExecuteCount).IsEqualTo(1);
        await Assert.That(provider.TimerDueTimes).IsEmpty();
    }

    /// <summary>All delayed work shares one timer created by the provider on first use.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkSharesOneProviderTimerCreatedOnFirstUse()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        var sequencer = InlineThreadPool.Create(provider);

        await Assert.That(provider.TimerDueTimes).IsEmpty();

        sequencer.Schedule(new RecordingWorkItem(), sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        sequencer.Schedule(new RecordingWorkItem(), sequencer.Timestamp + Sequencer.ToTimestampDelta(TwoSeconds));

        await Assert.That(provider.TimerDueTimes.Length).IsEqualTo(1);
    }

    /// <summary>A provider counting at a different frequency drives delayed work at the right wall time.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProviderWithAnotherFrequencyDrivesDelayedWork()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, MillisecondFrequency);
        var sequencer = InlineThreadPool.Create(provider);
        RecordingWorkItem item = new();

        await Assert.That(provider.TimestampFrequency).IsNotEqualTo(Stopwatch.Frequency);

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        clock.AdvanceBy(OneSecond - TimeSpan.FromMilliseconds(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromMilliseconds(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(sequencer.Timestamp).IsEqualTo(Sequencer.ToTimestampDelta(OneSecond));
    }

    /// <summary>Disposal cancels pending delayed work and stops the delay timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeCancelsPendingWorkAndStopsTheTimer()
    {
        VirtualClock clock = new();
        var sequencer = InlineThreadPool.Create(clock);
        RecordingWorkItem pending = new();

        sequencer.Schedule(pending, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        sequencer.Dispose();
        clock.AdvanceBy(TwoSeconds);

        await Assert.That(pending.IsDisposed).IsTrue();
        await Assert.That(pending.ExecuteCount).IsEqualTo(0);
    }
}
