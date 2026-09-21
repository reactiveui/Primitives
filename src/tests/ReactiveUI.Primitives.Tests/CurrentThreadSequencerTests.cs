// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests current-thread scheduling recovery after an interrupted initial wait, and provider-driven timing.</summary>
public class CurrentThreadSequencerTests
{
    /// <summary>The delay the blocking tests wait for.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>An interrupted initial wait clears the trampoline state so later work on that thread still executes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Schedule_InterruptedInitialWait_AllowsSubsequentWork()
    {
        TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var interrupted = false;
        List<int> firstWorkPending = [1];
        var subsequentWorkRan = false;
        var resetAfterInterruption = false;
        var resetAfterWork = false;
        Thread thread = new(() =>
        {
            try
            {
                var sequencer = Sequencer.CurrentThread;
                Thread.CurrentThread.Interrupt();
                try
                {
                    using var pending = sequencer.Schedule(TimeSpan.FromDays(1), firstWorkPending.Clear);
                }
                catch (ThreadInterruptedException)
                {
                    interrupted = true;
                }

                resetAfterInterruption = CurrentThreadSequencer.IsScheduleRequired;
                using var next = sequencer.Schedule(() => subsequentWorkRan = true);
                resetAfterWork = CurrentThreadSequencer.IsScheduleRequired;
                completion.SetResult();
            }
            catch (Exception error)
            {
                completion.SetException(error);
            }
        });

        thread.Start();
        await completion.Task;

        await Assert.That(interrupted).IsTrue();
        await Assert.That(firstWorkPending.Count).IsEqualTo(1);
        await Assert.That(resetAfterInterruption).IsTrue();
        await Assert.That(subsequentWorkRan).IsTrue();
        await Assert.That(resetAfterWork).IsTrue();
    }

    /// <summary>A sequencer rejects a missing provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsANullProvider() =>
        await Assert.That(static () => new CurrentThreadSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        CurrentThreadSequencer sequencer = new(clock);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(OneSecond);

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + OneSecond);
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(OneSecond));
    }

    /// <summary>Work already due runs on the calling thread without waiting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WorkAlreadyDueRunsWithoutCreatingATimer()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        CurrentThreadSequencer sequencer = new(provider);
        RecordingWorkItem item = new();
        RecordingWorkItem cancelled = new();
        cancelled.Dispose();

        sequencer.Schedule(item);
        sequencer.Schedule(cancelled, sequencer.Timestamp);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(cancelled.ExecuteCount).IsEqualTo(0);
        await Assert.That(provider.TimerDueTimes).IsEmpty();
    }

    /// <summary>Delayed work blocks the calling thread until the provider's timer fires, then runs.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkBlocksUntilTheProviderIsAdvancedPastTheDueTime()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, TimeSpan.TicksPerSecond);
        CurrentThreadSequencer sequencer = new(provider);
        RecordingWorkItem item = new();
        var due = sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond);

        var blocked = Task.Run(() => sequencer.Schedule(item, due));
        await provider.TimerCreated;

        await Assert.That(provider.TimerDueTimes.SequenceEqual([OneSecond])).IsTrue();
        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(OneSecond - TimeSpan.FromTicks(1));

        await Assert.That(blocked.IsCompleted).IsFalse();

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        await blocked;

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Work scheduled on another instance while a trampoline runs joins that trampoline.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WorkFromAnotherInstanceJoinsTheRunningTrampoline()
    {
        CurrentThreadSequencer running = new(new VirtualClock());
        CurrentThreadSequencer other = new(new VirtualClock());
        RecordingWorkItem nested = new();
        var ranAfterOuter = false;

        _ = running.Schedule(() =>
        {
            other.Schedule(nested, other.Timestamp);
            ranAfterOuter = nested.ExecuteCount == 0;
        });

        await Assert.That(ranAfterOuter).IsTrue();
        await Assert.That(nested.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Work scheduled on the same instance while its trampoline runs keeps its own timestamp.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WorkFromTheRunningInstanceJoinsTheTrampolineAtItsOwnTimestamp()
    {
        CurrentThreadSequencer sequencer = new(new VirtualClock());
        RecordingWorkItem nested = new();

        _ = sequencer.Schedule(() => sequencer.Schedule(nested, sequencer.Timestamp));

        await Assert.That(nested.ExecuteCount).IsEqualTo(1);
    }
}
