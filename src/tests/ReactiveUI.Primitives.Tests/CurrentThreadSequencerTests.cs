// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests current-thread scheduling recovery after an interrupted initial wait.</summary>
public class CurrentThreadSequencerTests
{
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
}
