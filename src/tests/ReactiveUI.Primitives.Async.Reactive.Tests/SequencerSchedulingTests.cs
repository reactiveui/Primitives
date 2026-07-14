// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Async.Reactive.Tests;

/// <summary>
/// Exercises the scheduling seam the Reactive async leaf uses to give <c>IScheduler</c> the sequencer shape the
/// shared source expects. Every overload must run the work it is handed on the supplied scheduler; the immediate
/// scheduler used here runs it before <c>Schedule</c> returns.
/// <para>
/// This file deliberately does not import <c>System.Reactive.Concurrency</c>: the <c>Scheduler</c> class in that
/// namespace carries extension methods with the same signatures, and importing it would make every call below
/// ambiguous. The scheduler types are therefore spelled out in full.
/// </para>
/// </summary>
public class SequencerSchedulingTests
{
    /// <summary>State threaded through the closure-free stateful overloads.</summary>
    private const int ScheduledState = 42;

    /// <summary>How many times the recursive overload reschedules itself before it stops.</summary>
    private const int ExpectedRecursiveRuns = 3;

    /// <summary>The scheduler the seam forwards to, typed as the interface the seam extends.</summary>
    private static readonly System.Reactive.Concurrency.IScheduler InlineScheduler =
        System.Reactive.Concurrency.ImmediateScheduler.Instance;

    /// <summary>A due time that has already passed, so absolute scheduling runs inline.</summary>
    private static readonly DateTimeOffset ElapsedDueTime = DateTimeOffset.UnixEpoch;

    /// <summary>Verifies the plain action overload runs the action on the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenActionScheduled_ThenItRunsOnTheScheduler()
    {
        RunRecorder recorder = new();

        using var handle = InlineScheduler.Schedule(recorder.Record);

        await Assert.That(recorder.HasRun).IsTrue();
    }

    /// <summary>Verifies the relative-due-time overload runs the action once the due time has elapsed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenActionScheduledAfterRelativeDueTime_ThenItRuns()
    {
        RunRecorder recorder = new();

        using var handle = InlineScheduler.Schedule(TimeSpan.Zero, recorder.Record);

        await Assert.That(recorder.HasRun).IsTrue();
    }

    /// <summary>Verifies the absolute-due-time overload runs the action once the due time has passed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenActionScheduledAtAbsoluteDueTime_ThenItRuns()
    {
        RunRecorder recorder = new();

        using var handle = InlineScheduler.Schedule(ElapsedDueTime, recorder.Record);

        await Assert.That(recorder.HasRun).IsTrue();
    }

    /// <summary>Verifies the recursive overload keeps running until the action stops asking for another turn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRecursiveActionScheduled_ThenItRerunsUntilItStops()
    {
        var runs = 0;

        using var handle = InlineScheduler.Schedule(self =>
        {
            runs++;
            if (runs >= ExpectedRecursiveRuns)
            {
                return;
            }

            self();
        });

        await Assert.That(runs).IsEqualTo(ExpectedRecursiveRuns);
    }

    /// <summary>Verifies the stateful overload hands the state to the action instead of capturing it.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatefulActionScheduled_ThenTheStateIsHandedToTheAction()
    {
        var received = 0;

        using var handle = InlineScheduler.Schedule(ScheduledState, x => received = x);

        await Assert.That(received).IsEqualTo(ScheduledState);
    }

    /// <summary>Verifies the stateful relative-due-time overload hands the state to the action once the due time elapses.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatefulActionScheduledAfterRelativeDueTime_ThenTheStateIsHandedToTheAction()
    {
        var received = 0;

        using var handle = InlineScheduler.Schedule(ScheduledState, TimeSpan.Zero, x => received = x);

        await Assert.That(received).IsEqualTo(ScheduledState);
    }

    /// <summary>
    /// Records that the scheduler ran a plain action. Holding the flag here lets the action overloads be handed
    /// <see cref="Record"/> as a method group, so the callback closes over nothing.
    /// </summary>
    private sealed class RunRecorder
    {
        /// <summary>Gets a value indicating whether the scheduler ran the action.</summary>
        public bool HasRun { get; private set; }

        /// <summary>Records that the scheduled action ran.</summary>
        public void Record() => HasRun = true;
    }
}
