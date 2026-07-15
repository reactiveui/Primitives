// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Reactive.Tests;

/// <summary>
/// Verifies the seam that gives a System.Reactive scheduler the sequencer scheduling shape the shared Extensions
/// source calls. This leaf recompiles its own copy of the seam, so the Reactive leaf's tests do not cover it.
/// The file deliberately does not import <c>System.Reactive.Concurrency</c>: the <c>Scheduler</c> class in that
/// namespace carries extension methods with the same signatures, and importing it would make every call below
/// ambiguous. The scheduler types are therefore spelled out in full.
/// </summary>
public class SequencerSchedulingExtensionsTests
{
    /// <summary>State threaded through the closure-free stateful overloads.</summary>
    private const int ScheduledState = 42;

    /// <summary>How many times the three action overloads run between them.</summary>
    private const int ExpectedActionRuns = 3;

    /// <summary>How many times the recursive overload reschedules itself before it stops.</summary>
    private const int ExpectedRecursiveRuns = 3;

    /// <summary>The scheduler the seam forwards to, typed as the interface the seam extends.</summary>
    private static readonly System.Reactive.Concurrency.IScheduler InlineScheduler =
        System.Reactive.Concurrency.ImmediateScheduler.Instance;

    /// <summary>A due time that has already passed, so absolute scheduling runs inline.</summary>
    private static readonly DateTimeOffset ElapsedDueTime = DateTimeOffset.UnixEpoch;

    /// <summary>The state both stateful overloads must hand back, once each.</summary>
    private static readonly int[] ExpectedStatePair = [ScheduledState, ScheduledState];

    /// <summary>Verifies a work item scheduled through the seam is executed by the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ScheduleWorkItemExecutesIt()
    {
        CountingWorkItem item = new();

        InlineScheduler.Schedule(item);

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Verifies the seam rejects a missing work item rather than posting nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ScheduleWorkItemRejectsNull() =>
        await Assert.That(static () => InlineScheduler.Schedule((IWorkItem)null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the immediate, relative, and absolute action overloads all run their action.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ActionOverloadsRunOnTheScheduler()
    {
        RunCounter counter = new();

        using var immediate = InlineScheduler.Schedule(counter.Record);
        using var relative = InlineScheduler.Schedule(TimeSpan.Zero, counter.Record);
        using var absolute = InlineScheduler.Schedule(ElapsedDueTime, counter.Record);

        await Assert.That(counter.RunCount).IsEqualTo(ExpectedActionRuns);
    }

    /// <summary>Verifies the recursive overload keeps running until the action stops asking for another turn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task RecursiveOverloadRunsUntilItStopsRescheduling()
    {
        var ran = 0;

        using var subscription = InlineScheduler.Schedule(self =>
        {
            ran++;
            if (ran >= ExpectedRecursiveRuns)
            {
                return;
            }

            self();
        });

        await Assert.That(ran).IsEqualTo(ExpectedRecursiveRuns);
    }

    /// <summary>Verifies the stateful overloads hand the caller's state back without a captured closure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task StatefulOverloadsPassStateThrough()
    {
        List<int> received = [];

        using var immediate = InlineScheduler.Schedule(ScheduledState, received.Add);
        using var delayed = InlineScheduler.Schedule(ScheduledState, TimeSpan.Zero, received.Add);

        await Assert.That(received).IsEquivalentTo(ExpectedStatePair, EqualityComparer<int>.Default);
    }

    /// <summary>Work item that counts how often the scheduler executed it.</summary>
    private sealed class CountingWorkItem : IWorkItem
    {
        /// <summary>Gets the number of executions.</summary>
        public int ExecuteCount { get; private set; }

        /// <inheritdoc/>
        public void Execute() => ExecuteCount++;
    }

    /// <summary>
    /// Counts how often the scheduler ran a plain action. Holding the count here lets the action overloads be
    /// handed <see cref="Record"/> as a method group, so the callback closes over nothing.
    /// </summary>
    private sealed class RunCounter
    {
        /// <summary>Gets the number of times the scheduler ran the action.</summary>
        public int RunCount { get; private set; }

        /// <summary>Records one run of the scheduled action.</summary>
        public void Record() => RunCount++;
    }
}
