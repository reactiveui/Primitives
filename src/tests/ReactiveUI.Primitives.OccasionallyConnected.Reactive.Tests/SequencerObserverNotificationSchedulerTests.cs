// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.OccasionallyConnected.Reactive;
using TUnit.Assertions;
using TUnit.Core;

namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive.Tests;

/// <summary>Verifies observer notification scheduling uses System.Reactive.</summary>
public sealed class SequencerObserverNotificationSchedulerTests
{
    /// <summary>Verifies scheduled work executes through the System.Reactive scheduler.</summary>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task ScheduleExecutesWorkOnSystemReactiveScheduler()
    {
        var workItem = new RecordingWorkItem();
        var scheduler = new SequencerObserverNotificationScheduler(CurrentThreadScheduler.Instance);

        scheduler.Schedule(workItem);

        await Assert.That(workItem.ExecutionCount).IsEqualTo(1);
    }

    /// <summary>Records scheduled execution.</summary>
    private sealed class RecordingWorkItem : IWorkItem
    {
        /// <summary>Gets the number of times the work item executed.</summary>
        public int ExecutionCount { get; private set; }

        /// <summary>Records one execution.</summary>
        public void Execute() => ExecutionCount++;
    }
}
