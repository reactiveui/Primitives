// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for ThreadPoolObserverNotificationScheduler.</summary>
public sealed class ThreadPoolObserverNotificationSchedulerTests
{
    /// <summary>Verifies thread pool scheduler argument validation and rejected queue attempts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThreadPoolSchedulerValidationAndRejectionAreReported()
    {
        var scheduler = new ThreadPoolObserverNotificationScheduler(static (_, _) => false);

        await Assert.That(() => scheduler.Schedule(new NoOpWorkItem())).ThrowsExactly<InvalidOperationException>();
        var schedule = typeof(ThreadPoolObserverNotificationScheduler).GetMethod(nameof(ThreadPoolObserverNotificationScheduler.Schedule));
        ArgumentNullException.ThrowIfNull(schedule);
        await Assert.That(() => schedule.Invoke(scheduler, BindingFlags.DoNotWrapExceptions, null, [null], null)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the thread pool scheduler invokes a queued work item through its callback.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThreadPoolSchedulerInvokesQueuedCallback()
    {
        var workItem = new RecordingWorkItem();
        var scheduler = new ThreadPoolObserverNotificationScheduler(
            static (callback, item) =>
            {
                callback(item);
                return true;
            });

        scheduler.Schedule(workItem);

        await Assert.That(workItem.Executed).IsTrue();
    }

    /// <summary>Provides no-op work for scheduler validation.</summary>
    private sealed class NoOpWorkItem : IWorkItem
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute()
        {
        }
    }

    /// <summary>Records whether work item execution occurred.</summary>
    private sealed class RecordingWorkItem : IWorkItem
    {
        /// <summary>Gets a value indicating whether the work item ran.</summary>
        internal bool Executed { get; private set; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => Executed = true;
    }
}
