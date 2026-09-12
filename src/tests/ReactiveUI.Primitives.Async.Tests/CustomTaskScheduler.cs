// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>A scheduler distinct from <see cref="TaskScheduler.Default"/> that runs each queued task on the thread pool.</summary>
internal sealed class CustomTaskScheduler : TaskScheduler
{
    /// <summary>Singleton instance.</summary>
    internal static readonly CustomTaskScheduler Instance = new();

    /// <summary>Initializes a new instance of the <see cref="CustomTaskScheduler"/> class.</summary>
    private CustomTaskScheduler()
    {
    }

    /// <inheritdoc/>
    protected override void QueueTask(Task task) =>
        ThreadPool.UnsafeQueueUserWorkItem(
            static state => IgnoredResult.Of(state.Scheduler.ExecuteQueued(state.Work)),
            (Scheduler: this, Work: task),
            false);

    /// <inheritdoc/>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

    /// <inheritdoc/>
    protected override IEnumerable<Task>? GetScheduledTasks() => null;

    /// <summary>Runs a queued task on the pool thread that picked it up.</summary>
    /// <param name="task">The queued task.</param>
    /// <returns><see langword="true"/> when the task was executed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ExecuteQueued(Task task) => TryExecuteTask(task);
}
