// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Retains scheduled tasks until explicitly executed.</summary>
internal sealed class ManualTaskScheduler : TaskScheduler
{
    /// <summary>Tasks waiting for execution.</summary>
    private readonly Queue<Task> _tasks = new();

    /// <summary>Gets the number of tasks waiting for execution.</summary>
    internal int PendingCount => _tasks.Count;

    /// <summary>Executes the oldest queued task.</summary>
    internal void RunNext() => _ = TryExecuteTask(_tasks.Dequeue());

    /// <inheritdoc/>
    protected override void QueueTask(Task task) => _tasks.Enqueue(task);

    /// <inheritdoc/>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

    /// <inheritdoc/>
    protected override IEnumerable<Task> GetScheduledTasks() => _tasks;
}
