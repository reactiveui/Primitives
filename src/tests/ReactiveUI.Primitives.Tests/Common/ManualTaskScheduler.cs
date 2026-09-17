// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Runs queued tasks only when explicitly drained.</summary>
internal sealed class ManualTaskScheduler : TaskScheduler
{
    /// <summary>Tasks awaiting execution.</summary>
    private readonly Queue<Task> _tasks = new();

    /// <summary>Runs every queued task.</summary>
    internal void RunPending()
    {
        while (_tasks.TryDequeue(out var task))
        {
            _ = TryExecuteTask(task);
        }
    }

    /// <inheritdoc/>
    protected override IEnumerable<Task> GetScheduledTasks() => _tasks.ToArray();

    /// <inheritdoc/>
    protected override void QueueTask(Task task) => _tasks.Enqueue(task);

    /// <inheritdoc/>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
}
