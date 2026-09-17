// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Runs tasks on the calling thread, queuing nested work until the current task returns.</summary>
internal sealed class CustomTaskScheduler : TaskScheduler
{
    /// <summary>Serializes access to pending work.</summary>
    private readonly Lock _gate = new();

    /// <summary>Tasks waiting for the current task to return.</summary>
    private readonly Queue<Task> _tasks = new();

    /// <summary>Whether a caller is executing pending work.</summary>
    private bool _isDraining;

    /// <summary>Initializes a new instance of the <see cref="CustomTaskScheduler"/> class.</summary>
    internal CustomTaskScheduler()
    {
    }

    /// <inheritdoc/>
    protected override void QueueTask(Task task)
    {
        lock (_gate)
        {
            _tasks.Enqueue(task);
            if (_isDraining)
            {
                return;
            }

            _isDraining = true;
        }

        while (true)
        {
            Task? next;
            lock (_gate)
            {
                if (!_tasks.TryDequeue(out next))
                {
                    _isDraining = false;
                    return;
                }
            }

            IgnoredResult.Of(TryExecuteTask(next));
        }
    }

    /// <inheritdoc/>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

    /// <inheritdoc/>
    protected override IEnumerable<Task>? GetScheduledTasks()
    {
        lock (_gate)
        {
            return _tasks.ToArray();
        }
    }
}
