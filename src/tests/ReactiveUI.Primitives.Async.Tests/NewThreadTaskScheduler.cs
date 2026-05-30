// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// A <see cref="TaskScheduler"/> that launches each queued task on its own dedicated background thread.
/// Provides deterministic concurrency for tests that need real parallel execution without competing for
/// limited <see cref="ThreadPool"/> resources (which can deadlock when many tests run in parallel and each
/// blocks waiting on inner work that also needs a pool thread).
/// </summary>
internal sealed class NewThreadTaskScheduler : TaskScheduler
{
    /// <summary>Singleton instance.</summary>
    public static readonly NewThreadTaskScheduler Instance = new();

    /// <summary>Initializes a new instance of the <see cref="NewThreadTaskScheduler"/> class.</summary>
    private NewThreadTaskScheduler()
    {
    }

    /// <inheritdoc/>
    protected override void QueueTask(Task task) =>
        new Thread(state => TryExecuteTask((Task)state!))
        {
            IsBackground = true,
            Name = "NewThreadTaskScheduler",
        }.Start(task);

    /// <inheritdoc/>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

    /// <inheritdoc/>
    protected override IEnumerable<Task>? GetScheduledTasks() => null;
}
