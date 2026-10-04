// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Runs test actions on dedicated background threads and waits for them with a bound.</summary>
internal static class BackgroundThread
{
    /// <summary>How long a test waits for a thread that should finish promptly.</summary>
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    /// <summary>Runs an action on a new background thread.</summary>
    /// <param name="action">The action to run.</param>
    /// <returns>A task that completes with the thread's managed id once the action returns.</returns>
    internal static Task<int> Start(Action action)
    {
        TaskCompletionSource<int> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        new Thread(() =>
        {
            try
            {
                action();
                done.SetResult(Environment.CurrentManagedThreadId);
            }
            catch (Exception ex)
            {
                done.SetException(ex);
            }
        }) { IsBackground = true }.Start();
        return done.Task;
    }

    /// <summary>Waits a bounded time for a task to finish.</summary>
    /// <param name="task">The task to wait for.</param>
    /// <returns><see langword="true"/> when the task finished within the bound.</returns>
    internal static Task<bool> FinishesPromptly(Task task)
    {
        // The task-owned event signals completion directly. WhenAny can instead race
        // a queued notification against the timer even after the worker finished.
        // This checks completion only; awaiting the task still propagates its outcome.
        return Task.FromResult(((IAsyncResult)task).AsyncWaitHandle.WaitOne(Prompt));
    }
}
