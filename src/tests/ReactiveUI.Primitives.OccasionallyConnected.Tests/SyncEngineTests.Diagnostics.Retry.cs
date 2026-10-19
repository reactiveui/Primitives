// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Retry diagnostics helpers for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Waits for the trigger's replacement retry timer before virtual time can advance.</summary>
    /// <param name="engine">The running engine.</param>
    /// <param name="clock">The manual clock.</param>
    /// <param name="session">The upload session.</param>
    /// <param name="firstSync">The initial synchronization task.</param>
    /// <param name="retryDelay">The unchanged retry delay.</param>
    /// <param name="releaseTimer">The timer gate that outlives engine disposal.</param>
    /// <param name="pauseReplacementTimer">Whether to hold timer registration.</param>
    /// <returns>The pending retry synchronization task.</returns>
    private static async Task<Task> TriggerDiagnosticsRetryAfterTimerRearmsAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        PreparedSession session,
        Task firstSync,
        TimeSpan retryDelay,
        ManualResetEventSlim releaseTimer,
        bool pauseReplacementTimer)
    {
        var creatingTimer = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var timerReleased = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var timerCount = clock.CreatedTimerCount;
        clock.OnCreatingTimer = dueTime =>
        {
            if (dueTime != retryDelay)
            {
                return;
            }

            _ = creatingTimer.TrySetResult(true);
            try
            {
                if (!releaseTimer.Wait(GuardTimeout))
                {
                    throw new TimeoutException("Replacement retry timer was not released.");
                }
            }
            finally
            {
                _ = timerReleased.TrySetResult(true);
            }
        };
        Task retrySync;
        try
        {
            retrySync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await creatingTimer.Task.WaitAsync(GuardTimeout);
            await Assert.That(firstSync.IsCompleted).IsFalse();
            await Assert.That(retrySync.IsCompleted).IsFalse();
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            if (pauseReplacementTimer)
            {
                await Assert.That(clock.CreatedTimerCount).IsEqualTo(timerCount);
            }
        }
        finally
        {
            releaseTimer.Set();
            clock.OnCreatingTimer = null;
            if (creatingTimer.Task.IsCompleted)
            {
                await timerReleased.Task.WaitAsync(GuardTimeout);
            }
        }

        await WaitForConditionAsync(() => clock.CreatedTimerCount > timerCount && clock.HasTimerDueIn(retryDelay));
        return retrySync;
    }
}
