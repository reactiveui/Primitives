// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Checks the dedicated test worker's bounded completion wait.</summary>
public sealed class BackgroundThreadTests
{
    /// <summary>Checks successful, faulted, and canceled operations count as completed.</summary>
    /// <param name="completion">The completion kind.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments("success")]
    [Arguments("fault")]
    [Arguments("cancel")]
    public async Task CompletedOperationsRetainTheirOriginalOutcome(string completion)
    {
        TaskCompletionSource source = new(TaskCreationOptions.RunContinuationsAsynchronously);
        switch (completion)
        {
            case "fault":
            {
                source.SetException(new InvalidOperationException("worker failed"));
                break;
            }

            case "cancel":
            {
                source.SetCanceled();
                break;
            }

            default:
            {
                source.SetResult();
                break;
            }
        }

        await Assert.That(await BackgroundThread.FinishesPromptly(source.Task)).IsTrue();
        await Assert.That(source.Task.IsFaulted).IsEqualTo(completion == "fault");
        await Assert.That(source.Task.IsCanceled).IsEqualTo(completion == "cancel");
    }

    /// <summary>Checks a blocked worker fails the original bound even when it later completes.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task BlockedWorkerDoesNotPassAfterItsDeadline()
    {
        using ManualResetEventSlim release = new(false);
        var worker = BackgroundThread.Start(release.Wait);
        bool completed;
        try
        {
            completed = await BackgroundThread.FinishesPromptly(worker);
        }
        finally
        {
            release.Set();
        }

        _ = await worker;
        await Assert.That(completed).IsFalse();
        await Assert.That(worker.IsCompletedSuccessfully).IsTrue();
    }
}
