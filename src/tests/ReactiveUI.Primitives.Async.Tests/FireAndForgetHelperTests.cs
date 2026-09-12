// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Helpers;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests detached action completion and exception reporting.</summary>
public sealed class FireAndForgetHelperTests
{
    /// <summary>Verifies that asynchronous action completion includes exception reporting.</summary>
    /// <param name="fail">Whether the released action throws.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenDetachedActionFinishes_ThenFailureIsReported(bool fail)
    {
        using UnhandledExceptionCapture capture = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("action failed");
        var completed = false;
        var pending = FireAndForgetHelper.RunAsync(async () =>
        {
            await release.Task;
            completed = true;
            if (fail)
            {
                throw expected;
            }
        });
        await Assert.That(pending.IsCompleted).IsFalse();
        release.SetResult();
        await pending;
        await Assert.That(completed).IsTrue();
        if (!fail)
        {
            return;
        }

        await Assert.That(await capture.WaitForAsync(expected.Message)).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that action validation rejects null delegates.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDetachedActionIsNull_ThenValidationFails() =>
        await Assert.That(static async () => await FireAndForgetHelper.RunAsync(null!)).ThrowsExactly<ArgumentNullException>();
}
