// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests task cancellation and result delivery.</summary>
public sealed class SignalExtensionsTests
{
    /// <summary>The action-free overload consumes cancellation from a task without a result.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task HandleCancellation_WhenTaskIsCanceled_ThenCompletesNormally()
    {
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();
        var task = Task.FromCanceled(cancellation.Token);

        var handled = task.HandleCancellation();
        await handled;

        await Assert.That(handled.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Cancellation ends the wait while the original task remains free to finish later.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task WhenCancelled_WhenTokenWins_ThenPreservesTheTokenAndOriginalTask()
    {
        const int result = 7;
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource<int> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiting = source.Task.WhenCancelled(cancellation.Token);

        await cancellation.CancelAsync();
        var error = await Assert.That(async () => await waiting).Throws<TaskCanceledException>();

        await Assert.That(error!.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(source.Task.IsCompleted).IsFalse();
        source.SetResult(result);
        await Assert.That(await source.Task).IsEqualTo(result);
    }
}
