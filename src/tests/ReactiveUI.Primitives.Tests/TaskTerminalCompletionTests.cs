// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the task-terminal completion helper.</summary>
public class TaskTerminalCompletionTests
{
    /// <summary>Expected value used by terminal completion assertions.</summary>
    private const int ExpectedValue = 42;

    /// <summary>Verifies that the terminal task is exposed and resolved with the supplied value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskReturnsResolvedResult()
    {
        TaskTerminalCompletion<int> completion = new();

        var task = completion.Task;
        completion.Resolve(ExpectedValue);

        await Assert.That(await task).IsEqualTo(ExpectedValue);
    }

    /// <summary>Verifies that attaching after synchronous completion disposes the subscription immediately.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AttachAfterResolveDisposesSubscription()
    {
        TaskTerminalCompletion<int> completion = new();
        RecordingDisposable subscription = new();

        completion.Resolve(ExpectedValue);
        var task = completion.Attach(subscription, CancellationToken.None);

        await Assert.That(await task).IsEqualTo(ExpectedValue);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that cancellation disposes the attached subscription and cancels the terminal task.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AttachWithCanceledTokenDisposesSubscription()
    {
        TaskTerminalCompletion<int> completion = new();
        RecordingDisposable subscription = new();
        using CancellationTokenSource cts = new();

        var task = completion.Attach(subscription, cts.Token);
        await cts.CancelAsync();

        await Assert.That(() => task).Throws<TaskCanceledException>();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
