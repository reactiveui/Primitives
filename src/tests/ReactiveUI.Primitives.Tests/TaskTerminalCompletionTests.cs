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

    /// <summary>Verifies that attaching with an already-canceled token cancels inline and releases the registration.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AttachWithAlreadyCanceledTokenCancelsAndDisposesSubscription()
    {
        TaskTerminalCompletion<int> completion = new();
        RecordingDisposable subscription = new();

        var task = completion.Attach(subscription, new(true));

        await Assert.That(() => task).Throws<TaskCanceledException>();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that a fault releases the subscription and surfaces through the terminal task.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FailReleasesSubscriptionAndFaultsTask()
    {
        TaskTerminalCompletion<int> completion = new();
        RecordingDisposable subscription = new();
        InvalidOperationException error = new("terminal");

        var task = completion.Attach(subscription, CancellationToken.None);
        completion.Fail(error);

        var caught = await Assert.That(() => task).Throws<InvalidOperationException>();
        await Assert.That(caught!).IsSameReferenceAs(error);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies the shared empty-source failure shape.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FailEmptyFaultsWithEmptySourceError()
    {
        TaskTerminalCompletion<int> completion = new();

        completion.FailEmpty();

        var caught = await Assert.That(() => completion.Task).Throws<InvalidOperationException>();
        await Assert.That(caught!.Message).Contains("without producing a value");
    }

    /// <summary>Verifies that resolving after attach releases the registration so later cancellation is a no-op.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResolveAfterAttachReleasesRegistration()
    {
        TaskTerminalCompletion<int> completion = new();
        RecordingDisposable subscription = new();
        using CancellationTokenSource cts = new();

        var task = completion.Attach(subscription, cts.Token);
        completion.Resolve(ExpectedValue);
        await cts.CancelAsync();

        await Assert.That(await task).IsEqualTo(ExpectedValue);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
