// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies task-instance results and disposal.</summary>
public class TaskInstanceSignalTests
{
    /// <summary>The value emitted by a successful task.</summary>
    private const int Value = 7;

    /// <summary>Pending task instances forward success, failure, or cancellation.</summary>
    /// <param name="outcome">Zero for success, one for failure, or two for cancellation.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task PendingTaskForwardsItsOutcome(int outcome)
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskNotificationObserver observer = new();
        using var subscription = Signal.FromTask(pending.Task).Subscribe(observer);
        if (outcome == 0)
        {
            pending.SetResult(Value);
        }
        else if (outcome == 1)
        {
            pending.SetException(new InvalidOperationException());
        }
        else
        {
            pending.SetCanceled();
        }

        await observer.Terminal.Task;
        await Assert.That(observer.Values.Count).IsEqualTo(outcome == 0 ? 1 : 0);
        await Assert.That(observer.Completions).IsEqualTo(outcome == 0 ? 1 : 0);
        if (outcome == 0)
        {
            await Assert.That(observer.Values[0]).IsEqualTo(Value);
            await Assert.That(observer.Error).IsNull();
        }
        else if (outcome == 1)
        {
            await Assert.That(observer.Error).IsTypeOf<InvalidOperationException>();
        }
        else
        {
            await Assert.That(observer.Error).IsTypeOf<TaskCanceledException>();
        }
    }

    /// <summary>Disposed subscriptions suppress results and faults after observation completes.</summary>
    /// <param name="fault">Whether the task fails.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisposedSubscriptionSuppressesOutcome(bool fault)
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskNotificationObserver observer = new();
        TaskInstanceSubscription subscription = new();
        subscription.Dispose();
        if (fault)
        {
            pending.SetException(new InvalidOperationException());
        }
        else
        {
            pending.SetResult(Value);
        }

        await TaskInstanceSignal<int>.ObserveTaskAsync(pending.Task, observer, subscription);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsNull();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }
}
