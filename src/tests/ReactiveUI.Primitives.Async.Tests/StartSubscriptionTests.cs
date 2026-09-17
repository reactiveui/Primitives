// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests ordered execution and failure delivery for start subscriptions.</summary>
public sealed class StartSubscriptionTests
{
    /// <summary>Both start paths deliver the action result before successful completion.</summary>
    /// <param name="schedule">Whether to queue the action.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Action_ThenValueAndCompletionAreDeliveredInOrder(bool schedule)
    {
        ManualTaskScheduler scheduler = new();
        List<string> calls = [];
        CallbackWitnessAsync<RxVoid> observer = new(
            (_, _) =>
        {
            calls.Add("value");
            return default;
        },
            null,
            result =>
        {
            calls.Add(result.IsSuccess ? "completed" : "failed");
            return default;
        });
        await using StartSubscription subscription = new(observer, () => calls.Add("action"), schedule ? scheduler : null);
        var running = TaskSignalState.ExecuteAsync(subscription, observer, CancellationToken.None).AsTask();
        if (schedule)
        {
            await Assert.That(calls).IsEmpty();
            scheduler.RunNext();
        }

        await running;
        await Assert.That(calls).IsCollectionEqualTo(["action", "value", "completed"]);
    }

    /// <summary>A function failure completes the observer without emitting a value.</summary>
    /// <param name="schedule">Whether to queue the function.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Function_WhenItThrows_ThenCompletesWithTheSameFailure(bool schedule)
    {
        ManualTaskScheduler scheduler = new();
        InvalidOperationException expected = new("function failed");
        List<int> values = [];
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            values.Add(value);
            return default;
        },
            null,
            result =>
        {
            completions.Add(result);
            return default;
        });
        await using StartSubscription<int> subscription = new(observer, () => throw expected, schedule ? scheduler : null);
        var running = TaskSignalState.ExecuteAsync(subscription, observer, CancellationToken.None).AsTask();
        if (schedule)
        {
            await Assert.That(completions).IsEmpty();
            scheduler.RunNext();
        }

        await running;
        await Assert.That(values).IsEmpty();
        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Cancellation before dispatch prevents the user function from running.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Function_WhenCanceledBeforeDispatch_ThenDoesNotRun()
    {
        ManualTaskScheduler scheduler = new();
        using CancellationTokenSource cancellation = new();
        var calls = 0;
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(static (_, _) => default, null, result =>
        {
            completions.Add(result);
            return default;
        });
        await using StartSubscription<int> subscription = new(observer, () => ++calls, scheduler);
        var running = TaskSignalState.ExecuteAsync(subscription, observer, cancellation.Token).AsTask();
        await cancellation.CancelAsync();
        scheduler.RunNext();
        await running;
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsTypeOf<TaskCanceledException>();
    }
}
