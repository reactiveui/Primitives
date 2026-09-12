// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the <c>Expire</c> operator's inter-element deadline, which completes the sequence with a <see cref="TimeoutException"/>.</summary>
public class ExpireOperatorTests
{
    /// <summary>The inter-element deadline used by the tests.</summary>
    private static readonly TimeSpan DueTime = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies that a source which never produces a value trips the deadline and the sequence completes with a <see cref="TimeoutException"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNoValueArrivesWithinTheDeadline_ThenCompletesWithTimeoutException()
    {
        ManualTimeProvider time = new();
        var pending = SignalAsync.Never<int>().Timeout(DueTime, time).FirstAsync().AsTask();
        await time.FireNextAsync();
        await Assert.That(() => pending).ThrowsExactly<TimeoutException>();
    }

    /// <summary>Verifies that timer control becomes a no-op after disposal removes the timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimerDisposed_ThenRearmAndStopDoNothing()
    {
        ManualTimeProvider time = new();
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(static (_, _) => default, null, result =>
        {
            completions.Add(result);
            return default;
        });
        SignalAsyncExtensions.TimeoutSignal<int>.TimeoutWitness witness = new(observer, DueTime, time);
        witness.StartTimer(CancellationToken.None);
        var timer = await time.NextTimerAsync();
        witness.StopTimer();
        await Assert.That(timer.DueTime).IsEqualTo(Timeout.InfiniteTimeSpan);
        witness.RearmTimer();
        await Assert.That(timer.DueTime).IsEqualTo(DueTime);
        await witness.DisposeAsync();
        witness.RearmTimer();
        witness.StopTimer();
        timer.Fire();
        await Assert.That(completions).IsEmpty();
    }

    /// <summary>Verifies that rearming an expired deadline cannot produce a second completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTheRearmedDeadlineTicksAfterExpiry_ThenNoSecondCompletionIsSent()
    {
        ManualTimeProvider time = new();
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(static (_, _) => default, null, result =>
        {
            completions.Add(result);
            return default;
        });
        await using SignalAsyncExtensions.TimeoutSignal<int>.TimeoutWitness witness = new(observer, DueTime, time);
        witness.StartTimer(CancellationToken.None);
        var timer = await time.NextTimerAsync();
        timer.Fire();
        await Assert.That(completions).Count().IsEqualTo(1);
        await witness.OnNextAsync(1, CancellationToken.None);
        await Assert.That(timer.DueTime).IsEqualTo(DueTime);
        timer.Fire();
        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsTypeOf<TimeoutException>();
    }
}
