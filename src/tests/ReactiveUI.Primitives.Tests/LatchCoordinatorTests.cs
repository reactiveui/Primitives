// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="LatchCoordinator{TLeft, TRight, TResult}"/>.</summary>
public sealed class LatchCoordinatorTests
{
    /// <summary>Iteration count for concurrent completion stress loops.</summary>
    private const int RaceIterations = 150;

    /// <summary>Maximum wait in seconds for concurrent completion.</summary>
    private const int StressTimeoutSeconds = 5;

    /// <summary>Secondary arm value.</summary>
    private const int SecondaryValue = 2;

    /// <summary>Expected combined value.</summary>
    private const int ExpectedCombined = 3;

    /// <summary>Concurrent primary emission/completion and secondary update never drops completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_ConcurrentPrimaryCompletionAndSecondaryUpdate_AlwaysDeliversCompletion()
    {
        for (var i = 0; i < RaceIterations; i++)
        {
            using Signal<int> primary = new();
            using Signal<int> secondary = new();
            TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var observed = 0;
            using var sub = new LatchCoordinator<int, int, int>(
                new DelegateWitness<int>(x => observed = x, static _ => { }, () => done.TrySetResult(true)),
                static (l, r) => l + r).Run(primary, secondary);

            secondary.OnNext(SecondaryValue);
            var t1 = Task.Run(() =>
            {
                primary.OnNext(1);
                primary.OnCompleted();
            });
            var t2 = Task.Run(() =>
            {
                secondary.OnNext(SecondaryValue);
                secondary.OnCompleted();
            });

            await Task.WhenAll(t1, t2);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(StressTimeoutSeconds));
            await Assert.That(await done.Task.WaitAsync(timeout.Token)).IsTrue();
            await Assert.That(observed).IsEqualTo(ExpectedCombined);
        }
    }

    /// <summary>Verifies the latch delivers its terminal both during subscribe and after its subscriptions are stored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Terminal_DuringSubscribeAndAfterSubscribe_DeliversOnce()
    {
        InvalidOperationException boom = new("latch");

        RecordingWitness<int> syncFailed = new();
        using (new LatchCoordinator<int, int, int>(syncFailed, static (l, r) => l + r).Run(Signal.Throw<int>(boom), Signal.Return(SecondaryValue)))
        {
            await Assert.That(syncFailed.Errors.SequenceEqual([boom])).IsTrue();
        }

        using Signal<int> primary = new();
        using Signal<int> secondary = new();
        RecordingWitness<int> live = new();
        using (new LatchCoordinator<int, int, int>(live, static (l, r) => l + r).Run(primary, secondary))
        {
            secondary.OnNext(SecondaryValue);
            primary.OnNext(1);
            primary.OnCompleted();
        }

        await Assert.That(live.Values.SequenceEqual([ExpectedCombined])).IsTrue();
        await Assert.That(live.Completed).IsEqualTo(1);
    }
}
