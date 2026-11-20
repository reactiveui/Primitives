// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests concurrent completion races on <see cref="CombineLatestCoordinator{TLeft, TRight, TResult}"/> and <see cref="CombineLatestCoordinator{TResult}"/>.</summary>
public sealed class CombineLatestCoordinatorTests
{
    /// <summary>Iteration count for concurrent completion stress loops.</summary>
    private const int RaceIterations = 150;

    /// <summary>Maximum wait in seconds for concurrent completion.</summary>
    private const int StressTimeoutSeconds = 5;

    /// <summary>Second arm value.</summary>
    private const int SecondValue = 2;

    /// <summary>Third arm value.</summary>
    private const int ThirdValue = 3;

    /// <summary>Concurrent completion of both binary CombineLatest arms never drops completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BinaryCoordinator_ConcurrentArmCompletion_AlwaysDeliversCompletion()
    {
        for (var i = 0; i < RaceIterations; i++)
        {
            using Signal<int> left = new();
            using Signal<int> right = new();
            TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            using var sub = new CombineLatestCoordinator<int, int, int>(
                new DelegateWitness<int>(static _ => { }, static _ => { }, () => done.TrySetResult(true)),
                static (l, r) => l + r).Run(left, right);

            left.OnNext(1);
            right.OnNext(SecondValue);
            var t1 = Task.Run(left.OnCompleted);
            var t2 = Task.Run(right.OnCompleted);

            await Task.WhenAll(t1, t2);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(StressTimeoutSeconds));
            await Assert.That(await done.Task.WaitAsync(timeout.Token)).IsTrue();
        }
    }

    /// <summary>Concurrent completion across multi-source CombineLatest arms never drops completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MultiSourceCoordinator_ConcurrentArmCompletion_AlwaysDeliversCompletion()
    {
        for (var i = 0; i < RaceIterations; i++)
        {
            using Signal<int> first = new();
            using Signal<int> second = new();
            using Signal<int> third = new();
            TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            using var sub = LinqExtensions.CombineLatest<int>(first, second, third).Subscribe(
                static _ => { },
                static _ => { },
                () => done.TrySetResult(true));

            first.OnNext(1);
            second.OnNext(SecondValue);
            third.OnNext(ThirdValue);
            var t1 = Task.Run(first.OnCompleted);
            var t2 = Task.Run(second.OnCompleted);
            var t3 = Task.Run(third.OnCompleted);

            await Task.WhenAll(t1, t2, t3);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(StressTimeoutSeconds));
            await Assert.That(await done.Task.WaitAsync(timeout.Token)).IsTrue();
        }
    }
}
