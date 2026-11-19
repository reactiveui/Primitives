// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="FlatMapCoordinator{TSource, TResult}"/>.</summary>
public sealed class FlatMapCoordinatorTests
{
    /// <summary>Iteration count for deep synchronous stack-safety verification.</summary>
    private const int StackSafetyDepth = 10_000;

    /// <summary>Iteration count for concurrent completion stress loops.</summary>
    private const int RaceIterations = 150;

    /// <summary>Maximum wait in seconds for concurrent completion.</summary>
    private const int StressTimeoutSeconds = 5;

    /// <summary>Second value used in reentrancy and race tests.</summary>
    private const int SecondValue = 2;

    /// <summary>An inner source completing during Subscribe while the outer pushes the next value does not leak the second subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_InnerCompletesDuringSubscribeAndOuterPushesNext_DisposesSecondInnerOnDispose()
    {
        using Signal<int> outer = new();
        var secondDisposals = 0;
        var second = Signal.Create<int>(_ => new ActionDisposable(() => secondDisposals++));
        var first = Signal.Create<int>(obs =>
        {
            obs.OnCompleted();
            outer.OnNext(SecondValue);
            return EmptyDisposable.Instance;
        });

        using FlatMapCoordinator<int, int> coordinator = new(
            outer,
            key => key == 1 ? first : second,
            new RecordingWitness<int>());
        var sub = coordinator.Run();
        outer.OnNext(1);
        sub.Dispose();

        await Assert.That(secondDisposals).IsEqualTo(1);
    }

    /// <summary>Outer emissions raised during an inner Subscribe are queued and subscribed sequentially without losing handles.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_OuterEmitsDuringInnerSubscribe_QueuesAndSubscribesInOrder()
    {
        using Signal<int> outer = new();
        var secondDisposals = 0;
        IObserver<int>? secondObserver = null;
        var second = Signal.Create<int>(obs =>
        {
            secondObserver = obs;
            return new ActionDisposable(() => secondDisposals++);
        });
        var first = Signal.Create<int>(obs =>
        {
            outer.OnNext(SecondValue);
            obs.OnNext(1);
            obs.OnCompleted();
            return EmptyDisposable.Instance;
        });

        RecordingWitness<int> downstream = new();
        using FlatMapCoordinator<int, int> coordinator = new(
            outer,
            key => key == 1 ? first : second,
            downstream);
        var sub = coordinator.Run();
        outer.OnNext(1);
        secondObserver!.OnNext(SecondValue);
        sub.Dispose();

        await Assert.That(downstream.Values.SequenceEqual([1, SecondValue])).IsTrue();
        await Assert.That(secondDisposals).IsEqualTo(1);
    }

    /// <summary>Concurrent outer completion and active inner completion never drop the terminal completion signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_ConcurrentOuterAndInnerCompletion_AlwaysDeliversCompletion()
    {
        for (var i = 0; i < RaceIterations; i++)
        {
            using Signal<int> outer = new();
            using Signal<int> inner = new();
            TaskCompletionSource<bool> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            using FlatMapCoordinator<int, int> coordinator = new(
                outer,
                _ => inner,
                new DelegateWitness<int>(static _ => { }, static _ => { }, () => done.TrySetResult(true)));
            using var sub = coordinator.Run();

            outer.OnNext(1);
            var t1 = Task.Run(outer.OnCompleted);
            var t2 = Task.Run(() =>
            {
                inner.OnNext(SecondValue);
                inner.OnCompleted();
            });

            await Task.WhenAll(t1, t2);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(StressTimeoutSeconds));
            await Assert.That(await done.Task.WaitAsync(timeout.Token)).IsTrue();
        }
    }

    /// <summary>Chaining ten thousand synchronous inner sequences completes iteratively without stack overflow.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_TenThousandSynchronousInnerSources_CompletesWithoutStackOverflow()
    {
        var sum = 0;
        _ = Signal.Sequence(0, StackSafetyDepth).FlatMap(static _ => Signal.Emit(1)).Subscribe(x => sum += x);
        await Assert.That(sum).IsEqualTo(StackSafetyDepth);
    }
}
