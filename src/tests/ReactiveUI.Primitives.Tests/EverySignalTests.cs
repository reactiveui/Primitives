// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the recurring <c>Every</c> timer on the current-thread sequencer, whose trampoline runs the
/// ticks on the subscribing thread and therefore has to hand the subscription back before it starts ticking.
/// </summary>
public sealed class EverySignalTests
{
    /// <summary>The number of ticks the bounded subscriptions ask for.</summary>
    private const int RequestedTicks = 3;

    /// <summary>The tick indices a three-tick subscription must observe.</summary>
    private static readonly long[] ExpectedTicks = [0L, 1L, 2L];

    /// <summary>The period between ticks.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(10);

    /// <summary>The time a subscribing thread is given to finish before it is declared livelocked.</summary>
    private static readonly TimeSpan LivelockTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a bounded <c>Every</c> on the current-thread sequencer terminates instead of livelocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerStopsWhenTakeReachesItsCount()
    {
        List<long> ticks = [];
        var completions = 0;

        var subscriber = RunOnDedicatedThread(() =>
        {
            using var subscription = Signal.Every(TickPeriod, Sequencer.CurrentThread)
                .Take(RequestedTicks)
                .Subscribe(ticks.Add, static _ => { }, () => completions++);
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(ticks.SequenceEqual(ExpectedTicks)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies the current-thread ticks stay on the subscribing thread rather than moving to a pool thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerTicksOnTheSubscribingThread()
    {
        List<int> tickThreadIds = [];
        var subscriberThreadId = 0;

        var subscriber = RunOnDedicatedThread(() =>
        {
            subscriberThreadId = Environment.CurrentManagedThreadId;
            using var subscription = Signal.Every(TickPeriod, Sequencer.CurrentThread)
                .Take(RequestedTicks)
                .Subscribe(_ => tickThreadIds.Add(Environment.CurrentManagedThreadId));
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(tickThreadIds.Count).IsEqualTo(RequestedTicks);
        await Assert.That(tickThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
    }

    /// <summary>Runs the subscription body on its own background thread so a livelock cannot hang the test host.</summary>
    /// <param name="body">The subscription body to run.</param>
    /// <returns>A task that completes when the body returns.</returns>
    private static Task<bool> RunOnDedicatedThread(Action body)
    {
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new(() =>
        {
            try
            {
                body();
                completion.SetResult(true);
            }
            catch (Exception error)
            {
                completion.SetException(error);
            }
        })
        {
            IsBackground = true,
        };
        thread.Start();
        return completion.Task;
    }

    /// <summary>Waits for the subscribing thread to finish within the livelock timeout.</summary>
    /// <param name="subscriber">The task that completes when the subscribing thread returns.</param>
    /// <returns><see langword="true"/> when the subscribing thread finished in time.</returns>
    private static async Task<bool> CompletedWithinTimeout(Task<bool> subscriber)
    {
        var finished = await Task.WhenAny(subscriber, Task.Delay(LivelockTimeout)).ConfigureAwait(false);
        if (!ReferenceEquals(finished, subscriber))
        {
            return false;
        }

        await subscriber.ConfigureAwait(false);
        return true;
    }
}
