// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the recurring <c>Every</c> timer on the current-thread sequencer, whose trampoline runs the
/// ticks on the subscribing thread and therefore has to hand the subscription back before it starts ticking.
/// Every bounding operator layered over it — <c>Take</c>, <c>TakeWhile</c>, <c>TakeUntil</c>, <c>Any</c>,
/// <c>All</c>, <c>Contains</c>, <c>IsEmpty</c>, <c>Expire</c> — has to enter the trampoline itself, so that the
/// source only queues its first tick and the sink owns the upstream handle in time to dispose it when the bound
/// is reached. An operator that skips that step drains the trampoline from inside the source's own subscribe
/// call and never learns its bound was hit, livelocking the subscribing thread.
/// </summary>
public sealed class EverySignalTests
{
    /// <summary>The number of ticks the bounded subscriptions ask for.</summary>
    private const int RequestedTicks = 3;

    /// <summary>The tick index the value-seeking bounds (<c>Any</c>, <c>All</c>, <c>Contains</c>) look for.</summary>
    private const long SoughtTick = 2L;

    /// <summary>The tick indices a three-tick subscription must observe.</summary>
    private static readonly long[] ExpectedTicks = [0L, 1L, 2L];

    /// <summary>The period between ticks.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// A tick period an order of magnitude longer than <see cref="ExpiryPeriod"/>, so the inactivity timeout always
    /// fires first, yet short enough that the trampoline's wait for the tick that never arrives stays inside
    /// <see cref="LivelockTimeout"/>.
    /// </summary>
    private static readonly TimeSpan QuietTickPeriod = TimeSpan.FromSeconds(1);

    /// <summary>The inactivity window <c>Expire</c> allows before it times the sequence out.</summary>
    private static readonly TimeSpan ExpiryPeriod = TimeSpan.FromMilliseconds(50);

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

    /// <summary>Verifies <c>TakeWhile</c> stops the current-thread ticks once its predicate rejects one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerStopsWhenTakeWhileRejectsATick()
    {
        List<long> ticks = [];
        List<int> tickThreadIds = [];
        var completions = 0;
        var subscriberThreadId = 0;

        var subscriber = RunOnDedicatedThread(() =>
        {
            subscriberThreadId = Environment.CurrentManagedThreadId;
            using var subscription = Signal.Every(TickPeriod, Sequencer.CurrentThread)
                .TakeWhile(static tick => tick < RequestedTicks)
                .Subscribe(
                    tick =>
                    {
                        ticks.Add(tick);
                        tickThreadIds.Add(Environment.CurrentManagedThreadId);
                    },
                    static _ => { },
                    () => completions++);
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(ticks.SequenceEqual(ExpectedTicks)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(tickThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
    }

    /// <summary>Verifies <c>TakeUntil</c> stops the current-thread ticks once its stop source notifies.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerStopsWhenTakeUntilIsNotified()
    {
        List<long> ticks = [];
        List<int> tickThreadIds = [];
        var completions = 0;
        var subscriberThreadId = 0;
        using CancellationTokenSource stop = new();

        var subscriber = RunOnDedicatedThread(() =>
        {
            subscriberThreadId = Environment.CurrentManagedThreadId;
            using var subscription = Signal.Every(TickPeriod, Sequencer.CurrentThread)
                .TakeUntil(stop.Token)
                .Subscribe(
                    tick =>
                    {
                        ticks.Add(tick);
                        tickThreadIds.Add(Environment.CurrentManagedThreadId);
                        if (ticks.Count != RequestedTicks)
                        {
                            return;
                        }

                        stop.Cancel();
                    },
                    static _ => { },
                    () => completions++);
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(ticks.SequenceEqual(ExpectedTicks)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(tickThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
    }

    /// <summary>Verifies <c>Any</c> stops the current-thread ticks on the first one it sees.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAnySeesItsFirstTick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Any(),
            expectedResult: true);

    /// <summary>Verifies a predicated <c>Any</c> stops the current-thread ticks on the first tick that matches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAnyMatchesATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Any(static tick => tick == SoughtTick),
            expectedResult: true);

    /// <summary>Verifies <c>All</c> stops the current-thread ticks on the first tick its predicate rejects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAllRejectsATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.All(static tick => tick < SoughtTick),
            expectedResult: false);

    /// <summary>Verifies <c>Contains</c> stops the current-thread ticks on the tick it was looking for.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenContainsFindsATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Contains(SoughtTick),
            expectedResult: true);

    /// <summary>Verifies <c>IsEmpty</c> stops the current-thread ticks on the first one it sees.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenIsEmptySeesItsFirstTick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.IsEmpty(),
            expectedResult: false);

    /// <summary>
    /// Verifies <c>Expire</c> times a silent current-thread source out instead of livelocking on it. The tick period
    /// is far longer than the expiry window, so the inactivity timeout is guaranteed to fire before the first tick.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerExpiresWhenNoTickArrivesInTime()
    {
        var ticks = 0;
        Exception? failure = null;

        var subscriber = RunOnDedicatedThread(() =>
        {
            using var subscription = Signal.Every(QuietTickPeriod, Sequencer.CurrentThread)
                .Expire(ExpiryPeriod)
                .Subscribe(
                    _ => ticks++,
                    error => failure = error,
                    static () => { });
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(ticks).IsEqualTo(0);
        await Assert.That(failure).IsTypeOf<TimeoutException>();
    }

    /// <summary>
    /// Asserts a boolean bounding operator terminates the current-thread ticks at the first tick that satisfies it,
    /// and that it hands its single result back on the subscribing thread the trampoline ticks on.
    /// </summary>
    /// <param name="bound">Applies the bounding operator to the current-thread tick source.</param>
    /// <param name="expectedResult">The value the bounded sequence must emit before it completes.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertBoundedByFirstMatchingTick(
        Func<IObservable<long>, IObservable<bool>> bound,
        bool expectedResult)
    {
        List<bool> results = [];
        List<int> resultThreadIds = [];
        var completions = 0;
        var subscriberThreadId = 0;

        var subscriber = RunOnDedicatedThread(() =>
        {
            subscriberThreadId = Environment.CurrentManagedThreadId;
            using var subscription = bound(Signal.Every(TickPeriod, Sequencer.CurrentThread))
                .Subscribe(
                    result =>
                    {
                        results.Add(result);
                        resultThreadIds.Add(Environment.CurrentManagedThreadId);
                    },
                    static _ => { },
                    () => completions++);
        });

        await Assert.That(await CompletedWithinTimeout(subscriber)).IsTrue();
        await Assert.That(results).IsEquivalentTo([expectedResult], EqualityComparer<bool>.Default);
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(resultThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
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
