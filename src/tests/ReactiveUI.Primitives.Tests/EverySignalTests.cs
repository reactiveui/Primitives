// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests recurring ticks and bounded subscriptions on the current-thread sequencer.</summary>
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

    /// <summary>The inactivity window <c>Expire</c> allows before it times the sequence out.</summary>
    private static readonly TimeSpan ExpiryPeriod = TimeSpan.FromMilliseconds(50);

    /// <summary>A tick that fires inline during scheduling keeps the successor tick it arms.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryRetainsTheTickArmedByAnInlineFirstTick()
    {
        FirstInlineSequencer sequencer = new(TimeSpan.Zero);
        List<long> ticks = [];
        using var subscription = Signal.Every(TickPeriod, sequencer).Subscribe(ticks.Add);

        await Assert.That(ticks.SequenceEqual([0L])).IsTrue();

        sequencer.RunPending();

        await Assert.That(ticks.SequenceEqual([0L, 1L])).IsTrue();
    }

    /// <summary>Verifies a bounded <c>Every</c> on the current-thread sequencer terminates instead of livelocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerStopsWhenTakeReachesItsCount()
    {
        List<long> ticks = [];
        var completions = 0;
        using var subscription = Signal.Every(TimeSpan.Zero, Sequencer.CurrentThread)
            .Take(RequestedTicks)
            .Subscribe(ticks.Add, static _ => { }, () => completions++);
        await Assert.That(ticks.SequenceEqual(ExpectedTicks)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies the current-thread ticks are delivered on the subscribing thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryOnTheCurrentThreadSequencerTicksOnTheSubscribingThread()
    {
        List<int> tickThreadIds = [];
        var subscriberThreadId = Environment.CurrentManagedThreadId;
        using var subscription = Signal.Every(TimeSpan.Zero, Sequencer.CurrentThread)
            .Take(RequestedTicks)
            .Subscribe(_ => tickThreadIds.Add(Environment.CurrentManagedThreadId));
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
        var subscriberThreadId = Environment.CurrentManagedThreadId;
        using var subscription = Signal.Every(TimeSpan.Zero, Sequencer.CurrentThread)
            .TakeWhile(static tick => tick < RequestedTicks)
            .Subscribe(
                tick =>
                {
                    ticks.Add(tick);
                    tickThreadIds.Add(Environment.CurrentManagedThreadId);
                },
                static _ => { },
                () => completions++);
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
        subscriberThreadId = Environment.CurrentManagedThreadId;
        using var subscription = Signal.Every(TimeSpan.Zero, Sequencer.CurrentThread)
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
        await Assert.That(ticks.SequenceEqual(ExpectedTicks)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(tickThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
    }

    /// <summary>Verifies <c>Any</c> stops the current-thread ticks on the first one it sees.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAnySeesItsFirstTick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Any(),
            expectedResult: true);

    /// <summary>Verifies a predicated <c>Any</c> stops the current-thread ticks on the first tick that matches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAnyMatchesATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Any(static tick => tick == SoughtTick),
            expectedResult: true);

    /// <summary>Verifies <c>All</c> stops the current-thread ticks on the first tick its predicate rejects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenAllRejectsATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.All(static tick => tick < SoughtTick),
            expectedResult: false);

    /// <summary>Verifies <c>Contains</c> stops the current-thread ticks on the tick it was looking for.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenContainsFindsATick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.Contains(SoughtTick),
            expectedResult: true);

    /// <summary>Verifies <c>IsEmpty</c> stops the current-thread ticks on the first one it sees.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public Task EveryOnTheCurrentThreadSequencerStopsWhenIsEmptySeesItsFirstTick() =>
        AssertBoundedByFirstMatchingTick(
            static source => source.IsEmpty(),
            expectedResult: false);

    /// <summary>Advancing to the expiry boundary terminates the source before its first scheduled tick.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryExpiresBeforeTheFirstVirtualTick()
{
        VirtualClock clock = new();
        var ticks = 0;
        Exception? failure = null;
        using var subscription = Signal.Every(ExpiryPeriod + ExpiryPeriod, clock)
            .Expire(ExpiryPeriod, clock)
            .Subscribe(_ => ticks++, error => failure = error, static () => { });
        clock.AdvanceBy(ExpiryPeriod);
        await Assert.That(ticks).IsEqualTo(0);
        await Assert.That(failure).IsTypeOf<TimeoutException>();
    }

    /// <summary>A tick that runs after the subscription was disposed emits nothing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryTickRunningAfterDisposalEmitsNothing()
    {
        DisposingSequencer sequencer = new();
        List<long> ticks = [];
        using var subscription = Signal.Every(TickPeriod, sequencer).Subscribe(ticks.Add);
        sequencer.BeforeInlineRun = subscription.Dispose;

        sequencer.RunPending();

        await Assert.That(ticks.SequenceEqual([0L])).IsTrue();
    }

    /// <summary>Checks the bounding result and completion on the subscribing thread.</summary>
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
        var subscriberThreadId = Environment.CurrentManagedThreadId;
        using var subscription = bound(Signal.Every(TimeSpan.Zero, Sequencer.CurrentThread))
            .Subscribe(
                result =>
                {
                    results.Add(result);
                    resultThreadIds.Add(Environment.CurrentManagedThreadId);
                },
                static _ => { },
                () => completions++);
        await Assert.That(results).IsEquivalentTo([expectedResult], EqualityComparer<bool>.Default);
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(resultThreadIds.TrueForAll(id => id == subscriberThreadId)).IsTrue();
    }

    /// <summary>Queues work, or runs it inline after a callback the test installs for the next schedule.</summary>
    private sealed class DisposingSequencer : ISequencer
    {
        /// <summary>Work items queued and not yet run.</summary>
        private readonly Queue<IWorkItem> _pending = new();

        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <summary>Gets or sets the callback run before the next scheduled item, which then runs inline.</summary>
        internal Action? BeforeInlineRun { get; set; }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            var beforeInlineRun = BeforeInlineRun;
            if (beforeInlineRun is null)
            {
                _pending.Enqueue(item);
                return;
            }

            BeforeInlineRun = null;
            beforeInlineRun();
            item.Execute();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Runs the queued work items.</summary>
        internal void RunPending()
        {
            while (_pending.Count > 0)
            {
                _pending.Dequeue().Execute();
            }
        }
    }
}
