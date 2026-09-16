// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Reactive.Tests;

/// <summary>Tests dispatch acceptance, rejection, reentrancy, and delayed delivery using explicit drain callbacks.</summary>
public sealed class CoalescingDispatchStateTests
{
    /// <summary>The scheduled state value the tests pass through the scheduler.</summary>
    private const int State = 0;

    /// <summary>The number of posts expected once a second drain has been requested.</summary>
    private const int TwoPosts = 2;

    /// <summary>A short but non-zero due time that forces the delayed dispatch path.</summary>
    private static readonly TimeSpan ShortDelay = TimeSpan.FromMilliseconds(20);

    /// <summary>Immediate work is queued and executed when the posted drain runs.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateWorkRunsWhenThePostedDrainIsInvoked()
    {
        TestDispatchScheduler scheduler = new();
        var ran = false;

        _ = scheduler.Schedule(State, (_, _) =>
        {
            ran = true;
            return Disposable.Empty;
        });

        await Assert.That(ran).IsFalse();
        await Assert.That(scheduler.PostCount).IsEqualTo(1);

        scheduler.RunPostedDrains();

        await Assert.That(ran).IsTrue();
    }

    /// <summary>A zero due time is normalized to immediate scheduling and enqueued for the next drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ZeroDueTimeSchedulesImmediately()
    {
        TestDispatchScheduler scheduler = new();
        var ran = false;

        _ = scheduler.Schedule(State, TimeSpan.Zero, (_, _) =>
        {
            ran = true;
            return Disposable.Empty;
        });

        scheduler.RunPostedDrains();

        await Assert.That(ran).IsTrue();
    }

    /// <summary>A positive due time defers work until the delay scheduler advances.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PositiveDueTimeRunsAfterTheDelaySchedulerAdvances()
    {
        HistoricalScheduler clock = new();
        TestDispatchScheduler scheduler = new(clock) { RunDrainInline = true };
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using var handle = scheduler.Schedule(State, ShortDelay, (_, _) =>
        {
            _ = ran.TrySetResult();
            return Disposable.Empty;
        });

        await Assert.That(ran.Task.IsCompleted).IsFalse();
        clock.AdvanceBy(ShortDelay);
        await Assert.That(ran.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A null action is rejected on both scheduling paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NullActionIsRejected()
    {
        TestDispatchScheduler scheduler = new();

        await Assert.That(() => scheduler.Schedule(State, (Func<IScheduler, int, IDisposable>)null!)).Throws<ArgumentNullException>();
        await Assert.That(() => scheduler.Schedule(State, ShortDelay, (Func<IScheduler, int, IDisposable>)null!)).Throws<ArgumentNullException>();
    }

    /// <summary>A dispatcher that refuses the drain resets the coalescing gate so a later drain can be posted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ADispatcherThatRefusesTheDrainResetsTheGate()
    {
        TestDispatchScheduler scheduler = new() { PostResult = false };

        _ = scheduler.Schedule(State, static (_, _) => Disposable.Empty);

        await Assert.That(scheduler.PostCount).IsEqualTo(1);

        scheduler.PostResult = true;
        scheduler.RequestDrain();

        await Assert.That(scheduler.PostCount).IsEqualTo(TwoPosts);
    }

    /// <summary>A dispatcher post that throws resets the gate and surfaces the failure to the caller.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ADispatcherPostThatThrowsResetsTheGateAndSurfaces()
    {
        TestDispatchScheduler scheduler = new() { ThrowOnPost = true };

        await Assert.That(() => scheduler.Schedule(State, static (_, _) => Disposable.Empty))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Requesting a drain with nothing queued posts nothing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RequestingADrainWithNothingQueuedPostsNothing()
    {
        TestDispatchScheduler scheduler = new();

        scheduler.RequestDrain();

        await Assert.That(scheduler.PostCount).IsEqualTo(0);
    }

    /// <summary>A second schedule coalesces onto the drain posted for the first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ASecondScheduleCoalescesOntoThePendingDrain()
    {
        TestDispatchScheduler scheduler = new();

        _ = scheduler.Schedule(State, static (_, _) => Disposable.Empty);
        _ = scheduler.Schedule(State, static (_, _) => Disposable.Empty);

        await Assert.That(scheduler.PostCount).IsEqualTo(1);

        scheduler.RunPostedDrains();
    }

    /// <summary>Work queued while a drain is running re-posts a drain to run it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WorkQueuedDuringADrainRepostsAnotherDrain()
    {
        TestDispatchScheduler scheduler = new();
        var secondRan = false;

        _ = scheduler.Schedule(State, (_, _) =>
        {
            _ = scheduler.Schedule(State, (_, _) =>
            {
                secondRan = true;
                return Disposable.Empty;
            });
            return Disposable.Empty;
        });

        scheduler.RunPostedDrains();
        scheduler.RunPostedDrains();

        await Assert.That(secondRan).IsTrue();
        await Assert.That(scheduler.PostCount).IsEqualTo(TwoPosts);
    }

    /// <summary>Verifies a nested drain may empty the queue before the outer batch finishes.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task NestedDrainClaimsRemainingItemsExactlyOnce()
    {
        const int First = 1;
        const int Second = 2;
        const int Third = 3;
        TestDispatchScheduler scheduler = new();
        List<int> values = [];
        _ = scheduler.Schedule(First, (owner, value) =>
        {
            values.Add(value);
            _ = owner.Schedule(Third, (_, next) =>
            {
                values.Add(next);
                return Disposable.Empty;
            });
            return Disposable.Empty;
        });
        _ = scheduler.Schedule(Second, (_, value) =>
        {
            values.Add(value);
            return Disposable.Empty;
        });
        scheduler.RunDrainInline = true;
        scheduler.RunPostedDrains();

        await Assert.That(values).IsEquivalentTo([First, Second, Third], EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>A scheduler embedding <see cref="CoalescingDispatchState"/> whose dispatcher post the test drives explicitly.</summary>
    private sealed class TestDispatchScheduler : LocalScheduler
    {
        /// <summary>Drains handed to the post that have not yet been run.</summary>
        private readonly Queue<Action> _postedDrains = new();

        /// <summary>Queues work and coalesces drains.</summary>
        private CoalescingDispatchState _dispatch;

        /// <summary>Initializes a new instance of the <see cref="TestDispatchScheduler"/> class.</summary>
        /// <param name="clock">The delay scheduler.</param>
        public TestDispatchScheduler(IScheduler? clock = null) =>
            _dispatch = new(RunDrain, clock ?? new HistoricalScheduler());

        /// <summary>Gets the number of times the dispatcher was asked to post a drain.</summary>
        public int PostCount { get; private set; }

        /// <summary>Gets or sets a value indicating whether the dispatcher accepts the drain.</summary>
        public bool PostResult { get; set; } = true;

        /// <summary>Gets or sets a value indicating whether the dispatcher throws instead of posting.</summary>
        public bool ThrowOnPost { get; set; }

        /// <summary>Gets or sets a value indicating whether the drain runs inline on the posting thread.</summary>
        public bool RunDrainInline { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
            _dispatch.Schedule(new DispatchHost(this), this, state, action);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
            _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

        /// <summary>Requests a re-drain, as a platform adapter does when its dispatcher becomes ready.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RequestDrain() => _dispatch.RequestDrain(new DispatchHost(this));

        /// <summary>Runs every drain the dispatcher has accepted so far.</summary>
        public void RunPostedDrains()
        {
            while (_postedDrains.Count > 0)
            {
                _postedDrains.Dequeue()();
            }
        }

        /// <summary>Records the post and applies the configured dispatcher behaviour.</summary>
        /// <param name="drain">The drain callback.</param>
        /// <returns>Whether the dispatcher accepted the drain.</returns>
        /// <exception cref="InvalidOperationException">The test configured the dispatcher to throw.</exception>
        private bool Post(Action drain)
        {
            PostCount++;
            if (ThrowOnPost)
            {
                throw new InvalidOperationException("dispatcher rejected the drain");
            }

            if (RunDrainInline)
            {
                drain();
                return true;
            }

            if (PostResult)
            {
                _postedDrains.Enqueue(drain);
            }

            return PostResult;
        }

        /// <summary>Runs one batch.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

        /// <summary>Reaches the test scheduler for its dispatch state.</summary>
        /// <param name="Owner">The scheduler.</param>
        private readonly record struct DispatchHost(TestDispatchScheduler Owner) : IDispatchHost
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Post(Action drain) => Owner.Post(drain);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
                Owner._dispatch.ScheduleThroughDelayScheduler(Owner, work, dueTime);
        }
    }
}
