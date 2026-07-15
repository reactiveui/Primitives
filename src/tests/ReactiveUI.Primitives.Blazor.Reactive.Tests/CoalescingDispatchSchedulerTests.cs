// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Reactive.Tests;

/// <summary>
/// Tests for <see cref="CoalescingDispatchScheduler"/>, driven through a test subclass that controls what its
/// dispatcher <c>Post</c> does: whether it accepts the drain, defers it, rejects it, or throws. This exercises
/// the immediate and delayed scheduling paths and the coalescing/drain bookkeeping without a real UI dispatcher.
/// </summary>
public sealed class CoalescingDispatchSchedulerTests
{
    /// <summary>The scheduled state value the tests pass through the scheduler.</summary>
    private const int State = 0;

    /// <summary>The number of posts expected once a second drain has been requested.</summary>
    private const int TwoPosts = 2;

    /// <summary>Guard timeout for the timer-driven delayed dispatch.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

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

    /// <summary>A positive due time defers the work through the default dispatcher timer path.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PositiveDueTimeRunsThroughTheDispatcherTimerPath()
    {
        TestDispatchScheduler scheduler = new() { RunDrainInline = true };
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using var handle = scheduler.Schedule(State, ShortDelay, (_, _) =>
        {
            _ = ran.TrySetResult();
            return Disposable.Empty;
        });

        await ran.Task.WaitAsync(GuardTimeout);
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
        scheduler.RequestDrainForTest();

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

        scheduler.RequestDrainForTest();

        await Assert.That(scheduler.PostCount).IsEqualTo(0);
    }

    /// <summary>A second schedule coalesces onto the drain already posted for the first.</summary>
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

    /// <summary>A <see cref="CoalescingDispatchScheduler"/> whose dispatcher post the test drives explicitly.</summary>
    private sealed class TestDispatchScheduler : CoalescingDispatchScheduler
    {
        /// <summary>Drains handed to <see cref="Post"/> that have not yet been run.</summary>
        private readonly Queue<Action> _postedDrains = new();

        /// <summary>Gets the number of times the dispatcher was asked to post a drain.</summary>
        public int PostCount { get; private set; }

        /// <summary>Gets or sets a value indicating whether the dispatcher accepts the drain.</summary>
        public bool PostResult { get; set; } = true;

        /// <summary>Gets or sets a value indicating whether the dispatcher throws instead of posting.</summary>
        public bool ThrowOnPost { get; set; }

        /// <summary>Gets or sets a value indicating whether the drain runs inline on the posting thread.</summary>
        public bool RunDrainInline { get; set; }

        /// <summary>Exposes the protected re-drain request for the test.</summary>
        public void RequestDrainForTest() => RequestDrain();

        /// <summary>Runs every drain the dispatcher has accepted so far.</summary>
        public void RunPostedDrains()
        {
            while (_postedDrains.Count > 0)
            {
                _postedDrains.Dequeue()();
            }
        }

        /// <inheritdoc/>
        protected override bool Post(Action drain)
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
    }
}
