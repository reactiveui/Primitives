// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Reactive.Concurrency;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.WinUI.Reactive.Tests;

/// <summary>Tests dispatcher queue batching, rejection, and cancellation through controlled callbacks.</summary>
public sealed class DispatcherQueueSequencerTests
{
    /// <summary>The second value in a scheduled batch.</summary>
    private const int SecondValue = 2;

    /// <summary>Expected values after both queued items run.</summary>
    private static readonly int[] BatchValues = [1, SecondValue];

    /// <summary>Expected values before the reentrant batch runs.</summary>
    private static readonly int[] FirstBatchValues = [1];

    /// <summary>Public constructors reject a missing dispatcher queue.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcherQueue()
    {
        await Assert.That(static () => new DispatcherQueueSequencer(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new DispatcherQueueSequencer(null!, DispatcherQueuePriority.High))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Public constructors retain a live queue and the selected priority.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_RetainDispatcherQueueAndPriority()
    {
        var controller = DispatcherQueueController.CreateOnDedicatedThread();
        try
        {
            var queue = controller.DispatcherQueue;
            DispatcherQueueSequencer defaults = new(queue);
            DispatcherQueueSequencer selected = new(queue, DispatcherQueuePriority.High);

            await Assert.That(defaults.DispatcherQueue).IsSameReferenceAs(queue);
            await Assert.That(defaults.Priority).IsEqualTo(DispatcherQueuePriority.Normal);
            await Assert.That(selected.DispatcherQueue).IsSameReferenceAs(queue);
            await Assert.That(selected.Priority).IsEqualTo(DispatcherQueuePriority.High);
        }
        finally
        {
            await controller.ShutdownQueueAsync();
        }
    }

    /// <summary>Queued actions preserve order and cancellation until the drain runs.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        List<int> values = [];
        using var first = scheduler.Schedule(() => values.Add(1));
        var cancelled = scheduler.Schedule(() => values.Add(0));
        using var second = scheduler.Schedule(TimeSpan.Zero, () => values.Add(SecondValue));
        cancelled.Dispose();
        await Assert.That(values).IsEmpty();
        await Assert.That(dispatcher.Drains).Count().IsEqualTo(1);
        await Assert.That(dispatcher.LastPriority).IsEqualTo(DispatcherQueuePriority.High);
        await Assert.That(scheduler.Priority).IsEqualTo(DispatcherQueuePriority.High);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>A rejected post retains queued actions and reuses its cached callback on retry.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task RejectedPostCanRetryWithoutLosingQueuedWork()
    {
        ManualDispatcher dispatcher = new() { AcceptsPosts = false };
        var scheduler = dispatcher.Create();
        List<int> values = [];
        await Assert.That(() => scheduler.Schedule(() => values.Add(1)))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(values).IsEmpty();
        var rejectedHandler = dispatcher.LastHandler;
        dispatcher.AcceptsPosts = true;
        using var next = scheduler.Schedule(() => values.Add(SecondValue));
        await Assert.That(dispatcher.LastHandler).IsSameReferenceAs(rejectedHandler);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Actions queued from inside a callback wait for a later drain.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ReentrantScheduleRunsInTheNextBatch()
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        List<int> values = [];
        using var first = scheduler.Schedule(() =>
        {
            values.Add(1);
            _ = scheduler.Schedule(() => values.Add(SecondValue));
        });
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(FirstBatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Timer cancellation suppresses a callback even when it is delivered late.</summary>
    /// <param name="cancel">Whether cancellation precedes delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleHonorsCancellationBeforeDelivery(bool cancel)
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        var calls = 0;
        var delay = TimeSpan.FromSeconds(1);
        var handle = scheduler.Schedule(delay, () => calls++);
        var pending = dispatcher.Delays.Dequeue();
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(dispatcher.Drains).IsEmpty();
        await Assert.That(pending.Delay).IsEqualTo(delay);
        if (cancel)
        {
            handle.Dispose();
        }

        pending.Callback();
        await Assert.That(pending.Cancellation.IsDisposed).IsEqualTo(cancel);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
        if (cancel)
        {
            return;
        }

        handle.Dispose();
    }

    /// <summary>A thread without a dispatcher queue gets an error rather than a sequencer that could never run work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CurrentThrowsWhenTheThreadHasNoDispatcherQueue() =>
        await Assert.That(static () => RunOnNewThread<DispatcherQueueSequencer?>(static () => DispatcherQueueSequencer.Current))
            .ThrowsExactly<InvalidOperationException>();

    /// <summary>Each dispatcher queue thread gets its own cached sequencer bound to that thread's queue.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CurrentIsCachedPerThreadAndBoundToThatThreadsQueue()
    {
        var firstController = DispatcherQueueController.CreateOnDedicatedThread();
        var secondController = DispatcherQueueController.CreateOnDedicatedThread();
        try
        {
            var first = await RunOnQueue(firstController.DispatcherQueue, CaptureCurrent);
            var second = await RunOnQueue(secondController.DispatcherQueue, CaptureCurrent);

            await Assert.That(first.Repeat).IsSameReferenceAs(first.Sequencer);
            await Assert.That(first.Sequencer.DispatcherQueue).IsSameReferenceAs(firstController.DispatcherQueue);
            await Assert.That(first.Sequencer.Priority).IsEqualTo(DispatcherQueuePriority.Normal);
            await Assert.That(second.Sequencer).IsNotSameReferenceAs(first.Sequencer);
            await Assert.That(second.Sequencer.DispatcherQueue).IsSameReferenceAs(secondController.DispatcherQueue);
        }
        finally
        {
            await firstController.ShutdownQueueAsync();
            await secondController.ShutdownQueueAsync();
        }
    }

    /// <summary>Once a UI thread binds Main, a thread without a dispatcher queue gets the same sequencer.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task MainIsSharedWithEveryThreadOnceAUiThreadBindsIt()
    {
        var controller = DispatcherQueueController.CreateOnDedicatedThread();
        try
        {
            var onQueue = await RunOnQueue(controller.DispatcherQueue, static () => DispatcherQueueSequencer.Main);
            var offQueue = await RunOnNewThread(static () => DispatcherQueueSequencer.Main);

            await Assert.That(offQueue).IsSameReferenceAs(onQueue);
        }
        finally
        {
            await controller.ShutdownQueueAsync();
        }
    }

    /// <summary>Without a dispatcher queue there is nothing to bind, and nothing is cached.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BindMainReturnsNullWithoutADispatcherQueue()
    {
        DispatcherQueueSequencer? slot = null;
        await Assert.That(DispatcherQueueSequencer.BindMain(ref slot, null)).IsNull();
        await Assert.That(slot).IsNull();
    }

    /// <summary>The first dispatcher queue bound stays bound.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BindMainKeepsTheFirstBinding()
    {
        var firstController = DispatcherQueueController.CreateOnDedicatedThread();
        var secondController = DispatcherQueueController.CreateOnDedicatedThread();
        try
        {
            DispatcherQueueSequencer? slot = null;

            var bound = DispatcherQueueSequencer.BindMain(ref slot, firstController.DispatcherQueue);
            var rebound = DispatcherQueueSequencer.BindMain(ref slot, secondController.DispatcherQueue);

            await Assert.That(bound).IsNotNull();
            await Assert.That(bound!.DispatcherQueue).IsSameReferenceAs(firstController.DispatcherQueue);
            await Assert.That(slot).IsSameReferenceAs(bound);
            await Assert.That(rebound).IsSameReferenceAs(bound);
        }
        finally
        {
            await firstController.ShutdownQueueAsync();
            await secondController.ShutdownQueueAsync();
        }
    }

    /// <summary>Reads <see cref="DispatcherQueueSequencer.Current"/> twice on the calling thread.</summary>
    /// <returns>Both reads.</returns>
    private static (DispatcherQueueSequencer Sequencer, DispatcherQueueSequencer Repeat) CaptureCurrent() =>
        (DispatcherQueueSequencer.Current, DispatcherQueueSequencer.Current);

    /// <summary>Runs a function on a dispatcher queue's thread and returns its result or exception.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="queue">The dispatcher queue whose thread runs the function.</param>
    /// <param name="func">The function to run.</param>
    /// <returns>The function's result.</returns>
    private static Task<T> RunOnQueue<T>(DispatcherQueue queue, Func<T> func)
    {
        TaskCompletionSource<T> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!queue.TryEnqueue(() => Complete(completion, func)))
        {
            completion.SetException(new InvalidOperationException("The dispatcher queue rejected the work."));
        }

        return completion.Task;
    }

    /// <summary>Runs a function on a fresh thread that has no dispatcher queue and returns its result or exception.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="func">The function to run.</param>
    /// <returns>The function's result.</returns>
    private static Task<T> RunOnNewThread<T>(Func<T> func)
    {
        TaskCompletionSource<T> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new(() => Complete(completion, func));
        thread.Start();
        return completion.Task;
    }

    /// <summary>Completes a task with a function's result or exception.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="completion">The task to complete.</param>
    /// <param name="func">The function to run.</param>
    private static void Complete<T>(TaskCompletionSource<T> completion, Func<T> func)
    {
        try
        {
            completion.SetResult(func());
        }
        catch (Exception ex)
        {
            completion.SetException(ex);
        }
    }

    /// <summary>Retains callbacks until they are explicitly delivered.</summary>
    private sealed class ManualDispatcher
    {
        /// <summary>Gets queued drains.</summary>
        public Queue<DispatcherQueueHandler> Drains { get; } = new();

        /// <summary>Gets delayed callbacks and their cancellation handles.</summary>
        public Queue<(Action Callback, TimeSpan Delay, BooleanDisposable Cancellation)> Delays { get; } = new();

        /// <summary>Gets or sets whether enqueue attempts succeed.</summary>
        public bool AcceptsPosts { get; set; } = true;

        /// <summary>Gets the most recently posted handler.</summary>
        public DispatcherQueueHandler? LastHandler { get; private set; }

        /// <summary>Gets the priority of the most recent post.</summary>
        public DispatcherQueuePriority LastPriority { get; private set; }

        /// <summary>Creates a scheduler using the retained callbacks.</summary>
        /// <returns>The scheduler.</returns>
        public DispatcherQueueSequencer Create() =>
            new(DispatcherQueuePriority.High, TryEnqueue, (callback, delay) =>
            {
                BooleanDisposable cancellation = new();
                Delays.Enqueue((callback, delay, cancellation));
                return cancellation;
            });

        /// <summary>Accepts or rejects a drain without executing it.</summary>
        /// <param name="priority">The requested priority.</param>
        /// <param name="handler">The drain callback.</param>
        /// <returns>Whether the callback was accepted.</returns>
        private bool TryEnqueue(DispatcherQueuePriority priority, DispatcherQueueHandler handler)
        {
            LastPriority = priority;
            LastHandler = handler;
            if (!AcceptsPosts)
            {
                return false;
            }

            Drains.Enqueue(handler);
            return true;
        }
    }
}
