// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Avalonia.Threading;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.Avalonia.Tests;

/// <summary>Tests scheduler ordering and cancellation through controlled dispatch callbacks.</summary>
public sealed class AvaloniaSchedulerTests
{
    /// <summary>The second value in an ordered batch.</summary>
    private const int SecondValue = 2;

    /// <summary>Public constructors reject a missing dispatcher.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher()
    {
        await Assert.That(static () => new AvaloniaScheduler(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new AvaloniaScheduler(null!, DispatcherPriority.Normal))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Constructors and the singleton retain the selected dispatcher and priority.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorsAndSingletonPreserveDispatcherAndPriority()
    {
        var dispatcher = Dispatcher.UIThread;
        AvaloniaScheduler defaults = new(dispatcher);
        AvaloniaScheduler selected = new(dispatcher, DispatcherPriority.Normal);
        await Assert.That(defaults.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(defaults.Priority).IsEqualTo(DispatcherPriority.Background);
        await Assert.That(selected.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(selected.Priority).IsEqualTo(DispatcherPriority.Normal);
        await Assert.That(AvaloniaScheduler.Instance.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(AvaloniaScheduler.Instance.Priority).IsEqualTo(DispatcherPriority.Background);
    }

    /// <summary>The scheduler exposes UTC time and the shared monotonic timestamp scale.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClockProperties_UseSharedSequencerClock()
    {
        AvaloniaScheduler scheduler = new(Dispatcher.UIThread);
        var before = System.Diagnostics.Stopwatch.GetTimestamp();
        var timestamp = scheduler.Timestamp;
        var after = System.Diagnostics.Stopwatch.GetTimestamp();

        await Assert.That(scheduler.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(timestamp).IsGreaterThanOrEqualTo(before);
        await Assert.That(timestamp).IsLessThanOrEqualTo(after);
    }

    /// <summary>Immediate and already-due items share an ordered batch that skips cancelled work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        List<int> values = [];
        RecordingWorkItem cancelled = new(() => values.Add(0));
        scheduler.Schedule(new RecordingWorkItem(() => values.Add(1)));
        scheduler.Schedule(cancelled);
        scheduler.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)), long.MinValue);
        cancelled.Dispose();
        await Assert.That(values).IsEmpty();
        await Assert.That(dispatcher.Drains).Count().IsEqualTo(1);
        await Assert.That(dispatcher.Delays).IsEmpty();
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Work scheduled during a callback waits for a later posted batch.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ReentrantScheduleRunsInTheNextBatch()
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        List<int> values = [];
        scheduler.Schedule(new RecordingWorkItem(() =>
        {
            values.Add(1);
            scheduler.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)));
        }));
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        await Assert.That(dispatcher.Drains).Count().IsEqualTo(1);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>A rejected post retains work for a later accepted post.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task RejectedPostRetainsQueuedWork()
    {
        ManualDispatcher dispatcher = new() { AcceptsPosts = false };
        var scheduler = dispatcher.Create();
        List<int> values = [];
        scheduler.Schedule(new RecordingWorkItem(() => values.Add(1)));
        await Assert.That(dispatcher.Drains).IsEmpty();
        dispatcher.AcceptsPosts = true;
        scheduler.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)));
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Delayed work retains its due timestamp and observes cancellation before delivery.</summary>
    /// <param name="cancel">Whether cancellation happens before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleWaitsForItsCallback(bool cancel)
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        var calls = 0;
        RecordingWorkItem item = new(() => calls++);
        scheduler.Schedule(item, long.MaxValue);
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(dispatcher.Drains).IsEmpty();
        var pending = dispatcher.Delays.Dequeue();
        await Assert.That(pending.Item).IsSameReferenceAs(item);
        await Assert.That(pending.Due).IsEqualTo(long.MaxValue);
        if (cancel)
        {
            item.Dispose();
        }

        DispatchSequencerState.RunIfActive(pending.Item);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
    }

    /// <summary>Both scheduling overloads reject missing work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullWorkItems()
    {
        var scheduler = new ManualDispatcher().Create();
        await Assert.That(() => scheduler.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.Schedule(null!, long.MaxValue)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Retains callbacks until the test delivers them.</summary>
    private sealed class ManualDispatcher
    {
        /// <summary>Gets the posted batches.</summary>
        public Queue<Action> Drains { get; } = new();

        /// <summary>Gets work awaiting its due timestamp.</summary>
        public Queue<(IWorkItem Item, long Due)> Delays { get; } = new();

        /// <summary>Gets or sets whether a post is accepted.</summary>
        public bool AcceptsPosts { get; set; } = true;

        /// <summary>Creates a scheduler controlled by these callbacks.</summary>
        /// <returns>The scheduler.</returns>
        public AvaloniaScheduler Create() =>
            new(Dispatcher.UIThread, DispatcherPriority.Normal, Post, (item, due) => Delays.Enqueue((item, due)));

        /// <summary>Retains an accepted callback without running it.</summary>
        /// <param name="drain">The callback to retain.</param>
        /// <returns>Whether the callback was accepted.</returns>
        private bool Post(Action drain)
        {
            if (!AcceptsPosts)
            {
                return false;
            }

            Drains.Enqueue(drain);
            return true;
        }
    }

    /// <summary>Records execution and supports cancellation.</summary>
    /// <param name="action">The callback to run.</param>
    private sealed class RecordingWorkItem(Action action) : IWorkItem, IsDisposed
    {
        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => action();
    }
}
