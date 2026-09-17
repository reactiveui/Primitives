// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using Avalonia.Threading;
using ReactiveUI.Primitives.Reactive.Concurrency;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.Avalonia.Reactive.Tests;

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

    /// <summary>Immediate and nonpositive delays share an ordered batch that skips cancelled work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        List<int> values = [];
        using var first = scheduler.Schedule(() => values.Add(1));
        var cancelled = scheduler.Schedule(TimeSpan.Zero, () => values.Add(0));
        using var last = scheduler.Schedule(TimeSpan.FromTicks(-1), () => values.Add(SecondValue));
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
        using var first = scheduler.Schedule(() =>
        {
            values.Add(1);
            _ = scheduler.Schedule(() => values.Add(SecondValue));
        });
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        await Assert.That(dispatcher.Drains).Count().IsEqualTo(1);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Delayed work observes its due time and cancellation before or after delivery.</summary>
    /// <param name="cancelBeforeDelivery">Whether cancellation happens before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleWaitsForItsCallback(bool cancelBeforeDelivery)
    {
        ManualDispatcher dispatcher = new();
        var scheduler = dispatcher.Create();
        BooleanDisposable resource = new();
        var calls = 0;
        var delay = TimeSpan.FromTicks(1);
        var subscription = scheduler.Schedule(resource, delay, (_, state) =>
        {
            calls++;
            return state;
        });
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(dispatcher.Drains).IsEmpty();
        var pending = dispatcher.Delays.Dequeue();
        await Assert.That(pending.Due).IsEqualTo(delay);
        if (cancelBeforeDelivery)
        {
            subscription.Dispose();
        }

        pending.Work();
        await Assert.That(calls).IsEqualTo(cancelBeforeDelivery ? 0 : 1);
        subscription.Dispose();
        await Assert.That(pending.Cancellation.IsDisposed).IsTrue();
        await Assert.That(resource.IsDisposed).IsEqualTo(!cancelBeforeDelivery);
    }

    /// <summary>Both scheduling overloads reject missing actions.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullActions()
    {
        var scheduler = new ManualDispatcher().Create();
        await Assert.That(() => scheduler.Schedule(0, (Func<IScheduler, int, IDisposable>)null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => scheduler.Schedule(0, TimeSpan.Zero, (Func<IScheduler, int, IDisposable>)null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Retains callbacks until the test delivers them.</summary>
    private sealed class ManualDispatcher
    {
        /// <summary>Gets the posted batches.</summary>
        public Queue<Action> Drains { get; } = new();

        /// <summary>Gets delayed callbacks and their cancellation handles.</summary>
        public Queue<(Action Work, TimeSpan Due, BooleanDisposable Cancellation)> Delays { get; } = new();

        /// <summary>Creates a scheduler controlled by these callbacks.</summary>
        /// <returns>The scheduler.</returns>
        public AvaloniaScheduler Create() =>
            new(Dispatcher.UIThread, DispatcherPriority.Normal, Drains.Enqueue, ScheduleDelayed);

        /// <summary>Retains delayed work without running it.</summary>
        /// <param name="work">The callback to retain.</param>
        /// <param name="dueTime">The requested delay.</param>
        /// <returns>The cancellation handle.</returns>
        private BooleanDisposable ScheduleDelayed(Action work, TimeSpan dueTime)
        {
            BooleanDisposable cancellation = new();
            Delays.Enqueue((work, dueTime, cancellation));
            return cancellation;
        }
    }
}
