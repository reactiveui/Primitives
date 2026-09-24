// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Windows.Threading;
using ReactiveUI.Primitives.Reactive.Concurrency;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.Wpf.Reactive.Tests;

/// <summary>Tests dispatcher batching and cancellation with manually invoked callbacks.</summary>
public sealed class DispatcherSequencerTests
{
    /// <summary>The second value in a scheduled batch.</summary>
    private const int SecondValue = 2;

    /// <summary>Constructor validation rejects a missing dispatcher.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher()
    {
        await Assert.That(static () => new DispatcherSequencer(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new DispatcherSequencer(null!, DispatcherPriority.Normal))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Construction retains dispatcher identity and priority.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRetainsDispatcherAndPriority()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        DispatcherSequencer sequencer = new(dispatcher, DispatcherPriority.Background);
        await Assert.That(sequencer.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(sequencer.Priority).IsEqualTo(DispatcherPriority.Background);
        await Assert.That(new DispatcherSequencer(dispatcher).Priority).IsEqualTo(DispatcherPriority.Normal);
    }

    /// <summary>A thread without a dispatcher gets an error rather than a scheduler that could never run work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CurrentThrowsWhenTheThreadHasNoDispatcher() =>
        await Assert.That(static () => RunOnNewThread<DispatcherSequencer?>(static () => DispatcherSequencer.Current))
            .ThrowsExactly<InvalidOperationException>();

    /// <summary>Each thread gets its own cached scheduler bound to that thread's dispatcher.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CurrentIsCachedPerThreadAndBoundToThatThreadsDispatcher()
    {
        var first = await RunOnNewThread(CaptureCurrent);
        var second = await RunOnNewThread(CaptureCurrent);

        await Assert.That(first.Repeat).IsSameReferenceAs(first.Sequencer);
        await Assert.That(first.Sequencer.Dispatcher).IsSameReferenceAs(first.Dispatcher);
        await Assert.That(first.Sequencer.Priority).IsEqualTo(DispatcherPriority.Normal);
        await Assert.That(second.Sequencer).IsNotSameReferenceAs(first.Sequencer);
        await Assert.That(second.Sequencer.Dispatcher).IsSameReferenceAs(second.Dispatcher);
    }

    /// <summary>Before an application exists, Main uses the calling thread's dispatcher without caching it.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task MainFallsBackToCurrentBeforeAnApplicationExists()
    {
        if (System.Windows.Application.Current is not null)
        {
            return;
        }

        var captured = await RunOnNewThread(static () =>
        {
            _ = Dispatcher.CurrentDispatcher;
            return (DispatcherSequencer.Main, DispatcherSequencer.Current);
        });

        await Assert.That(captured.Main).IsSameReferenceAs(captured.Current);
        await Assert.That(static () => RunOnNewThread<DispatcherSequencer?>(static () => DispatcherSequencer.Main))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Without an application dispatcher there is nothing to bind, and nothing is cached.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BindMainReturnsNullWithoutAnApplicationDispatcher()
    {
        DispatcherSequencer? slot = null;
        await Assert.That(DispatcherSequencer.BindMain(ref slot, null)).IsNull();
        await Assert.That(slot).IsNull();
    }

    /// <summary>The first application dispatcher bound stays bound.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BindMainKeepsTheFirstBinding()
    {
        var first = await RunOnNewThread(static () => Dispatcher.CurrentDispatcher);
        var second = await RunOnNewThread(static () => Dispatcher.CurrentDispatcher);
        DispatcherSequencer? slot = null;

        var bound = DispatcherSequencer.BindMain(ref slot, first);
        var rebound = DispatcherSequencer.BindMain(ref slot, second);

        await Assert.That(bound).IsNotNull();
        await Assert.That(bound!.Dispatcher).IsSameReferenceAs(first);
        await Assert.That(slot).IsSameReferenceAs(bound);
        await Assert.That(rebound).IsSameReferenceAs(bound);
    }

    /// <summary>A posted batch preserves order and skips cancelled work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        Queue<Action> drains = new();
        DispatcherSequencer scheduler = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            null);
        List<int> values = [];
        using var first = scheduler.Schedule(() => values.Add(1));
        var cancelled = scheduler.Schedule(() => values.Add(0));
        using var second = scheduler.Schedule(TimeSpan.Zero, () => values.Add(SecondValue));
        cancelled.Dispose();
        await Assert.That(values).IsEmpty();
        await Assert.That(drains).Count().IsEqualTo(1);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Scheduling inside a callback posts another batch.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleDuringDrainWaitsForTheNextDrain()
    {
        Queue<Action> drains = new();
        DispatcherSequencer scheduler = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            null);
        List<int> values = [];
        using var first = scheduler.Schedule(() =>
        {
            values.Add(1);
            _ = scheduler.Schedule(() => values.Add(SecondValue));
        });
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Disposal cancels a delayed timer and suppresses even a late callback.</summary>
    /// <param name="cancel">Whether to cancel before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleHonorsCancellationBeforeDelivery(bool cancel)
    {
        Queue<(Action Callback, TimeSpan Delay, BooleanDisposable Cancellation)> delayed = new();
        DispatcherSequencer scheduler = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            static _ => throw new InvalidOperationException("Unexpected immediate dispatch."),
            (callback, delay) =>
            {
                BooleanDisposable cancellation = new();
                delayed.Enqueue((callback, delay, cancellation));
                return cancellation;
            });
        var calls = 0;
        var delay = TimeSpan.FromSeconds(1);
        var handle = scheduler.Schedule(delay, () => calls++);
        var pending = delayed.Dequeue();
        await Assert.That(pending.Delay).IsEqualTo(delay);
        await Assert.That(calls).IsEqualTo(0);
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

    /// <summary>Gives the calling thread a dispatcher, then reads <see cref="DispatcherSequencer.Current"/> twice.</summary>
    /// <returns>The thread's dispatcher and both reads.</returns>
    private static (Dispatcher Dispatcher, DispatcherSequencer Sequencer, DispatcherSequencer Repeat) CaptureCurrent() =>
        (Dispatcher.CurrentDispatcher, DispatcherSequencer.Current, DispatcherSequencer.Current);

    /// <summary>Runs a function on a fresh STA thread and returns its result or exception.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="func">The function to run.</param>
    /// <returns>The function's result.</returns>
    private static Task<T> RunOnNewThread<T>(Func<T> func)
    {
        TaskCompletionSource<T> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new(() =>
        {
            try
            {
                completion.SetResult(func());
            }
            catch (Exception ex)
            {
                completion.SetException(ex);
            }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        return completion.Task;
    }
}
