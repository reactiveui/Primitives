// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Windows.Threading;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.Wpf.Tests;

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

    /// <summary>Construction retains dispatcher identity, priority, and UTC clock semantics.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ConstructorRetainsDispatcherAndPriority()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        DispatcherSequencer sequencer = new(dispatcher, DispatcherPriority.Background);
        await Assert.That(sequencer.Dispatcher).IsSameReferenceAs(dispatcher);
        await Assert.That(sequencer.Priority).IsEqualTo(DispatcherPriority.Background);
        await Assert.That(new DispatcherSequencer(dispatcher).Priority).IsEqualTo(DispatcherPriority.Normal);
        await Assert.That(sequencer.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(sequencer.DebuggerDisplay).IsEqualTo(typeof(DispatcherSequencer).FullName);
    }

    /// <summary>A thread without a dispatcher gets an error rather than a sequencer that could never run work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CurrentThrowsWhenTheThreadHasNoDispatcher() =>
        await Assert.That(static () => RunOnNewThread<DispatcherSequencer?>(static () => DispatcherSequencer.Current))
            .ThrowsExactly<InvalidOperationException>();

    /// <summary>Each thread gets its own cached sequencer bound to that thread's dispatcher.</summary>
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

    /// <summary>Without an application dispatcher there is nothing to bind.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BindMainReturnsNullWithoutAnApplicationDispatcher() =>
        await Assert.That(DispatcherSequencer.BindMain(null)).IsNull();

    /// <summary>The dispatcher sequencer shares the monotonic timestamp scale used by scheduled work.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Timestamp_UsesSharedSequencerClock()
    {
        DispatcherSequencer sequencer = new(Dispatcher.CurrentDispatcher);
        var before = System.Diagnostics.Stopwatch.GetTimestamp();
        var timestamp = sequencer.Timestamp;
        var after = System.Diagnostics.Stopwatch.GetTimestamp();

        await Assert.That(timestamp).IsGreaterThanOrEqualTo(before);
        await Assert.That(timestamp).IsLessThanOrEqualTo(after);
    }

    /// <summary>A posted batch preserves order and skips cancelled work.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        Queue<Action> drains = new();
        DispatcherSequencer sequencer = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            null);
        List<int> values = [];
        RecordingWorkItem cancelled = new(() => values.Add(0));
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(1)));
        sequencer.Schedule(cancelled);
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)), 0);
        cancelled.Dispose();
        await Assert.That(values).IsEmpty();
        await Assert.That(drains).Count().IsEqualTo(1);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Reentrant scheduling is delivered by a later batch.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleDuringDrainWaitsForTheNextDrain()
    {
        Queue<Action> drains = new();
        DispatcherSequencer sequencer = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            null);
        List<int> values = [];
        sequencer.Schedule(new RecordingWorkItem(() =>
        {
            values.Add(1);
            sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)));
        }));
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1], EqualityComparer<int>.Default, CollectionOrdering.Matching);
        drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo([1, SecondValue], EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Delayed callbacks preserve cancellation before their explicit delivery.</summary>
    /// <param name="cancel">Whether to cancel before delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleWaitsForItsCallback(bool cancel)
    {
        Queue<(IWorkItem Item, long Due)> delayed = new();
        DispatcherSequencer sequencer = new(
            Dispatcher.CurrentDispatcher,
            DispatcherPriority.Normal,
            static _ => throw new InvalidOperationException("Unexpected immediate dispatch."),
            (item, due) => delayed.Enqueue((item, due)));
        var calls = 0;
        RecordingWorkItem item = new(() => calls++);
        sequencer.Schedule(item, long.MaxValue);
        await Assert.That(calls).IsEqualTo(0);
        var pending = delayed.Dequeue();
        await Assert.That(pending.Due).IsEqualTo(long.MaxValue);
        if (cancel)
        {
            item.Dispose();
        }

        DispatchSequencerState.RunIfActive(pending.Item);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
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
