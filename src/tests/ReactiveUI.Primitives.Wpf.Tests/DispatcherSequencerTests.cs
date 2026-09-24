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

    /// <summary>The shared main-thread sequencer is a single instance bound to a dispatcher at normal priority.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task MainIsSharedAndBoundToADispatcher()
    {
        var main = DispatcherSequencer.Main;
        await Assert.That(DispatcherSequencer.Main).IsSameReferenceAs(main);
        await Assert.That(main.Dispatcher).IsNotNull();
        await Assert.That(main.Priority).IsEqualTo(DispatcherPriority.Normal);
    }

    /// <summary>Without a running application the main dispatcher falls back to the calling thread's dispatcher.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ResolveMainDispatcherFallsBackToCurrentDispatcher()
    {
        var expected = System.Windows.Application.Current?.Dispatcher ?? Dispatcher.CurrentDispatcher;
        await Assert.That(DispatcherSequencer.ResolveMainDispatcher()).IsSameReferenceAs(expected);
    }

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
