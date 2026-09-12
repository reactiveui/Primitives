// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using TUnit.Assertions.Enums;

namespace ReactiveUI.Primitives.WinUI.Tests;

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

    /// <summary>Queued work preserves order and cancellation until the drain runs.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ScheduleCoalescesOrderedWorkAndSkipsCancellation()
    {
        ManualDispatcher dispatcher = new();
        var sequencer = dispatcher.Create();
        List<int> values = [];
        RecordingWorkItem cancelled = new(() => values.Add(0));
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(1)));
        sequencer.Schedule(cancelled);
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)), 0);
        cancelled.Dispose();
        await Assert.That(values).IsEmpty();
        await Assert.That(dispatcher.Drains).Count().IsEqualTo(1);
        await Assert.That(dispatcher.LastPriority).IsEqualTo(DispatcherQueuePriority.High);
        await Assert.That(sequencer.Priority).IsEqualTo(DispatcherQueuePriority.High);
        await Assert.That(sequencer.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(sequencer.DebuggerDisplay).IsEqualTo(typeof(DispatcherQueueSequencer).FullName);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>A rejected post retains queued work and reuses its cached callback on retry.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task RejectedPostCanRetryWithoutLosingQueuedWork()
    {
        ManualDispatcher dispatcher = new() { AcceptsPosts = false };
        var sequencer = dispatcher.Create();
        List<int> values = [];
        await Assert.That(() => sequencer.Schedule(new RecordingWorkItem(() => values.Add(1))))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(values).IsEmpty();
        var rejectedHandler = dispatcher.LastHandler;
        dispatcher.AcceptsPosts = true;
        sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)));
        await Assert.That(dispatcher.LastHandler).IsSameReferenceAs(rejectedHandler);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Work queued from inside a callback waits for a later drain.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ReentrantScheduleRunsInTheNextBatch()
    {
        ManualDispatcher dispatcher = new();
        var sequencer = dispatcher.Create();
        List<int> values = [];
        sequencer.Schedule(new RecordingWorkItem(() =>
        {
            values.Add(1);
            sequencer.Schedule(new RecordingWorkItem(() => values.Add(SecondValue)));
        }));
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(FirstBatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
        dispatcher.Drains.Dequeue()();
        await Assert.That(values).IsEquivalentTo(BatchValues, EqualityComparer<int>.Default, CollectionOrdering.Matching);
    }

    /// <summary>Delayed callbacks retain their due time and honor cancellation.</summary>
    /// <param name="cancel">Whether cancellation precedes delivery.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DelayedScheduleWaitsForItsCallback(bool cancel)
    {
        ManualDispatcher dispatcher = new();
        var sequencer = dispatcher.Create();
        var calls = 0;
        RecordingWorkItem item = new(() => calls++);
        sequencer.Schedule(item, long.MaxValue);
        await Assert.That(calls).IsEqualTo(0);
        await Assert.That(dispatcher.Drains).IsEmpty();
        var pending = dispatcher.Delays.Dequeue();
        await Assert.That(pending.Due).IsEqualTo(long.MaxValue);
        if (cancel)
        {
            item.Dispose();
        }

        DispatchSequencerState.RunIfActive(pending.Item);
        await Assert.That(calls).IsEqualTo(cancel ? 0 : 1);
    }

    /// <summary>Retains callbacks until they are explicitly delivered.</summary>
    private sealed class ManualDispatcher
    {
        /// <summary>Gets the queued drains.</summary>
        public Queue<DispatcherQueueHandler> Drains { get; } = new();

        /// <summary>Gets delayed work and requested timestamps.</summary>
        public Queue<(IWorkItem Item, long Due)> Delays { get; } = new();

        /// <summary>Gets or sets whether enqueue attempts succeed.</summary>
        public bool AcceptsPosts { get; set; } = true;

        /// <summary>Gets the most recently posted handler.</summary>
        public DispatcherQueueHandler? LastHandler { get; private set; }

        /// <summary>Gets the priority of the most recent post.</summary>
        public DispatcherQueuePriority LastPriority { get; private set; }

        /// <summary>Creates a sequencer using the retained callbacks.</summary>
        /// <returns>The sequencer.</returns>
        public DispatcherQueueSequencer Create() =>
            new(DispatcherQueuePriority.High, TryEnqueue, (item, due) => Delays.Enqueue((item, due)));

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
