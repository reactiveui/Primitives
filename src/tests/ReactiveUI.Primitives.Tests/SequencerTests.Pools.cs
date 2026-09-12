// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests delayed dispatch, cancellation, and failure routing through pool and context sequencers.</summary>
public partial class SequencerTests
{
    /// <summary>Message carried by the work item that faults on purpose.</summary>
    private const string FaultMessage = "scheduled work failed";

    /// <summary>Verifies a faulting work item is handed to the sequencer's unhandled-exception handler.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolSequencerRoutesWorkItemFailuresToItsHandler()
    {
        InlineTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));
        Exception? handled = null;
        sequencer.UnhandledExceptionHandler = ex => handled = ex;

        sequencer.Schedule(new ThrowingWorkItem());

        await Assert.That(handled).IsNotNull();
        await Assert.That(handled!.Message).IsEqualTo(FaultMessage);

        // The handler owns the failure, so the scheduled task must not also fault.
        await Assert.That(scheduler.LastTask!.IsFaulted).IsFalse();
    }

    /// <summary>Verifies a faulting work item is rethrown onto its scheduled task when no handler is installed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolSequencerRethrowsWorkItemFailuresWithoutAHandler()
    {
        InlineTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));

        sequencer.Schedule(new ThrowingWorkItem());

        var scheduled = scheduler.LastTask!;
        await Assert.That(scheduled.IsFaulted).IsTrue();
        await Assert.That(scheduled.Exception!.InnerException!.Message).IsEqualTo(FaultMessage);
    }

    /// <summary>Verifies delayed work runs once due and the sequencer drains its delay queue afterwards.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerRunsDelayedWorkAndDrainsItsQueue()
{
        using ManualThreadPool pool = new();
        CancellableWorkItem first = new();
        CancellableWorkItem second = new();
        pool.Sequencer.Schedule(first, One);
        pool.Sequencer.Schedule(second, Two);
        pool.RunDue(0);
        await Assert.That(first.ExecuteCount).IsEqualTo(0);
        pool.RunDue(One);
        await Assert.That(first.ExecuteCount).IsEqualTo(1);
        await Assert.That(second.ExecuteCount).IsEqualTo(0);
        pool.RunDue(Two);
        await Assert.That(second.ExecuteCount).IsEqualTo(1);
        await Assert.That(pool.Delays[^1]).IsEqualTo(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Elapsed and current timestamps queue immediate work without arming a timer.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ThreadPoolSequencerRoutesElapsedTimestampsToItsImmediateQueue()
    {
        using ManualThreadPool pool = new() { Timestamp = Two };
        CancellableWorkItem elapsed = new();
        CancellableWorkItem current = new();
        pool.Sequencer.Schedule(elapsed, One);
        pool.Sequencer.Schedule(current, Two);
        await Assert.That(elapsed.ExecuteCount).IsEqualTo(0);
        await Assert.That(current.ExecuteCount).IsEqualTo(0);
        await Assert.That(pool.Delays).IsEmpty();
        pool.RunReady();
        await Assert.That(elapsed.ExecuteCount).IsEqualTo(1);
        await Assert.That(current.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>A disposed thread-pool sequencer rejects immediate and delayed work.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerRejectsWorkScheduledAfterDispose()
{
        using ManualThreadPool pool = new();
        CancellableWorkItem immediate = new();
        CancellableWorkItem delayed = new();
        pool.Sequencer.Dispose();
        await Assert.That(() => pool.Sequencer.Schedule(immediate)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => pool.Sequencer.Schedule(delayed, One)).ThrowsExactly<ObjectDisposedException>();
        pool.RunReady();
        pool.RunDue(One);
        await Assert.That(immediate.ExecuteCount).IsEqualTo(0);
        await Assert.That(delayed.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Disposal cancels queued delayed work.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeCancelsQueuedDelayedWork()
{
        using ManualThreadPool pool = new();
        CancellableWorkItem pending = new();
        pool.Sequencer.Schedule(pending, One);
        pool.Sequencer.Dispose();
        pool.RunDue(One);
        await Assert.That(pending.IsDisposed).IsTrue();
        await Assert.That(pending.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Verifies posted work cancelled before the dispatcher ran it is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizationContextSequencerSkipsCancelledPostedWork()
    {
        RecordingSynchronizationContext context = new();
        SynchronizationContextSequencer sequencer = new(context);
        CancellableWorkItem item = new();
        item.Dispose();

        sequencer.Schedule(item);

        await Assert.That(context.PostCount).IsEqualTo(1);
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>The current-context factory captures the active context and rejects an absent context.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SynchronizationContextSequencerCapturesTheCurrentContext()
    {
        var previous = SynchronizationContext.Current;
        RecordingSynchronizationContext context = new();
        CancellableWorkItem item = new();
        try
        {
            SynchronizationContext.SetSynchronizationContext(context);
            SynchronizationContextSequencer.Current.Schedule(item);
            SynchronizationContext.SetSynchronizationContext(null);
            _ = Assert.Throws<InvalidOperationException>(static () => _ = SynchronizationContextSequencer.Current);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        await Assert.That(context.PostCount).IsEqualTo(1);
        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Verifies delayed work is marshalled back through the synchronization context once it is due.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizationContextSequencerPostsDelayedWorkOnceItIsDue()
{
        RecordingSynchronizationContext context = new();
        ManualSequencer delays = new();
        SynchronizationContextSequencer sequencer = new(context, delays);
        CancellableWorkItem item = new();
        sequencer.Schedule(item, long.MaxValue);
        await Assert.That(context.PostCount).IsEqualTo(0);
        delays.RunPending();
        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(context.PostCount).IsEqualTo(1);
    }

    /// <summary>Verifies delayed work cancelled before its due time never reaches the synchronization context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizationContextSequencerDropsDelayedWorkCancelledBeforeItIsDue()
{
        RecordingSynchronizationContext context = new();
        ManualSequencer delays = new();
        SynchronizationContextSequencer sequencer = new(context, delays);
        CancellableWorkItem item = new();
        sequencer.Schedule(item, long.MaxValue);
        item.Dispose();
        delays.RunPending();
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
        await Assert.That(context.PostCount).IsEqualTo(0);
    }

    /// <summary>Repeated disposal releases queued work once and keeps the sequencer closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeIsIdempotent()
{
        using ManualThreadPool pool = new();
        DisposeCountingWorkItem pending = new();
        pool.Sequencer.Schedule(pending, One);
        pool.Sequencer.Dispose();
        await Assert.That(pending.DisposeCount).IsEqualTo(1);
        await Assert.That(pool.Sequencer.Dispose).ThrowsNothing();
        await Assert.That(pending.DisposeCount).IsEqualTo(1);
        await Assert.That(() => pool.Sequencer.Schedule(new CancellableWorkItem()))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Disposal during a drain cancels pending work and prevents timer rearming.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeDuringADrainStopsTheDrainRearmingTheTimer()
{
        using ManualThreadPool pool = new();
        CancellableWorkItem queued = new();
        pool.Sequencer.Schedule(new CallbackWorkItem(pool.Sequencer.Dispose), One);
        pool.Sequencer.Schedule(queued, Two);
        var changes = pool.Delays.Count;
        pool.RunDue(One);
        await Assert.That(queued.IsDisposed).IsTrue();
        await Assert.That(queued.ExecuteCount).IsEqualTo(0);
        await Assert.That(pool.Delays.Count).IsEqualTo(changes);
        await Assert.That(pool.Sequencer.Dispose).ThrowsNothing();
    }

    /// <summary>Work item that counts how many times a sequencer released it.</summary>
    private sealed class DisposeCountingWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>Backing count of disposals.</summary>
        private int _disposeCount;

        /// <summary>Gets the number of times this item was disposed.</summary>
        public int DisposeCount => Volatile.Read(ref _disposeCount);

        /// <inheritdoc/>
        public bool IsDisposed => DisposeCount != 0;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Interlocked.Increment(ref _disposeCount);

        /// <inheritdoc/>
        public void Execute()
        {
        }
    }

    /// <summary>Synchronization context that runs posted work inline and counts the posts it received.</summary>
    private sealed class RecordingSynchronizationContext : SynchronizationContext
    {
        /// <summary>Gets the number of posted callbacks.</summary>
        public int PostCount { get; private set; }

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state)
        {
            PostCount++;
            d(state);
        }
    }

    /// <summary>Task scheduler that runs queued work inline and keeps the most recently queued task.</summary>
    private sealed class InlineTaskScheduler : TaskScheduler
    {
        /// <summary>Gets the most recently queued task.</summary>
        public Task? LastTask { get; private set; }

        /// <inheritdoc/>
        protected override IEnumerable<Task> GetScheduledTasks() => [];

        /// <inheritdoc/>
        protected override void QueueTask(Task task)
        {
            LastTask = task;
            _ = TryExecuteTask(task);
        }

        /// <inheritdoc/>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
    }

    /// <summary>Work item that throws when executed.</summary>
    private sealed class ThrowingWorkItem : IWorkItem
    {
        /// <inheritdoc/>
        public void Execute() => throw new InvalidOperationException(FaultMessage);
    }
}
