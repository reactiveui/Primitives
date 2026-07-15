// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the sequencers that hand work to a pool or a dispatcher: how a faulting work item is routed, how the
/// shared delay timer is armed and released, and how delayed work is marshalled back onto its own thread.
/// </summary>
public partial class SequencerTests
{
    /// <summary>Message carried by the work item that faults on purpose.</summary>
    private const string FaultMessage = "scheduled work failed";

    /// <summary>How far ahead delayed work is scheduled, so the sequencer's delay timer really has to arm.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(20);

    /// <summary>How far ahead work that is cancelled before it becomes due is scheduled.</summary>
    private static readonly TimeSpan CancelledDueTime = TimeSpan.FromMilliseconds(100);

    /// <summary>How long a test watches for work that must never run.</summary>
    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(400);

    /// <summary>How long a test lets a drain finish disarming before it releases the sequencer's timer.</summary>
    private static readonly TimeSpan DrainSettleWindow = TimeSpan.FromMilliseconds(100);

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
        using var sequencer = CreateIsolatedThreadPoolSequencer();
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(
            new CallbackWorkItem(() => ran.TrySetResult()),
            Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime));

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(ran.Task.IsCompletedSuccessfully).IsTrue();

        // Let the drain finish disarming the timer before this scope releases it.
        await Task.Delay(DrainSettleWindow);
    }

    /// <summary>
    /// Verifies a disposed thread-pool sequencer rejects new work rather than accepting work it can never run: the
    /// delay timer is gone, so an accepted delayed item would sit in the queue forever, and an accepted immediate
    /// item would run on a sequencer its owner has already torn down. Both overloads fail fast instead.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerRejectsWorkScheduledAfterDispose()
    {
        var sequencer = CreateIsolatedThreadPoolSequencer();
        CancellableWorkItem immediate = new();
        CancellableWorkItem delayed = new();

        sequencer.Dispose();

        await Assert.That(() => sequencer.Schedule(immediate)).ThrowsExactly<ObjectDisposedException>();
        await Assert
            .That(() => sequencer.Schedule(delayed, Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime)))
            .ThrowsExactly<ObjectDisposedException>();

        await Task.Delay(CancelObservationWindow);
        await Assert.That(immediate.ExecuteCount).IsEqualTo(0);
        await Assert.That(delayed.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies disposing a thread-pool sequencer releases the delayed work still queued behind its timer: the
    /// pending item is cancelled, not stranded in the queue of a sequencer that can no longer arm a timer for it.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeCancelsQueuedDelayedWork()
    {
        var sequencer = CreateIsolatedThreadPoolSequencer();
        CancellableWorkItem pending = new();

        sequencer.Schedule(pending, Sequencer.AddTimestamp(sequencer.Timestamp, CancelledDueTime));
        sequencer.Dispose();

        await Assert.That(pending.IsDisposed).IsTrue();

        await Task.Delay(CancelObservationWindow);
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

    /// <summary>Verifies delayed work is marshalled back through the synchronization context once it is due.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizationContextSequencerPostsDelayedWorkOnceItIsDue()
    {
        RecordingSynchronizationContext context = new();
        SynchronizationContextSequencer sequencer = new(context);
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(
            new CallbackWorkItem(() => ran.TrySetResult()),
            Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime));

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(context.PostCount).IsEqualTo(1);
    }

    /// <summary>Verifies delayed work cancelled before its due time never reaches the synchronization context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizationContextSequencerDropsDelayedWorkCancelledBeforeItIsDue()
    {
        RecordingSynchronizationContext context = new();
        SynchronizationContextSequencer sequencer = new(context);
        CancellableWorkItem item = new();

        sequencer.Schedule(item, Sequencer.AddTimestamp(sequencer.Timestamp, CancelledDueTime));
        item.Dispose();

        await Task.Delay(CancelObservationWindow);

        await Assert.That(item.ExecuteCount).IsEqualTo(0);
        await Assert.That(context.PostCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies disposing a thread-pool sequencer twice releases its queued work exactly once and leaves the sequencer
    /// closed. The second disposal must be a no-op rather than a second release of work the first disposal already
    /// cancelled and handed back to its owner.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeIsIdempotent()
    {
        var sequencer = CreateIsolatedThreadPoolSequencer();
        DisposeCountingWorkItem pending = new();

        sequencer.Schedule(pending, Sequencer.AddTimestamp(sequencer.Timestamp, CancelledDueTime));

        sequencer.Dispose();
        await Assert.That(pending.DisposeCount).IsEqualTo(1);

        await Assert.That(sequencer.Dispose).ThrowsNothing();

        // The queued item was released once, and the sequencer is still closed rather than reopened by the second call.
        await Assert.That(pending.DisposeCount).IsEqualTo(1);
        await Assert.That(() => sequencer.Schedule(new CancellableWorkItem()))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>
    /// Verifies a drain still unwinding when the sequencer is disposed does not re-arm the timer disposal has already
    /// released. The drain runs its items outside the gate, so disposal can land mid-drain; when the drain takes the
    /// gate again it must observe the disposal and stop rather than arm a timer that no longer exists.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeDuringADrainStopsTheDrainRearmingTheTimer()
    {
        var sequencer = CreateIsolatedThreadPoolSequencer();
        TaskCompletionSource draining = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim release = new(false);

        // Park the drain inside a due item, so the disposal below provably lands while RunDue is mid-loop.
        sequencer.Schedule(
            new CallbackWorkItem(() =>
            {
                _ = draining.TrySetResult();
                _ = release.Wait(TimeSpan.FromSeconds(TimeoutSeconds));
            }),
            Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime));

        await draining.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));

        CancellableWorkItem queued = new();
        sequencer.Schedule(queued, Sequencer.AddTimestamp(sequencer.Timestamp, CancelledDueTime));

        sequencer.Dispose();

        // Let the parked drain resume: it must unwind quietly instead of arming the timer disposal released.
        release.Set();
        await Task.Delay(CancelObservationWindow);

        await Assert.That(queued.IsDisposed).IsTrue();
        await Assert.That(queued.ExecuteCount).IsEqualTo(0);
        await Assert.That(sequencer.Dispose).ThrowsNothing();
    }

    /// <summary>
    /// Creates a thread-pool sequencer that owns its own delay queue and timer, so a test can dispose it without
    /// disturbing the shared singleton every other test schedules through.
    /// </summary>
    /// <returns>The isolated sequencer.</returns>
    private static ThreadPoolSequencer CreateIsolatedThreadPoolSequencer() => new();

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

    /// <summary>Task scheduler that runs queued work inline and keeps the last task so a test can read its outcome.</summary>
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

    /// <summary>Work item that always faults, so a sequencer's failure routing can be observed.</summary>
    private sealed class ThrowingWorkItem : IWorkItem
    {
        /// <inheritdoc/>
        public void Execute() => throw new InvalidOperationException(FaultMessage);
    }
}
