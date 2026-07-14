// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

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
    /// Verifies disposing a thread-pool sequencer releases the delay timer it owns: delayed work scheduled
    /// afterwards is still accepted, but the released timer can never arm, so that work never runs. Disposal
    /// releases the timer and nothing else — the immediate path does not go through the timer and keeps working.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThreadPoolSequencerDisposeReleasesItsDelayTimer()
    {
        var sequencer = CreateIsolatedThreadPoolSequencer();
        TaskCompletionSource delayedRan = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource immediateRan = new(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Dispose();

        // The sequencer carries no disposed flag, and re-arming a released timer is a silent no-op, so the
        // schedule is accepted; the work simply never becomes due.
        sequencer.Schedule(
            new CallbackWorkItem(() => delayedRan.TrySetResult()),
            Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime));

        await Task.Delay(CancelObservationWindow);
        await Assert.That(delayedRan.Task.IsCompleted).IsFalse();

        sequencer.Schedule(new CallbackWorkItem(() => immediateRan.TrySetResult()));

        await immediateRan.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(immediateRan.Task.IsCompletedSuccessfully).IsTrue();
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
    /// Creates a thread-pool sequencer that owns its own delay queue and timer, so a test can dispose it without
    /// disturbing the shared singleton every other test schedules through.
    /// </summary>
    /// <returns>The isolated sequencer.</returns>
    private static ThreadPoolSequencer CreateIsolatedThreadPoolSequencer() =>
        (ThreadPoolSequencer)Activator.CreateInstance(typeof(ThreadPoolSequencer), true)!;

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
