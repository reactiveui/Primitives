// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the work-item shapes the sequencers queue: the run-once handshake, cancellation before and after a run,
/// and the delay conversion that decides when a due item runs.
/// </summary>
public partial class SequencerTests
{
    /// <summary>A monotonic timestamp delta used to drive the delay conversions.</summary>
    private const long DueTimestamp = 1000;

    /// <summary>Verifies a monotonic delta that has already elapsed converts to no delay at all.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimestampDeltaAtOrBeforeNowConvertsToZeroDelay()
    {
        await Assert.That(Sequencer.ToTimeSpanDelta(0)).IsEqualTo(TimeSpan.Zero);
        await Assert.That(Sequencer.ToTimeSpanDelta(-DueTimestamp)).IsEqualTo(TimeSpan.Zero);
        await Assert.That(Sequencer.ToTimeSpanDelta(DueTimestamp) > TimeSpan.Zero).IsTrue();
    }

    /// <summary>Verifies the immediate sequencer runs an action inline and hands back the shared empty disposable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateSequencerRunsActionsInline()
    {
        var ran = 0;

        var subscription = Sequencer.Immediate.Schedule(() => ran++);

        await Assert.That(ran).IsEqualTo(1);
        await Assert.That(subscription).IsSameReferenceAs(EmptyDisposable.Instance);
    }

    /// <summary>Verifies the immediate sequencer drops work items cancelled before it could run them.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateSequencerSkipsCancelledWorkItems()
    {
        CancellableWorkItem item = new();
        item.Dispose();

        Sequencer.Immediate.Schedule(item);
        Sequencer.Immediate.Schedule(item, Sequencer.Immediate.Timestamp - DueTimestamp);

        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Verifies a cancelled action work item releases its action and never runs it again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ActionWorkItemDoesNotRunAfterCancellation()
    {
        var ran = 0;
        Sequencer.ActionWorkItem<int> item = new(One, _ => ran++);

        item.Execute();
        item.Dispose();
        item.Execute();

        await Assert.That(ran).IsEqualTo(1);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>Verifies a delegate work item cancelled before it started never invokes its action.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelegateWorkItemDoesNotRunAfterCancellation()
    {
        var ran = 0;
        Sequencer.DelegateWorkItem<int> item = new(Sequencer.Immediate, One, (_, _) =>
        {
            ran++;
            return EmptyDisposable.Instance;
        });

        item.Dispose();
        item.Execute();

        await Assert.That(ran).IsEqualTo(0);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>Verifies a thread-pool work item holds the disposable its action returned until it is cancelled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemReleasesItsActionResultOnCancellation()
    {
        var disposed = 0;
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(
            ThreadPoolSequencer.Instance,
            One,
            (_, _) => new ActionDisposable(() => disposed++));

        item.Execute();
        await Assert.That(disposed).IsEqualTo(0);

        item.Dispose();
        await Assert.That(disposed).IsEqualTo(1);

        // Disposal is a single transition: a second cancel must not dispose the action's result twice.
        item.Dispose();
        await Assert.That(disposed).IsEqualTo(1);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>Verifies a thread-pool work item cancelled before it started never invokes its action.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemDoesNotRunAfterCancellation()
    {
        var ran = 0;
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(
            ThreadPoolSequencer.Instance,
            One,
            (_, _) =>
            {
                ran++;
                return EmptyDisposable.Instance;
            });

        item.Dispose();
        item.Execute();

        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>Verifies queueing a thread-pool work item hands it to the pool for execution.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemQueueRunsOnTheThreadPool()
    {
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(
            ThreadPoolSequencer.Instance,
            One,
            (_, _) =>
            {
                _ = ran.TrySetResult();
                return EmptyDisposable.Instance;
            });

        item.Queue();

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        await Assert.That(ran.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Verifies the stateful scheduling overloads that take a due time run their callbacks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleActionDueTimeOverloadsRunTheirCallbacks()
    {
        List<int> values = [];

        Sequencer.Immediate.ScheduleAction(One, TimeSpan.Zero, value =>
        {
            values.Add(value);
            return EmptyDisposable.Instance;
        }).Dispose();
        Sequencer.Immediate.ScheduleAction(Two, AbsoluteDueTime, values.Add).Dispose();

        await Assert.That(values.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>Work item that counts executions and can be cancelled before a sequencer reaches it.</summary>
    private sealed class CancellableWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>Gets the number of executions.</summary>
        public int ExecuteCount { get; private set; }

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        public void Execute() => ExecuteCount++;
    }
}
