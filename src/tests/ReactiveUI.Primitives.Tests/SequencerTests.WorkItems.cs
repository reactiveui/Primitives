// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the queued work-item shapes: run-once execution, cancellation, and delay conversion.</summary>
public partial class SequencerTests
{
    /// <summary>A monotonic timestamp delta that drives the delay conversions.</summary>
    private const long DueTimestamp = 1000;

    /// <summary>Verifies a monotonic delta at or before the current instant converts to no delay at all.</summary>
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

        var subscription = ImmediateSequencer.Schedule(() => ran++);

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
    public async Task ScheduledWorkItemQueuesItsExecutionCallback()
{
        using ManualThreadPool pool = new();
        var ran = false;
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(pool.Sequencer, One, (_, _) =>
        {
            ran = true;
            return EmptyDisposable.Instance;
        });
        item.Queue();
        await Assert.That(ran).IsFalse();
        pool.RunReady();
        await Assert.That(ran).IsTrue();
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

    /// <summary>Timed work item equality and hashing use work item identity and due timestamp.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimedWorkItemComparesByWorkItemIdentityAndDueTimestamp()
    {
        CancellableWorkItem first = new();
        CancellableWorkItem second = new();

        ThreadPoolSequencer.TimedWorkItem item = new(first, DueTimestamp);
        ThreadPoolSequencer.TimedWorkItem same = new(first, DueTimestamp);
        ThreadPoolSequencer.TimedWorkItem otherItem = new(second, DueTimestamp);
        ThreadPoolSequencer.TimedWorkItem otherDueTimestamp = new(first, DueTimestamp + One);

        await Assert.That(item.Equals(same)).IsTrue();
        await Assert.That(item.GetHashCode()).IsEqualTo(same.GetHashCode());

        // Identity, not structure: a different work item due at the same instant is a different entry.
        await Assert.That(item.Equals(otherItem)).IsFalse();
        await Assert.That(item.Equals(otherDueTimestamp)).IsFalse();

        await Assert.That(item.Equals((object)same)).IsTrue();
        await Assert.That(item.Equals((object)otherItem)).IsFalse();
        await Assert.That(item.Equals(new object())).IsFalse();
    }

    /// <summary>Cancellation during invocation disposes the action's returned resource exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemReleasesItsActionResultWhenCanceledDuringInvocation()
    {
        var disposed = 0;
        ThreadPoolSequencer.ScheduledWorkItem<int>? item = null;
        item = new(ThreadPoolSequencer.Instance, One, (_, _) =>
        {
            item!.Dispose();
            return new ActionDisposable(() => disposed++);
        });
        item.Execute();
        item.Dispose();
        await Assert.That(item.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>A delegate work item whose action returns no disposable runs and cancels cleanly.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelegateWorkItemToleratesANullActionResult()
    {
        var ran = 0;
        Sequencer.DelegateWorkItem<int> item = new(Sequencer.Immediate, One, (_, _) =>
        {
            ran++;
            return null!;
        });

        item.Execute();
        item.Dispose();

        await Assert.That(ran).IsEqualTo(1);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>A thread-pool work item whose action returns no disposable runs and cancels cleanly.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemToleratesANullActionResult()
    {
        var ran = 0;
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(ThreadPoolSequencer.Instance, One, (_, _) =>
        {
            ran++;
            return null!;
        });

        item.Execute();
        item.Dispose();

        await Assert.That(ran).IsEqualTo(1);
        await Assert.That(item.IsDisposed).IsTrue();
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
