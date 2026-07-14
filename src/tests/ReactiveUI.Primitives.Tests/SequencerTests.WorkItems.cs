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

    /// <summary>How many times a test replays the cancel-versus-start race before checking nothing leaked.</summary>
    private const int StartCancelRaceAttempts = 2000;

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

    /// <summary>
    /// Verifies the delayed work item the sequencer's heap stores is a value with identity semantics: two entries are
    /// equal only when they carry the very same work item and the same due timestamp. The heap dedupes and reorders
    /// entries, so two distinct items that merely look alike must never compare equal, and the hash must agree.
    /// </summary>
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

        // The boxed overload agrees with the strongly typed one, and rejects anything that is not an entry.
        await Assert.That(item.Equals((object)same)).IsTrue();
        await Assert.That(item.Equals((object)otherItem)).IsFalse();
        await Assert.That(item.Equals(new object())).IsFalse();
    }

    /// <summary>
    /// Verifies a cancellation that lands while the action is starting still releases whatever the action returned.
    /// The work item claims cancellation and the action's result in two separate steps, so a dispose that slips
    /// between them would otherwise leave the returned disposable owned by nobody and never torn down.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledWorkItemReleasesItsActionResultWhenCancellationRacesTheStart()
    {
        var created = 0;
        var disposed = 0;

        for (var attempt = 0; attempt < StartCancelRaceAttempts; attempt++)
        {
            using ManualResetEventSlim actionReturning = new(false);
            ThreadPoolSequencer.ScheduledWorkItem<int> item = new(
                ThreadPoolSequencer.Instance,
                One,
                (_, _) =>
                {
                    // Let the canceller run at the moment the action hands its result back.
                    actionReturning.Set();
                    _ = Interlocked.Increment(ref created);
                    return new ActionDisposable(() => Interlocked.Increment(ref disposed));
                });

            var canceller = Task.Run(() =>
            {
                _ = actionReturning.Wait(TimeSpan.FromSeconds(TimeoutSeconds));
                item.Dispose();
            });

            item.Execute();
            await canceller;

            // Whichever side won, the item is cancelled, so the action's result must not survive it.
            item.Dispose();
        }

        // Every disposable the action handed back was released: none was stranded by the cancel-versus-start race.
        await Assert.That(Volatile.Read(ref disposed)).IsEqualTo(Volatile.Read(ref created));
        await Assert.That(Volatile.Read(ref created)).IsEqualTo(StartCancelRaceAttempts);
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
