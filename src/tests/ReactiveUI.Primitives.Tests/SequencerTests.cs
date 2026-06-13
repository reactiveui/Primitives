// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies sequencer, virtual-clock, and scheduled-item contracts.</summary>
public class SequencerTests
{
    /// <summary>A reusable value for one.</summary>
    private const int One = 1;

    /// <summary>A reusable value for two.</summary>
    private const int Two = 2;

    /// <summary>A reusable value for three.</summary>
    private const int Three = 3;

    /// <summary>A reusable value for four.</summary>
    private const int Four = 4;

    /// <summary>A reusable value for seven.</summary>
    private const int Seven = 7;

    /// <summary>A reusable value for eight.</summary>
    private const int Eight = 8;

    /// <summary>A reusable negative value.</summary>
    private const int NegativeOne = -1;

    /// <summary>Timeout used when waiting for background scheduled work.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>Reused first-error message.</summary>
    private const string FirstMessage = "first";

    /// <summary>Reused stopped event name.</summary>
    private const string StoppedMessage = "stopped";

    /// <summary>Expected immediate sequencer value sequence.</summary>
    private static readonly int[] ExpectedImmediateValues = [One, Two, Three];

    /// <summary>Expected one-two value sequence.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>Expected repeated scheduled item invocation sequence.</summary>
    private static readonly string[] ExpectedRepeatedScheduledItemInvocations = [FirstMessage, FirstMessage];

    /// <summary>Deterministic absolute due time for scheduler overload tests.</summary>
    private static readonly DateTimeOffset AbsoluteDueTime = DateTimeOffset.UnixEpoch;

    /// <summary>Expected values produced by simple scheduling extension overloads.</summary>
    private static readonly int[] ScheduleExpected = [One, Two, Three, Four];

    /// <summary>Expected virtual-time event sequence.</summary>
    private static readonly string[] VirtualEventsExpected = [FirstMessage, StoppedMessage];

    /// <summary>Verifies nested current-thread work is queued until the current action finishes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CurrentThreadSequencerQueuesNestedWorkUntilCurrentActionCompletes()
    {
        const int FirstCall = 1;
        const int SecondCall = 2;
        const int ThirdCall = 3;
        List<int> calls = [];
        Sequencer.CurrentThread.Schedule(() =>
        {
            calls.Add(FirstCall);
            Sequencer.CurrentThread.Schedule(() => calls.Add(ThirdCall));
            calls.Add(SecondCall);
        });
        int[] expected = [FirstCall, SecondCall, ThirdCall];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies the immediate sequencer waits until an absolute due time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateSequencerHonorsAbsoluteDueTime()
    {
        var elapsed = Stopwatch.StartNew();
        Sequencer.Immediate.Schedule(Sequencer.Immediate.Now + TimeSpan.FromMilliseconds(30), () => { });
        elapsed.Stop();
        await Assert.That(elapsed.Elapsed >= TimeSpan.FromMilliseconds(20)).IsTrue();
    }

    /// <summary>Verifies virtual-clock work runs only after the clock reaches the due time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockRunsScheduledWorkOnlyWhenAdvancedPastDueTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        VirtualClock clock = new();
        List<long> calls = [];
        clock.Schedule(TimeSpan.FromTicks(DueTicks), () => calls.Add(clock.Clock.Ticks));
        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        await Assert.That(calls.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        long[] expected = [DueTicks];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies virtual-clock timestamp scheduling converts monotonic ticks back to virtual time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockConvertsMonotonicTimestampDeltasToVirtualTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        VirtualClock clock = new();
        List<long> calls = [];
        var dueTimestamp = Sequencer.AddTimestamp(clock.Timestamp, TimeSpan.FromTicks(DueTicks));
        clock.Schedule(new CallbackWorkItem(() => calls.Add(clock.Clock.Ticks)), dueTimestamp);
        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        await Assert.That(calls.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        long[] expected = [DueTicks];
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies default sequencer aliases expose migration-friendly names.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulerDefaultAliasesExposeMigrationFriendlyNames()
    {
        await Assert.That(TaskPoolSequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Instance);
        await Assert.That(Sequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Default);
        await Assert.That(ThreadPoolSequencer.Instance).IsSameReferenceAs(ThreadPoolSequencer.Instance);
    }

    /// <summary>Verifies scheduled-item constructor argument validation.</summary>
    [Test]
    public void ScheduledItemConstructorValidatesSchedulerAndAction()
    {
        const int State = 42;
        Assert.Throws<ArgumentNullException>(() =>
            CreateScheduledItem(null!, State, (_, _) => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() => CreateScheduledItem(Sequencer.Immediate, State, null!));

        static void CreateScheduledItem(
            ISequencer scheduler,
            int state,
            Func<ISequencer, int, IDisposable> action) =>
            GC.KeepAlive(
                new ScheduledItem<DateTimeOffset, int>(scheduler, state, action, DateTimeOffset.UnixEpoch));
    }

    /// <summary>Covers priority-queue ordering, shrink, peek, and removal branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PriorityQueuesCoverOrderingShrinkAndRemovalBranches()
    {
        PriorityQueue<int> queue = new(Two);
        queue.Enqueue(Three);
        queue.Enqueue(One);
        queue.Enqueue(Two);
        await Assert.That(queue.Peek()).IsEqualTo(One);
        await Assert.That(queue.Remove(Two)).IsTrue();
        await Assert.That(queue.Remove(Four)).IsFalse();
        await Assert.That(queue.Dequeue()).IsEqualTo(One);
        await Assert.That(queue.Dequeue()).IsEqualTo(Three);
        Assert.Throws<InvalidOperationException>(() => queue.Peek());
        PriorityQueue<int> shrinkQueue = new(Eight);
        for (var i = 0; i < Eight; i++)
        {
            shrinkQueue.Enqueue(i);
        }

        for (var i = 0; i < Seven; i++)
        {
            await Assert.That(shrinkQueue.Dequeue()).IsEqualTo(i);
        }

        await Assert.That(shrinkQueue.Dequeue()).IsEqualTo(Seven);
        Assert.Throws<ArgumentOutOfRangeException>(CreateInvalidSequencerQueue);
    }

    /// <summary>Covers scheduled-item comparison, invocation, disposal, and clock branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledItemsCoverComparisonInvocationAndDisposalBranches()
    {
        List<string> invoked = [];
        var disposed = 0;
        ScheduledItem<int, string> first = new(
            Sequencer.Immediate,
            "first",
            (_, state) =>
            {
                invoked.Add(state);
                return new ActionDisposable(() => disposed++);
            },
            One);
        ScheduledItem<int, string> second = new(
            Sequencer.Immediate,
            "second",
            (_, _) => EmptyDisposable.Instance,
            Two);
        ScheduledItem<int, string> equalDue = new(
            Sequencer.Immediate,
            "equal",
            (_, _) => EmptyDisposable.Instance,
            One);
        await Assert.That(first < second).IsTrue();
        await Assert.That(first <= equalDue).IsTrue();
        await Assert.That(second > first).IsTrue();
        await Assert.That(second >= first).IsTrue();
        await Assert.That(first == second).IsFalse();
        await Assert.That(first != second).IsTrue();
        await Assert.That(first.Equals(second)).IsFalse();
        await Assert.That(first.CompareTo(null)).IsEqualTo(One);
        Assert.Throws<ArgumentException>(() => CompareScheduledItemWithInvalidObject(first));
        SequencerQueue<int> sequencerQueue = new(Two);
        sequencerQueue.Enqueue(second);
        sequencerQueue.Enqueue(first);
        await Assert.That(sequencerQueue.Peek()).IsSameReferenceAs(first);
        await Assert.That(sequencerQueue.Remove(second)).IsTrue();
        await Assert.That(sequencerQueue.Dequeue()).IsSameReferenceAs(first);
        first.Invoke();
        first.Invoke();
        first.Dispose();
        first.Dispose();
        await Assert.That(invoked.SequenceEqual(ExpectedRepeatedScheduledItemInvocations)).IsTrue();
        await Assert.That(disposed).IsEqualTo(Two);
        ScheduledItem<int, string> cancelled = new(
            Sequencer.Immediate,
            "cancelled",
            (_, state) =>
            {
                invoked.Add(state);
                return EmptyDisposable.Instance;
            },
            Three);
        cancelled.Cancel();
        cancelled.Invoke();
        await Assert.That(invoked).DoesNotContain("cancelled");
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutSequencer);
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutAction);
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutComparer);
        TestClock defaultClock = new();
        TestClock initialClock = new(DateTimeOffset.UnixEpoch);
        await Assert.That(defaultClock.Now).IsEqualTo(DateTimeOffset.MinValue);
        await Assert.That(initialClock.Now).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Covers immediate and background sequencer argument validation and execution paths.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task SequencersCoverValidationAndExecutionBranches()
    {
        await Assert.That(Sequencer.Immediate).IsSameReferenceAs(ImmediateSequencer.Instance);
        await Assert.That(TaskPoolSequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Instance);
        await Assert.That(Sequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Default);
        await Assert.That(Sequencer.Immediate.Now > DateTimeOffset.MinValue).IsTrue();
        await Assert.That(Sequencer.Normalize(TimeSpan.FromTicks(NegativeOne))).IsEqualTo(TimeSpan.Zero);
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(One, TimeSpan.Zero, null!));
        List<int> immediateValues = [];
        Sequencer.Immediate.Schedule(One, (_, state) =>
        {
            immediateValues.Add(state);
            return EmptyDisposable.Instance;
        }).Dispose();
        Sequencer.Immediate.Schedule(Two, TimeSpan.FromTicks(NegativeOne), (_, state) =>
        {
            immediateValues.Add(state);
            return EmptyDisposable.Instance;
        }).Dispose();
        Sequencer.Immediate.Schedule(Three, Sequencer.Immediate.Now.AddTicks(NegativeOne), (_, state) =>
        {
            immediateValues.Add(state);
            return EmptyDisposable.Instance;
        }).Dispose();
        await Assert.That(immediateValues.SequenceEqual(ExpectedImmediateValues)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => TaskPoolSequencer.Instance.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskPoolSequencer.Instance.Schedule(One, TimeSpan.Zero, null!));
        TaskCompletionSource taskPoolCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var taskPoolSubscription = TaskPoolSequencer.Instance.Schedule(Seven, (_, _) =>
        {
            taskPoolCompletion.SetResult();
            return EmptyDisposable.Instance;
        });
        await WaitForAsync(taskPoolCompletion.Task);
        TaskCompletionSource threadPoolCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var threadPoolSubscription = ThreadPoolSequencer.Instance.Schedule(Eight, TimeSpan.Zero, (_, _) =>
        {
            threadPoolCompletion.SetResult();
            return EmptyDisposable.Instance;
        });
        await WaitForAsync(threadPoolCompletion.Task);
        Assert.Throws<ArgumentNullException>(() => ThreadPoolSequencer.Instance.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => ThreadPoolSequencer.Instance.Schedule(One, TimeSpan.Zero, null!));
        ImmediateSynchronizationContext synchronizationContext = new();
        Assert.Throws<ArgumentNullException>(CreateSynchronizationContextSequencerWithoutContext);
        var previousContext = SynchronizationContext.Current;
        try
        {
            SynchronizationContext.SetSynchronizationContext(synchronizationContext);
            await Assert.That(SynchronizationContextSequencer.Current.Context)
                .IsSameReferenceAs(synchronizationContext);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        SynchronizationContextSequencer synchronizationSequencer = new(synchronizationContext);
        await Assert.That(synchronizationSequencer.Now > DateTimeOffset.MinValue).IsTrue();
        Assert.Throws<ArgumentNullException>(() => synchronizationSequencer.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => synchronizationSequencer.Schedule(One, TimeSpan.Zero, null!));
        List<int> synchronizationValues = [];
        using var synchronizationSubscription = synchronizationSequencer.Schedule(One, (_, state) =>
        {
            synchronizationValues.Add(state);
            return EmptyDisposable.Instance;
        });
        TaskCompletionSource<int> delayedSynchronizationCompletion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var delayedSynchronizationSubscription = synchronizationSequencer.Schedule(
            Two,
            TimeSpan.Zero,
            (_, state) =>
            {
                delayedSynchronizationCompletion.TrySetResult(state);
                return EmptyDisposable.Instance;
            });
        var delayedValue = await delayedSynchronizationCompletion.Task
            .WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        var synchronizedValues = synchronizationValues.Append(delayedValue);
        await Assert.That(synchronizedValues.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>Covers virtual-time extension validation and action scheduling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerExtensionsValidateAndRunActions()
    {
        TestClock clock = new(DateTimeOffset.UnixEpoch);
        var invoked = 0;
        Assert.Throws<ArgumentNullException>(() =>
            VirtualTimeSequencerExtensions.ScheduleRelative<DateTimeOffset, TimeSpan>(
                null!,
                TimeSpan.Zero,
                () => { }));
        Assert.Throws<ArgumentNullException>(() => clock.ScheduleRelative(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() =>
            VirtualTimeSequencerExtensions.ScheduleAbsolute<DateTimeOffset, TimeSpan>(
                null!,
                DateTimeOffset.UnixEpoch,
                () => { }));
        Assert.Throws<ArgumentNullException>(() => clock.ScheduleAbsolute(DateTimeOffset.UnixEpoch, null!));
        clock.ScheduleRelative(TimeSpan.FromTicks(One), () => invoked += One);
        clock.ScheduleAbsolute(DateTimeOffset.UnixEpoch.AddTicks(Two), () => invoked += Two);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(invoked).IsEqualTo(One);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(invoked).IsEqualTo(Three);
    }

    /// <summary>Covers simple sequencer extension validation, delayed overloads, state overloads, and recursive scheduling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SimpleSequencerExtensionsCoverValidationAndRecursiveScheduling()
    {
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(() => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(TimeSpan.Zero, () => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(AbsoluteDueTime, () => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(AbsoluteDueTime, null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).Schedule(self => self()));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule((Action<Action>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(One, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.ScheduleAction(One, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() =>
            ((ISequencer)null!).ScheduleAction(One, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() =>
            Sequencer.Immediate.ScheduleAction(One, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(One, TimeSpan.Zero, _ => { }));
        Assert.Throws<ArgumentNullException>(() =>
            Sequencer.Immediate.ScheduleAction(One, TimeSpan.Zero, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() =>
            ((ISequencer)null!).ScheduleAction(One, TimeSpan.Zero, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() =>
            Sequencer.Immediate.ScheduleAction(One, TimeSpan.Zero, (Func<int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => ((ISequencer)null!).ScheduleAction(One, AbsoluteDueTime, _ => { }));
        Assert.Throws<ArgumentNullException>(() =>
            Sequencer.Immediate.ScheduleAction(One, AbsoluteDueTime, (Action<int>)null!));
        Assert.Throws<ArgumentNullException>(() =>
            ((ISequencer)null!).ScheduleAction(One, AbsoluteDueTime, _ => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() =>
            Sequencer.Immediate.ScheduleAction(One, AbsoluteDueTime, (Func<int, IDisposable>)null!));
        List<int> values = [];
        Sequencer.Immediate.ScheduleAction(One, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Two, value =>
        {
            values.Add(value);
            return EmptyDisposable.Instance;
        }).Dispose();
        Sequencer.Immediate.ScheduleAction(Three, TimeSpan.Zero, values.Add).Dispose();
        Sequencer.Immediate.ScheduleAction(Four, AbsoluteDueTime, value =>
        {
            values.Add(value);
            return EmptyDisposable.Instance;
        }).Dispose();
        var recursiveCount = 0;
        Sequencer.Immediate.Schedule(self =>
        {
            recursiveCount++;
            if (recursiveCount >= Three)
            {
                return;
            }

            self();
        }).Dispose();
        await Assert.That(values.SequenceEqual(ScheduleExpected)).IsTrue();
        await Assert.That(recursiveCount).IsEqualTo(Three);
    }

    /// <summary>Covers virtual-time service lookup, stopwatch, stop, sleep, and nested-run guard paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerBaseCoversServicesStopwatchAndRunGuards()
    {
        TestClock clock = new(DateTimeOffset.UnixEpoch);
        var provider = (IServiceProvider)clock;
        await Assert.That(provider.GetService(typeof(IStopwatchProvider))!).IsSameReferenceAs(clock);
        await Assert.That(provider.GetService(typeof(string))).IsNull();
        var stopwatch = clock.StartStopwatch();
        clock.Sleep(TimeSpan.FromTicks(One));
        await Assert.That(stopwatch.Elapsed).IsEqualTo(TimeSpan.FromTicks(One));
        List<string> events = [];
        using var firstSchedule = clock.ScheduleAction(FirstMessage, TimeSpan.FromTicks(One), value =>
        {
            events.Add(value);
            Assert.Throws<InvalidOperationException>(() => clock.AdvanceTo(clock.Now.AddTicks(One)));
            Assert.Throws<InvalidOperationException>(() => clock.AdvanceBy(TimeSpan.FromTicks(One)));
        });
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        using var stoppedSchedule = clock.ScheduleAction(StoppedMessage, TimeSpan.FromTicks(One), events.Add);
        clock.Stop();
        clock.Start();
        await Assert.That(events.SequenceEqual(VirtualEventsExpected)).IsTrue();
    }

    /// <summary>Covers timestamp scheduling work-item argument validation.</summary>
    [Test]
    public void ScheduleWithTimestampValidatesWorkItem()
    {
        SynchronizationContextSequencer sequencer = new(new ImmediateSynchronizationContext());
        Assert.Throws<ArgumentNullException>(() => sequencer.Schedule(null!, long.MaxValue));
    }

    /// <summary>Covers timestamp scheduling executing due and past-due work items.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleWithTimestampExecutesDueAndPastDueWork()
    {
        SynchronizationContextSequencer sequencer = new(new ImmediateSynchronizationContext());
        CountingWorkItem workItem = new();
        var dueTimestamp = sequencer.Timestamp;
        sequencer.Schedule(workItem, dueTimestamp);
        sequencer.Schedule(workItem, dueTimestamp - 1);
        await Assert.That(workItem.ExecuteCount).IsEqualTo(Two);
    }

    /// <summary>Covers scheduled-item probe comparison, equality, and invocation branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduledProbeComparisonAndInvocationCoverContracts()
    {
        var scheduledDisposed = false;
        ScheduledProbe scheduled = new(One, () => new ActionDisposable(() => scheduledDisposed = true));
        await Assert.That(scheduled.CompareTo(null)).IsEqualTo(1);
        await Assert.That(scheduled.CompareTo(new ScheduledProbe(One, () => EmptyDisposable.Instance))).IsEqualTo(0);
        Assert.Throws<ArgumentException>(() => scheduled.CompareTo("not-scheduled"));
        await Assert.That(scheduled.Equals((object)scheduled)).IsTrue();
        await Assert.That(scheduled.Equals(new())).IsFalse();
        await Assert.That(scheduled.GetHashCode()).IsNotEqualTo(0);
        scheduled.Invoke();
        scheduled.Cancel();
        await Assert.That(scheduledDisposed).IsTrue();
    }

    /// <summary>Waits for a task with a bounded timeout.</summary>
    /// <param name="task">The task to wait for.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task WaitForAsync(Task task)
    {
        var timeout = Task.Delay(TimeSpan.FromSeconds(TimeoutSeconds));
        var completed = await Task.WhenAny(task, timeout).ConfigureAwait(false);
        if (completed == timeout)
        {
            throw new TimeoutException("Timed out waiting for scheduled work.");
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>Creates an invalid sequencer queue.</summary>
    private static void CreateInvalidSequencerQueue() => _ = new SequencerQueue<int>(NegativeOne);

    /// <summary>Creates a scheduled item without a sequencer.</summary>
    private static void CreateScheduledItemWithoutSequencer() =>
        _ = new ScheduledItem<int, string>(null!, "x", (_, _) => EmptyDisposable.Instance, One);

    /// <summary>Creates a scheduled item without an action.</summary>
    private static void CreateScheduledItemWithoutAction() =>
        _ = new ScheduledItem<int, string>(Sequencer.Immediate, "x", null!, One);

    /// <summary>Creates a scheduled item without a comparer.</summary>
    private static void CreateScheduledItemWithoutComparer() =>
        _ = new ScheduledItem<int, string>(
            Sequencer.Immediate,
            "x",
            (_, _) => EmptyDisposable.Instance,
            One,
            null!);

    /// <summary>Creates a synchronization-context sequencer without a context.</summary>
    private static void CreateSynchronizationContextSequencerWithoutContext() =>
        _ = new SynchronizationContextSequencer(null!);

    /// <summary>Compares a scheduled item through the non-generic comparable interface.</summary>
    /// <param name="item">The scheduled item.</param>
    private static void CompareScheduledItemWithInvalidObject(ScheduledItem<int, string> item) =>
        item.CompareTo("bad");

    /// <summary>Synchronization context that runs posted work immediately.</summary>
    private sealed class ImmediateSynchronizationContext : SynchronizationContext
    {
        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => d(state);
    }

    /// <summary>Counts work item executions.</summary>
    private sealed class CountingWorkItem : IWorkItem
    {
        /// <summary>Gets the number of executions.</summary>
        public int ExecuteCount { get; private set; }

        /// <inheritdoc/>
        public void Execute() => ExecuteCount++;
    }

    /// <summary>Test work item that invokes a supplied callback.</summary>
    private sealed class CallbackWorkItem : IWorkItem
    {
        /// <summary>Callback invoked by the work item.</summary>
        private readonly Action _callback;

        /// <summary>Initializes a new instance of the <see cref="CallbackWorkItem"/> class.</summary>
        /// <param name="callback">Callback invoked by the work item.</param>
        public CallbackWorkItem(Action callback) => _callback = callback;

        /// <inheritdoc/>
        public void Execute() => _callback();
    }
}
