// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Covers runtime support types that are internal implementation details but must remain visible to coverage.</summary>
public class CoverageRuntimeTests
{
    /// <summary>A reusable value for one.</summary>
    private const int One = 1;

    /// <summary>A reusable value for two.</summary>
    private const int Two = 2;

    /// <summary>A reusable value for three.</summary>
    private const int Three = 3;

    /// <summary>A reusable value for four.</summary>
    private const int Four = 4;

    /// <summary>A reusable value for five.</summary>
    private const int Five = 5;

    /// <summary>A reusable value for six.</summary>
    private const int Six = 6;

    /// <summary>A reusable value for seven.</summary>
    private const int Seven = 7;

    /// <summary>A reusable value for eight.</summary>
    private const int Eight = 8;

    /// <summary>A reusable negative value.</summary>
    private const int NegativeOne = -1;

    /// <summary>Timeout used when waiting for background scheduled work.</summary>
    private const int TimeoutSeconds = 2;

    /// <summary>Expected two-only value sequence.</summary>
    private static readonly int[] ExpectedTwoOnly = [Two];

    /// <summary>Expected one-two value sequence.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>Expected three-four value sequence.</summary>
    private static readonly int[] ExpectedThreeFour = [Three, Four];

    /// <summary>Expected five-six value sequence.</summary>
    private static readonly int[] ExpectedFiveSix = [Five, Six];

    /// <summary>Expected immediate sequencer value sequence.</summary>
    private static readonly int[] ExpectedImmediateValues = [One, Two, Three];

    /// <summary>Expected handled error sequence.</summary>
    private static readonly string[] ExpectedHandledErrors = ["handled"];

    /// <summary>Expected safe witness event sequence.</summary>
    private static readonly string[] ExpectedSafeEvents = ["next:3", "completed"];

    /// <summary>Expected repeated scheduled item invocation sequence.</summary>
    private static readonly string[] ExpectedRepeatedScheduledItemInvocations = ["first", "first"];

    /// <summary>Covers disposable slot constructor, disposal, removal, and disposed-assignment branches.</summary>
    [Test]
    public void DisposableSlotsCoverAssignmentReplacementAndRemovalBranches()
    {
        var disposedBeforeAssign = 0;
        var lateSingle = new SingleDisposable(() => disposedBeforeAssign++);
        lateSingle.Dispose();
        lateSingle.Create(new ActionDisposable(() => disposedBeforeAssign++));
        Assert.Equal(Two, disposedBeforeAssign);
        Assert.Throws<ArgumentNullException>(() => lateSingle.Create(null!));

        var replaced = 0;
        var replaceable = new SingleReplaceableDisposable(new ActionDisposable(() => replaced++), () => replaced++);
        replaceable.Create(new ActionDisposable(() => replaced++));
        replaceable.Dispose();
        replaceable.Create(new ActionDisposable(() => replaced++));
        Assert.Equal(Five, replaced);
        Assert.Throws<ArgumentNullException>(() => replaceable.Create(null!));

        var disposeFalse = 0;
        var single = new ExposedSingleDisposable(new ActionDisposable(() => disposeFalse++));
        single.DisposeFalse();
        single.Dispose();
        Assert.Equal(1, disposeFalse);

        var replaceableFalse = 0;
        var exposedReplaceable = new ExposedSingleReplaceableDisposable(new ActionDisposable(() => replaceableFalse++));
        exposedReplaceable.DisposeFalse();
        exposedReplaceable.Dispose();
        Assert.Equal(1, replaceableFalse);

        var multipleFalse = 0;
        var exposedMultiple = new ExposedMultipleDisposable(new ActionDisposable(() => multipleFalse++));
        exposedMultiple.DisposeFalse();
        exposedMultiple.Dispose();
        Assert.Equal(1, multipleFalse);

        var firstDisposed = 0;
        var secondDisposed = 0;
        var thirdDisposed = 0;
        var fourthDisposed = 0;
        var missing = EmptyDisposable.Instance;
        var first = new ActionDisposable(() => firstDisposed++);
        var second = new ActionDisposable(() => secondDisposed++);
        var third = new ActionDisposable(() => thirdDisposed++);
        var fourth = new ActionDisposable(() => fourthDisposed++);
        MultipleDisposable group = [first, second, third, fourth];

        Assert.True(group.Remove(first));
        Assert.True(group.Remove(second));
        Assert.True(group.Remove(third));
        Assert.False(group.Remove(missing));
        group.Dispose();

        Assert.Equal(1, firstDisposed);
        Assert.Equal(1, secondDisposed);
        Assert.Equal(1, thirdDisposed);
        Assert.Equal(1, fourthDisposed);

        var factoryDisposed = 0;
        var factoryGroup = MultipleDisposable.Create(
            new ActionDisposable(() => factoryDisposed++),
            null!,
            new ActionDisposable(() => factoryDisposed++));
        factoryGroup.Dispose();
        factoryGroup.Dispose();
        Assert.Equal(Two, factoryDisposed);
        Assert.Throws<ArgumentNullException>(() => MultipleDisposable.Create(null!));

        _ = new AssignmentSlot();
        _ = new AssignmentSlot(() => { });
        _ = new AssignmentSlot(EmptyDisposable.Instance);
        _ = new Slot();
        _ = new Slot(() => { });
        _ = new Slot(EmptyDisposable.Instance);
        _ = new Pocket();
        _ = new Pocket(EmptyDisposable.Instance, EmptyDisposable.Instance);
        _ = new Pocket(EmptyDisposable.Instance, EmptyDisposable.Instance, EmptyDisposable.Instance);
    }

    /// <summary>Covers internal witness implementations and safe observer terminal behavior.</summary>
    [Test]
    public void WitnessesCoverDisposedThrowEmptyAndSafeBranches()
    {
        Assert.Throws<ObjectDisposedException>(() => DisposedWitness<int>.Instance.OnNext(One));
        Assert.Throws<ObjectDisposedException>(DisposedWitness<int>.Instance.OnCompleted);
        Assert.Throws<ObjectDisposedException>(() => DisposedWitness<int>.Instance.OnError(new InvalidOperationException("disposed")));

        ThrowWitness<int>.Instance.OnNext(One);
        ThrowWitness<int>.Instance.OnCompleted();
        Assert.Throws<InvalidOperationException>(() => ThrowWitness<int>.Instance.OnError(new InvalidOperationException("throw")));

        var values = new List<int>();
        var errors = new List<string>();
        var completed = 0;
        EmptyWitness<int>.Instance.OnNext(One);
        new EmptyWitness<int>(values.Add).OnNext(Two);
        new EmptyWitness<int>(values.Add, ex => errors.Add(ex.Message)).OnError(new InvalidOperationException("handled"));
        new EmptyWitness<int>(values.Add, () => completed++).OnCompleted();
        new EmptyWitness<int>(values.Add, ex => errors.Add(ex.Message), () => completed++).OnCompleted();
        Assert.Throws<InvalidOperationException>(() => new EmptyWitness<int>(values.Add).OnError(new InvalidOperationException("rethrown")));

        Assert.Equal(ExpectedTwoOnly, values);
        Assert.Equal(ExpectedHandledErrors, errors);
        Assert.Equal(Two, completed);

        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, (Action)null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Create<int>(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Witness.Safe(Witness.Create<int>(_ => { }), null!));

        var events = new List<string>();
        var cancelDisposed = 0;
        var safe = Witness.Safe(
            Witness.Create<int>(
                value => events.Add("next:" + value),
                ex => events.Add("error:" + ex.Message),
                () => events.Add("completed")),
            new ActionDisposable(() => cancelDisposed++));
        safe.OnNext(Three);
        safe.OnCompleted();
        safe.OnCompleted();
        safe.OnNext(Four);
        safe.OnError(new InvalidOperationException("late"));
        Assert.Equal(ExpectedSafeEvents, events);
        Assert.Equal(1, cancelDisposed);

        var throwingCancel = 0;
        var throwing = Witness.Safe(
            Witness.Create<int>(_ => throw new InvalidOperationException("next-failed")),
            new ActionDisposable(() => throwingCancel++));
        Assert.Throws<InvalidOperationException>(() => throwing.OnNext(One));
        throwing.OnNext(Two);
        Assert.Equal(1, throwingCancel);
        Assert.Throws<ArgumentNullException>(() => safe.OnError(null!));
    }

    /// <summary>Covers priority queues, sequencer queues, and scheduled item comparison and disposal paths.</summary>
    [Test]
    public void QueuesAndScheduledItemsCoverOrderingComparisonAndDisposalBranches()
    {
        var queue = new PriorityQueue<int>(Two);
        queue.Enqueue(Three);
        queue.Enqueue(One);
        queue.Enqueue(Two);
        Assert.Equal(One, queue.Peek());
        Assert.True(queue.Remove(Two));
        Assert.False(queue.Remove(Four));
        Assert.Equal(One, queue.Dequeue());
        Assert.Equal(Three, queue.Dequeue());
        Assert.Throws<InvalidOperationException>(() => queue.Peek());

        var shrinkQueue = new PriorityQueue<int>(Eight);
        for (var i = 0; i < Eight; i++)
        {
            shrinkQueue.Enqueue(i);
        }

        for (var i = 0; i < Seven; i++)
        {
            Assert.Equal(i, shrinkQueue.Dequeue());
        }

        Assert.Equal(Seven, shrinkQueue.Dequeue());
        Assert.Throws<ArgumentOutOfRangeException>(CreateInvalidSequencerQueue);

        var invoked = new List<string>();
        var disposed = 0;
        var first = new ScheduledItem<int, string>(
            Sequencer.Immediate,
            "first",
            (_, state) =>
            {
                invoked.Add(state);
                return new ActionDisposable(() => disposed++);
            },
            One);
        var second = new ScheduledItem<int, string>(Sequencer.Immediate, "second", (_, _) => EmptyDisposable.Instance, Two);
        var equalDue = new ScheduledItem<int, string>(Sequencer.Immediate, "equal", (_, _) => EmptyDisposable.Instance, One);

        Assert.True(first < second);
        Assert.True(first <= equalDue);
        Assert.True(second > first);
        Assert.True(second >= first);
        Assert.False(first == second);
        Assert.True(first != second);
        Assert.False(first.Equals(second));
        Assert.Equal(One, first.CompareTo(null));
        Assert.Throws<ArgumentException>(() => CompareScheduledItemWithInvalidObject(first));

        var sequencerQueue = new SequencerQueue<int>(Two);
        sequencerQueue.Enqueue(second);
        sequencerQueue.Enqueue(first);
        Assert.Same(first, sequencerQueue.Peek());
        Assert.True(sequencerQueue.Remove(second));
        Assert.Same(first, sequencerQueue.Dequeue());

        first.Invoke();
        first.Invoke();
        first.Dispose();
        first.Dispose();
        Assert.Equal(ExpectedRepeatedScheduledItemInvocations, invoked);
        Assert.Equal(Two, disposed);

        var cancelled = new ScheduledItem<int, string>(
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
        Assert.DoesNotContain("cancelled", invoked);
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutSequencer);
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutAction);
        Assert.Throws<ArgumentNullException>(CreateScheduledItemWithoutComparer);

        var defaultClock = new TestClock();
        var initialClock = new TestClock(DateTimeOffset.UnixEpoch);
        Assert.Equal(DateTimeOffset.MinValue, defaultClock.Now);
        Assert.Equal(DateTimeOffset.UnixEpoch, initialClock.Now);
    }

    /// <summary>Covers enumerable signal fast paths for arrays, read-only lists, iterators, and delegate subscriptions.</summary>
    [Test]
    public void FromEnumerableSignalCoversAllSynchronousFastPaths()
    {
        var arrayObserver = new RecordingWitness<int>();
        var arraySignal = new FromEnumerableSignal<int>([One, Two]);
        Assert.False(arraySignal.IsRequiredSubscribeOnCurrentThread());
        arraySignal.Subscribe(arrayObserver).Dispose();
        Assert.Equal(ExpectedOneTwo, arrayObserver.Values);
        Assert.Equal(1, arrayObserver.Completed);

        var listValues = new List<int>();
        var listCompleted = 0;
        new FromEnumerableSignal<int>([Three, Four]).Subscribe(
            listValues.Add,
            ex => throw ex,
            () => listCompleted++).Dispose();
        Assert.Equal(ExpectedThreeFour, listValues);
        Assert.Equal(1, listCompleted);

        var iteratorObserver = new RecordingWitness<int>();
        new FromEnumerableSignal<int>(YieldValues()).Subscribe(iteratorObserver).Dispose();
        Assert.Equal(ExpectedFiveSix, iteratorObserver.Values);
        Assert.Equal(1, iteratorObserver.Completed);

        var iteratorValues = new List<int>();
        var iteratorCompleted = 0;
        new FromEnumerableSignal<int>(YieldValues()).Subscribe(
            iteratorValues.Add,
            ex => throw ex,
            () => iteratorCompleted++).Dispose();
        Assert.Equal(ExpectedFiveSix, iteratorValues);
        Assert.Equal(1, iteratorCompleted);

        Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe(null!, ex => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe(_ => { }, ex => { }, null!));
    }

    /// <summary>Covers immediate and background sequencer argument validation and execution paths.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task SequencersCoverValidationAndExecutionBranches()
    {
        Assert.Same(ImmediateSequencer.Instance, Sequencer.Immediate);
        Assert.Same(TaskPoolSequencer.Instance, TaskPoolSequencer.Default);
        Assert.Same(TaskPoolSequencer.Default, Sequencer.Default);
        Assert.True(Sequencer.Immediate.Now > DateTimeOffset.MinValue);
        Assert.Equal(TimeSpan.Zero, Sequencer.Normalize(TimeSpan.FromTicks(NegativeOne)));

        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => Sequencer.Immediate.Schedule(One, TimeSpan.Zero, null!));

        var immediateValues = new List<int>();
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
        Assert.Equal(ExpectedImmediateValues, immediateValues);

        Assert.Throws<ArgumentNullException>(() => TaskPoolSequencer.Instance.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskPoolSequencer.Instance.Schedule(One, TimeSpan.Zero, null!));

        var taskPoolCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var taskPoolSubscription = TaskPoolSequencer.Instance.Schedule(Seven, (_, _) =>
        {
            taskPoolCompletion.SetResult();
            return EmptyDisposable.Instance;
        });
        await WaitForAsync(taskPoolCompletion.Task);

        var threadPoolCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var threadPoolSubscription = ThreadPoolSequencer.Instance.Schedule(Eight, TimeSpan.Zero, (_, _) =>
        {
            threadPoolCompletion.SetResult();
            return EmptyDisposable.Instance;
        });
        await WaitForAsync(threadPoolCompletion.Task);

        Assert.Throws<ArgumentNullException>(() => ThreadPoolSequencer.Instance.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => ThreadPoolSequencer.Instance.Schedule(One, TimeSpan.Zero, null!));

        var synchronizationContext = new ImmediateSynchronizationContext();
        Assert.Throws<ArgumentNullException>(CreateSynchronizationContextSequencerWithoutContext);
        var previousContext = SynchronizationContext.Current;
        try
        {
            SynchronizationContext.SetSynchronizationContext(synchronizationContext);
            Assert.Same(synchronizationContext, SynchronizationContextSequencer.Current.Context);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        var synchronizationSequencer = new SynchronizationContextSequencer(synchronizationContext);
        Assert.True(synchronizationSequencer.Now > DateTimeOffset.MinValue);
        Assert.Throws<ArgumentNullException>(() => synchronizationSequencer.Schedule(One, null!));
        Assert.Throws<ArgumentNullException>(() => synchronizationSequencer.Schedule(One, TimeSpan.Zero, null!));

        var synchronizationValues = new List<int>();
        using var synchronizationSubscription = synchronizationSequencer.Schedule(One, (_, state) =>
        {
            synchronizationValues.Add(state);
            return EmptyDisposable.Instance;
        });

        var delayedSynchronizationCompletion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var delayedSynchronizationSubscription = synchronizationSequencer.Schedule(Two, TimeSpan.Zero, (_, state) =>
        {
            delayedSynchronizationCompletion.TrySetResult(state);
            return EmptyDisposable.Instance;
        });
        var delayedValue = await delayedSynchronizationCompletion.Task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds));
        Assert.Equal(ExpectedOneTwo, (IEnumerable<int>)[.. synchronizationValues, delayedValue]);
    }

    /// <summary>Covers virtual-time extension validation and action scheduling.</summary>
    [Test]
    public void VirtualTimeSequencerExtensionsValidateAndRunActions()
    {
        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var invoked = 0;

        Assert.Throws<ArgumentNullException>(() => VirtualTimeSequencerExtensions.ScheduleRelative<DateTimeOffset, TimeSpan>(null!, TimeSpan.Zero, () => { }));
        Assert.Throws<ArgumentNullException>(() => clock.ScheduleRelative(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => VirtualTimeSequencerExtensions.ScheduleAbsolute<DateTimeOffset, TimeSpan>(null!, DateTimeOffset.UnixEpoch, () => { }));
        Assert.Throws<ArgumentNullException>(() => clock.ScheduleAbsolute(DateTimeOffset.UnixEpoch, null!));

        clock.ScheduleRelative(TimeSpan.FromTicks(One), () => invoked += One);
        clock.ScheduleAbsolute(DateTimeOffset.UnixEpoch.AddTicks(Two), () => invoked += Two);

        clock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.Equal(One, invoked);

        clock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.Equal(Three, invoked);
    }

    /// <summary>Creates an iterator-backed enumerable for the non-indexable enumerable path.</summary>
    /// <returns>The yielded values.</returns>
    private static IEnumerable<int> YieldValues()
    {
        yield return Five;
        yield return Six;
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
        _ = new ScheduledItem<int, string>(Sequencer.Immediate, "x", (_, _) => EmptyDisposable.Instance, One, null!);

    /// <summary>Creates a synchronization-context sequencer without a context.</summary>
    private static void CreateSynchronizationContextSequencerWithoutContext() =>
        _ = new SynchronizationContextSequencer(null!);

    /// <summary>Compares a scheduled item through the non-generic comparable interface.</summary>
    /// <param name="item">The scheduled item.</param>
    private static void CompareScheduledItemWithInvalidObject(ScheduledItem<int, string> item) =>
        item.CompareTo("bad");

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

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedSingleDisposable : SingleDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedSingleDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedSingleDisposable(IDisposable disposable)
            : base(disposable)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        public void DisposeFalse() => Dispose(false);
    }

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedSingleReplaceableDisposable : SingleReplaceableDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedSingleReplaceableDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedSingleReplaceableDisposable(IDisposable disposable)
            : base(disposable)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        public void DisposeFalse() => Dispose(false);
    }

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedMultipleDisposable : MultipleDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedMultipleDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedMultipleDisposable(IDisposable disposable)
            : base(disposable, EmptyDisposable.Instance)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        public void DisposeFalse() => Dispose(false);
    }

    /// <summary>Synchronization context that runs posted work immediately.</summary>
    private sealed class ImmediateSynchronizationContext : SynchronizationContext
    {
        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => d(state);
    }

    /// <summary>Records observer values and terminal signals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the completion count.</summary>
        public int Completed { get; private set; }

        /// <summary>Gets the observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
