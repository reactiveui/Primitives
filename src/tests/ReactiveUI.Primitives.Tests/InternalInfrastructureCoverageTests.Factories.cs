// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal infrastructure coverage.</summary>
public partial class InternalInfrastructureCoverageTests
{
    /// <summary>Covers factory scheduling, task continuations, and timer aliases with deterministic time.</summary>
    /// <returns>A task that completes when asynchronous continuations are observed.</returns>
    [Test]
    public async Task FactoryAliasesScheduledRangesTasksAndTimersCoverRemainderBranches()
    {
        var rangeValues = new List<int>();
        var repeatValues = new List<string>();
        var repeatCountValues = new List<int>();
        var startValues = new List<int>();
        var startActions = 0;
        var taskValues = new List<int>();
        var taskErrors = new List<string>();
        var afterValues = new List<long>();
        var everyValues = new List<long>();
        var timerDateValues = new List<long>();
        var timerPeriodicValues = new List<long>();
        var clock = new TestClock(DateTimeOffset.UnixEpoch);

        Signal.Sequence(Three, Three, Sequencer.CurrentThread).Subscribe(rangeValues.Add);
        Signal.Loop("r").Take(Three).Subscribe(repeatValues.Add);
        Signal.Loop(Five, Two).Subscribe(repeatCountValues.Add);
        Signal.Start(() => Seven, Sequencer.CurrentThread).Subscribe(startValues.Add);
        Signal.Start(() => startActions++, Sequencer.CurrentThread).Subscribe(_ => { });

        Signal.FromTask(Task.FromResult(Four)).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        Signal.FromTask(Task.FromException<int>(new InvalidOperationException("task-fault"))).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        Signal.FromTask(Task.FromCanceled<int>(new(true))).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        await SpinUntil(() => taskValues.Count == One && taskErrors.Count == Two, TimeSpan.FromSeconds(TimeoutSeconds));

        using var disposedTaskSubscription = Signal.FromTask(Task.FromResult(NinetyNine)).Subscribe(_ => taskValues.Add(NinetyNine));
        disposedTaskSubscription.Dispose();

        Signal.After(TimeSpan.FromTicks(Two), clock).Subscribe(afterValues.Add);
        Signal.Every(TimeSpan.FromTicks(Two), clock).Take(Three).Subscribe(everyValues.Add);
        Signal.After(DateTimeOffset.UnixEpoch.AddTicks(Three), clock).Subscribe(timerDateValues.Add);
        Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerPeriodicValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));

        Assert.Equal(ExpectedThreeToFive, rangeValues);
        Assert.Equal(ExpectedRepeatValues, repeatValues);
        Assert.Equal(ExpectedFiveFive, repeatCountValues);
        Assert.Equal(ExpectedSingleSeven, startValues);
        Assert.Equal(1, startActions);
        Assert.Contains(Four, taskValues);
        Assert.Equal(ExpectedTaskErrorNames, taskErrors);
        Assert.Equal(ExpectedSingleZeroTick, afterValues);
        Assert.Equal(ExpectedZeroToTwoTicks, everyValues);
        Assert.Equal(ExpectedSingleZeroTick, timerDateValues);
        Assert.Equal(ExpectedZeroToTwoTicks, timerPeriodicValues);

        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, Two, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Sequence(One, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Loop(One, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start(() => One, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start((Action)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start(() => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Every(TimeSpan.FromTicks(One), null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, TimeSpan.FromTicks(One), null!));
    }

    /// <summary>Covers task-signal cancellation registration and disposal branches.</summary>
    [Test]
    public void TaskSignalCoversCancellationAndDisposeBranches()
    {
        var canceled = new List<Exception>();
        using var cts = new CancellationTokenSource();
        var taskSignal = new TaskSignal<int>(_ => Signal.Silent<int>(), Sequencer.CurrentThread, cts);
        taskSignal.GetOperationCanceled(Witness.Create<Exception>(canceled.Add));
        Assert.False(taskSignal.IsCancellationRequested);
        taskSignal.Dispose();
        taskSignal.Dispose();
        Assert.True(taskSignal.IsDisposed);
        Assert.True(taskSignal.IsCancellationRequested);
        Assert.Equal(1, canceled.Count);

        Assert.Throws<ArgumentNullException>(() => _ = new TaskSignal<int>(null!));
    }

    /// <summary>Covers non-completed task factory continuations for success, fault, cancellation, and disposed subscriptions.</summary>
    /// <returns>A task that completes when all continuations have been observed.</returns>
    [Test]
    public async Task TaskFactoryContinuationsCoverPendingTaskBranches()
    {
        var values = new ConcurrentQueue<int>();
        var errors = new ConcurrentQueue<string>();
        void AddValue(int value) => values.Enqueue(value);

        void AddError(Exception error) => errors.Enqueue(error.GetType().Name);

        bool ObservedPendingBranches()
        {
            var observedValues = values.ToArray();
            var observedErrors = errors.ToArray();
            return Array.IndexOf(observedValues, Seven) >= 0
                && Array.IndexOf(observedErrors, nameof(InvalidOperationException)) >= 0
                && Array.IndexOf(observedErrors, nameof(TaskCanceledException)) >= 0;
        }

        var success = new TaskCompletionSource<int>();
        var fault = new TaskCompletionSource<int>();
        var canceled = new TaskCompletionSource<int>();
        var disposed = new TaskCompletionSource<int>();
        var disposedSubscription = Signal.FromTask(disposed.Task).Subscribe(_ => AddValue(NinetyNine), AddError);
        disposedSubscription.Dispose();

        Signal.FromTask(success.Task).Subscribe(AddValue, AddError);
        Signal.FromTask(fault.Task).Subscribe(AddValue, AddError);
        Signal.FromTask(canceled.Task).Subscribe(AddValue, AddError);
        success.SetResult(Seven);
        fault.SetException(new InvalidOperationException("pending-fault"));
        canceled.SetCanceled(new(true));
        disposed.SetResult(NinetyNine);

        await SpinUntil(ObservedPendingBranches, TimeSpan.FromSeconds(TimeoutSeconds)).ConfigureAwait(false);
        var finalValues = values.ToArray();
        var finalErrors = errors.ToArray();
        Assert.Equal(1, finalValues.Length);
        Assert.Equal(Seven, finalValues[0]);
        Assert.Contains(nameof(InvalidOperationException), finalErrors);
        Assert.Contains(nameof(TaskCanceledException), finalErrors);
    }

    /// <summary>Covers small value/factory/inline branches with public surface behavior.</summary>
    [Test]
    public void ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        var sender = new object();
        var args = EventArgs.Empty;
        var pattern = new EventPattern<EventArgs>(sender, args);
        var same = new EventPattern<EventArgs>(sender, args);
        var other = new EventPattern<EventArgs>(new(), args);
        Assert.True(pattern == same);
        Assert.True(pattern != other);
        Assert.True(pattern.Equals((object)same));
        Assert.False(pattern.Equals("not an event"));
        Assert.NotEqual(0, pattern.GetHashCode());
        Assert.True(pattern.ToString().Contains(nameof(EventArgs), StringComparison.Ordinal));
        Assert.Throws<ArgumentNullException>(() => _ = new EventPattern<EventArgs>(sender, null!));

        var emptyScheduled = new List<int>();
        var emptyCompleted = 0;
        var emptyClock = new TestClock(DateTimeOffset.UnixEpoch);
        Signal.None<int>(emptyClock).Subscribe(emptyScheduled.Add, ex => throw ex, () => emptyCompleted++);
        Assert.Equal(0, emptyCompleted);
        emptyClock.Start();
        Assert.Equal(1, emptyCompleted);
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Subscribe((IObserver<int>)null!));

        var repeatValues = new List<int>();
        var repeatCompleted = 0;
        var repeat = Signal.Loop(Seven, Three);
        Assert.False(((IRequireCurrentThread<int>)repeat).IsRequiredSubscribeOnCurrentThread());
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)repeat).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, ex => throw ex, () => repeatCompleted++);
        Assert.Equal(ExpectedSevenSevenSeven, repeatValues);
        Assert.Equal(1, repeatCompleted);

        var zippedValues = new List<int>();
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), (left, right) => left + right);
        Assert.False(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread());
        Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)zipped).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, ex => throw ex, () => zippedCompleted++);
        Assert.Equal(ExpectedFiveSevenNine, zippedValues);
        Assert.Equal(1, zippedCompleted);

        var returned = new List<string>();
        var returnCompleted = 0;
        var returnClock = new TestClock(DateTimeOffset.UnixEpoch);
        Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        Assert.Equal(0, returnCompleted);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.Equal(ExpectedScheduledReturn, returned);
        Assert.Equal(1, returnCompleted);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));

        var mappedErrors = new List<string>();
        Signal.FromEnumerable([One, Two]).Map(value => value == One ? value : throw new InvalidOperationException("map-fault"))
            .Subscribe(_ => { }, ex => mappedErrors.Add(ex.Message));
        Assert.Equal(ExpectedMappedErrors, mappedErrors);
    }

    /// <summary>Covers low-level equality, scheduling, witness, and create/defer/throw observer defensive paths.</summary>
    [Test]
    public void LowLevelReflectionAndSchedulingPathsCoverRemainingBranches()
    {
        var left = new PriorityQueue<int>.IndexedItem { Id = 1L, Value = One };
        var right = new PriorityQueue<int>.IndexedItem { Id = 1L, Value = One };
        Assert.True(left.Equals(right));
        Assert.True(left.Equals((object)right));
        Assert.False(left.Equals("not-item"));
        Assert.NotEqual(0, left.GetHashCode());

        var scheduledDisposed = false;
        var scheduled = new ScheduledProbe(One, () => new ActionDisposable(() => scheduledDisposed = true));
        Assert.Equal(1, scheduled.CompareTo(null));
        Assert.Equal(0, scheduled.CompareTo(new ScheduledProbe(One, () => EmptyDisposable.Instance)));
        Assert.Throws<ArgumentException>(() => scheduled.CompareTo("not-scheduled"));
        Assert.True(scheduled.Equals((object)scheduled));
        Assert.False(scheduled.Equals(new()));
        Assert.NotEqual(0, scheduled.GetHashCode());
        scheduled.Invoke();
        scheduled.Cancel();
        Assert.True(scheduledDisposed);

        var cancelDisposed = false;
        Witness.SafeWitness<int> safe = new(
            new ThrowingWitness<int>(throwOnError: true),
            new ActionDisposable(() => cancelDisposed = true));
        Assert.Throws<InvalidOperationException>(() => safe.OnError(new InvalidOperationException("safe")));
        Assert.True(cancelDisposed);
        safe.OnError(new InvalidOperationException("ignored"));

        var createErrors = new RecordingWitness<int>();
        Signal.CreateWithState<int, int>(
            0,
            static (_, observer) =>
            {
                observer.OnError(new InvalidOperationException("create-error"));
                return null!;
            }).Subscribe(createErrors).Dispose();
        Assert.Equal("create-error", createErrors.Errors[0].Message);

        var deferErrors = new RecordingWitness<int>();
        Signal.Lazy<int>(() => throw new InvalidOperationException("defer-factory")).Subscribe(deferErrors).Dispose();
        Assert.Equal("defer-factory", deferErrors.Errors[0].Message);

        var immediateThrow = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("immediate-throw"), Sequencer.Immediate).Subscribe(immediateThrow).Dispose();
        Assert.Equal("immediate-throw", immediateThrow.Errors[0].Message);
    }
}
