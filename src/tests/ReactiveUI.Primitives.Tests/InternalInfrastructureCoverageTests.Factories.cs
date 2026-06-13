// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103 // Coverage tests intentionally group branch-heavy scenarios.

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
        Signal.Start(() => startActions++, Sequencer.CurrentThread).Subscribe(_ =>
        {
        });
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
        await Assert.That(rangeValues.SequenceEqual(ExpectedThreeToFive)).IsTrue();
        await Assert.That(repeatValues.SequenceEqual(ExpectedRepeatValues)).IsTrue();
        await Assert.That(repeatCountValues.SequenceEqual(ExpectedFiveFive)).IsTrue();
        await Assert.That(startValues.SequenceEqual(ExpectedSingleSeven)).IsTrue();
        await Assert.That(startActions).IsEqualTo(1);
        await Assert.That(taskValues).Contains(Four);
        await Assert.That(taskErrors.SequenceEqual(ExpectedTaskErrorNames)).IsTrue();
        await Assert.That(afterValues.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(everyValues.SequenceEqual(ExpectedZeroToTwoTicks)).IsTrue();
        await Assert.That(timerDateValues.SequenceEqual(ExpectedSingleZeroTick)).IsTrue();
        await Assert.That(timerPeriodicValues.SequenceEqual(ExpectedZeroToTwoTicks)).IsTrue();
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
        Assert.Throws<ArgumentNullException>(() => Signal.Start(
            () =>
{
},
            null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Every(TimeSpan.FromTicks(One), null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, TimeSpan.FromTicks(One), null!));
    }

    /// <summary>Covers task-signal cancellation registration and disposal branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskSignalCoversCancellationAndDisposeBranches()
    {
        var canceled = new List<Exception>();
        using var cts = new CancellationTokenSource();
        var taskSignal = new TaskSignal<int>(_ => Signal.Silent<int>(), Sequencer.CurrentThread, cts);
        taskSignal.GetOperationCanceled(Witness.Create<Exception>(canceled.Add));
        await Assert.That(taskSignal.IsCancellationRequested).IsFalse();
        taskSignal.Dispose();
        taskSignal.Dispose();
        await Assert.That(taskSignal.IsDisposed).IsTrue();
        await Assert.That(taskSignal.IsCancellationRequested).IsTrue();
        await Assert.That(canceled.Count).IsEqualTo(1);
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
            return Array.IndexOf(observedValues, Seven) >= 0 && Array.IndexOf(observedErrors, nameof(InvalidOperationException)) >= 0 && Array.IndexOf(observedErrors, nameof(TaskCanceledException)) >= 0;
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
        await Assert.That(finalValues.Length).IsEqualTo(1);
        await Assert.That(finalValues[0]).IsEqualTo(Seven);
        await Assert.That(finalErrors).Contains(nameof(InvalidOperationException));
        await Assert.That(finalErrors).Contains(nameof(TaskCanceledException));
    }

    /// <summary>Covers small value/factory/inline branches with public surface behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        var sender = new object();
        var args = EventArgs.Empty;
        var pattern = new EventPattern<EventArgs>(sender, args);
        var same = new EventPattern<EventArgs>(sender, args);
        var other = new EventPattern<EventArgs>(new(), args);
        await Assert.That(pattern == same).IsTrue();
        await Assert.That(pattern != other).IsTrue();
        await Assert.That(pattern.Equals((object)same)).IsTrue();
        await Assert.That(pattern.Equals("not an event")).IsFalse();
        await Assert.That(pattern.GetHashCode()).IsNotEqualTo(0);
        await Assert.That(pattern.ToString().Contains(nameof(EventArgs), StringComparison.Ordinal)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => _ = new EventPattern<EventArgs>(sender, null!));
        var emptyScheduled = new List<int>();
        var emptyCompleted = 0;
        var emptyClock = new TestClock(DateTimeOffset.UnixEpoch);
        Signal.None<int>(emptyClock).Subscribe(emptyScheduled.Add, ex => throw ex, () => emptyCompleted++);
        await Assert.That(emptyCompleted).IsEqualTo(0);
        emptyClock.Start();
        await Assert.That(emptyCompleted).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Subscribe((IObserver<int>)null!));
        var repeatValues = new List<int>();
        var repeatCompleted = 0;
        var repeat = Signal.Loop(Seven, Three);
        await Assert.That(((IRequireCurrentThread<int>)repeat).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)repeat).Subscribe(
            null!,
            _ =>
{
},
            () =>
{
}));
        ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, ex => throw ex, () => repeatCompleted++);
        await Assert.That(repeatValues.SequenceEqual(ExpectedSevenSevenSeven)).IsTrue();
        await Assert.That(repeatCompleted).IsEqualTo(1);
        var zippedValues = new List<int>();
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), (left, right) => left + right);
        await Assert.That(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)zipped).Subscribe(
            null!,
            _ =>
{
},
            () =>
{
}));
        ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, ex => throw ex, () => zippedCompleted++);
        await Assert.That(zippedValues.SequenceEqual(ExpectedFiveSevenNine)).IsTrue();
        await Assert.That(zippedCompleted).IsEqualTo(1);
        var returned = new List<string>();
        var returnCompleted = 0;
        var returnClock = new TestClock(DateTimeOffset.UnixEpoch);
        Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        await Assert.That(returnCompleted).IsEqualTo(0);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(returned.SequenceEqual(ExpectedScheduledReturn)).IsTrue();
        await Assert.That(returnCompleted).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));
        var mappedErrors = new List<string>();
        Signal.FromEnumerable([One, Two]).Map(value => value == One ? value : throw new InvalidOperationException("map-fault")).Subscribe(
            _ =>
        {
        },
            ex => mappedErrors.Add(ex.Message));
        await Assert.That(mappedErrors.SequenceEqual(ExpectedMappedErrors)).IsTrue();
    }

    /// <summary>Covers low-level equality, scheduling, witness, and create/defer/throw observer defensive paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LowLevelReflectionAndSchedulingPathsCoverRemainingBranches()
    {
        var left = new PriorityQueue<int>.IndexedItem
        {
            Id = 1L,
            Value = One
        };
        var right = new PriorityQueue<int>.IndexedItem
        {
            Id = 1L,
            Value = One
        };
        await Assert.That(left.Equals(right)).IsTrue();
        await Assert.That(left.Equals((object)right)).IsTrue();
        await Assert.That(left.Equals("not-item")).IsFalse();
        await Assert.That(left.GetHashCode()).IsNotEqualTo(0);
        var scheduledDisposed = false;
        var scheduled = new ScheduledProbe(One, () => new ActionDisposable(() => scheduledDisposed = true));
        await Assert.That(scheduled.CompareTo(null)).IsEqualTo(1);
        await Assert.That(scheduled.CompareTo(new ScheduledProbe(One, () => EmptyDisposable.Instance))).IsEqualTo(0);
        Assert.Throws<ArgumentException>(() => scheduled.CompareTo("not-scheduled"));
        await Assert.That(scheduled.Equals((object)scheduled)).IsTrue();
        await Assert.That(scheduled.Equals(new())).IsFalse();
        await Assert.That(scheduled.GetHashCode()).IsNotEqualTo(0);
        scheduled.Invoke();
        scheduled.Cancel();
        await Assert.That(scheduledDisposed).IsTrue();
        var cancelDisposed = false;
        Witness.SafeWitness<int> safe = new(new ThrowingWitness<int>(throwOnError: true), new ActionDisposable(() => cancelDisposed = true));
        Assert.Throws<InvalidOperationException>(() => safe.OnError(new InvalidOperationException("safe")));
        await Assert.That(cancelDisposed).IsTrue();
        safe.OnError(new InvalidOperationException("ignored"));
        var createErrors = new RecordingWitness<int>();
        Signal.CreateWithState<int, int>(0, static (_, observer) =>
        {
            observer.OnError(new InvalidOperationException("create-error"));
            return null!;
        }).Subscribe(createErrors).Dispose();
        await Assert.That(createErrors.Errors[0].Message).IsEqualTo("create-error");
        var deferErrors = new RecordingWitness<int>();
        Signal.Lazy<int>(() => throw new InvalidOperationException("defer-factory")).Subscribe(deferErrors).Dispose();
        await Assert.That(deferErrors.Errors[0].Message).IsEqualTo("defer-factory");
        var immediateThrow = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("immediate-throw"), Sequencer.Immediate).Subscribe(immediateThrow).Dispose();
        await Assert.That(immediateThrow.Errors[0].Message).IsEqualTo("immediate-throw");
    }
}
