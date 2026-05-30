// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

#pragma warning disable SA1600, S109, S3400, S6354, CA1806, S138, S3257, CA1861, IDE0300, RCS1196, S6966, S103, CA1065, S3011, RCS1151, RCS1208, RCGS0005, SA1116, SA1117

/// <summary>
/// Adds deterministic coverage for operator edge branches left after the broad contract suites.
/// </summary>
public class CoverageRemainderTests
{
    private const int One = 1;
    private const int Two = 2;
    private const int Three = 3;
    private const int Four = 4;
    private const int Five = 5;
    private const int Six = 6;
    private const int Seven = 7;
    private const int Nine = 9;
    private const int NinetyNine = 99;
    private const int TimeoutSeconds = 2;
    private const int PollDelayMilliseconds = 10;

    /// <summary>
    /// Covers parity operator overloads, aliases, and argument guards that are not hit by scenario tests.
    /// </summary>
    [Test]
    public void ParityOperatorAliasesAndGuardsCoverRemainingBranches()
    {
        IObservable<int> source = Signal.FromEnumerable([One, Two, Three, Four]);
        var values = new List<int>();
        var sideEffects = new List<string>();
        var completed = 0;
        var defaultValue = new List<int?>();
        var ignoreCompleted = 0;
        var takeWhile = new List<int>();
        var skipWhile = new List<int>();
        var distinctKeys = new List<string>();
        var isEmptyValues = new List<bool>();
        var listValues = new List<IList<int>>();
        var arrayValues = new List<int[]>();
        var rangeListValues = new List<IList<int>>();
        var rangeArrayValues = new List<int[]>();
        var forkJoinRange = new List<int>();

        source
            .Tap(
                value => sideEffects.Add("next:" + value),
                error => sideEffects.Add("error:" + error.Message),
                () => sideEffects.Add("completed"))
            .Subscribe(values.Add, ex => throw ex, () => completed++);
        Signal.Fail<int>(new InvalidOperationException("do-error"))
            .Tap(value => sideEffects.Add(value.ToString()), error => sideEffects.Add("error:" + error.Message), () => sideEffects.Add("unused"))
            .Subscribe(_ => { }, _ => { }, () => { });

        Signal.None<int?>().DefaultIfEmpty().Subscribe(defaultValue.Add);
        source.IgnoreValues().Subscribe(_ => values.Add(NinetyNine), ex => throw ex, () => ignoreCompleted++);
        Signal.FromEnumerable([One, Two, Three, Four]).TakeWhile(value => value < Three).Subscribe(takeWhile.Add);
        Signal.FromEnumerable([One, Two, Three, Four]).SkipWhile(value => value < Three).Subscribe(skipWhile.Add);
        Signal.FromEnumerable(["aa", "bb", "ccc", "dd", "e"])
            .UniqueBy(value => value.Length)
            .Subscribe(distinctKeys.Add);
        Signal.None<int>().IsEmpty().Subscribe(isEmptyValues.Add);
        Signal.Sequence(One, Three).IsEmpty().Subscribe(isEmptyValues.Add);
        source.CollectList().Subscribe(listValues.Add);
        source.CollectArray().Subscribe(arrayValues.Add);
        Signal.Sequence(Three, Three).CollectList().Subscribe(rangeListValues.Add);
        Signal.Sequence(Three, Three).CollectArray().Subscribe(rangeArrayValues.Add);
        Signal.Sequence(One, Four).ForkJoin(Signal.Sequence(Three, Three), (left, right) => left + right).Subscribe(forkJoinRange.Add);

        Assert.Equal(new[] { One, Two, Three, Four }, values);
        Assert.Equal(new[] { "next:1", "next:2", "next:3", "next:4", "completed", "error:do-error" }, sideEffects);
        Assert.Equal(1, completed);
        Assert.Equal(1, ignoreCompleted);
        Assert.Equal(new int?[] { null }, defaultValue);
        Assert.Equal(new[] { One, Two }, takeWhile);
        Assert.Equal(new[] { Three, Four }, skipWhile);
        Assert.Equal(new[] { "aa", "ccc", "dd", "e" }, distinctKeys);
        Assert.Equal(new[] { true, false }, isEmptyValues);
        Assert.Equal<int>([One, Two, Three, Four], listValues[0]);
        Assert.Equal<int>([One, Two, Three, Four], arrayValues[0]);
        Assert.Equal<int>([Three, Four, Five], rangeListValues[0]);
        Assert.Equal<int>([Three, Four, Five], rangeArrayValues[0]);
        Assert.Equal(new[] { Nine }, forkJoinRange);

        Assert.Throws<ArgumentNullException>(() => LinqMixins.Prepend<int>(null!, One, Two));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((int[])null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Prepend<int>(null!, (IEnumerable<int>)[One]));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((IEnumerable<int>)null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.ObserveOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.ObserveOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.SubscribeOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.SubscribeOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Tap<int>(null!, _ => { }, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.IgnoreValues<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.DistinctBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.UniqueBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.TakeWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.SkipWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.FlatMap<int, int>(null!, value => Signal.Emit(value)));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int, int>(null!, (outer, inner) => outer + inner));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap(value => Signal.Emit(value), (Func<int, int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Count<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.Count(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.LongCount<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.LongCount(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Any<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Any<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.Any(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.All<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Contains<int>(null!, One));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.DelayStart<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Calm<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Probe(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Timestamp<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.TimeInterval<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.ForkJoin<int, int, int>(null!, Signal.Emit(One), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.ForkJoin<int, int, int>(source, null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.ForkJoin<int, int, int>(source, Signal.Emit(One), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).AsObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable(CancellationToken.None));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Take(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Skip(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Reattempt(-1));
    }

    /// <summary>
    /// Covers operator Subscribe(null) and current-thread propagation for internal optimized signal classes.
    /// </summary>
    [Test]
    public void InternalOptimizedOperatorSignalsValidateObserversAndThreadRequirements()
    {
        IObservable<int>[] intSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).DistinctBy(value => value),
            Signal.FromEnumerable([One, Two, Three]).Count(),
            Signal.FromEnumerable([One, Two, Three]).Count(value => value > One),
        ];
        IObservable<long>[] longSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).LongCount(),
            Signal.FromEnumerable([One, Two, Three]).LongCount(value => value > One),
        ];
        IObservable<bool>[] boolSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).All(value => value > 0),
            Signal.FromEnumerable([One, Two, Three]).Contains(Two),
            Signal.FromEnumerable([One, Two, Three]).Any(),
            Signal.FromEnumerable([One, Two, Three]).Any(value => value > Two),
        ];

        for (var i = 0; i < intSignals.Length; i++)
        {
            var signal = intSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<int>)null!));
            if (signal is IRequireCurrentThread<int> required)
            {
                Assert.False(required.IsRequiredSubscribeOnCurrentThread());
            }
        }

        for (var i = 0; i < longSignals.Length; i++)
        {
            var signal = longSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<long>)null!));
            if (signal is IRequireCurrentThread<long> required)
            {
                Assert.False(required.IsRequiredSubscribeOnCurrentThread());
            }
        }

        for (var i = 0; i < boolSignals.Length; i++)
        {
            var signal = boolSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<bool>)null!));
            if (signal is IRequireCurrentThread<bool> required)
            {
                Assert.False(required.IsRequiredSubscribeOnCurrentThread());
            }
        }
    }

    /// <summary>
    /// Covers factory scheduling, task continuations, and timer aliases with deterministic time.
    /// </summary>
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
        Signal.FromTask(Task.FromCanceled<int>(new CancellationToken(true))).Subscribe(taskValues.Add, ex => taskErrors.Add(ex.GetType().Name));
        await SpinUntil(() => taskValues.Count == 1 && taskErrors.Count == 2, TimeSpan.FromSeconds(TimeoutSeconds));

        using var disposedTaskSubscription = Signal.FromTask(Task.FromResult(NinetyNine)).Subscribe(_ => taskValues.Add(NinetyNine));
        disposedTaskSubscription.Dispose();

        Signal.After(TimeSpan.FromTicks(Two), clock).Subscribe(afterValues.Add);
        Signal.Every(TimeSpan.FromTicks(Two), clock).Take(Three).Subscribe(everyValues.Add);
        Signal.After(DateTimeOffset.UnixEpoch.AddTicks(Three), clock).Subscribe(timerDateValues.Add);
        Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerPeriodicValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));

        Assert.Equal(new[] { Three, Four, Five }, rangeValues);
        Assert.Equal(new[] { "r", "r", "r" }, repeatValues);
        Assert.Equal(new[] { Five, Five }, repeatCountValues);
        Assert.Equal(new[] { Seven }, startValues);
        Assert.Equal(1, startActions);
        Assert.Contains(Four, taskValues);
        Assert.Equal(new[] { nameof(InvalidOperationException), nameof(TaskCanceledException) }, taskErrors);
        Assert.Equal(new long[] { 0L }, afterValues);
        Assert.Equal(new long[] { 0L, 1L, 2L }, everyValues);
        Assert.Equal(new long[] { 0L }, timerDateValues);
        Assert.Equal(new long[] { 0L, 1L, 2L }, timerPeriodicValues);

        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, Two, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Sequence(One, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Loop(One, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask<int>((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync<int>((Func<Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync<int>((Func<CancellationToken, Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start<int>(() => One, null!));
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

    /// <summary>
    /// Covers Signal and AsyncSignal subscriber churn, late subscriptions, disposal, and terminal no-op branches.
    /// </summary>
    [Test]
    public void SubjectsCoverMultipleSubscriberChurnLateTerminalsAndDisposalBranches()
    {
        var subject = new Signal<int>();
        var first = new RecordingObserver<int>();
        var second = new RecordingObserver<int>();
        var third = new RecordingObserver<int>();
        var fourth = new RecordingObserver<int>();
        var actionValues = new List<int>();
        using var action = subject.Subscribe(actionValues.Add);
        using var firstSubscription = subject.Subscribe(first);
        using var secondSubscription = subject.Subscribe(second);
        using var thirdSubscription = subject.Subscribe(third);
        using var fourthSubscription = subject.Subscribe(fourth);
        secondSubscription.Dispose();
        subject.OnNext(One);
        action.Dispose();
        subject.OnCompleted();
        subject.OnCompleted();
        subject.OnNext(Two);
        var lateCompleted = new RecordingObserver<int>();
        subject.Subscribe(lateCompleted).Dispose();

        Assert.Equal(new[] { One }, first.Values);
        Assert.Equal(1, first.Completed);
        Assert.Equal(0, second.Values.Count);
        Assert.Equal(new[] { One }, third.Values);
        Assert.Equal(new[] { One }, fourth.Values);
        Assert.Equal(new[] { One }, actionValues);
        Assert.Equal(1, lateCompleted.Completed);

        var faulted = new Signal<int>();
        var faultObserver = new RecordingObserver<int>();
        var actionFaults = 0;
        using var faultSubscription = faulted.Subscribe(faultObserver);
        var fault = new InvalidOperationException("fault");
        faulted.OnError(fault);
        faulted.OnError(new InvalidOperationException("late"));
        var lateFault = new RecordingObserver<int>();
        faulted.Subscribe(lateFault).Dispose();
        Assert.Same(fault, lateFault.Errors[0]);
        Assert.Equal(0, actionFaults);
        Assert.Throws<ArgumentNullException>(() => faulted.OnError(null!));

        var actionFaulted = new Signal<int>();
        using var faultingAction = actionFaulted.Subscribe(_ => actionFaults++);
        Assert.Throws<InvalidOperationException>(() => actionFaulted.OnError(new InvalidOperationException("action-fault")));

        var disposedSubject = new Signal<int>();
        disposedSubject.Dispose();
        disposedSubject.Dispose();
        Assert.Throws<ObjectDisposedException>(() => disposedSubject.Subscribe(new RecordingObserver<int>()));
        Assert.Throws<ObjectDisposedException>(() => disposedSubject.OnNext(One));

        var asyncSignal = new AsyncSignal<int>();
        Assert.Throws<InvalidOperationException>(() => _ = asyncSignal.Value);
        Assert.Throws<ArgumentNullException>(() => asyncSignal.OnCompleted(null!));
        Assert.Throws<ArgumentNullException>(() => asyncSignal.OnError(null!));
        var asyncFirst = new RecordingObserver<int>();
        var asyncSecond = new RecordingObserver<int>();
        using var asyncSubscription = asyncSignal.Subscribe(asyncFirst);
        using var asyncSecondSubscription = asyncSignal.Subscribe(asyncSecond);
        asyncSecondSubscription.Dispose();
        asyncSignal.OnNext(Five);
        asyncSignal.OnCompleted(() => actionFaults++);
        asyncSignal.OnCompleted();
        asyncSignal.OnCompleted();
        asyncSignal.OnNext(Six);
        var asyncLate = new RecordingObserver<int>();
        asyncSignal.Subscribe(asyncLate).Dispose();
        Assert.Equal(Five, asyncSignal.Value);
        Assert.Equal(Five, asyncSignal.GetResult());
        Assert.Equal(new[] { Five }, asyncFirst.Values);
        Assert.Equal(0, asyncSecond.Values.Count);
        Assert.Equal(new[] { Five }, asyncLate.Values);
        Assert.Equal(1, asyncLate.Completed);

        var asyncError = new AsyncSignal<int>();
        var asyncErrorObserver = new RecordingObserver<int>();
        asyncError.Subscribe(asyncErrorObserver).Dispose();
        var asyncFault = new InvalidOperationException("async-fault");
        asyncError.OnError(asyncFault);
        asyncError.OnError(new InvalidOperationException("late"));
        Assert.Throws<InvalidOperationException>(() => asyncError.GetResult());
        var asyncErrorLate = new RecordingObserver<int>();
        asyncError.Subscribe(asyncErrorLate).Dispose();
        Assert.Same(asyncFault, asyncErrorLate.Errors[0]);

        var disposedAsync = new AsyncSignal<int>();
        disposedAsync.Dispose();
        disposedAsync.Dispose();
        Assert.Throws<ObjectDisposedException>(() => disposedAsync.OnNext(One));
        Assert.Throws<ObjectDisposedException>(() => disposedAsync.Subscribe(new RecordingObserver<int>()));
    }

    /// <summary>
    /// Covers task-signal cancellation registration and disposal branches.
    /// </summary>
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

        Assert.Throws<ArgumentNullException>(() => new TaskSignal<int>(null!));
    }

    /// <summary>
    /// Covers remaining public alias, immutable-return, and virtual-time edge branches.
    /// </summary>
    [Test]
    public void AliasRangeImmutableAndVirtualTimeBranchesCoverRemainingEdges()
    {
        IObservable<int> source = Signal.FromEnumerable([Three, Four]);
        var startOne = new List<int>();
        var startMany = new List<int>();
        var delayed = new List<int>();
        var delayErrors = new List<string>();
        var timeoutErrors = new List<string>();
        var clock = new TestClock(DateTimeOffset.UnixEpoch);

        source.Prepend(Two).Subscribe(startOne.Add);
        source.Prepend((IEnumerable<int>)[One, Two]).Subscribe(startMany.Add);
        Assert.Same(source, source.ObserveOn(Sequencer.Immediate));
        var range = Signal.Sequence(One, Three);
        Assert.Same(range, range.DefaultIfEmpty(NinetyNine));
        Assert.NotNull(source.Shift(TimeSpan.Zero));
        Assert.NotNull(source.Expire(TimeSpan.FromTicks(One)));
        source.DelayStart(TimeSpan.FromTicks(Two), clock).Subscribe(delayed.Add);
        Signal.Fail<int>(new InvalidOperationException("delay-error")).Shift(TimeSpan.FromTicks(Two), clock).Subscribe(_ => { }, ex => delayErrors.Add(ex.Message));
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(Three), clock).Subscribe(_ => { }, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Three));

        Assert.Equal(new[] { Two, Three, Four }, startOne);
        Assert.Equal(new[] { One, Two, Three, Four }, startMany);
        Assert.Equal(new[] { Three, Four }, delayed);
        Assert.Equal(new[] { "delay-error" }, delayErrors);
        Assert.Equal(new[] { nameof(TimeoutException) }, timeoutErrors);

        var trueValues = new List<bool>();
        var falseValues = new List<bool>();
        var rxVoidValues = new List<RxVoid>();
        var inlineCompleted = 0;
        var trueSignal = Signal.Emit(true);
        var falseSignal = Signal.Emit(false);
        var rxVoidSignal = Signal.EmitRxVoid();
        trueSignal.Subscribe(new RecordingObserver<bool>());
        falseSignal.Subscribe(new RecordingObserver<bool>());
        rxVoidSignal.Subscribe(new RecordingObserver<RxVoid>());
        trueSignal.Subscribe(trueValues.Add, _ => { }, () => inlineCompleted++);
        falseSignal.Subscribe(falseValues.Add, _ => { }, () => inlineCompleted++);
        rxVoidSignal.Subscribe(rxVoidValues.Add, _ => { }, () => inlineCompleted++);
        Assert.False(((IRequireCurrentThread<bool>)trueSignal).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<bool>)falseSignal).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<RxVoid>)rxVoidSignal).IsRequiredSubscribeOnCurrentThread());
        Assert.Equal(new[] { true }, trueValues);
        Assert.Equal(new[] { false }, falseValues);
        Assert.Equal(1, rxVoidValues.Count);
        Assert.Equal(3, inlineCompleted);

        var virtualClock = new MinimalVirtualClock();
        var scheduled = new List<int>();
        Assert.Throws<ArgumentNullException>(() => new MinimalVirtualClock(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceBy(-1));
        virtualClock.AdvanceBy(0);
        Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceTo(-1));
        virtualClock.AdvanceTo(0);
        Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, (Func<ISequencer, int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.ScheduleRelative(One, 0, null!));
        virtualClock.Schedule(Seven, DateTimeOffset.UnixEpoch.AddTicks(Three), (_, state) =>
        {
            scheduled.Add(state);
            return Disposable.Empty;
        });
        virtualClock.AdvanceTo(Three);
        Assert.Equal(new[] { Seven }, scheduled);
    }

    /// <summary>
    /// Covers internal broadcaster copy-on-write branches, signal action late-terminal branches, and buffer disposal/error branches.
    /// </summary>
    [Test]
    public void InternalInfrastructureBranchesCoverObserverChurnAndTerminalEdges()
    {
        Broadcaster<int> broadcaster = default;
        var first = new RecordingObserver<int>();
        var second = new RecordingObserver<int>();
        var third = new RecordingObserver<int>();
        var fourth = new RecordingObserver<int>();
        var missing = new RecordingObserver<int>();
        broadcaster.Add(first);
        broadcaster.Add(second);
        broadcaster.Add(third);
        broadcaster.Add(fourth);
        Assert.True(broadcaster.HasObservers);
        broadcaster.Remove(missing);
        broadcaster.Remove(second);
        broadcaster.Next(One);
        broadcaster.Error(new InvalidOperationException("broadcast"));
        broadcaster.Completed();
        var copy = broadcaster;
        Assert.True(broadcaster.Equals(copy));
        Assert.True(broadcaster.Equals((object)copy));
        Assert.False(broadcaster.Equals("not a broadcaster"));
        Assert.NotEqual(0, broadcaster.GetHashCode());
        broadcaster.Clear();
        Assert.False(broadcaster.HasObservers);

        Assert.Equal(new[] { One }, first.Values);
        Assert.Equal(0, second.Values.Count);
        Assert.Equal(new[] { One }, third.Values);
        Assert.Equal(new[] { One }, fourth.Values);
        Assert.Equal(1, first.Errors.Count);
        Assert.Equal(1, third.Completed);
        Assert.Equal(1, fourth.Completed);

        var completedSignal = new Signal<int>();
        completedSignal.OnCompleted();
        completedSignal.Subscribe(_ => { }).Dispose();
        var failedSignal = new Signal<int>();
        failedSignal.OnError(new InvalidOperationException("late action"));
        Assert.Throws<InvalidOperationException>(() => failedSignal.Subscribe(_ => { }).Dispose());

        var source = new Signal<int>();
        var buffers = new List<IList<int>>();
        using (source.Buffer(Three, Two).Subscribe(buffers.Add))
        {
            source.OnNext(One);
            source.OnNext(Two);

            // The window (size 3) is incomplete; completion flushes the partial trailing window.
            source.OnCompleted();
        }

        Assert.Equal(1, buffers.Count);
        Assert.Equal<int>(new[] { One, Two }, buffers[0]);

        var errorSource = new Signal<int>();
        var bufferError = false;
        using (errorSource.Buffer(Two, One).Subscribe(_ => { }, _ => bufferError = true, () => { }))
        {
            errorSource.OnError(new InvalidOperationException("buffer-error"));
        }

        Assert.True(bufferError);
    }

    /// <summary>
    /// Covers observer exception paths and typed catch/finally branches with deterministic synchronous sources.
    /// </summary>
    [Test]
    public void ObserverExceptionCatchFinallyAndTerminalPredicateBranchesCoverRemainders()
    {
        var keepErrors = new List<string>();
        var allErrors = new List<string>();
        var distinctErrors = new List<string>();
        var catchValues = new List<int>();
        var catchErrors = new List<string>();
        var finallyCalls = 0;

        Signal.FromEnumerable([One, Two]).Keep(value => value == One ? true : throw new InvalidOperationException("keep-predicate"))
            .Subscribe(_ => { }, ex => keepErrors.Add(ex.Message));
        Signal.FromEnumerable([One, Two]).All(value => value == One ? true : throw new InvalidOperationException("all-predicate"))
            .Subscribe(_ => { }, ex => allErrors.Add(ex.Message));
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable(["a", "bb"])
            .DistinctBy(value => value.Length == 1 ? value.Length : throw new InvalidOperationException("distinct-key"))
            .Subscribe(_ => { }, ex => distinctErrors.Add(ex.Message)).Dispose());
        ReactiveUI.Primitives.Signals.Signal.Recover<int, InvalidOperationException>(
                Signal.Fail<int>(new InvalidOperationException("typed-catch")),
                _ => Signal.Emit(Five))
            .Subscribe(catchValues.Add, ex => catchErrors.Add(ex.Message));
        ReactiveUI.Primitives.Signals.Signal.Recover<int, InvalidOperationException>(
                Signal.Fail<int>(new InvalidOperationException("handler-fault")),
                _ => throw new FormatException("handler-threw"))
            .Subscribe(_ => { }, ex => catchErrors.Add(ex.Message));
        ReactiveUI.Primitives.Signals.Signal.Recover<int, InvalidOperationException>(
                Signal.Fail<int>(new ArgumentException("not-matched")),
                _ => Signal.Emit(Six))
            .Subscribe(_ => { }, ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("finally-error"))
            .OnCleanup(() => finallyCalls++)
            .Subscribe(_ => { }, _ => { });

        Assert.Equal(new[] { "keep-predicate" }, keepErrors);
        Assert.Equal(new[] { "all-predicate" }, allErrors);
        Assert.Equal(0, distinctErrors.Count);
        Assert.Equal(new[] { Five }, catchValues);
        Assert.Equal(new[] { "handler-threw", "not-matched" }, catchErrors);
        Assert.Equal(1, finallyCalls);
    }

    /// <summary>
    /// Covers terminal observers that must ignore protocol violations after their first terminal signal.
    /// </summary>
    [Test]
    public void TerminalObserversIgnoreLateSignalsAndForwardPredicateFailures()
    {
        var errors = new List<string>();
        var values = new List<object>();
        var badIntegers = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnError(new InvalidOperationException("first-terminal"));
            observer.OnNext(Two);
            observer.OnCompleted();
        });
        var lateCompletionIntegers = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnError(new InvalidOperationException("late-error"));
            observer.OnNext(Two);
        });

        badIntegers.Count().Subscribe(value => values.Add(value), ex => errors.Add("count:" + ex.Message));
        badIntegers.LongCount().Subscribe(value => values.Add(value), ex => errors.Add("long-count:" + ex.Message));
        badIntegers.Count(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("count-predicate:" + ex.Message));
        badIntegers.LongCount(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("long-count-predicate:" + ex.Message));
        badIntegers.Any().Subscribe(value => values.Add(value), ex => errors.Add("any:" + ex.Message));
        badIntegers.Any(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("any-predicate:" + ex.Message));
        badIntegers.All(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("all:" + ex.Message));
        badIntegers.Contains(Two).Subscribe(value => values.Add(value), ex => errors.Add("contains:" + ex.Message));
        lateCompletionIntegers.Count().Subscribe(value => values.Add(value), ex => errors.Add("late-count:" + ex.Message));
        lateCompletionIntegers.LongCount().Subscribe(value => values.Add(value), ex => errors.Add("late-long-count:" + ex.Message));
        lateCompletionIntegers.Any(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("late-any:" + ex.Message));
        lateCompletionIntegers.All(value => value > 0).Subscribe(value => values.Add(value), ex => errors.Add("late-all:" + ex.Message));
        lateCompletionIntegers.Contains(One).Subscribe(value => values.Add(value), ex => errors.Add("late-contains:" + ex.Message));

        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .Count(value => value == One ? true : throw new InvalidOperationException("count-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .LongCount(value => value == One ? true : throw new InvalidOperationException("long-count-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .Any(value => value == One ? false : throw new InvalidOperationException("any-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        Signal.FromEnumerable([One, Two])
            .Contains(Two, new ThrowingComparer())
            .Subscribe(_ => { }, ex => errors.Add(ex.Message));

        Assert.Contains("count:first-terminal", errors);
        Assert.Contains("long-count:first-terminal", errors);
        Assert.Contains("count-predicate:first-terminal", errors);
        Assert.Contains("long-count-predicate:first-terminal", errors);
        Assert.Contains("all:first-terminal", errors);
        Assert.Contains("contains:first-terminal", errors);
        Assert.Contains("comparer-fault", errors);
        Assert.Contains(1, values);
        Assert.Contains(1L, values);
        Assert.Contains(true, values);
    }

    /// <summary>
    /// Covers non-completed task factory continuations for success, fault, cancellation, and disposed subscriptions.
    /// </summary>
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
        canceled.SetCanceled(new CancellationToken(true));
        disposed.SetResult(NinetyNine);

        await SpinUntil(ObservedPendingBranches, TimeSpan.FromSeconds(TimeoutSeconds)).ConfigureAwait(false);
        var finalValues = values.ToArray();
        var finalErrors = errors.ToArray();
        Assert.Equal(1, finalValues.Length);
        Assert.Equal(Seven, finalValues[0]);
        Assert.Contains(nameof(InvalidOperationException), finalErrors);
        Assert.Contains(nameof(TaskCanceledException), finalErrors);
    }

    /// <summary>
    /// Covers small value/factory/inline branches with public surface behavior.
    /// </summary>
    [Test]
    public void ValueFactoryAndInlineBranchesCoverPublicEdgeBehavior()
    {
        var sender = new object();
        var args = EventArgs.Empty;
        var pattern = new EventPattern<EventArgs>(sender, args);
        var same = new EventPattern<EventArgs>(sender, args);
        var other = new EventPattern<EventArgs>(new object(), args);
        Assert.True(pattern == same);
        Assert.True(pattern != other);
        Assert.True(pattern.Equals((object)same));
        Assert.False(pattern.Equals("not an event"));
        Assert.NotEqual(0, pattern.GetHashCode());
        Assert.True(pattern.ToString().Contains(nameof(EventArgs), StringComparison.Ordinal));
        Assert.Throws<ArgumentNullException>(() => new EventPattern<EventArgs>(sender, null!));

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
        repeat.Subscribe(new RecordingObserver<int>()).Dispose();
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)repeat).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)repeat).Subscribe(repeatValues.Add, ex => throw ex, () => repeatCompleted++);
        Assert.Equal(new[] { Seven, Seven, Seven }, repeatValues);
        Assert.Equal(1, repeatCompleted);

        var zippedValues = new List<int>();
        var zippedCompleted = 0;
        var zipped = Signal.Sequence(One, Three).Pair(Signal.Sequence(Four, Three), (left, right) => left + right);
        Assert.False(((IRequireCurrentThread<int>)zipped).IsRequiredSubscribeOnCurrentThread());
        Assert.Throws<ArgumentNullException>(() => zipped.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)zipped).Subscribe(null!, _ => { }, () => { }));
        ((IInlineSignal<int>)zipped).Subscribe(zippedValues.Add, ex => throw ex, () => zippedCompleted++);
        Assert.Equal(new[] { Five, Seven, Nine }, zippedValues);
        Assert.Equal(1, zippedCompleted);

        var returned = new List<string>();
        var returnCompleted = 0;
        var returnClock = new TestClock(DateTimeOffset.UnixEpoch);
        Signal.Emit("scheduled", returnClock).Subscribe(returned.Add, ex => throw ex, () => returnCompleted++);
        Assert.Equal(0, returnCompleted);
        returnClock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.Equal(new[] { "scheduled" }, returned);
        Assert.Equal(1, returnCompleted);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit("immediate").Subscribe((IObserver<string>)null!));

        var mappedErrors = new List<string>();
        Signal.FromEnumerable([One, Two]).Map(value => value == One ? value : throw new InvalidOperationException("map-fault"))
            .Subscribe(_ => { }, ex => mappedErrors.Add(ex.Message));
        Assert.Equal(new[] { "map-fault" }, mappedErrors);
    }

    /// <summary>
    /// Covers observer-based inline operator paths and private observer error cleanup paths left by action-subscribe scenarios.
    /// </summary>
    [Test]
    public void InlineOperatorObserverAndErrorCleanupPathsCoverRemainingBranches()
    {
        var observerValues = new RecordingObserver<int>();
        Signal.Emit(Three).Prepend(Two).Subscribe(observerValues).Dispose();
        Assert.Equal(new[] { Two, Three }, observerValues.Values);
        Assert.Equal(1, observerValues.Completed);

        var enumerableValues = new RecordingObserver<int>();
        LinqMixins.Prepend(Signal.Emit(Three), (IEnumerable<int>)[One, Two]).Subscribe(enumerableValues).Dispose();
        Assert.Equal(new[] { One, Two, Three }, enumerableValues.Values);
        Assert.Equal(1, enumerableValues.Completed);

        var prependAppendValues = new RecordingObserver<int>();
        Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(prependAppendValues).Dispose();
        Assert.Equal(new[] { One, Two, Three }, prependAppendValues.Values);
        Assert.Equal(1, prependAppendValues.Completed);

        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Prepend(Signal.Emit(One), (IEnumerable<int>)[Two]).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Append(Three).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Append(Two).Subscribe((IObserver<int>)null!));

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Append(Two).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());
        var appendErrorObserver = new ThrowingObserver<int>(throwOnError: true);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("append-error")).Append(Two).Subscribe(appendErrorObserver).Dispose());
        Assert.True(appendErrorObserver.SeenError);

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).DefaultIfEmpty(Two).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());

        var delegateErrors = 0;
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(_ => throw new InvalidOperationException("delegate-next"), _ => delegateErrors++, () => { }).Dispose());
        Signal.Fail<int>(new InvalidOperationException("delegate-error")).Prepend(One).Append(Three).Subscribe(_ => { }, _ => delegateErrors++, () => { }).Dispose();
        Assert.Equal(1, delegateErrors);
    }

    /// <summary>
    /// Covers optimized aggregate helper paths with custom comparers, selector exceptions, and range-backed count aliases.
    /// </summary>
    [Test]
    public void AggregateOptimizedSignalsCoverComparerAndExceptionPaths()
    {
        var distinctValues = new RecordingObserver<int>();
        Signal.FromEnumerable([One, Two, Three, Four])
            .DistinctBy(value => value % Two, EqualityComparer<int>.Default)
            .Subscribe(distinctValues)
            .Dispose();
        Assert.Equal(new[] { One, Two }, distinctValues.Values);

        var rangeDistinctCount = new RecordingObserver<int>();
        var rangeDistinctLongCount = new RecordingObserver<long>();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).Count().Subscribe(rangeDistinctCount).Dispose();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).LongCount().Subscribe(rangeDistinctLongCount).Dispose();
        Assert.Equal(new[] { Two }, rangeDistinctCount.Values);
        Assert.Equal(new long[] { 2L }, rangeDistinctLongCount.Values);

        var distinctErrors = new RecordingObserver<int>();
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).DistinctBy<int, int>(_ => throw new InvalidOperationException("distinct-key"))
            .Subscribe(distinctErrors)
            .Dispose());
        Assert.Equal(0, distinctErrors.Values.Count);

        var countErrors = new RecordingObserver<int>();
        var longCountErrors = new RecordingObserver<long>();
        var anyErrors = new RecordingObserver<bool>();
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).Count(_ => throw new InvalidOperationException("count-predicate")).Subscribe(countErrors).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).LongCount(_ => throw new InvalidOperationException("long-count-predicate")).Subscribe(longCountErrors).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).Any(_ => throw new InvalidOperationException("any-predicate")).Subscribe(anyErrors).Dispose());
        Assert.Equal(0, countErrors.Values.Count);
        Assert.Equal(0, longCountErrors.Values.Count);
        Assert.Equal(0, anyErrors.Values.Count);

        var containsRange = new RecordingObserver<bool>();
        Signal.Sequence(One, Four).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsRange).Dispose();
        Assert.Equal(new[] { true }, containsRange.Values);
    }

    /// <summary>
    /// Covers coordinator paths where later sources complete or error after another source has won or supplied both values.
    /// </summary>
    [Test]
    public void HigherOrderCoordinatorRaceCombineSwitchPathsCoverLateBranches()
    {
        var raceErrorWinner = new RecordingObserver<int>();
        var raceLoser = new Signal<int>();
        var raceErrorOuter = new Signal<IObservable<int>>();
        var raceErrorSubscription = raceErrorOuter.Race().Subscribe(raceErrorWinner);
        raceErrorOuter.OnNext(Signal.Fail<int>(new InvalidOperationException("race-error")));
        raceErrorOuter.OnNext(raceLoser);
        raceLoser.OnCompleted();
        raceErrorSubscription.Dispose();
        Assert.Equal("race-error", raceErrorWinner.Errors[0].Message);

        var raceCompletionWinner = new RecordingObserver<int>();
        var raceCompletionOuter = new Signal<IObservable<int>>();
        var completedWinner = new Signal<int>();
        var lateWinner = new Signal<int>();
        var raceCompletionSubscription = raceCompletionOuter.Race().Subscribe(raceCompletionWinner);
        raceCompletionOuter.OnNext(completedWinner);
        completedWinner.OnCompleted();
        raceCompletionOuter.OnNext(lateWinner);
        lateWinner.OnError(new InvalidOperationException("ignored"));
        raceCompletionSubscription.Dispose();
        Assert.Equal(1, raceCompletionWinner.Completed);

        var left = new Signal<int>();
        var right = new Signal<int>();
        var combined = new RecordingObserver<int>();
        var combineSubscription = left.SyncLatest(right, (l, r) => l + r).Subscribe(combined);
        left.OnNext(One);
        right.OnNext(Two);
        left.OnNext(Three);
        left.OnCompleted();
        Assert.Equal(new[] { Three, Five }, combined.Values);
        Assert.Equal(0, combined.Completed);
        right.OnCompleted();
        combineSubscription.Dispose();
        Assert.Equal(1, combined.Completed);

        var switchOuter = new Signal<IObservable<int>>();
        var firstInner = new Signal<int>();
        var secondInner = new Signal<int>();
        var switched = new RecordingObserver<int>();
        var switchSubscription = switchOuter.SwitchTo().Subscribe(switched);
        switchOuter.OnNext(firstInner);
        switchOuter.OnNext(secondInner);
        firstInner.OnNext(One);
        firstInner.OnCompleted();
        secondInner.OnNext(Two);
        switchOuter.OnCompleted();
        Assert.Equal(new[] { Two }, switched.Values);
        Assert.Equal(0, switched.Completed);
        secondInner.OnCompleted();
        switchSubscription.Dispose();
        Assert.Equal(1, switched.Completed);
    }

    /// <summary>
    /// Covers low-level equality, scheduling, witness, and create/defer/throw observer defensive paths.
    /// </summary>
    [Test]
    public void LowLevelReflectionAndSchedulingPathsCoverRemainingBranches()
    {
#pragma warning disable IL3050
        var priorityItemType = typeof(PriorityQueue<int>).GetNestedType("IndexedItem", BindingFlags.NonPublic)!.MakeGenericType(typeof(int));
#pragma warning restore IL3050
        var left = Activator.CreateInstance(priorityItemType)!;
        var right = Activator.CreateInstance(priorityItemType)!;
        priorityItemType.GetField("Id")!.SetValue(left, 1L);
        priorityItemType.GetField("Value")!.SetValue(left, One);
        priorityItemType.GetField("Id")!.SetValue(right, 1L);
        priorityItemType.GetField("Value")!.SetValue(right, One);
        Assert.True((bool)priorityItemType.GetMethod("Equals", [priorityItemType])!.Invoke(left, [right])!);
        Assert.True((bool)priorityItemType.GetMethod("Equals", [typeof(object)])!.Invoke(left, [right])!);
        Assert.False((bool)priorityItemType.GetMethod("Equals", [typeof(object)])!.Invoke(left, ["not-item"])!);
        Assert.NotEqual(0, (int)priorityItemType.GetMethod("GetHashCode")!.Invoke(left, [])!);

        var scheduledDisposed = false;
        var scheduled = new ScheduledProbe(One, () => Disposable.Create(() => scheduledDisposed = true));
        Assert.Equal(1, ((IComparable)scheduled).CompareTo(null));
        Assert.Equal(0, ((IComparable)scheduled).CompareTo(new ScheduledProbe(One, () => Disposable.Empty)));
        Assert.Throws<ArgumentException>(() => ((IComparable)scheduled).CompareTo("not-scheduled"));
        Assert.True(scheduled.Equals((object)scheduled));
        Assert.False(scheduled.Equals(new object()));
        Assert.NotEqual(0, scheduled.GetHashCode());
        scheduled.Invoke();
        scheduled.Cancel();
        Assert.True(scheduledDisposed);

        var cancelDisposed = false;
#pragma warning disable IL3050
        var safeType = typeof(Witness).GetNestedType("SafeWitness`1", BindingFlags.NonPublic)!.MakeGenericType(typeof(int));
#pragma warning restore IL3050
        var safe = (IObserver<int>)Activator.CreateInstance(
            safeType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            args: [new ThrowingObserver<int>(throwOnError: true), Disposable.Create(() => cancelDisposed = true)],
            culture: null)!;
        Assert.Throws<InvalidOperationException>(() => safe.OnError(new InvalidOperationException("safe")));
        Assert.True(cancelDisposed);
        safe.OnError(new InvalidOperationException("ignored"));

        var createErrors = new RecordingObserver<int>();
        Signal.CreateWithState<int, int>(
            0,
            static (_, observer) =>
            {
                observer.OnError(new InvalidOperationException("create-error"));
                return null!;
            }).Subscribe(createErrors).Dispose();
        Assert.Equal("create-error", createErrors.Errors[0].Message);

        var deferErrors = new RecordingObserver<int>();
        Signal.Lazy<int>(() => throw new InvalidOperationException("defer-factory")).Subscribe(deferErrors).Dispose();
        Assert.Equal("defer-factory", deferErrors.Errors[0].Message);

        var immediateThrow = new RecordingObserver<int>();
        Signal.Fail<int>(new InvalidOperationException("immediate-throw"), Sequencer.Immediate).Subscribe(immediateThrow).Dispose();
        Assert.Equal("immediate-throw", immediateThrow.Errors[0].Message);
    }

    private static async Task SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var attempts = (int)(timeout.TotalMilliseconds / PollDelayMilliseconds);
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(PollDelayMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("Timed out waiting for asynchronous coverage branch.");
    }

    private sealed class ThrowingComparer : IEqualityComparer<int>
    {
        public bool Equals(int x, int y) => throw new InvalidOperationException("comparer-fault");

        public int GetHashCode(int obj) => obj.GetHashCode();
    }

    private sealed class ScriptedObservable<T> : IObservable<T>
    {
        private readonly Action<IObserver<T>> _script;

        public ScriptedObservable(Action<IObserver<T>> script) => _script = script;

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _script(observer);
            return Disposable.Empty;
        }
    }

    private sealed class MinimalVirtualClock : VirtualTimeSequencerBase<long, long>
    {
        private readonly SortedDictionary<long, Queue<Scheduled>> _scheduled = [];

        public MinimalVirtualClock()
        {
        }

        public MinimalVirtualClock(IComparer<long> comparer)
            : base(0L, comparer)
        {
        }

        public override IDisposable ScheduleAbsolute<TState>(TState state, long dueTime, Func<ISequencer, TState, IDisposable> action)
        {
            var scheduled = new Scheduled(dueTime, () => action(this, state));
            if (!_scheduled.TryGetValue(dueTime, out var queue))
            {
                queue = new Queue<Scheduled>();
                _scheduled.Add(dueTime, queue);
            }

            queue.Enqueue(scheduled);
            return Disposable.Create(() => scheduled.IsCancelled = true);
        }

        protected override long Add(long absolute, long relative) => absolute + relative;

        protected override IScheduledItem<long>? GetNext()
        {
            while (_scheduled.Count > 0)
            {
                using var enumerator = _scheduled.GetEnumerator();
                enumerator.MoveNext();
                var first = enumerator.Current;
                var item = first.Value.Dequeue();
                if (first.Value.Count == 0)
                {
                    _scheduled.Remove(first.Key);
                }

                if (!item.IsCancelled)
                {
                    return item;
                }
            }

            return null;
        }

        protected override DateTimeOffset ToDateTimeOffset(long absolute) => DateTimeOffset.UnixEpoch.AddTicks(absolute);

        protected override long ToRelative(TimeSpan timeSpan) => timeSpan.Ticks;

        private sealed class Scheduled : IScheduledItem<long>
        {
            private readonly Func<IDisposable> _action;

            public Scheduled(long dueTime, Func<IDisposable> action)
            {
                DueTime = dueTime;
                _action = action;
            }

            public long DueTime { get; }

            public bool IsCancelled { get; set; }

            public void Invoke()
            {
                if (IsCancelled)
                {
                    return;
                }

                _action().Dispose();
            }
        }
    }

    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        private readonly bool _throwOnNext;
        private readonly bool _throwOnError;
        private readonly bool _throwOnCompleted;

        public ThrowingObserver(bool throwOnNext = false, bool throwOnError = false, bool throwOnCompleted = false)
        {
            _throwOnNext = throwOnNext;
            _throwOnError = throwOnError;
            _throwOnCompleted = throwOnCompleted;
        }

        public bool SeenError { get; private set; }

        public void OnCompleted()
        {
            if (_throwOnCompleted)
            {
                throw new InvalidOperationException("observer-completed");
            }
        }

        public void OnError(Exception error)
        {
            SeenError = true;
            if (_throwOnError)
            {
                throw new InvalidOperationException("observer-error");
            }
        }

        public void OnNext(T value)
        {
            if (_throwOnNext)
            {
                throw new InvalidOperationException("observer-next");
            }
        }
    }

    private sealed class ScheduledProbe : ScheduledItem<int>
    {
        private readonly Func<IDisposable> _invoke;

        public ScheduledProbe(int dueTime, Func<IDisposable> invoke)
            : base(dueTime, Comparer<int>.Default)
        {
            _invoke = invoke;
        }

        protected override IDisposable InvokeCore() => _invoke();
    }

    private sealed class RecordingObserver<T> : IObserver<T>
    {
        public List<T> Values { get; } = [];

        public List<Exception> Errors { get; } = [];

        public int Completed { get; private set; }

        public void OnCompleted() => Completed++;

        public void OnError(Exception error) => Errors.Add(error);

        public void OnNext(T value) => Values.Add(value);
    }
}
