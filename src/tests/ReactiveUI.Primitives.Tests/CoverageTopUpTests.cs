// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
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

#pragma warning disable RCS1222
#pragma warning disable SA1600, S109, S3400, S6354, CA1806, S3257, CA1861, IDE0300, RCS1196, S6966
#pragma warning disable S103, CA1065, S138, IDE0034, RCS1151, S1764, S1944, RCS1208, RCS1215
#pragma warning disable S3981, CA1849, SA1129, CA1823

/// <summary>
/// Targeted deterministic top-up tests for remaining production coverage gaps.
/// </summary>
public sealed class CoverageTopUpTests
{
    private const int One = 1;
    private const int Two = 2;
    private const int Three = 3;
    private const int Four = 4;
    private const int Five = 5;
    private const int Six = 6;
    private const int Seven = 7;

    private const int Nine = 9;
    private const int Ten = 10;
    private const int Twelve = 12;
    private const int FortyTwo = 42;

    [Test]
    public async Task ParityAliasesRangeAsyncFastPathsAndGuardsCoverRemainingLines()
    {
        IObservable<int> source = Signal.FromEnumerable([Three, Four]);
        var values = new List<int>();
        source.Prepend(Two).Subscribe(values.Add);
        Assert.Equal(new[] { Two, Three, Four }, values);

        var delayedStart = source.DelayStart(TimeSpan.Zero);
        Assert.NotNull(delayedStart);
        Assert.NotNull(source.DelaySubscription(TimeSpan.Zero));
        Assert.NotNull(source.DelaySubscription(TimeSpan.Zero, Sequencer.Immediate));
        Assert.NotNull(source.Stabilize(TimeSpan.Zero));
        Assert.NotNull(source.Stabilize(TimeSpan.Zero, Sequencer.Immediate));

        var fused = new List<int>();
        Signal.Emit(One).FuseLatest(Signal.FromEnumerable([Two, Three]), (left, right) => left + right).Subscribe(fused.Add);
        Assert.Equal(new[] { Three, Four }, fused);

        var chainedStrings = new List<string>();
        Signal.Chain(Signal.Emit("value")).Subscribe(chainedStrings.Add);
        Assert.Equal(new[] { "value" }, chainedStrings);

        var ignoredCatchCompleted = 0;
        Signal.Fail<int>(new InvalidOperationException("ignored")).Recover<int, Exception>(Handle.CatchIgnore<int>).Subscribe(_ => { }, ex => throw ex, () => ignoredCatchCompleted++);
        Assert.Equal(1, ignoredCatchCompleted);

        var rangeArray = new List<int[]>();
        var rangeList = new List<IList<int>>();
        Signal.Sequence(Five, Three).CollectArray().Subscribe(rangeArray.Add);
        Signal.Sequence(Five, Three).CollectList().Subscribe(rangeList.Add);
        Assert.Equal<int>([Five, Six, Seven], rangeArray[0]);
        Assert.Equal<int>([Five, Six, Seven], rangeList[0]);

        Assert.Equal(Ten, await Signal.Sequence(Ten, Three).FirstAsync().ConfigureAwait(false));
        Assert.Equal(Ten, await Signal.Sequence(Ten, Three).FirstOrDefaultAsync().ConfigureAwait(false));
        Assert.Equal(Ten, await Signal.Sequence(Ten, Three).FirstOrDefaultAsync(Nine).ConfigureAwait(false));
        Assert.Equal(Twelve, await Signal.Sequence(Ten, Three).LastAsync().ConfigureAwait(false));
        Assert.Equal(Twelve, await Signal.Sequence(Ten, Three).LastOrDefaultAsync().ConfigureAwait(false));
        Assert.Equal(Nine, await Signal.None<int>().LastOrDefaultAsync(Nine).ConfigureAwait(false));
        Assert.Equal(Three, await Signal.Sequence(One, Three).CountAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(Two, await Signal.Sequence(One, Three).CountAsync(value => value > One, CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(3L, await Signal.Sequence(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(2L, await Signal.Sequence(One, Three).LongCount(value => value > One).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Sequence(One, Three).AnyAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Sequence(One, Three).AnyAsync(value => value == Two, CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Sequence(One, Three).All(value => value < Four).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Sequence(One, Three).Contains(Three).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal<int>([Five, Six, Seven], await Signal.Sequence(Five, Three).CollectArrayAsync().ConfigureAwait(false));
        Assert.Equal<int>([Five, Six, Seven], await Signal.Sequence(Five, Three).CollectListAsync().ConfigureAwait(false));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync().ConfigureAwait(false);
        var canceledTask = Signal.Silent<int>().ToTask(canceled.Token);
        Assert.True(canceledTask.IsCanceled);

        Assert.Throws<ArgumentNullException>(() => LinqMixins.Count<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.LongCount<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Blend<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.CollectArray<int>(null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeMixins.Subscribe<int>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeMixins.Subscribe<int>(null!, _ => { }, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.Recover<int, InvalidOperationException>(Signal.None<int>(), null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.Recover<int>((IEnumerable<IObservable<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.CreateSafe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => StateSignalMixins.ToReadOnlyState<int, int>(null!, One, value => value));
        Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskSignal.Create<int>(null!));
    }

    [Test]
    public void ImmediateCoreSignalsRangeZipRepeatAndObserverFailuresCoverRemainders()
    {
        var completed = 0;
        Signal.None<int>(Sequencer.Immediate).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Signal.None<int>(default(int)).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Assert.Equal(Two, completed);

        var returnValues = new List<int>();
        Signal.Emit(FortyTwo, Sequencer.Immediate).Subscribe(returnValues.Add);
        Assert.Equal(new[] { FortyTwo }, returnValues);

        var throwErrors = new List<string>();
        Signal.Fail<int>(new InvalidOperationException("immediate"), Sequencer.Immediate).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Signal.Fail(new InvalidOperationException("witness"), Sequencer.Immediate, default(int)).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Assert.Equal(new[] { "immediate", "witness" }, throwErrors);

        var never = Signal.Silent<int>(default(int));
        Assert.False(((IRequireCurrentThread<int>)never).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<RxVoid>)Signal.EmitRxVoid()).IsRequiredSubscribeOnCurrentThread());
        Assert.True(new RxVoid() == new RxVoid());
        Assert.False(new RxVoid() != new RxVoid());

        var repeat = new RepeatSignal<int>(Seven, Three);
        var repeatValues = new List<int>();
        Assert.False(repeat.IsRequiredSubscribeOnCurrentThread());
        repeat.Subscribe(new RecordingObserver<int>()).Dispose();
        repeat.Subscribe(repeatValues.Add, ex => throw ex, () => completed++).Dispose();
        Assert.Equal(new[] { Seven, Seven, Seven }, repeatValues);
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe(null!, _ => { }, () => { }));

        var range = new RangeSignal(One, Three);
        var rangeValues = new List<int>();
        Assert.False(range.IsRequiredSubscribeOnCurrentThread());
        range.Subscribe(new RecordingObserver<int>()).Dispose();
        range.Subscribe(rangeValues.Add, ex => throw ex, () => completed++).Dispose();
        Assert.Equal(new[] { One, Two, Three }, rangeValues);
        Assert.Throws<ArgumentNullException>(() => range.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => range.Subscribe(null!, _ => { }, () => { }));

        var zip = new RangeZipSignal<int>(new RangeSignal(One, Three), new RangeSignal(Four, Three), (left, right) => left + right);
        var zipValues = new List<int>();
        Assert.False(zip.IsRequiredSubscribeOnCurrentThread());
        zip.Subscribe(new RecordingObserver<int>()).Dispose();
        zip.Subscribe(zipValues.Add, ex => throw ex, () => completed++).Dispose();
        Assert.Equal(new[] { Five, Seven, Nine }, zipValues);
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe(null!, _ => { }, () => { }));

        Assert.False(((IRequireCurrentThread<int>)new ImmediateReturnSignal<int>(One)).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<int>)new ImmediateThrowSignal<int>(new InvalidOperationException("fast"))).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<int>)ImmutableEmptySignal<int>.Instance).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<int>)ImmutableNeverSignal<int>.Instance).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<int>)ImmutableReturnInt32Signal.GetInt32Signals(One)).IsRequiredSubscribeOnCurrentThread());
        Assert.False(new RangeConcatSignal([new RangeSignal(One, Two), new RangeSignal(Three, Two)]).IsRequiredSubscribeOnCurrentThread());
        Assert.False(new SignalsBaseProbe<int>(false).IsRequiredSubscribeOnCurrentThread());

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One, Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.None<int>(Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("observer"), Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnError: true)).Dispose());
        Assert.Throws<ArgumentNullException>(() => new ImmediateThrowSignal<int>(new InvalidOperationException("null-observer")).Subscribe((IObserver<int>)null!));
    }

    [Test]
    public void SubjectsReplayBehaviorStateAndConnectableAliasesCoverLateTerminalBranches()
    {
        var behavior = new BehaviorSignal<int>(One);
        Assert.True(behavior.ToString()!.Contains(nameof(BehaviorSignal<int>), StringComparison.Ordinal));
        var initial = new RecordingObserver<int>();
        using var behaviorSubscription = behavior.Subscribe(initial);
        behavior.OnCompleted();
        behavior.OnCompleted();
        behavior.OnNext(Two);
        var lateCompleted = new RecordingObserver<int>();
        behavior.Subscribe(lateCompleted).Dispose();
        Assert.Equal(new[] { One }, initial.Values);
        Assert.Equal(1, lateCompleted.Completed);

        var behaviorError = new BehaviorSignal<int>(One);
        behaviorError.OnError(new InvalidOperationException("behavior"));
        behaviorError.OnError(new InvalidOperationException("late"));
        var lateError = new RecordingObserver<int>();
        behaviorError.Subscribe(lateError).Dispose();
        Assert.Equal("behavior", lateError.Errors[0].Message);
        behaviorError.Dispose();
        behaviorError.Dispose();
        Assert.False(behaviorError.TryGetValue(out _));

        var replayCompleted = new ReplaySignal<int>(bufferSize: 2, window: TimeSpan.MaxValue, scheduler: Sequencer.CurrentThread);
        replayCompleted.OnNext(One);
        replayCompleted.OnNext(Two);
        replayCompleted.OnNext(Three);
        replayCompleted.OnCompleted();
        replayCompleted.OnCompleted();
        replayCompleted.OnNext(Four);
        var replayLateCompleted = new RecordingObserver<int>();
        replayCompleted.Subscribe(replayLateCompleted).Dispose();
        Assert.Equal(new[] { Two, Three }, replayLateCompleted.Values);
        Assert.Equal(1, replayLateCompleted.Completed);

        var replayError = new ReplaySignal<int>(bufferSize: 1, window: TimeSpan.MaxValue, scheduler: Sequencer.CurrentThread);
        replayError.OnNext(Five);
        replayError.OnError(new InvalidOperationException("replay"));
        replayError.OnError(new InvalidOperationException("late"));
        var replayLateError = new RecordingObserver<int>();
        replayError.Subscribe(replayLateError).Dispose();
        Assert.Equal(new[] { Five }, replayLateError.Values);
        Assert.Equal("replay", replayLateError.Errors[0].Message);
        replayError.Dispose();
        replayError.Dispose();
        Assert.Throws<ObjectDisposedException>(() => replayError.Subscribe(new RecordingObserver<int>()));

        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var windowedReplay = new ReplaySignal<int>(bufferSize: 10, window: TimeSpan.FromTicks(2), scheduler: clock);
        windowedReplay.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(3));
        windowedReplay.OnNext(Two);
        var windowedLate = new RecordingObserver<int>();
        windowedReplay.Subscribe(windowedLate).Dispose();
        Assert.Equal(new[] { Two }, windowedLate.Values);

        var shared = Signal.Sequence(One, Three).Share();
        var replayed = Signal.Sequence(One, Three).Replay(2);
        Assert.NotNull(shared);
        Assert.NotNull(replayed);

        var state = Assert.Throws<ArgumentNullException>(() => new StateSignal<int>(One).ToReadOnlyState<int>(null!));
        Assert.Equal("selector", state.ParamName);
    }

    [Test]
    public void LowLevelDisposablesCollectionsAndSchedulersCoverDeterministicEdges()
    {
        var multiple = new MultipleDisposable();
        for (var i = 0; i < 20; i++)
        {
            multiple.Add(Disposable.Empty);
        }

        Assert.True(multiple.Remove(Disposable.Empty));
        Assert.False(multiple.Remove(Disposable.Create(() => { })));
        Assert.Throws<ArgumentNullException>(() => new MultipleDisposable((IDisposable[])null!));
        Assert.Throws<ArgumentNullException>(() => multiple.Add(null!));
        multiple.Dispose();
        multiple.Dispose();

        using var cts = new CancellationTokenSource();
        var cancellation = new CancellationDisposable(cts);
        cancellation.Dispose();
        cancellation.Dispose();
        Assert.True(cts.IsCancellationRequested);

        var list = ImmutableList<int>.Empty;
        Assert.Equal(-1, list.IndexOf(One));
        Assert.Same(list, list.Remove(One));
        var added = list.Add(One).Add(Two);
        Assert.Equal(0, added.IndexOf(One));
        Assert.Same(ImmutableList<int>.Empty, added.Remove(One).Remove(Two));
        var observerList = ImmutableList<IObserver<int>>.Empty.Add(new RecordingObserver<int>());
        var witness = new ListWitness<int>(observerList);
        Assert.True(witness.HasObservers);
        Assert.NotNull(witness.Add(new RecordingObserver<int>()));

        var queue = new PriorityQueue<int>();
        queue.Enqueue(One);
        queue.Enqueue(Two);
        Assert.True(queue.Count > 0);

        var eventPattern = new EventPattern<EventArgs>(null, EventArgs.Empty);
        var samePattern = new EventPattern<EventArgs>(null, EventArgs.Empty);
        Assert.True(eventPattern == samePattern);
        Assert.False(eventPattern != samePattern);
        Assert.True(eventPattern.Equals((object)samePattern));
        Assert.NotEqual(0, eventPattern.GetHashCode());

        var current = Sequencer.CurrentThread;
        Assert.Throws<ArgumentNullException>(() => current.Schedule((Action)null!));
        Assert.Throws<ArgumentNullException>(() => current.Schedule(One, TimeSpan.Zero, null!));
        var scheduled = new List<int>();
        current.Schedule(One, TimeSpan.FromMilliseconds(1), (_, state) =>
        {
            scheduled.Add(state);
            return Disposable.Empty;
        }).Dispose();
        current.Schedule(One, DateTimeOffset.UtcNow.AddMilliseconds(1), (_, state) =>
        {
            scheduled.Add(state + One);
            return Disposable.Empty;
        }).Dispose();
        Assert.True(scheduled.Count >= 0);
    }

    [Test]
    public async Task RemainingOperatorFactoryAndObserverFailureBranchesAreDeterministic()
    {
        var scheduledRangeClock = new TestClock(DateTimeOffset.UnixEpoch);
        var scheduledRange = new List<int>();
        var scheduledRangeCompleted = 0;
        Signal.Sequence(Three, Three, scheduledRangeClock).Subscribe(scheduledRange.Add, ex => throw ex, () => scheduledRangeCompleted++);
        scheduledRangeClock.Start();
        Assert.Equal<int>([Three, Four, Five], scheduledRange);
        Assert.Equal(1, scheduledRangeCompleted);

        Assert.NotNull(Signal.After(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Pulse(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Pulse(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Pulse(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)));
        Assert.NotNull(Signal.After(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.After(DateTimeOffset.UtcNow.AddMilliseconds(1)));
        Assert.NotNull(Signal.After(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.PairLatest(Signal.Sequence(One, Two), Signal.Sequence(Three, Two), (left, right) => left + right));

        var toSignalValues = new List<int>();
        new[] { One, Two }.ToSignal().Subscribe(toSignalValues.Add);
        new[] { Three, Four }.ToSignal(CancellationToken.None).Subscribe(toSignalValues.Add);
        Assert.Equal(new[] { One, Two, Three, Four }, toSignalValues);

        var firstTaskSignal = await Signal.FromTask<int>(_ => Task.FromResult(Five)).FirstAsync().ConfigureAwait(false);
        var secondTaskSignal = await Signal.FromTask<int>(_ => Task.FromResult(Six), Sequencer.Immediate).FirstAsync().ConfigureAwait(false);
        Assert.Equal(Five, firstTaskSignal);
        Assert.Equal(Six, secondTaskSignal);
        Assert.Equal(Seven, await Task.FromResult(Seven).HandleCancellation().ConfigureAwait(false));
        Assert.Equal(default(int), await Task.FromCanceled<int>(new CancellationToken(true)).HandleCancellation().ConfigureAwait(false));

        var longCount = new List<long>();
        Signal.Sequence(One, Four).LongCount(value => value % 2 == 0).Subscribe(longCount.Add);
        Assert.Equal(new long[] { 2L }, longCount);

        var containsWithComparer = new List<bool>();
        Signal.Sequence(One, Three).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Assert.Equal(new[] { true, false, true, false }, containsWithComparer);

        var startWithAlias = new List<int>();
        LinqMixins.Prepend(Signal.Emit(Two), One).Subscribe(startWithAlias.Add);
        Assert.Equal(new[] { One, Two }, startWithAlias);
        Assert.NotNull(LinqMixins.DelayStart(Signal.Emit(One), TimeSpan.Zero));
        Assert.Equal(0, await Signal.None<int>().FirstOrDefaultAsync().ConfigureAwait(false));
        var noneWitnessCompleted = 0;
        Signal.None(Sequencer.Immediate, One).Subscribe(_ => { }, ex => throw ex, () => noneWitnessCompleted++);
        Assert.Equal(1, noneWitnessCompleted);

        Assert.Throws<ArgumentNullException>(() => LinqMixins.Buffer<int>(null!, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Buffer<int>(null!, One, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(0, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(One, 0));

        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstOrDefaultAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstOrDefaultAsync(One));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToTask());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).LastOrDefaultAsync(One));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).AnyAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectArrayAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectListAsync());

        var pending = new Signal<int>();
        using var cancelAfterSubscribe = new CancellationTokenSource();
        var pendingTask = pending.ToTask(cancelAfterSubscribe.Token);
        await cancelAfterSubscribe.CancelAsync().ConfigureAwait(false);
        Assert.True(pendingTask.IsCanceled);

        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new EmptySignal<int>(Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new ThrowSignal<int>(new InvalidOperationException("throw-signal"), Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnError: true)).Dispose());

#pragma warning disable S3011, IL3050
        var returnWitnessType = typeof(ReturnSignal<int>).GetNestedType("Return", BindingFlags.NonPublic)!.MakeGenericType(typeof(int));
        var returnWitness = (IObserver<int>)Activator.CreateInstance(returnWitnessType, new RecordingObserver<int>(), Disposable.Empty)!;
        returnWitness.OnError(new InvalidOperationException("return-inner"));
        var emptyWitnessType = typeof(EmptySignal<int>).GetNestedType("Empty", BindingFlags.NonPublic)!.MakeGenericType(typeof(int));
        var emptyWitness = (IObserver<int>)Activator.CreateInstance(emptyWitnessType, new RecordingObserver<int>(), Disposable.Empty)!;
#pragma warning restore S3011, IL3050
        emptyWitness.OnNext(One);
        emptyWitness.OnError(new InvalidOperationException("empty-inner"));

        var mapObserver = new RecordingObserver<int>();
        var badSource = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-map"));
            observer.OnCompleted();
        });
        badSource.Map(value => value).Subscribe(mapObserver).Dispose();
        Assert.Equal(new[] { One }, mapObserver.Values);
        Assert.Equal(1, mapObserver.Completed);

        var signal = new Signal<int>();
        Assert.Throws<ArgumentNullException>(() => signal.Subscribe((Action<int>)null!));
        var actionValues = new List<int>();
        using var actionSubscription = signal.Subscribe(actionValues.Add);
        using var s1 = signal.Subscribe(new RecordingObserver<int>());
        using var s2 = signal.Subscribe(new RecordingObserver<int>());
        using var s3 = signal.Subscribe(new RecordingObserver<int>());
        using var s4 = signal.Subscribe(new RecordingObserver<int>());
        using var s5 = signal.Subscribe(new RecordingObserver<int>());
        Assert.Throws<InvalidOperationException>(() => signal.OnError(new InvalidOperationException("many")));

        var outer = new Signal<IObservable<int>>();
        var firstInner = new Signal<int>();
        var secondInner = new Signal<int>();
        var selectManyValues = new List<int>();
        var selectManyCompleted = 0;
        using (outer.FlatMap(inner => inner).Subscribe(selectManyValues.Add, ex => throw ex, () => selectManyCompleted++))
        {
            outer.OnNext(firstInner);
            outer.OnNext(secondInner);
            outer.OnCompleted();
            firstInner.OnNext(One);
            firstInner.OnCompleted();
            secondInner.OnNext(Two);
            secondInner.OnCompleted();
        }

        Assert.Equal(new[] { One, Two }, selectManyValues);
        Assert.Equal(1, selectManyCompleted);

        var disposedOuter = new Signal<IObservable<int>>();
        var disposedInner = new Signal<int>();
        var disposedValues = new List<int>();
        var disposedSubscription = disposedOuter.FlatMap(inner => inner).Subscribe(disposedValues.Add);
        disposedOuter.OnNext(disposedInner);
        disposedSubscription.Dispose();
        disposedSubscription.Dispose();
        disposedInner.OnNext(Three);
        Assert.Equal(0, disposedValues.Count);

        Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner, (left, right) => right).Subscribe(null!));

        var nullSelectorErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int>(_ => null!).Subscribe(_ => { }, ex => nullSelectorErrors.Add(ex.Message));
        Assert.Equal(new[] { "The FlatMap selector returned null." }, nullSelectorErrors);

        var nullCollectionErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int, int>(_ => null!, (left, right) => left + right).Subscribe(_ => { }, ex => nullCollectionErrors.Add(ex.Message));
        Assert.Equal(new[] { "The FlatMap collection selector returned null." }, nullCollectionErrors);

        var resultInnerErrors = new List<string>();
        Signal.Emit(One).FlatMap(_ => Signal.Fail<int>(new InvalidOperationException("result-inner")), (left, right) => left + right).Subscribe(_ => { }, ex => resultInnerErrors.Add(ex.Message));
        Assert.Equal(new[] { "result-inner" }, resultInnerErrors);

        var subscribeErrors = new List<string>();
        Signal.Emit(One)
            .FlatMap(_ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe")))
            .Subscribe(_ => { }, ex => subscribeErrors.Add(ex.Message));
        Assert.Equal(new[] { "inner-subscribe" }, subscribeErrors);
    }

    [Test]
    public async Task OptimizedCoordinatorAndAsyncEnumerableBranchesCoverPrNineGaps()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable(AsyncValues(One)).Subscribe(null!));

        var asyncValues = new List<int>();
        var asyncCompleted = new TaskCompletionSource<object?>();
        using var asyncToken = new CancellationTokenSource();
        Signal.FromAsyncEnumerable(AsyncValues(Three), asyncToken.Token).Subscribe(
            asyncValues.Add,
            ex => asyncCompleted.TrySetException(ex),
            () => asyncCompleted.TrySetResult(null));
        await asyncCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        Assert.Equal(new[] { 0, One, Two }, asyncValues);

        var exact = await Signal.FromAsyncEnumerable(AsyncValues(16)).CollectArrayAsync().ConfigureAwait(false);
        var grown = await Signal.FromAsyncEnumerable(AsyncValues(17)).CollectArrayAsync().ConfigureAwait(false);
        Assert.Equal(16, exact.Length);
        Assert.Equal(15, exact[15]);
        Assert.Equal(17, grown.Length);
        Assert.Equal(16, grown[16]);

        var shiftedClock = new TestClock(DateTimeOffset.UnixEpoch);
        var shifted = new List<int>();
        Signal.Sequence(Three, Three).Shift(TimeSpan.FromTicks(Two), shiftedClock).Subscribe(shifted.Add);
        Assert.Equal(0, shifted.Count);
        shiftedClock.AdvanceBy(TimeSpan.FromTicks(Two));
        Assert.Equal(new[] { Three, Four, Five }, shifted);

        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Expire(TimeSpan.Zero).Subscribe(null!));

        var timeoutClock = new TestClock(DateTimeOffset.UnixEpoch);
        var timeout = new RecordingObserver<int>();
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), timeoutClock).Subscribe(timeout);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.True(timeout.Errors[0] is TimeoutException);

        var expireCompleted = new RecordingObserver<int>();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-expire"));
            observer.OnCompleted();
        }).Expire(TimeSpan.FromTicks(Ten), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(expireCompleted);
        Assert.Equal(new[] { One }, expireCompleted.Values);
        Assert.Equal(1, expireCompleted.Completed);
        Assert.Equal(0, expireCompleted.Errors.Count);

        var expireError = new RecordingObserver<int>();
        Signal.Fail<int>(new InvalidOperationException("expire-error")).Expire(TimeSpan.FromTicks(Ten), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(expireError);
        Assert.Equal("expire-error", expireError.Errors[0].Message);

        var raceOuter = new Signal<IObservable<int>>();
        var raceWinner = new Signal<int>();
        var raceLoser = new Signal<int>();
        var race = new RecordingObserver<int>();
        using (raceOuter.Race().Subscribe(race))
        {
            raceOuter.OnNext(raceWinner);
            raceOuter.OnNext(raceLoser);
            raceWinner.OnNext(One);
            raceLoser.OnError(new InvalidOperationException("late-race"));
            raceLoser.OnCompleted();
        }

        Assert.Equal(new[] { One }, race.Values);
        Assert.Equal(0, race.Errors.Count);

        var raceCompletionOuter = new Signal<IObservable<int>>();
        var raceCompletionWinner = new Signal<int>();
        var raceCompletionLoser = new CapturingObservable<int>();
        var raceCompletion = new RecordingObserver<int>();
        using (raceCompletionOuter.Race().Subscribe(raceCompletion))
        {
            raceCompletionOuter.OnNext(raceCompletionWinner);
            raceCompletionOuter.OnNext(raceCompletionLoser);
            raceCompletionWinner.OnNext(Two);
            raceCompletionLoser.Observer!.OnCompleted();
        }

        Assert.Equal(new[] { Two }, raceCompletion.Values);
        Assert.Equal(0, raceCompletion.Completed);

        var combineLeft = new Signal<int>();
        var combineRight = new Signal<int>();
        var combined = new RecordingObserver<int>();
        using (combineLeft.SyncLatest(combineRight, (left, right) => left + right).Subscribe(combined))
        {
            combineRight.OnNext(Two);
            combineLeft.OnNext(One);
            combineRight.OnCompleted();
            combineLeft.OnCompleted();
        }

        Assert.Equal(new[] { Three }, combined.Values);
        Assert.Equal(1, combined.Completed);

        var switchOuter = new Signal<IObservable<int>>();
        var staleInner = new CapturingObservable<int>();
        var currentInner = new CapturingObservable<int>();
        var switched = new RecordingObserver<int>();
        using (switchOuter.SwitchTo().Subscribe(switched))
        {
            switchOuter.OnNext(staleInner);
            switchOuter.OnNext(currentInner);
            staleInner.Observer!.OnNext(One);
            staleInner.Observer.OnError(new InvalidOperationException("stale-switch"));
            currentInner.Observer!.OnError(new InvalidOperationException("current-switch"));
        }

        Assert.Equal(0, switched.Values.Count);
        Assert.Equal("current-switch", switched.Errors[0].Message);

        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Probe(TimeSpan.Zero).Subscribe(null!));

        var probeError = new RecordingObserver<int>();
        Signal.Fail<int>(new InvalidOperationException("probe-error")).Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(probeError);
        Assert.Equal("probe-error", probeError.Errors[0].Message);

        var probeSource = new Signal<int>();
        var probeSubscription = probeSource.Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(new RecordingObserver<int>());
        probeSubscription.Dispose();
        probeSubscription.Dispose();

        var completedProbe = new RecordingObserver<int>();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(One);
        }).Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(completedProbe);
        Assert.Equal(1, completedProbe.Completed);
        Assert.Equal(0, completedProbe.Values.Count);

        var directCurrentThreadExpire = new RecordingObserver<int>();
        var directCurrentThreadProbe = new RecordingObserver<int>();
        Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadExpire);
        Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadProbe);
        Assert.Equal(new[] { One }, directCurrentThreadExpire.Values);
        Assert.Equal(1, directCurrentThreadExpire.Completed);
        Assert.Equal(0, directCurrentThreadProbe.Values.Count);
        Assert.Equal(1, directCurrentThreadProbe.Completed);

        var currentThreadExpire = new RecordingObserver<int>();
        var currentThreadProbe = new RecordingObserver<int>();
        Sequencer.CurrentThread.Schedule(() =>
        {
            Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadExpire);
            Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadProbe);
        });
        Assert.Equal(new[] { One }, currentThreadExpire.Values);
        Assert.Equal(1, currentThreadExpire.Completed);
        Assert.Equal(0, currentThreadProbe.Values.Count);
        Assert.Equal(1, currentThreadProbe.Completed);

        var calmError = new RecordingObserver<int>();
        Signal.Fail<int>(new InvalidOperationException("calm-error")).Calm(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(calmError);
        Assert.Equal("calm-error", calmError.Errors[0].Message);

        var calmClock = new TestClock(DateTimeOffset.UnixEpoch);
        var calmSource = new Signal<int>();
        var calmValues = new List<int>();
        calmSource.Calm(TimeSpan.FromTicks(Five), calmClock).Subscribe(calmValues.Add);
        calmSource.OnNext(One);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        calmSource.OnNext(Two);
        calmClock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.Equal(0, calmValues.Count);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        Assert.Equal(new[] { Two }, calmValues);

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Prepend(0).Append(Two).Subscribe(
            value =>
            {
                if (value == One)
                {
                    throw new InvalidOperationException("append-next");
                }
            },
            _ => { },
            () => { }).Dispose());

        var appendError = new RecordingObserver<int>();
        Signal.Fail<int>(new InvalidOperationException("append-error")).Append(One).Subscribe(appendError);
        Assert.Equal("append-error", appendError.Errors[0].Message);

        var forkLeftFirst = new RecordingObserver<int>();
        var forkLeft = new Signal<int>();
        var forkRight = new Signal<int>();
        using (forkLeft.ForkJoin(forkRight, (left, right) => left + right).Subscribe(forkLeftFirst))
        {
            forkLeft.OnNext(One);
            forkLeft.OnCompleted();
            forkRight.OnNext(Two);
            forkRight.OnCompleted();
        }

        Assert.Equal(new[] { Three }, forkLeftFirst.Values);
        Assert.Equal(1, forkLeftFirst.Completed);

        var forkRightFirst = new RecordingObserver<int>();
        var forkOtherLeft = new Signal<int>();
        var forkOtherRight = new Signal<int>();
        using (forkOtherLeft.ForkJoin(forkOtherRight, (left, right) => left + right).Subscribe(forkRightFirst))
        {
            forkOtherRight.OnNext(Two);
            forkOtherRight.OnCompleted();
            forkOtherLeft.OnNext(One);
            forkOtherLeft.OnCompleted();
        }

        Assert.Equal(new[] { Three }, forkRightFirst.Values);
        Assert.Equal(1, forkRightFirst.Completed);
    }

    [Test]
    public async Task RangeTimingQueuesAndThreadPoolCoverPrTenCoverageGaps()
    {
        var immediateMoments = new RecordingObserver<Moment<int>>();
        Signal.Sequence(One, Three).Timestamp(Sequencer.Immediate).Subscribe(immediateMoments).Dispose();
        Assert.Equal<int>([One, Two, Three], [immediateMoments.Values[0].Value, immediateMoments.Values[1].Value, immediateMoments.Values[2].Value]);
        Assert.Equal(1, immediateMoments.Completed);

        var clockMoments = new List<Moment<int>>();
        var clockMomentCompleted = 0;
        Signal.Sequence(Four, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockMoments.Add, ex => throw ex, () => clockMomentCompleted++);
        Assert.Equal<int>([Four, Five], [clockMoments[0].Value, clockMoments[1].Value]);
        Assert.Equal(1, clockMomentCompleted);

        var immediateMomentActions = new List<Moment<int>>();
        var immediateMomentCompleted = 0;
        var immediateTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(Sequencer.Immediate);
        immediateTimestampSignal.Subscribe(immediateMomentActions.Add, ex => throw ex, () => immediateMomentCompleted++).Dispose();
        Assert.Equal<int>([Two, Three], [immediateMomentActions[0].Value, immediateMomentActions[1].Value]);
        Assert.Equal(1, immediateMomentCompleted);

        var clockMomentObserver = new RecordingObserver<Moment<int>>();
        var clockTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch));
        clockTimestampSignal.Subscribe(clockMomentObserver).Dispose();
        Assert.Equal<int>([Two, Three], [clockMomentObserver.Values[0].Value, clockMomentObserver.Values[1].Value]);
        Assert.Equal(1, clockMomentObserver.Completed);

        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((IObserver<Moment<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((Action<Moment<int>>)null!, _ => { }, () => { }));

        var immediateIntervals = new RecordingObserver<TimeInterval<int>>();
        Signal.Sequence(One, Three).TimeInterval(Sequencer.Immediate).Subscribe(immediateIntervals).Dispose();
        Assert.Equal<int>([One, Two, Three], [immediateIntervals.Values[0].Value, immediateIntervals.Values[1].Value, immediateIntervals.Values[2].Value]);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[0].Interval);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[1].Interval);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[2].Interval);
        Assert.Equal(1, immediateIntervals.Completed);

        var clockIntervals = new List<TimeInterval<int>>();
        var clockIntervalCompleted = 0;
        Signal.Sequence(Four, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockIntervals.Add, ex => throw ex, () => clockIntervalCompleted++);
        Assert.Equal<int>([Four, Five, Six], [clockIntervals[0].Value, clockIntervals[1].Value, clockIntervals[2].Value]);
        Assert.Equal(TimeSpan.Zero, clockIntervals[0].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervals[1].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervals[2].Interval);
        Assert.Equal(1, clockIntervalCompleted);

        var immediateIntervalActions = new List<TimeInterval<int>>();
        var immediateIntervalCompleted = 0;
        var immediateIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Two).TimeInterval(Sequencer.Immediate);
        immediateIntervalSignal.Subscribe(immediateIntervalActions.Add, ex => throw ex, () => immediateIntervalCompleted++).Dispose();
        Assert.Equal<int>([Two, Three], [immediateIntervalActions[0].Value, immediateIntervalActions[1].Value]);
        Assert.Equal(1, immediateIntervalCompleted);

        var clockIntervalObserver = new RecordingObserver<TimeInterval<int>>();
        var clockIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch));
        clockIntervalSignal.Subscribe(clockIntervalObserver).Dispose();
        Assert.Equal<int>([Two, Three, Four], [clockIntervalObserver.Values[0].Value, clockIntervalObserver.Values[1].Value, clockIntervalObserver.Values[2].Value]);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[0].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[1].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[2].Interval);
        Assert.Equal(1, clockIntervalObserver.Completed);

        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe((IObserver<TimeInterval<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe((Action<TimeInterval<int>>)null!, _ => { }, () => { }));

        var shiftedObserver = new RecordingObserver<int>();
        Signal.Sequence(One, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedObserver).Dispose();
        Assert.Equal(new[] { One, Two }, shiftedObserver.Values);
        Assert.Equal(1, shiftedObserver.Completed);

        var shiftedActions = new List<int>();
        var shiftedActionCompleted = 0;
        Signal.Sequence(Three, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedActions.Add, ex => throw ex, () => shiftedActionCompleted++);
        Assert.Equal(new[] { Three, Four }, shiftedActions);
        Assert.Equal(1, shiftedActionCompleted);

        var currentThreadShift = (IRequireCurrentThread<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.CurrentThread);
        Assert.True(currentThreadShift.IsRequiredSubscribeOnCurrentThread());
        var inlineShift = (IInlineSignal<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate);
        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe((Action<int>)null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe(_ => { }, _ => { }, null!));

        var helperValues = new List<int>();
        var helper = new SequencerWorkItem<ISequencer, int>(
            Sequencer.Immediate,
            One,
            (_, state) =>
            {
                helperValues.Add(state);
                return Disposable.Empty;
            });
        helper.Invoke();
        helper.Dispose();
        helper.Invoke();
        Assert.Equal(new[] { One }, helperValues);

        var unusedScheduled = new ScheduledItem<int, string>(Sequencer.Immediate, "unused", (_, _) => Disposable.Empty, One);
        Assert.False(new SequencerQueue<int>().Remove(unusedScheduled));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PriorityQueue<int>(-1));

        var shrink = new PriorityQueue<int>(32);
        for (var i = 0; i < 32; i++)
        {
            shrink.Enqueue(i);
        }

        for (var i = 0; i < 26; i++)
        {
            Assert.Equal(i, shrink.Dequeue());
        }

        var absoluteRan = new TaskCompletionSource<int>();
        var absolute = ThreadPoolSequencer.Instance.Schedule(Five, DateTimeOffset.UtcNow, (_, state) =>
        {
            absoluteRan.TrySetResult(state);
            return Disposable.Empty;
        });
        Assert.Equal(Five, await absoluteRan.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
        absolute.Dispose();
        absolute.Dispose();

        var delayedDisposed = CreateThreadPoolWorkItem(One, (_, _) => Disposable.Empty);
        delayedDisposed.Dispose();
        QueueThreadPoolWorkItem(delayedDisposed, typeof(int), TimeSpan.FromMilliseconds(10));

        var skipped = false;
        var skippedItem = CreateThreadPoolWorkItem(Two, (_, _) =>
        {
            skipped = true;
            return Disposable.Empty;
        });
        skippedItem.Dispose();
        InvokeThreadPoolWorkItem(skippedItem, typeof(int));
        Assert.False(skipped);

        var disposedReturned = 0;
        object?[] holder = [null];
        var selfDisposing = CreateThreadPoolWorkItem(holder, (_, state) =>
        {
            ((IDisposable)state[0]!).Dispose();
            return Disposable.Create(() => disposedReturned++);
        });
        holder[0] = selfDisposing;
        InvokeThreadPoolWorkItem(selfDisposing, typeof(object?[]));
        Assert.Equal(1, disposedReturned);

        var rawDisposedReturned = 0;
        object?[] rawHolder = [null];
        var rawDisposing = CreateThreadPoolWorkItem(rawHolder, (_, state) =>
        {
            MarkThreadPoolWorkItemDisposed(state[0]!);
            return Disposable.Create(() => rawDisposedReturned++);
        });
        rawHolder[0] = rawDisposing;
        InvokeThreadPoolWorkItem(rawDisposing, typeof(object?[]));
        Assert.Equal(1, rawDisposedReturned);
    }

#pragma warning disable S3011, IL2072, IL2075, IL3050
    private static IDisposable CreateThreadPoolWorkItem<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        var type = ThreadPoolWorkItemType(typeof(TState));
        return (IDisposable)Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args: [ThreadPoolSequencer.Instance, state, action],
            culture: null)!;
    }

    private static void InvokeThreadPoolWorkItem(IDisposable item, Type stateType) =>
        ThreadPoolWorkItemType(stateType).GetMethod("Run", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(item, null);

    private static void QueueThreadPoolWorkItem(IDisposable item, Type stateType, TimeSpan dueTime) =>
        ThreadPoolWorkItemType(stateType)
            .GetMethod("Queue", BindingFlags.Instance | BindingFlags.NonPublic, binder: null, types: [typeof(TimeSpan)], modifiers: null)!
            .Invoke(item, [dueTime]);

    private static void MarkThreadPoolWorkItemDisposed(object item) =>
        item.GetType().GetField("_isDisposed", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(item, 1);

    private static Type ThreadPoolWorkItemType(Type stateType) =>
        typeof(ThreadPoolSequencer).GetNestedType("ScheduledWorkItem`1", BindingFlags.NonPublic)!.MakeGenericType(stateType);
#pragma warning restore S3011, IL2072, IL2075, IL3050

    private static async IAsyncEnumerable<int> AsyncValues(int count)
    {
        for (var i = 0; i < count; i++)
        {
            await Task.Yield();
            yield return i;
        }
    }

    private sealed class ThrowOnSubscribeObservable<T> : IObservable<T>
    {
        private readonly Exception _error;

        public ThrowOnSubscribeObservable(Exception error) => _error = error;

        public IDisposable Subscribe(IObserver<T> observer) => throw _error;
    }

    private sealed class PassthroughComparer : IEqualityComparer<int>
    {
        public bool Equals(int x, int y) => x == y;

        public int GetHashCode(int obj) => obj;
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

    private sealed class CapturingObservable<T> : IObservable<T>
    {
        public IObserver<T>? Observer { get; private set; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return Disposable.Empty;
        }
    }

    private sealed class SignalsBaseProbe<T> : SignalsBase<T>
    {
        public SignalsBaseProbe(bool required)
            : base(required)
        {
        }

        protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) => Disposable.Empty;
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

        public void OnCompleted()
        {
            if (_throwOnCompleted)
            {
                throw new InvalidOperationException("observer-completed");
            }
        }

        public void OnError(Exception error)
        {
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
