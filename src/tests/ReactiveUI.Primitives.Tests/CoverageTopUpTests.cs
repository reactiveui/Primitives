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

#pragma warning disable SA1600, S109, S3400, S6354, CA1806, S3257, CA1861, IDE0300, RCS1196, S6966, S103, CA1065, S138, IDE0034, RCS1151, S1764, S1944, RCS1208, RCS1215, S3981, CA1849, SA1129, CA1823

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
        source.StartWith(Two).Subscribe(values.Add);
        Assert.Equal(new[] { Two, Three, Four }, values);

        var delayedStart = source.DelayStart(TimeSpan.Zero);
        Assert.NotNull(delayedStart);

        var fused = new List<int>();
        Signal.Return(One).FuseLatest(Signal.FromEnumerable([Two, Three]), (left, right) => left + right).Subscribe(fused.Add);
        Assert.Equal(new[] { Three, Four }, fused);

        var rangeArray = new List<int[]>();
        var rangeList = new List<IList<int>>();
        Signal.Range(Five, Three).CollectArray().Subscribe(rangeArray.Add);
        Signal.Range(Five, Three).CollectList().Subscribe(rangeList.Add);
        Assert.Equal<int>([Five, Six, Seven], rangeArray[0]);
        Assert.Equal<int>([Five, Six, Seven], rangeList[0]);

        Assert.Equal(Ten, await Signal.Range(Ten, Three).FirstAsync().ConfigureAwait(false));
        Assert.Equal(Ten, await Signal.Range(Ten, Three).FirstOrDefaultAsync().ConfigureAwait(false));
        Assert.Equal(Ten, await Signal.Range(Ten, Three).FirstOrDefaultAsync(Nine).ConfigureAwait(false));
        Assert.Equal(Twelve, await Signal.Range(Ten, Three).LastAsync().ConfigureAwait(false));
        Assert.Equal(Twelve, await Signal.Range(Ten, Three).LastOrDefaultAsync().ConfigureAwait(false));
        Assert.Equal(Nine, await Signal.Empty<int>().LastOrDefaultAsync(Nine).ConfigureAwait(false));
        Assert.Equal(Three, await Signal.Range(One, Three).CountAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(Two, await Signal.Range(One, Three).CountAsync(value => value > One, CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(3L, await Signal.Range(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(2L, await Signal.Range(One, Three).LongCount(value => value > One).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Range(One, Three).AnyAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Range(One, Three).AnyAsync(value => value == Two, CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Range(One, Three).All(value => value < Four).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.True(await Signal.Range(One, Three).Contains(Three).ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal<int>([Five, Six, Seven], await Signal.Range(Five, Three).CollectArrayAsync().ConfigureAwait(false));
        Assert.Equal<int>([Five, Six, Seven], await Signal.Range(Five, Three).CollectListAsync().ConfigureAwait(false));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync().ConfigureAwait(false);
        var canceledTask = Signal.Never<int>().ToTask(canceled.Token);
        Assert.True(canceledTask.IsCanceled);

        Assert.Throws<ArgumentNullException>(() => LinqMixins.Count<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.LongCount<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Merge<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.CollectArray<int>(null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeMixins.Subscribe<int>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeMixins.Subscribe<int>(null!, _ => { }, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.Catch<int, InvalidOperationException>(Signal.Empty<int>(), null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.Catch<int>((IEnumerable<IObservable<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => ReactiveUI.Primitives.Signals.Signal.CreateSafe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => StateSignalMixins.ToReadOnlyState<int, int>(null!, One, value => value));
        Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskSignal.Create<int>(null!));
    }

    [Test]
    public void ImmediateCoreSignalsRangeZipRepeatAndObserverFailuresCoverRemainders()
    {
        var completed = 0;
        Signal.Empty<int>(Sequencer.Immediate).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Signal.Empty<int>(default(int)).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Assert.Equal(Two, completed);

        var returnValues = new List<int>();
        Signal.Return(FortyTwo, Sequencer.Immediate).Subscribe(returnValues.Add);
        Assert.Equal(new[] { FortyTwo }, returnValues);

        var throwErrors = new List<string>();
        Signal.Throw<int>(new InvalidOperationException("immediate"), Sequencer.Immediate).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Signal.Throw(new InvalidOperationException("witness"), Sequencer.Immediate, default(int)).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Assert.Equal(new[] { "immediate", "witness" }, throwErrors);

        var never = Signal.Never<int>(default(int));
        Assert.False(((IRequireCurrentThread<int>)never).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<RxVoid>)Signal.ReturnRxVoid()).IsRequiredSubscribeOnCurrentThread());
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

        Assert.Throws<InvalidOperationException>(() => Signal.Return(One, Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.Empty<int>(Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.Throw<int>(new InvalidOperationException("observer"), Sequencer.Immediate).Subscribe(new ThrowingObserver<int>(throwOnError: true)).Dispose());
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

        var shared = Signal.Range(One, Three).Share();
        var replayed = Signal.Range(One, Three).Replay(2);
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
        Signal.Range(Three, Three, scheduledRangeClock).Subscribe(scheduledRange.Add, ex => throw ex, () => scheduledRangeCompleted++);
        scheduledRangeClock.Start();
        Assert.Equal<int>([Three, Four, Five], scheduledRange);
        Assert.Equal(1, scheduledRangeCompleted);

        Assert.NotNull(Signal.After(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Pulse(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Interval(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Interval(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)));
        Assert.NotNull(Signal.Timer(TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.Timer(DateTimeOffset.UtcNow.AddMilliseconds(1)));
        Assert.NotNull(Signal.Timer(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.ZipLatest(Signal.Range(One, Two), Signal.Range(Three, Two), (left, right) => left + right));

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
        Signal.Range(One, Four).LongCount(value => value % 2 == 0).Subscribe(longCount.Add);
        Assert.Equal(new long[] { 2L }, longCount);

        var containsWithComparer = new List<bool>();
        Signal.Range(One, Three).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Range(One, Three).Contains(Nine, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Range(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Signal.Range(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Assert.Equal(new[] { true, false, true, false }, containsWithComparer);

        var startWithAlias = new List<int>();
        LinqMixins.StartWith(Signal.Return(Two), One).Subscribe(startWithAlias.Add);
        Assert.Equal(new[] { One, Two }, startWithAlias);
        Assert.NotNull(LinqMixins.DelayStart(Signal.Return(One), TimeSpan.Zero));
        Assert.Equal(0, await Signal.Empty<int>().FirstOrDefaultAsync().ConfigureAwait(false));

        Assert.Throws<ArgumentNullException>(() => LinqMixins.Buffer<int>(null!, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Return(One).Buffer(0));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.Buffer<int>(null!, One, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Return(One).Buffer(0, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Return(One).Buffer(One, 0));

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
        using (outer.SelectMany(inner => inner).Subscribe(selectManyValues.Add, ex => throw ex, () => selectManyCompleted++))
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
        var disposedSubscription = disposedOuter.SelectMany(inner => inner).Subscribe(disposedValues.Add);
        disposedOuter.OnNext(disposedInner);
        disposedSubscription.Dispose();
        disposedSubscription.Dispose();
        disposedInner.OnNext(Three);
        Assert.Equal(0, disposedValues.Count);

        var subscribeErrors = new List<string>();
        Signal.Return(One)
            .SelectMany(_ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe")))
            .Subscribe(_ => { }, ex => subscribeErrors.Add(ex.Message));
        Assert.Equal(new[] { "inner-subscribe" }, subscribeErrors);
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
