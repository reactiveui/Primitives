// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Targeted deterministic top-up tests for remaining production coverage gaps.</summary>
public sealed class DeterministicEdgeCaseTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant five.</summary>
    private const int Five = 5;

    /// <summary>The integer constant six.</summary>
    private const int Six = 6;

    /// <summary>The integer constant seven.</summary>
    private const int Seven = 7;

    /// <summary>The integer constant nine.</summary>
    private const int Nine = 9;

    /// <summary>The integer constant ten.</summary>
    private const int Ten = 10;

    /// <summary>The integer constant twelve.</summary>
    private const int Twelve = 12;

    /// <summary>The integer constant forty-two.</summary>
    private const int FortyTwo = 42;

    /// <summary>The integer constant fifteen.</summary>
    private const int Fifteen = 15;

    /// <summary>The integer constant sixteen.</summary>
    private const int Sixteen = 16;

    /// <summary>The integer constant seventeen.</summary>
    private const int Seventeen = 17;

    /// <summary>The integer constant twenty.</summary>
    private const int Twenty = 20;

    /// <summary>The integer constant twenty-six.</summary>
    private const int TwentySix = 26;

    /// <summary>The integer constant thirty-two.</summary>
    private const int ThirtyTwo = 32;

    /// <summary>The long constant two.</summary>
    private const long TwoLong = 2L;

    /// <summary>The long constant three.</summary>
    private const long ThreeLong = 3L;

    /// <summary>The integer constant for the fixed timestamp year.</summary>
    private const int FixedTimestampYear = 2024;

    /// <summary>The expected single-element string sequence containing "value".</summary>
    private static readonly string[] ExpectedSingleValue = ["value"];

    /// <summary>The expected immediate and witness error messages.</summary>
    private static readonly string[] ExpectedImmediateWitness = ["immediate", "witness"];

    /// <summary>The expected contains-with-comparer results in subscription order.</summary>
    private static readonly bool[] ExpectedContainsResults = [true, false, true, false];

    /// <summary>The expected long count sequence containing a single two.</summary>
    private static readonly long[] ExpectedSingleTwoLong = [TwoLong];

    /// <summary>The error message produced when a FlatMap selector returns null.</summary>
    private static readonly string[] ExpectedFlatMapSelectorNull = ["The FlatMap selector returned null."];

    /// <summary>The error message produced when a FlatMap collection selector returns null.</summary>
    private static readonly string[] ExpectedFlatMapCollectionSelectorNull = ["The FlatMap collection selector returned null."];

    /// <summary>The error message produced when the FlatMap result inner sequence fails.</summary>
    private static readonly string[] ExpectedResultInner = ["result-inner"];

    /// <summary>The error message produced when the FlatMap inner subscription fails.</summary>
    private static readonly string[] ExpectedInnerSubscribe = ["inner-subscribe"];

    /// <summary>The fixed deterministic timestamp used in place of the current time.</summary>
    private static readonly DateTimeOffset FixedTimestamp = new(FixedTimestampYear, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies parity aliases, range async fast paths, and guard clauses cover remaining lines.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S6966:Awaitable method should be used", Justification = "Synchronous CollectArray/CollectList operators are deliberately covered.")]
    public async Task ParityAliasesRangeAsyncFastPathsAndGuardsCoverRemainingLines()
    {
        IObservable<int> source = Signal.FromEnumerable([Three, Four]);
        var values = new List<int>();
        source.Prepend(Two).Subscribe(values.Add);
        int[] expectedValues = [Two, Three, Four];
        Assert.Equal(expectedValues, values);

        var delayedStart = source.DelayStart(TimeSpan.Zero);
        Assert.NotNull(delayedStart);
        Assert.NotNull(source.DelaySubscription(TimeSpan.Zero));
        Assert.NotNull(source.DelaySubscription(TimeSpan.Zero, Sequencer.Immediate));
        Assert.NotNull(source.Stabilize(TimeSpan.Zero));
        Assert.NotNull(source.Stabilize(TimeSpan.Zero, Sequencer.Immediate));

        var fused = new List<int>();
        Signal.Emit(One).FuseLatest(Signal.FromEnumerable([Two, Three]), (left, right) => left + right).Subscribe(fused.Add);
        int[] expectedFused = [Three, Four];
        Assert.Equal(expectedFused, fused);

        var chainedStrings = new List<string>();
        Signal.Chain(Signal.Emit("value")).Subscribe(chainedStrings.Add);
        Assert.Equal(ExpectedSingleValue, chainedStrings);

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
        Assert.Equal(ThreeLong, await Signal.Sequence(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false));
        Assert.Equal(TwoLong, await Signal.Sequence(One, Three).LongCount(value => value > One).ToTask(CancellationToken.None).ConfigureAwait(false));
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

        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Count<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.LongCount<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Blend<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.CollectArray<int>(null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, _ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(null!, _ => { }, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Recover<int, InvalidOperationException>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<IObservable<int>>)null!).Recover());
        Assert.Throws<ArgumentNullException>(() => Signal.CreateSafe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => StateSignalExtensions.ToReadOnlyState<int, int>(null!, One, value => value));
        Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskSignal.Create<int>(null!));
    }

    /// <summary>Verifies immediate core signals, range, zip, repeat, and observer failures cover remainders.</summary>
    [Test]
    public void ImmediateCoreSignalsRangeZipRepeatAndObserverFailuresCoverRemainders()
    {
        var completed = 0;
        Signal.None<int>(Sequencer.Immediate).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Signal.None(0).Subscribe(_ => { }, ex => throw ex, () => completed++);
        Assert.Equal(Two, completed);

        var returnValues = new List<int>();
        Signal.Emit(FortyTwo, Sequencer.Immediate).Subscribe(returnValues.Add);
        int[] expectedReturnValues = [FortyTwo];
        Assert.Equal(expectedReturnValues, returnValues);

        var throwErrors = new List<string>();
        Signal.Fail<int>(new InvalidOperationException("immediate"), Sequencer.Immediate).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Signal.Fail(new InvalidOperationException("witness"), Sequencer.Immediate, 0).Subscribe(_ => { }, ex => throwErrors.Add(ex.Message));
        Assert.Equal(ExpectedImmediateWitness, throwErrors);

        var never = Signal.Silent(0);
        Assert.False(((IRequireCurrentThread<int>)never).IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<RxVoid>)Signal.EmitRxVoid()).IsRequiredSubscribeOnCurrentThread());
        RxVoid firstRxVoid = default;
        RxVoid secondRxVoid = default;
        Assert.True(firstRxVoid == secondRxVoid);
        Assert.False(firstRxVoid != secondRxVoid);

        var repeat = new RepeatSignal<int>(Seven, Three);
        var repeatValues = new List<int>();
        Assert.False(repeat.IsRequiredSubscribeOnCurrentThread());
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        repeat.Subscribe(repeatValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedRepeatValues = [Seven, Seven, Seven];
        Assert.Equal(expectedRepeatValues, repeatValues);
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe(null!, _ => { }, () => { }));

        var range = new RangeSignal(One, Three);
        var rangeValues = new List<int>();
        Assert.False(range.IsRequiredSubscribeOnCurrentThread());
        range.Subscribe(new RecordingWitness<int>()).Dispose();
        range.Subscribe(rangeValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedRangeValues = [One, Two, Three];
        Assert.Equal(expectedRangeValues, rangeValues);
        Assert.Throws<ArgumentNullException>(() => range.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => range.Subscribe(null!, _ => { }, () => { }));

        var zip = new RangeZipSignal<int>(new(One, Three), new(Four, Three), (left, right) => left + right);
        var zipValues = new List<int>();
        Assert.False(zip.IsRequiredSubscribeOnCurrentThread());
        zip.Subscribe(new RecordingWitness<int>()).Dispose();
        zip.Subscribe(zipValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedZipValues = [Five, Seven, Nine];
        Assert.Equal(expectedZipValues, zipValues);
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe(null!, _ => { }, () => { }));

        Assert.False(new ImmediateReturnSignal<int>(One).IsRequiredSubscribeOnCurrentThread());
        Assert.False(new ImmediateThrowSignal<int>(new InvalidOperationException("fast")).IsRequiredSubscribeOnCurrentThread());
        Assert.False(ImmutableEmptySignal<int>.Instance.IsRequiredSubscribeOnCurrentThread());
        Assert.False(ImmutableNeverSignal<int>.Instance.IsRequiredSubscribeOnCurrentThread());
        Assert.False(((IRequireCurrentThread<int>)ImmutableReturnInt32Signal.GetInt32Signals(One)).IsRequiredSubscribeOnCurrentThread());
        Assert.False(new RangeConcatSignal([new(One, Two), new(Three, Two)]).IsRequiredSubscribeOnCurrentThread());
        Assert.False(new SignalsBaseProbe<int>(false).IsRequiredSubscribeOnCurrentThread());

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.None<int>(Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Fail<int>(new InvalidOperationException("observer"), Sequencer.Immediate)
                .Subscribe(new ThrowingWitness<int>(throwOnError: true))
                .Dispose());
        Assert.Throws<ArgumentNullException>(() => new ImmediateThrowSignal<int>(new InvalidOperationException("null-observer")).Subscribe((IObserver<int>)null!));
    }

    /// <summary>Verifies subjects, replay, behavior, state, and connectable aliases cover late terminal branches.</summary>
    [Test]
    public void SubjectsReplayBehaviorStateAndConnectableAliasesCoverLateTerminalBranches()
    {
        var behavior = new BehaviorSignal<int>(One);
        Assert.True(behavior.ToString()!.Contains(nameof(BehaviorSignal<int>), StringComparison.Ordinal));
        var initial = new RecordingWitness<int>();
        using var behaviorSubscription = behavior.Subscribe(initial);
        behavior.OnCompleted();
        behavior.OnCompleted();
        behavior.OnNext(Two);
        var lateCompleted = new RecordingWitness<int>();
        behavior.Subscribe(lateCompleted).Dispose();
        int[] expectedInitial = [One];
        Assert.Equal(expectedInitial, initial.Values);
        Assert.Equal(1, lateCompleted.Completed);

        var behaviorError = new BehaviorSignal<int>(One);
        behaviorError.OnError(new InvalidOperationException("behavior"));
        behaviorError.OnError(new InvalidOperationException("late"));
        var lateError = new RecordingWitness<int>();
        behaviorError.Subscribe(lateError).Dispose();
        Assert.Equal("behavior", lateError.Errors[0].Message);
        behaviorError.Dispose();
        behaviorError.Dispose();
        Assert.False(behaviorError.TryGetValue(out _));

        var replayCompleted = new ReplaySignal<int>(bufferSize: Two, window: TimeSpan.MaxValue, scheduler: Sequencer.CurrentThread);
        replayCompleted.OnNext(One);
        replayCompleted.OnNext(Two);
        replayCompleted.OnNext(Three);
        replayCompleted.OnCompleted();
        replayCompleted.OnCompleted();
        replayCompleted.OnNext(Four);
        var replayLateCompleted = new RecordingWitness<int>();
        replayCompleted.Subscribe(replayLateCompleted).Dispose();
        int[] expectedReplayLateCompleted = [Two, Three];
        Assert.Equal(expectedReplayLateCompleted, replayLateCompleted.Values);
        Assert.Equal(1, replayLateCompleted.Completed);

        var replayError = new ReplaySignal<int>(bufferSize: 1, window: TimeSpan.MaxValue, scheduler: Sequencer.CurrentThread);
        replayError.OnNext(Five);
        replayError.OnError(new InvalidOperationException("replay"));
        replayError.OnError(new InvalidOperationException("late"));
        var replayLateError = new RecordingWitness<int>();
        replayError.Subscribe(replayLateError).Dispose();
        int[] expectedReplayLateError = [Five];
        Assert.Equal(expectedReplayLateError, replayLateError.Values);
        Assert.Equal("replay", replayLateError.Errors[0].Message);
        replayError.Dispose();
        replayError.Dispose();
        Assert.Throws<ObjectDisposedException>(() => replayError.Subscribe(new RecordingWitness<int>()));

        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var windowedReplay = new ReplaySignal<int>(bufferSize: Ten, window: TimeSpan.FromTicks(Two), scheduler: clock);
        windowedReplay.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        windowedReplay.OnNext(Two);
        var windowedLate = new RecordingWitness<int>();
        windowedReplay.Subscribe(windowedLate).Dispose();
        int[] expectedWindowedLate = [Two];
        Assert.Equal(expectedWindowedLate, windowedLate.Values);

        var shared = Signal.Sequence(One, Three).Share();
        var replayed = Signal.Sequence(One, Three).Replay(Two);
        Assert.NotNull(shared);
        Assert.NotNull(replayed);

        var state = Assert.Throws<ArgumentNullException>(() => new StateSignal<int>(One).ToReadOnlyState<int>(null!));
        Assert.Equal("selector", state.ParamName);
    }

    /// <summary>Verifies low-level disposables, collections, and schedulers cover deterministic edges.</summary>
    [Test]
    public void LowLevelDisposablesCollectionsAndSchedulersCoverDeterministicEdges()
    {
        var multiple = new MultipleDisposable();
        for (var i = 0; i < Twenty; i++)
        {
            multiple.Add(EmptyDisposable.Instance);
        }

        Assert.True(multiple.Remove(EmptyDisposable.Instance));
        Assert.False(multiple.Remove(new ActionDisposable(() => { })));
        Assert.Throws<ArgumentNullException>(() => _ = new MultipleDisposable((IDisposable[])null!));
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
        var observerList = ImmutableList<IObserver<int>>.Empty.Add(new RecordingWitness<int>());
        var witness = new ListWitness<int>(observerList);
        Assert.True(witness.HasObservers);
        Assert.NotNull(witness.Add(new RecordingWitness<int>()));

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
            return EmptyDisposable.Instance;
        }).Dispose();
        current.Schedule(One, FixedTimestamp.AddMilliseconds(1), (_, state) =>
        {
            scheduled.Add(state + One);
            return EmptyDisposable.Instance;
        }).Dispose();
        Assert.Equal(Two, scheduled.Count);
    }

    /// <summary>Verifies remaining operator, factory, and observer failure branches are deterministic.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RemainingOperatorFactoryAndObserverFailureBranchesAreDeterministic()
    {
        VerifyScheduledRangeAndTimingFactories();
        await VerifyTaskSignalsCountAndContainsAsync().ConfigureAwait(false);
        await VerifyAliasGuardsAndNullArgumentChecksAsync().ConfigureAwait(false);
        VerifyObserverFailureBranchesAndMap();
        VerifyMultiSubscriberOnErrorThrows();
        VerifyFlatMapTerminalAndErrorBranches();
    }

    /// <summary>Verifies optimized coordinator and async enumerable branches cover PR nine gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OptimizedCoordinatorAndAsyncEnumerableBranchesCoverPrNineGaps()
    {
        await VerifyAsyncEnumerableShiftAndExpireAsync().ConfigureAwait(false);
        VerifyRaceSyncLatestAndSwitchBranches();
        VerifyProbeBranches();
        VerifyCalmAppendAndForkJoinBranches();
    }

    /// <summary>Verifies range timing, queues, and thread pool cover PR ten coverage gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RangeTimingQueuesAndThreadPoolCoverPrTenCoverageGaps()
    {
        VerifyTimestampBranches();
        VerifyTimeIntervalBranches();
        VerifyDelayStartAndWorkItemBranches();
        await VerifyThreadPoolWorkItemBranchesAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies the scheduled range fast path and the timing factory aliases.</summary>
    private static void VerifyScheduledRangeAndTimingFactories()
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
        Assert.NotNull(Signal.After(FixedTimestamp.AddMilliseconds(1)));
        Assert.NotNull(Signal.After(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One)));
        Assert.NotNull(Signal.PairLatest(Signal.Sequence(One, Two), Signal.Sequence(Three, Two), (left, right) => left + right));

        var toSignalValues = new List<int>();
        new[] { One, Two }.ToSignal().Subscribe(toSignalValues.Add);
        new[] { Three, Four }.ToSignal(CancellationToken.None).Subscribe(toSignalValues.Add);
        int[] expectedToSignalValues = [One, Two, Three, Four];
        Assert.Equal(expectedToSignalValues, toSignalValues);
    }

    /// <summary>Verifies task-backed signals, long count, and contains operators.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyTaskSignalsCountAndContainsAsync()
    {
        var firstTaskSignal = await Signal.FromTask(_ => Task.FromResult(Five)).FirstAsync().ConfigureAwait(false);
        var secondTaskSignal = await Signal.FromTask(_ => Task.FromResult(Six), Sequencer.Immediate).FirstAsync().ConfigureAwait(false);
        Assert.Equal(Five, firstTaskSignal);
        Assert.Equal(Six, secondTaskSignal);
        Assert.Equal(Seven, await Task.FromResult(Seven).HandleCancellation().ConfigureAwait(false));
        Assert.Equal(0, await Task.FromCanceled<int>(new(true)).HandleCancellation().ConfigureAwait(false));

        var longCount = new List<long>();
        Signal.Sequence(One, Four).LongCount(value => value % Two == 0).Subscribe(longCount.Add);
        Assert.Equal(ExpectedSingleTwoLong, longCount);

        var containsWithComparer = new List<bool>();
        Signal.Sequence(One, Three).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Assert.Equal(ExpectedContainsResults, containsWithComparer);
    }

    /// <summary>Verifies alias operators, buffer guard clauses, null-argument guards, and cancellation.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAliasGuardsAndNullArgumentChecksAsync()
    {
        var startWithAlias = new List<int>();
        Signal.Emit(Two).Prepend(One).Subscribe(startWithAlias.Add);
        int[] expectedStartWithAlias = [One, Two];
        Assert.Equal(expectedStartWithAlias, startWithAlias);
        Assert.NotNull(Signal.Emit(One).DelayStart(TimeSpan.Zero));
        Assert.Equal(0, await Signal.None<int>().FirstOrDefaultAsync().ConfigureAwait(false));
        var noneWitnessCompleted = 0;
        Signal.None(Sequencer.Immediate, One).Subscribe(_ => { }, ex => throw ex, () => noneWitnessCompleted++);
        Assert.Equal(1, noneWitnessCompleted);

        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Buffer<int>(null!, One));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(0));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Buffer<int>(null!, One, One));
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
    }

    /// <summary>Verifies immediate signal observer failure branches and the map late-notification branch.</summary>
    private static void VerifyObserverFailureBranchesAndMap()
    {
        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new EmptySignal<int>(Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() =>
            new ThrowSignal<int>(new InvalidOperationException("throw-signal"), Sequencer.Immediate)
                .Subscribe(new ThrowingWitness<int>(throwOnError: true))
                .Dispose());

        var returnWitness = new ReturnSignal<int>.Return(new RecordingWitness<int>(), EmptyDisposable.Instance);
        returnWitness.OnError(new InvalidOperationException("return-inner"));
        var emptyWitness = new EmptySignal<int>.Empty(new RecordingWitness<int>(), EmptyDisposable.Instance);
        emptyWitness.OnNext(One);
        emptyWitness.OnError(new InvalidOperationException("empty-inner"));

        var mapObserver = new RecordingWitness<int>();
        var badSource = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-map"));
            observer.OnCompleted();
        });
        badSource.Map(value => value).Subscribe(mapObserver).Dispose();
        int[] expectedMapObserver = [One];
        Assert.Equal(expectedMapObserver, mapObserver.Values);
        Assert.Equal(1, mapObserver.Completed);
    }

    /// <summary>Verifies the multi-subscriber signal raises when it errors with many observers attached.</summary>
    private static void VerifyMultiSubscriberOnErrorThrows()
    {
        var signal = new Signal<int>();
        Assert.Throws<ArgumentNullException>(() => signal.Subscribe((Action<int>)null!));
        var actionValues = new List<int>();
        using var actionSubscription = signal.Subscribe(actionValues.Add);
        using var s1 = signal.Subscribe(new RecordingWitness<int>());
        using var s2 = signal.Subscribe(new RecordingWitness<int>());
        using var s3 = signal.Subscribe(new RecordingWitness<int>());
        using var s4 = signal.Subscribe(new RecordingWitness<int>());
        using var s5 = signal.Subscribe(new RecordingWitness<int>());
        Assert.Throws<InvalidOperationException>(() => signal.OnError(new InvalidOperationException("many")));
    }

    /// <summary>Verifies FlatMap terminal completion, disposal, and the null-selector and inner-error branches.</summary>
    private static void VerifyFlatMapTerminalAndErrorBranches()
    {
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

        int[] expectedSelectManyValues = [One, Two];
        Assert.Equal(expectedSelectManyValues, selectManyValues);
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
        Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner, (_, right) => right).Subscribe(null!));

        var nullSelectorErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int>(_ => null!).Subscribe(_ => { }, ex => nullSelectorErrors.Add(ex.Message));
        Assert.Equal(ExpectedFlatMapSelectorNull, nullSelectorErrors);

        var nullCollectionErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int, int>(_ => null!, (left, right) => left + right).Subscribe(_ => { }, ex => nullCollectionErrors.Add(ex.Message));
        Assert.Equal(ExpectedFlatMapCollectionSelectorNull, nullCollectionErrors);

        var resultInnerErrors = new List<string>();
        Signal.Emit(One).FlatMap(_ => Signal.Fail<int>(new InvalidOperationException("result-inner")), (left, right) => left + right).Subscribe(_ => { }, ex => resultInnerErrors.Add(ex.Message));
        Assert.Equal(ExpectedResultInner, resultInnerErrors);

        var subscribeErrors = new List<string>();
        Signal.Emit(One)
            .FlatMap(_ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe")))
            .Subscribe(_ => { }, ex => subscribeErrors.Add(ex.Message));
        Assert.Equal(ExpectedInnerSubscribe, subscribeErrors);
    }

    /// <summary>Verifies async enumerable subscription, shift timing, and expire timeout branches.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAsyncEnumerableShiftAndExpireAsync()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable(AsyncValues(One)).Subscribe(null!));

        var asyncValues = new List<int>();
        var asyncCompleted = new TaskCompletionSource<object?>();
        using var asyncToken = new CancellationTokenSource();
        Signal.FromAsyncEnumerable(AsyncValues(Three), asyncToken.Token).Subscribe(
            asyncValues.Add,
            ex => asyncCompleted.TrySetException(ex),
            () => asyncCompleted.TrySetResult(null));
        await asyncCompleted.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false);
        int[] expectedAsyncValues = [0, One, Two];
        Assert.Equal(expectedAsyncValues, asyncValues);

        var exact = await Signal.FromAsyncEnumerable(AsyncValues(Sixteen)).CollectArrayAsync().ConfigureAwait(false);
        var grown = await Signal.FromAsyncEnumerable(AsyncValues(Seventeen)).CollectArrayAsync().ConfigureAwait(false);
        Assert.Equal(Sixteen, exact.Length);
        Assert.Equal(Fifteen, exact[Fifteen]);
        Assert.Equal(Seventeen, grown.Length);
        Assert.Equal(Sixteen, grown[Sixteen]);

        var shiftedClock = new TestClock(DateTimeOffset.UnixEpoch);
        var shifted = new List<int>();
        Signal.Sequence(Three, Three).Shift(TimeSpan.FromTicks(Two), shiftedClock).Subscribe(shifted.Add);
        Assert.Equal(0, shifted.Count);
        shiftedClock.AdvanceBy(TimeSpan.FromTicks(Two));
        int[] expectedShifted = [Three, Four, Five];
        Assert.Equal(expectedShifted, shifted);

        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Expire(TimeSpan.Zero).Subscribe(null!));

        var timeoutClock = new TestClock(DateTimeOffset.UnixEpoch);
        var timeout = new RecordingWitness<int>();
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), timeoutClock).Subscribe(timeout);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(One));
        Assert.True(timeout.Errors[0] is TimeoutException);

        var expireCompleted = new RecordingWitness<int>();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-expire"));
            observer.OnCompleted();
        }).Expire(TimeSpan.FromTicks(Ten), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(expireCompleted);
        int[] expectedExpireCompleted = [One];
        Assert.Equal(expectedExpireCompleted, expireCompleted.Values);
        Assert.Equal(1, expireCompleted.Completed);
        Assert.Equal(0, expireCompleted.Errors.Count);

        var expireError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("expire-error")).Expire(TimeSpan.FromTicks(Ten), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(expireError);
        Assert.Equal("expire-error", expireError.Errors[0].Message);
    }

    /// <summary>Verifies the race, synchronized-latest, and switch coordinator branches.</summary>
    private static void VerifyRaceSyncLatestAndSwitchBranches()
    {
        var raceOuter = new Signal<IObservable<int>>();
        var raceWinner = new Signal<int>();
        var raceLoser = new Signal<int>();
        var race = new RecordingWitness<int>();
        using (raceOuter.Race().Subscribe(race))
        {
            raceOuter.OnNext(raceWinner);
            raceOuter.OnNext(raceLoser);
            raceWinner.OnNext(One);
            raceLoser.OnError(new InvalidOperationException("late-race"));
            raceLoser.OnCompleted();
        }

        int[] expectedRace = [One];
        Assert.Equal(expectedRace, race.Values);
        Assert.Equal(0, race.Errors.Count);

        var raceCompletionOuter = new Signal<IObservable<int>>();
        var raceCompletionWinner = new Signal<int>();
        var raceCompletionLoser = new CapturingObservable<int>();
        var raceCompletion = new RecordingWitness<int>();
        using (raceCompletionOuter.Race().Subscribe(raceCompletion))
        {
            raceCompletionOuter.OnNext(raceCompletionWinner);
            raceCompletionOuter.OnNext(raceCompletionLoser);
            raceCompletionWinner.OnNext(Two);
            raceCompletionLoser.Observer!.OnCompleted();
        }

        int[] expectedRaceCompletion = [Two];
        Assert.Equal(expectedRaceCompletion, raceCompletion.Values);
        Assert.Equal(0, raceCompletion.Completed);

        var combineLeft = new Signal<int>();
        var combineRight = new Signal<int>();
        var combined = new RecordingWitness<int>();
        using (combineLeft.SyncLatest(combineRight, (left, right) => left + right).Subscribe(combined))
        {
            combineRight.OnNext(Two);
            combineLeft.OnNext(One);
            combineRight.OnCompleted();
            combineLeft.OnCompleted();
        }

        int[] expectedCombined = [Three];
        Assert.Equal(expectedCombined, combined.Values);
        Assert.Equal(1, combined.Completed);

        var switchOuter = new Signal<IObservable<int>>();
        var staleInner = new CapturingObservable<int>();
        var currentInner = new CapturingObservable<int>();
        var switched = new RecordingWitness<int>();
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
    }

    /// <summary>
    /// Verifies the probe operator error, disposal, and completion branches alongside the
    /// direct and scheduled current-thread expire and probe branches.
    /// </summary>
    private static void VerifyProbeBranches()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Probe(TimeSpan.Zero).Subscribe(null!));

        var probeError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("probe-error")).Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(probeError);
        Assert.Equal("probe-error", probeError.Errors[0].Message);

        var probeSource = new Signal<int>();
        var probeSubscription = probeSource.Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(new RecordingWitness<int>());
        probeSubscription.Dispose();
        probeSubscription.Dispose();

        var completedProbe = new RecordingWitness<int>();
        new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(One);
        }).Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(completedProbe);
        Assert.Equal(1, completedProbe.Completed);
        Assert.Equal(0, completedProbe.Values.Count);

        var directCurrentThreadExpire = new RecordingWitness<int>();
        var directCurrentThreadProbe = new RecordingWitness<int>();
        Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadExpire);
        Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadProbe);
        int[] expectedDirectCurrentThreadExpire = [One];
        Assert.Equal(expectedDirectCurrentThreadExpire, directCurrentThreadExpire.Values);
        Assert.Equal(1, directCurrentThreadExpire.Completed);
        Assert.Equal(0, directCurrentThreadProbe.Values.Count);
        Assert.Equal(1, directCurrentThreadProbe.Completed);

        var currentThreadExpire = new RecordingWitness<int>();
        var currentThreadProbe = new RecordingWitness<int>();
        Sequencer.CurrentThread.Schedule(() =>
        {
            Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadExpire);
            Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadProbe);
        });
        int[] expectedCurrentThreadExpire = [One];
        Assert.Equal(expectedCurrentThreadExpire, currentThreadExpire.Values);
        Assert.Equal(1, currentThreadExpire.Completed);
        Assert.Equal(0, currentThreadProbe.Values.Count);
        Assert.Equal(1, currentThreadProbe.Completed);
    }

    /// <summary>Verifies the calm debounce, append observer failure, and fork-join completion branches.</summary>
    private static void VerifyCalmAppendAndForkJoinBranches()
    {
        var calmError = new RecordingWitness<int>();
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
        int[] expectedCalmValues = [Two];
        Assert.Equal(expectedCalmValues, calmValues);

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Prepend(0).Append(Two).Subscribe(
            value =>
            {
                if (value != One)
                {
                    return;
                }

                throw new InvalidOperationException("append-next");
            },
            _ => { },
            () => { }).Dispose());

        var appendError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("append-error")).Append(One).Subscribe(appendError);
        Assert.Equal("append-error", appendError.Errors[0].Message);

        var forkLeftFirst = new RecordingWitness<int>();
        var forkLeft = new Signal<int>();
        var forkRight = new Signal<int>();
        using (forkLeft.ForkJoin(forkRight, (left, right) => left + right).Subscribe(forkLeftFirst))
        {
            forkLeft.OnNext(One);
            forkLeft.OnCompleted();
            forkRight.OnNext(Two);
            forkRight.OnCompleted();
        }

        int[] expectedForkLeftFirst = [Three];
        Assert.Equal(expectedForkLeftFirst, forkLeftFirst.Values);
        Assert.Equal(1, forkLeftFirst.Completed);

        var forkRightFirst = new RecordingWitness<int>();
        var forkOtherLeft = new Signal<int>();
        var forkOtherRight = new Signal<int>();
        using (forkOtherLeft.ForkJoin(forkOtherRight, (left, right) => left + right).Subscribe(forkRightFirst))
        {
            forkOtherRight.OnNext(Two);
            forkOtherRight.OnCompleted();
            forkOtherLeft.OnNext(One);
            forkOtherLeft.OnCompleted();
        }

        int[] expectedForkRightFirst = [Three];
        Assert.Equal(expectedForkRightFirst, forkRightFirst.Values);
        Assert.Equal(1, forkRightFirst.Completed);
    }

    /// <summary>Verifies the timestamp operator immediate and clock-backed branches.</summary>
    private static void VerifyTimestampBranches()
    {
        var immediateMoments = new RecordingWitness<Moment<int>>();
        Signal.Sequence(One, Three).Timestamp(Sequencer.Immediate).Subscribe(immediateMoments).Dispose();
        IEnumerable<int> expectedImmediateMoments = [One, Two, Three];
        int[] immediateMomentValues = [immediateMoments.Values[0].Value, immediateMoments.Values[1].Value, immediateMoments.Values[Two].Value];
        Assert.Equal(expectedImmediateMoments, immediateMomentValues);
        Assert.Equal(1, immediateMoments.Completed);

        var clockMoments = new List<Moment<int>>();
        var clockMomentCompleted = 0;
        Signal.Sequence(Four, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockMoments.Add, ex => throw ex, () => clockMomentCompleted++);
        IEnumerable<int> expectedClockMoments = [Four, Five];
        int[] clockMomentValues = [clockMoments[0].Value, clockMoments[1].Value];
        Assert.Equal(expectedClockMoments, clockMomentValues);
        Assert.Equal(1, clockMomentCompleted);

        var immediateMomentActions = new List<Moment<int>>();
        var immediateMomentCompleted = 0;
        var immediateTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(Sequencer.Immediate);
        immediateTimestampSignal.Subscribe(immediateMomentActions.Add, ex => throw ex, () => immediateMomentCompleted++).Dispose();
        IEnumerable<int> expectedImmediateMomentActions = [Two, Three];
        int[] immediateMomentActionValues = [immediateMomentActions[0].Value, immediateMomentActions[1].Value];
        Assert.Equal(expectedImmediateMomentActions, immediateMomentActionValues);
        Assert.Equal(1, immediateMomentCompleted);

        var clockMomentObserver = new RecordingWitness<Moment<int>>();
        var clockTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch));
        clockTimestampSignal.Subscribe(clockMomentObserver).Dispose();
        IEnumerable<int> expectedClockMomentObserver = [Two, Three];
        int[] clockMomentObserverValues = [clockMomentObserver.Values[0].Value, clockMomentObserver.Values[1].Value];
        Assert.Equal(expectedClockMomentObserver, clockMomentObserverValues);
        Assert.Equal(1, clockMomentObserver.Completed);

        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((IObserver<Moment<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((Action<Moment<int>>)null!, _ => { }, () => { }));
    }

    /// <summary>Verifies the time-interval operator immediate and clock-backed branches.</summary>
    private static void VerifyTimeIntervalBranches()
    {
        var immediateIntervals = new RecordingWitness<TimeInterval<int>>();
        Signal.Sequence(One, Three).TimeInterval(Sequencer.Immediate).Subscribe(immediateIntervals).Dispose();
        IEnumerable<int> expectedImmediateIntervals = [One, Two, Three];
        int[] immediateIntervalValues = [immediateIntervals.Values[0].Value, immediateIntervals.Values[1].Value, immediateIntervals.Values[Two].Value];
        Assert.Equal(expectedImmediateIntervals, immediateIntervalValues);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[0].Interval);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[1].Interval);
        Assert.Equal(TimeSpan.Zero, immediateIntervals.Values[Two].Interval);
        Assert.Equal(1, immediateIntervals.Completed);

        var clockIntervals = new List<TimeInterval<int>>();
        var clockIntervalCompleted = 0;
        Signal.Sequence(Four, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockIntervals.Add, ex => throw ex, () => clockIntervalCompleted++);
        IEnumerable<int> expectedClockIntervals = [Four, Five, Six];
        int[] clockIntervalValues = [clockIntervals[0].Value, clockIntervals[1].Value, clockIntervals[Two].Value];
        Assert.Equal(expectedClockIntervals, clockIntervalValues);
        Assert.Equal(TimeSpan.Zero, clockIntervals[0].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervals[1].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervals[Two].Interval);
        Assert.Equal(1, clockIntervalCompleted);

        var immediateIntervalActions = new List<TimeInterval<int>>();
        var immediateIntervalCompleted = 0;
        var immediateIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Two).TimeInterval(Sequencer.Immediate);
        immediateIntervalSignal.Subscribe(immediateIntervalActions.Add, ex => throw ex, () => immediateIntervalCompleted++).Dispose();
        IEnumerable<int> expectedImmediateIntervalActions = [Two, Three];
        int[] immediateIntervalActionValues = [immediateIntervalActions[0].Value, immediateIntervalActions[1].Value];
        Assert.Equal(expectedImmediateIntervalActions, immediateIntervalActionValues);
        Assert.Equal(1, immediateIntervalCompleted);

        var clockIntervalObserver = new RecordingWitness<TimeInterval<int>>();
        var clockIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch));
        clockIntervalSignal.Subscribe(clockIntervalObserver).Dispose();
        IEnumerable<int> expectedClockIntervalObserver = [Two, Three, Four];
        int[] clockIntervalObserverValues = [clockIntervalObserver.Values[0].Value, clockIntervalObserver.Values[1].Value, clockIntervalObserver.Values[Two].Value];
        Assert.Equal(expectedClockIntervalObserver, clockIntervalObserverValues);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[0].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[1].Interval);
        Assert.Equal(TimeSpan.Zero, clockIntervalObserver.Values[Two].Interval);
        Assert.Equal(1, clockIntervalObserver.Completed);

        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe((IObserver<TimeInterval<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe((Action<TimeInterval<int>>)null!, _ => { }, () => { }));
    }

    /// <summary>Verifies delay-start signal branches, the sequencer work item, and queue guard clauses.</summary>
    private static void VerifyDelayStartAndWorkItemBranches()
    {
        var shiftedObserver = new RecordingWitness<int>();
        Signal.Sequence(One, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedObserver).Dispose();
        int[] expectedShiftedObserver = [One, Two];
        Assert.Equal(expectedShiftedObserver, shiftedObserver.Values);
        Assert.Equal(1, shiftedObserver.Completed);

        var shiftedActions = new List<int>();
        var shiftedActionCompleted = 0;
        Signal.Sequence(Three, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedActions.Add, ex => throw ex, () => shiftedActionCompleted++);
        int[] expectedShiftedActions = [Three, Four];
        Assert.Equal(expectedShiftedActions, shiftedActions);
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
                return EmptyDisposable.Instance;
            });
        helper.Invoke();
        helper.Dispose();
        helper.Invoke();
        int[] expectedHelperValues = [One];
        Assert.Equal(expectedHelperValues, helperValues);

        var unusedScheduled = new ScheduledItem<int, string>(Sequencer.Immediate, "unused", (_, _) => EmptyDisposable.Instance, One);
        Assert.False(new SequencerQueue<int>().Remove(unusedScheduled));
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new PriorityQueue<int>(-1));

        var shrink = new PriorityQueue<int>(ThirtyTwo);
        for (var i = 0; i < ThirtyTwo; i++)
        {
            shrink.Enqueue(i);
        }

        for (var i = 0; i < TwentySix; i++)
        {
            Assert.Equal(i, shrink.Dequeue());
        }
    }

    /// <summary>Verifies the thread pool absolute scheduling and scheduled work item disposal branches.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyThreadPoolWorkItemBranchesAsync()
    {
        var absoluteRan = new TaskCompletionSource<int>();
        var absolute = ThreadPoolSequencer.Instance.Schedule(Five, FixedTimestamp, (_, state) =>
        {
            absoluteRan.TrySetResult(state);
            return EmptyDisposable.Instance;
        });
        Assert.Equal(Five, await absoluteRan.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false));
        absolute.Dispose();
        absolute.Dispose();

        var delayedDisposed = CreateThreadPoolWorkItem(One, (_, _) => EmptyDisposable.Instance);
        delayedDisposed.Dispose();
        QueueThreadPoolWorkItem(delayedDisposed, TimeSpan.FromMilliseconds(Ten));

        var skipped = false;
        var skippedItem = CreateThreadPoolWorkItem(Two, (_, _) =>
        {
            skipped = true;
            return EmptyDisposable.Instance;
        });
        skippedItem.Dispose();
        InvokeThreadPoolWorkItem(skippedItem);
        Assert.False(skipped);

        var disposedReturned = 0;
        object?[] holder = [null];
        var selfDisposing = CreateThreadPoolWorkItem(holder, (_, state) =>
        {
            ((IDisposable)state[0]!).Dispose();
            return new ActionDisposable(() => disposedReturned++);
        });
        holder[0] = selfDisposing;
        InvokeThreadPoolWorkItem(selfDisposing);
        Assert.Equal(1, disposedReturned);
    }

    /// <summary>Creates a thread pool scheduled work item for the given state and action.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="state">The state passed to the scheduled action.</param>
    /// <param name="action">The action invoked when the work item runs.</param>
    /// <returns>A new scheduled work item.</returns>
    private static ThreadPoolSequencer.ScheduledWorkItem<TState> CreateThreadPoolWorkItem<TState>(TState state, Func<ISequencer, TState, IDisposable> action) =>
        new(ThreadPoolSequencer.Instance, state, action);

    /// <summary>Executes the supplied thread pool scheduled work item.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to execute.</param>
    private static void InvokeThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item) => item.Execute();

    /// <summary>Queues the supplied thread pool scheduled work item with the given due time.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to queue.</param>
    /// <param name="dueTime">The delay before the work item runs.</param>
    private static void QueueThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item, TimeSpan dueTime) => item.Queue(dueTime);

    /// <summary>Produces an asynchronous sequence of integers from zero to the given count.</summary>
    /// <param name="count">The number of values to yield.</param>
    /// <returns>An asynchronous enumerable of integers.</returns>
    private static async IAsyncEnumerable<int> AsyncValues(int count)
    {
        for (var i = 0; i < count; i++)
        {
            await Task.Yield();
            yield return i;
        }
    }

    /// <summary>An observable that throws the supplied exception when subscribed to.</summary>
    /// <typeparam name="T">The type of the observable sequence elements.</typeparam>
    private sealed class ThrowOnSubscribeObservable<T> : IObservable<T>
    {
        /// <summary>The exception thrown on subscription.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref="ThrowOnSubscribeObservable{T}"/> class.</summary>
        /// <param name="error">The exception to throw when subscribed to.</param>
        public ThrowOnSubscribeObservable(Exception error) => _error = error;

        /// <summary>Throws the configured exception instead of subscribing.</summary>
        /// <param name="observer">The observer that would receive notifications.</param>
        /// <returns>This method never returns; it always throws.</returns>
        public IDisposable Subscribe(IObserver<T> observer) => throw _error;
    }

    /// <summary>An equality comparer that compares integers by value without optimization.</summary>
    private sealed class PassthroughComparer : IEqualityComparer<int>
    {
        /// <summary>Determines whether two integers are equal.</summary>
        /// <param name="x">The first integer to compare.</param>
        /// <param name="y">The second integer to compare.</param>
        /// <returns><see langword="true"/> when the values are equal; otherwise, <see langword="false"/>.</returns>
        public bool Equals(int x, int y) => x == y;

        /// <summary>Returns the hash code for the supplied integer.</summary>
        /// <param name="obj">The integer to hash.</param>
        /// <returns>The integer value itself as the hash code.</returns>
        public int GetHashCode(int obj) => obj;
    }

    /// <summary>An observable that replays a scripted sequence of notifications to subscribers.</summary>
    /// <typeparam name="T">The type of the observable sequence elements.</typeparam>
    private sealed class ScriptedObservable<T> : IObservable<T>
    {
        /// <summary>The script invoked with each subscribing observer.</summary>
        private readonly Action<IObserver<T>> _script;

        /// <summary>Initializes a new instance of the <see cref="ScriptedObservable{T}"/> class.</summary>
        /// <param name="script">The script invoked with each subscribing observer.</param>
        public ScriptedObservable(Action<IObserver<T>> script) => _script = script;

        /// <summary>Runs the script against the supplied observer.</summary>
        /// <param name="observer">The observer to drive with the script.</param>
        /// <returns>An empty disposable.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>An observable that captures the most recent subscribing observer.</summary>
    /// <typeparam name="T">The type of the observable sequence elements.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the most recently captured observer, if any.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Captures the supplied observer for later use.</summary>
        /// <param name="observer">The observer to capture.</param>
        /// <returns>An empty disposable.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>A minimal <see cref="SignalsBase{T}"/> probe used to exercise base class behavior.</summary>
    /// <typeparam name="T">The type of the signal sequence elements.</typeparam>
    private sealed class SignalsBaseProbe<T> : SignalsBase<T>
    {
        /// <summary>Initializes a new instance of the <see cref="SignalsBaseProbe{T}"/> class.</summary>
        /// <param name="required">Whether subscription must occur on the current thread.</param>
        public SignalsBaseProbe(bool required)
            : base(required)
        {
        }

        /// <summary>Performs the core subscription by returning an empty disposable.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <param name="cancel">The disposable used to cancel the subscription.</param>
        /// <returns>An empty disposable.</returns>
        protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) => EmptyDisposable.Instance;
    }

    /// <summary>An observer that throws on selected notifications to exercise failure handling.</summary>
    /// <typeparam name="T">The type of the observed sequence elements.</typeparam>
    private sealed class ThrowingWitness<T> : IObserver<T>
    {
        /// <summary>Whether to throw when a value is received.</summary>
        private readonly bool _throwOnNext;

        /// <summary>Whether to throw when an error is received.</summary>
        private readonly bool _throwOnError;

        /// <summary>Whether to throw when completion is received.</summary>
        private readonly bool _throwOnCompleted;

        /// <summary>Initializes a new instance of the <see cref="ThrowingWitness{T}"/> class.</summary>
        /// <param name="throwOnNext">Whether to throw when a value is received.</param>
        /// <param name="throwOnError">Whether to throw when an error is received.</param>
        /// <param name="throwOnCompleted">Whether to throw when completion is received.</param>
        public ThrowingWitness(bool throwOnNext = false, bool throwOnError = false, bool throwOnCompleted = false)
        {
            _throwOnNext = throwOnNext;
            _throwOnError = throwOnError;
            _throwOnCompleted = throwOnCompleted;
        }

        /// <summary>Handles completion, throwing when configured to do so.</summary>
        public void OnCompleted()
        {
            if (!_throwOnCompleted)
            {
                return;
            }

            throw new InvalidOperationException("observer-completed");
        }

        /// <summary>Handles an error, throwing when configured to do so.</summary>
        /// <param name="error">The error received.</param>
        public void OnError(Exception error)
        {
            if (!_throwOnError)
            {
                return;
            }

            throw new InvalidOperationException("observer-error");
        }

        /// <summary>Handles a value, throwing when configured to do so.</summary>
        /// <param name="value">The value received.</param>
        public void OnNext(T value)
        {
            if (!_throwOnNext)
            {
                return;
            }

            throw new InvalidOperationException("observer-next");
        }
    }

    /// <summary>An observer that records all received values, errors, and completions.</summary>
    /// <typeparam name="T">The type of the observed sequence elements.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets the values received by the observer.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the errors received by the observer.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications received.</summary>
        public int Completed { get; private set; }

        /// <summary>Records a completion notification.</summary>
        public void OnCompleted() => Completed++;

        /// <summary>Records a received error.</summary>
        /// <param name="error">The error received.</param>
        public void OnError(Exception error) => Errors.Add(error);

        /// <summary>Records a received value.</summary>
        /// <param name="value">The value received.</param>
        public void OnNext(T value) => Values.Add(value);
    }
}
