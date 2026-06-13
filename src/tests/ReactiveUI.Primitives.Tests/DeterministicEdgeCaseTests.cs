// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103, S104, S138, S6966 // Coverage tests intentionally group branch-heavy scenarios.

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
        await Assert.That(values.SequenceEqual(expectedValues)).IsTrue();
        var delayedStart = source.DelayStart(TimeSpan.Zero);
        await Assert.That(delayedStart).IsNotNull();
        await Assert.That(source.DelaySubscription(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.DelaySubscription(TimeSpan.Zero, Sequencer.Immediate)).IsNotNull();
        await Assert.That(source.Stabilize(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.Stabilize(TimeSpan.Zero, Sequencer.Immediate)).IsNotNull();
        var fused = new List<int>();
        Signal.Emit(One).FuseLatest(Signal.FromEnumerable([Two, Three]), (left, right) => left + right).Subscribe(fused.Add);
        int[] expectedFused = [Three, Four];
        await Assert.That(fused.SequenceEqual(expectedFused)).IsTrue();
        var chainedStrings = new List<string>();
        Signal.Chain(Signal.Emit("value")).Subscribe(chainedStrings.Add);
        await Assert.That(chainedStrings.SequenceEqual(ExpectedSingleValue)).IsTrue();
        var ignoredCatchCompleted = 0;
        Signal.Fail<int>(new InvalidOperationException("ignored")).Recover<int, Exception>(Handle.CatchIgnore<int>).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => ignoredCatchCompleted++);
        await Assert.That(ignoredCatchCompleted).IsEqualTo(1);
        var rangeArray = new List<int[]>();
        var rangeList = new List<IList<int>>();
        Signal.Sequence(Five, Three).CollectArray().Subscribe(rangeArray.Add);
        Signal.Sequence(Five, Three).CollectList().Subscribe(rangeList.Add);
        await Assert.That(rangeArray[0].SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(rangeList[0].SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(await Signal.Sequence(Ten, Three).FirstAsync().ConfigureAwait(false)).IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).FirstOrDefaultAsync().ConfigureAwait(false)).IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).FirstOrDefaultAsync(Nine).ConfigureAwait(false)).IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).LastAsync().ConfigureAwait(false)).IsEqualTo(Twelve);
        await Assert.That(await Signal.Sequence(Ten, Three).LastOrDefaultAsync().ConfigureAwait(false)).IsEqualTo(Twelve);
        await Assert.That(await Signal.None<int>().LastOrDefaultAsync(Nine).ConfigureAwait(false)).IsEqualTo(Nine);
        await Assert.That(await Signal.Sequence(One, Three).CountAsync(CancellationToken.None).ConfigureAwait(false)).IsEqualTo(Three);
        await Assert.That(await Signal.Sequence(One, Three).CountAsync(value => value > One, CancellationToken.None).ConfigureAwait(false)).IsEqualTo(Two);
        await Assert.That(await Signal.Sequence(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false)).IsEqualTo(ThreeLong);
        await Assert.That(await Signal.Sequence(One, Three).LongCount(value => value > One).ToTask(CancellationToken.None).ConfigureAwait(false)).IsEqualTo(TwoLong);
        await Assert.That(await Signal.Sequence(One, Three).AnyAsync(CancellationToken.None).ConfigureAwait(false)).IsTrue();
        await Assert.That(await Signal.Sequence(One, Three).AnyAsync(value => value == Two, CancellationToken.None).ConfigureAwait(false)).IsTrue();
        await Assert.That(await Signal.Sequence(One, Three).All(value => value < Four).ToTask(CancellationToken.None).ConfigureAwait(false)).IsTrue();
        await Assert.That(await Signal.Sequence(One, Three).Contains(Three).ToTask(CancellationToken.None).ConfigureAwait(false)).IsTrue();
        var collectedArray = await Signal.Sequence(Five, Three).CollectArrayAsync().ConfigureAwait(false);
        var collectedList = await Signal.Sequence(Five, Three).CollectListAsync().ConfigureAwait(false);
        await Assert.That(collectedArray.SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(collectedList.SequenceEqual([Five, Six, Seven])).IsTrue();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync().ConfigureAwait(false);
        var canceledTask = Signal.Silent<int>().ToTask(canceled.Token);
        await Assert.That(canceledTask.IsCanceled).IsTrue();
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Count<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.LongCount<int>(null!, value => value > 0));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Blend<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.CollectArray<int>(null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(null!, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(
            _ =>
{
},
            _ =>
{
},
            null!));
        Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(
            null!,
            _ =>
{
},
            _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, _ =>
{
}));
        Assert.Throws<ArgumentNullException>(() => source.Subscribe(
            _ =>
{
},
            (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Recover<int, InvalidOperationException>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<IObservable<int>>)null!).Recover());
        Assert.Throws<ArgumentNullException>(() => Signal.CreateSafe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => StateSignalExtensions.ToReadOnlyState<int, int>(null!, One, value => value));
        Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        Assert.Throws<ArgumentNullException>(() => TaskSignal.Create<int>(null!));
    }

    /// <summary>Verifies immediate core signals, range, zip, repeat, and observer failures cover remainders.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateCoreSignalsRangeZipRepeatAndObserverFailuresCoverRemainders()
    {
        var completed = 0;
        Signal.None<int>(Sequencer.Immediate).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => completed++);
        Signal.None(0).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => completed++);
        await Assert.That(completed).IsEqualTo(Two);
        var returnValues = new List<int>();
        Signal.Emit(FortyTwo, Sequencer.Immediate).Subscribe(returnValues.Add);
        int[] expectedReturnValues = [FortyTwo];
        await Assert.That(returnValues.SequenceEqual(expectedReturnValues)).IsTrue();
        var throwErrors = new List<string>();
        Signal.Fail<int>(new InvalidOperationException("immediate"), Sequencer.Immediate).Subscribe(
            _ =>
        {
        },
            ex => throwErrors.Add(ex.Message));
        Signal.Fail(new InvalidOperationException("witness"), Sequencer.Immediate, 0).Subscribe(
            _ =>
        {
        },
            ex => throwErrors.Add(ex.Message));
        await Assert.That(throwErrors.SequenceEqual(ExpectedImmediateWitness)).IsTrue();
        var never = Signal.Silent(0);
        await Assert.That(((IRequireCurrentThread<int>)never).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<RxVoid>)Signal.EmitRxVoid()).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RxVoid firstRxVoid = default;
        RxVoid secondRxVoid = default;
        await Assert.That(firstRxVoid == secondRxVoid).IsTrue();
        await Assert.That(firstRxVoid != secondRxVoid).IsFalse();
        var repeat = new RepeatSignal<int>(Seven, Three);
        var repeatValues = new List<int>();
        await Assert.That(repeat.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        repeat.Subscribe(repeatValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedRepeatValues = [Seven, Seven, Seven];
        await Assert.That(repeatValues.SequenceEqual(expectedRepeatValues)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => repeat.Subscribe(
            null!,
            _ =>
{
},
            () =>
{
}));
        var range = new RangeSignal(One, Three);
        var rangeValues = new List<int>();
        await Assert.That(range.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        range.Subscribe(new RecordingWitness<int>()).Dispose();
        range.Subscribe(rangeValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedRangeValues = [One, Two, Three];
        await Assert.That(rangeValues.SequenceEqual(expectedRangeValues)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => range.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => range.Subscribe(
            null!,
            _ =>
{
},
            () =>
{
}));
        var zip = new RangeZipSignal<int>(new(One, Three), new(Four, Three), (left, right) => left + right);
        var zipValues = new List<int>();
        await Assert.That(zip.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        zip.Subscribe(new RecordingWitness<int>()).Dispose();
        zip.Subscribe(zipValues.Add, ex => throw ex, () => completed++).Dispose();
        int[] expectedZipValues = [Five, Seven, Nine];
        await Assert.That(zipValues.SequenceEqual(expectedZipValues)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => zip.Subscribe(
            null!,
            _ =>
{
},
            () =>
{
}));
        await Assert.That(new ImmediateReturnSignal<int>(One).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new ImmediateThrowSignal<int>(new InvalidOperationException("fast")).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(ImmutableEmptySignal<int>.Instance.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(ImmutableNeverSignal<int>.Instance.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<int>)ImmutableReturnInt32Signal.GetInt32Signals(One)).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new RangeConcatSignal([new(One, Two), new(Three, Two)]).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new SignalsBaseProbe<int>(false).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.None<int>(Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("observer"), Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnError: true)).Dispose());
        Assert.Throws<ArgumentNullException>(() => new ImmediateThrowSignal<int>(new InvalidOperationException("null-observer")).Subscribe((IObserver<int>)null!));
    }

    /// <summary>Verifies subjects, replay, behavior, state, and connectable aliases cover late terminal branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectsReplayBehaviorStateAndConnectableAliasesCoverLateTerminalBranches()
    {
        var behavior = new BehaviorSignal<int>(One);
        await Assert.That(behavior.ToString()!.Contains(nameof(BehaviorSignal<int>), StringComparison.Ordinal)).IsTrue();
        var initial = new RecordingWitness<int>();
        using var behaviorSubscription = behavior.Subscribe(initial);
        behavior.OnCompleted();
        behavior.OnCompleted();
        behavior.OnNext(Two);
        var lateCompleted = new RecordingWitness<int>();
        behavior.Subscribe(lateCompleted).Dispose();
        int[] expectedInitial = [One];
        await Assert.That(initial.Values.SequenceEqual(expectedInitial)).IsTrue();
        await Assert.That(lateCompleted.Completed).IsEqualTo(1);
        var behaviorError = new BehaviorSignal<int>(One);
        behaviorError.OnError(new InvalidOperationException("behavior"));
        behaviorError.OnError(new InvalidOperationException("late"));
        var lateError = new RecordingWitness<int>();
        behaviorError.Subscribe(lateError).Dispose();
        await Assert.That(lateError.Errors[0].Message).IsEqualTo("behavior");
        behaviorError.Dispose();
        behaviorError.Dispose();
        await Assert.That(behaviorError.TryGetValue(out _)).IsFalse();
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
        await Assert.That(replayLateCompleted.Values.SequenceEqual(expectedReplayLateCompleted)).IsTrue();
        await Assert.That(replayLateCompleted.Completed).IsEqualTo(1);
        var replayError = new ReplaySignal<int>(bufferSize: 1, window: TimeSpan.MaxValue, scheduler: Sequencer.CurrentThread);
        replayError.OnNext(Five);
        replayError.OnError(new InvalidOperationException("replay"));
        replayError.OnError(new InvalidOperationException("late"));
        var replayLateError = new RecordingWitness<int>();
        replayError.Subscribe(replayLateError).Dispose();
        int[] expectedReplayLateError = [Five];
        await Assert.That(replayLateError.Values.SequenceEqual(expectedReplayLateError)).IsTrue();
        await Assert.That(replayLateError.Errors[0].Message).IsEqualTo("replay");
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
        await Assert.That(windowedLate.Values.SequenceEqual(expectedWindowedLate)).IsTrue();
        var shared = Signal.Sequence(One, Three).Share();
        var replayed = Signal.Sequence(One, Three).Replay(Two);
        await Assert.That(shared).IsNotNull();
        await Assert.That(replayed).IsNotNull();
        var state = Assert.Throws<ArgumentNullException>(() => new StateSignal<int>(One).ToReadOnlyState<int>(null!));
        await Assert.That(state.ParamName).IsEqualTo("selector");
    }

    /// <summary>Verifies low-level disposables, collections, and schedulers cover deterministic edges.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LowLevelDisposablesCollectionsAndSchedulersCoverDeterministicEdges()
    {
        var multiple = new MultipleDisposable();
        for (var i = 0; i < Twenty; i++)
        {
            multiple.Add(EmptyDisposable.Instance);
        }

        await Assert.That(multiple.Remove(EmptyDisposable.Instance)).IsTrue();
        await Assert.That(multiple.Remove(new ActionDisposable(() =>
        {
        }))).IsFalse();
        Assert.Throws<ArgumentNullException>(() => _ = new MultipleDisposable((IDisposable[])null!));
        Assert.Throws<ArgumentNullException>(() => multiple.Add(null!));
        multiple.Dispose();
        multiple.Dispose();
        using var cts = new CancellationTokenSource();
        var cancellation = new CancellationDisposable(cts);
        cancellation.Dispose();
        cancellation.Dispose();
        await Assert.That(cts.IsCancellationRequested).IsTrue();
        var list = ImmutableList<int>.Empty;
        await Assert.That(list.IndexOf(One)).IsEqualTo(-1);
        await Assert.That(list.Remove(One)).IsSameReferenceAs(list);
        var added = list.Add(One).Add(Two);
        await Assert.That(added.IndexOf(One)).IsEqualTo(0);
        await Assert.That(added.Remove(One).Remove(Two)).IsSameReferenceAs(ImmutableList<int>.Empty);
        var observerList = ImmutableList<IObserver<int>>.Empty.Add(new RecordingWitness<int>());
        var witness = new ListWitness<int>(observerList);
        await Assert.That(witness.HasObservers).IsTrue();
        await Assert.That(witness.Add(new RecordingWitness<int>())).IsNotNull();
        var queue = new PriorityQueue<int>();
        queue.Enqueue(One);
        queue.Enqueue(Two);
        await Assert.That(queue.Count > 0).IsTrue();
        var eventPattern = new EventPattern<EventArgs>(null, EventArgs.Empty);
        var samePattern = new EventPattern<EventArgs>(null, EventArgs.Empty);
        await Assert.That(eventPattern == samePattern).IsTrue();
        await Assert.That(eventPattern != samePattern).IsFalse();
        await Assert.That(eventPattern.Equals((object)samePattern)).IsTrue();
        await Assert.That(eventPattern.GetHashCode()).IsNotEqualTo(0);
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
        await Assert.That(scheduled.Count).IsEqualTo(Two);
    }

    /// <summary>Verifies remaining operator, factory, and observer failure branches are deterministic.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RemainingOperatorFactoryAndObserverFailureBranchesAreDeterministic()
    {
        await VerifyScheduledRangeAndTimingFactories();
        await VerifyTaskSignalsCountAndContainsAsync().ConfigureAwait(false);
        await VerifyAliasGuardsAndNullArgumentChecksAsync().ConfigureAwait(false);
        await VerifyObserverFailureBranchesAndMap();
        VerifyMultiSubscriberOnErrorThrows();
        await VerifyFlatMapTerminalAndErrorBranches();
    }

    /// <summary>Verifies optimized coordinator and async enumerable branches cover PR nine gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OptimizedCoordinatorAndAsyncEnumerableBranchesCoverPrNineGaps()
    {
        await VerifyAsyncEnumerableShiftAndExpireAsync().ConfigureAwait(false);
        await VerifyRaceSyncLatestAndSwitchBranches();
        await VerifyProbeBranches();
        await VerifyCalmAppendAndForkJoinBranches();
    }

    /// <summary>Verifies range timing, queues, and thread pool cover PR ten coverage gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RangeTimingQueuesAndThreadPoolCoverPrTenCoverageGaps()
    {
        await VerifyTimestampBranches();
        await VerifyTimeIntervalBranches();
        await VerifyDelayStartAndWorkItemBranches();
        await VerifyThreadPoolWorkItemBranchesAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies the scheduled range fast path and the timing factory aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyScheduledRangeAndTimingFactories()
    {
        var scheduledRangeClock = new TestClock(DateTimeOffset.UnixEpoch);
        var scheduledRange = new List<int>();
        var scheduledRangeCompleted = 0;
        Signal.Sequence(Three, Three, scheduledRangeClock).Subscribe(scheduledRange.Add, ex => throw ex, () => scheduledRangeCompleted++);
        scheduledRangeClock.Start();
        await Assert.That(scheduledRange.SequenceEqual([Three, Four, Five])).IsTrue();
        await Assert.That(scheduledRangeCompleted).IsEqualTo(1);
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch))).IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.After(FixedTimestamp.AddMilliseconds(1))).IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.PairLatest(Signal.Sequence(One, Two), Signal.Sequence(Three, Two), (left, right) => left + right)).IsNotNull();
        var toSignalValues = new List<int>();
        new[]
        {
            One,
            Two
        }.ToSignal().Subscribe(toSignalValues.Add);
        new[]
        {
            Three,
            Four
        }.ToSignal(CancellationToken.None).Subscribe(toSignalValues.Add);
        int[] expectedToSignalValues = [One, Two, Three, Four];
        await Assert.That(toSignalValues.SequenceEqual(expectedToSignalValues)).IsTrue();
    }

    /// <summary>Verifies task-backed signals, long count, and contains operators.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyTaskSignalsCountAndContainsAsync()
    {
        var firstTaskSignal = await Signal.FromTask(_ => Task.FromResult(Five)).FirstAsync().ConfigureAwait(false);
        var secondTaskSignal = await Signal.FromTask(_ => Task.FromResult(Six), Sequencer.Immediate).FirstAsync().ConfigureAwait(false);
        await Assert.That(firstTaskSignal).IsEqualTo(Five);
        await Assert.That(secondTaskSignal).IsEqualTo(Six);
        await Assert.That(await Task.FromResult(Seven).HandleCancellation().ConfigureAwait(false)).IsEqualTo(Seven);
        await Assert.That(await Task.FromCanceled<int>(new(true)).HandleCancellation().ConfigureAwait(false)).IsEqualTo(0);
        var longCount = new List<long>();
        Signal.Sequence(One, Four).LongCount(value => value % Two == 0).Subscribe(longCount.Add);
        await Assert.That(longCount.SequenceEqual(ExpectedSingleTwoLong)).IsTrue();
        var containsWithComparer = new List<bool>();
        Signal.Sequence(One, Three).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        Signal.Sequence(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        await Assert.That(containsWithComparer.SequenceEqual(ExpectedContainsResults)).IsTrue();
    }

    /// <summary>Verifies alias operators, buffer guard clauses, null-argument guards, and cancellation.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAliasGuardsAndNullArgumentChecksAsync()
    {
        var startWithAlias = new List<int>();
        Signal.Emit(Two).Prepend(One).Subscribe(startWithAlias.Add);
        int[] expectedStartWithAlias = [One, Two];
        await Assert.That(startWithAlias.SequenceEqual(expectedStartWithAlias)).IsTrue();
        await Assert.That(Signal.Emit(One).DelayStart(TimeSpan.Zero)).IsNotNull();
        await Assert.That(await Signal.None<int>().FirstOrDefaultAsync().ConfigureAwait(false)).IsEqualTo(0);
        var noneWitnessCompleted = 0;
        Signal.None(Sequencer.Immediate, One).Subscribe(
            _ =>
        {
        },
            ex => throw ex,
            () => noneWitnessCompleted++);
        await Assert.That(noneWitnessCompleted).IsEqualTo(1);
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
        await Assert.That(pendingTask.IsCanceled).IsTrue();
    }

    /// <summary>Verifies immediate signal observer failure branches and the map late-notification branch.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyObserverFailureBranchesAndMap()
    {
        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new EmptySignal<int>(Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        Assert.Throws<InvalidOperationException>(() => new ThrowSignal<int>(new InvalidOperationException("throw-signal"), Sequencer.Immediate).Subscribe(new ThrowingWitness<int>(throwOnError: true)).Dispose());
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
        await Assert.That(mapObserver.Values.SequenceEqual(expectedMapObserver)).IsTrue();
        await Assert.That(mapObserver.Completed).IsEqualTo(1);
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
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyFlatMapTerminalAndErrorBranches()
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
        await Assert.That(selectManyValues.SequenceEqual(expectedSelectManyValues)).IsTrue();
        await Assert.That(selectManyCompleted).IsEqualTo(1);
        var disposedOuter = new Signal<IObservable<int>>();
        var disposedInner = new Signal<int>();
        var disposedValues = new List<int>();
        var disposedSubscription = disposedOuter.FlatMap(inner => inner).Subscribe(disposedValues.Add);
        disposedOuter.OnNext(disposedInner);
        disposedSubscription.Dispose();
        disposedSubscription.Dispose();
        disposedInner.OnNext(Three);
        await Assert.That(disposedValues.Count).IsEqualTo(0);
        Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner, (_, right) => right).Subscribe(null!));
        var nullSelectorErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int>(_ => null!).Subscribe(
            _ =>
        {
        },
            ex => nullSelectorErrors.Add(ex.Message));
        await Assert.That(nullSelectorErrors.SequenceEqual(ExpectedFlatMapSelectorNull)).IsTrue();
        var nullCollectionErrors = new List<string>();
        Signal.Emit(One).FlatMap<int, int, int>(_ => null!, (left, right) => left + right).Subscribe(
            _ =>
        {
        },
            ex => nullCollectionErrors.Add(ex.Message));
        await Assert.That(nullCollectionErrors.SequenceEqual(ExpectedFlatMapCollectionSelectorNull)).IsTrue();
        var resultInnerErrors = new List<string>();
        Signal.Emit(One).FlatMap(_ => Signal.Fail<int>(new InvalidOperationException("result-inner")), (left, right) => left + right).Subscribe(
            _ =>
        {
        },
            ex => resultInnerErrors.Add(ex.Message));
        await Assert.That(resultInnerErrors.SequenceEqual(ExpectedResultInner)).IsTrue();
        var subscribeErrors = new List<string>();
        Signal.Emit(One).FlatMap(_ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe"))).Subscribe(
            _ =>
        {
        },
            ex => subscribeErrors.Add(ex.Message));
        await Assert.That(subscribeErrors.SequenceEqual(ExpectedInnerSubscribe)).IsTrue();
    }

    /// <summary>Verifies async enumerable subscription, shift timing, and expire timeout branches.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAsyncEnumerableShiftAndExpireAsync()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable(AsyncValues(One)).Subscribe(null!));
        var asyncValues = new List<int>();
        var asyncCompleted = new TaskCompletionSource<object?>();
        using var asyncToken = new CancellationTokenSource();
        Signal.FromAsyncEnumerable(AsyncValues(Three), asyncToken.Token).Subscribe(asyncValues.Add, ex => asyncCompleted.TrySetException(ex), () => asyncCompleted.TrySetResult(null));
        await asyncCompleted.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false);
        int[] expectedAsyncValues = [0, One, Two];
        await Assert.That(asyncValues.SequenceEqual(expectedAsyncValues)).IsTrue();
        var exact = await Signal.FromAsyncEnumerable(AsyncValues(Sixteen)).CollectArrayAsync().ConfigureAwait(false);
        var grown = await Signal.FromAsyncEnumerable(AsyncValues(Seventeen)).CollectArrayAsync().ConfigureAwait(false);
        await Assert.That(exact.Length).IsEqualTo(Sixteen);
        await Assert.That(exact[Fifteen]).IsEqualTo(Fifteen);
        await Assert.That(grown.Length).IsEqualTo(Seventeen);
        await Assert.That(grown[Sixteen]).IsEqualTo(Sixteen);
        var shiftedClock = new TestClock(DateTimeOffset.UnixEpoch);
        var shifted = new List<int>();
        Signal.Sequence(Three, Three).Shift(TimeSpan.FromTicks(Two), shiftedClock).Subscribe(shifted.Add);
        await Assert.That(shifted.Count).IsEqualTo(0);
        shiftedClock.AdvanceBy(TimeSpan.FromTicks(Two));
        int[] expectedShifted = [Three, Four, Five];
        await Assert.That(shifted.SequenceEqual(expectedShifted)).IsTrue();
        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Expire(TimeSpan.Zero).Subscribe(null!));
        var timeoutClock = new TestClock(DateTimeOffset.UnixEpoch);
        var timeout = new RecordingWitness<int>();
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), timeoutClock).Subscribe(timeout);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(timeout.Errors[0] is TimeoutException).IsTrue();
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
        await Assert.That(expireCompleted.Values.SequenceEqual(expectedExpireCompleted)).IsTrue();
        await Assert.That(expireCompleted.Completed).IsEqualTo(1);
        await Assert.That(expireCompleted.Errors.Count).IsEqualTo(0);
        var expireError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("expire-error")).Expire(TimeSpan.FromTicks(Ten), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(expireError);
        await Assert.That(expireError.Errors[0].Message).IsEqualTo("expire-error");
    }

    /// <summary>Verifies the race, synchronized-latest, and switch coordinator branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyRaceSyncLatestAndSwitchBranches()
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
        await Assert.That(race.Values.SequenceEqual(expectedRace)).IsTrue();
        await Assert.That(race.Errors.Count).IsEqualTo(0);
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
        await Assert.That(raceCompletion.Values.SequenceEqual(expectedRaceCompletion)).IsTrue();
        await Assert.That(raceCompletion.Completed).IsEqualTo(0);
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
        await Assert.That(combined.Values.SequenceEqual(expectedCombined)).IsTrue();
        await Assert.That(combined.Completed).IsEqualTo(1);
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

        await Assert.That(switched.Values.Count).IsEqualTo(0);
        await Assert.That(switched.Errors[0].Message).IsEqualTo("current-switch");
    }

    /// <summary>
    /// Verifies the probe operator error, disposal, and completion branches alongside the
    /// direct and scheduled current-thread expire and probe branches.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyProbeBranches()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Probe(TimeSpan.Zero).Subscribe(null!));
        var probeError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("probe-error")).Probe(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(probeError);
        await Assert.That(probeError.Errors[0].Message).IsEqualTo("probe-error");
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
        await Assert.That(completedProbe.Completed).IsEqualTo(1);
        await Assert.That(completedProbe.Values.Count).IsEqualTo(0);
        var directCurrentThreadExpire = new RecordingWitness<int>();
        var directCurrentThreadProbe = new RecordingWitness<int>();
        Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadExpire);
        Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadProbe);
        int[] expectedDirectCurrentThreadExpire = [One];
        await Assert.That(directCurrentThreadExpire.Values.SequenceEqual(expectedDirectCurrentThreadExpire)).IsTrue();
        await Assert.That(directCurrentThreadExpire.Completed).IsEqualTo(1);
        await Assert.That(directCurrentThreadProbe.Values.Count).IsEqualTo(0);
        await Assert.That(directCurrentThreadProbe.Completed).IsEqualTo(1);
        var currentThreadExpire = new RecordingWitness<int>();
        var currentThreadProbe = new RecordingWitness<int>();
        Sequencer.CurrentThread.Schedule(() =>
        {
            Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadExpire);
            Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadProbe);
        });
        int[] expectedCurrentThreadExpire = [One];
        await Assert.That(currentThreadExpire.Values.SequenceEqual(expectedCurrentThreadExpire)).IsTrue();
        await Assert.That(currentThreadExpire.Completed).IsEqualTo(1);
        await Assert.That(currentThreadProbe.Values.Count).IsEqualTo(0);
        await Assert.That(currentThreadProbe.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies the calm debounce, append observer failure, and fork-join completion branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyCalmAppendAndForkJoinBranches()
    {
        var calmError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("calm-error")).Calm(TimeSpan.FromTicks(One), new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(calmError);
        await Assert.That(calmError.Errors[0].Message).IsEqualTo("calm-error");
        var calmClock = new TestClock(DateTimeOffset.UnixEpoch);
        var calmSource = new Signal<int>();
        var calmValues = new List<int>();
        calmSource.Calm(TimeSpan.FromTicks(Five), calmClock).Subscribe(calmValues.Add);
        calmSource.OnNext(One);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        calmSource.OnNext(Two);
        calmClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(calmValues.Count).IsEqualTo(0);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        int[] expectedCalmValues = [Two];
        await Assert.That(calmValues.SequenceEqual(expectedCalmValues)).IsTrue();
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Prepend(0).Append(Two).Subscribe(
            value =>
{
    if (value != One)
    {
        return;
    }

    throw new InvalidOperationException("append-next");
},
            _ =>
{
},
            () =>
{
}).Dispose());
        var appendError = new RecordingWitness<int>();
        Signal.Fail<int>(new InvalidOperationException("append-error")).Append(One).Subscribe(appendError);
        await Assert.That(appendError.Errors[0].Message).IsEqualTo("append-error");
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
        await Assert.That(forkLeftFirst.Values.SequenceEqual(expectedForkLeftFirst)).IsTrue();
        await Assert.That(forkLeftFirst.Completed).IsEqualTo(1);
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
        await Assert.That(forkRightFirst.Values.SequenceEqual(expectedForkRightFirst)).IsTrue();
        await Assert.That(forkRightFirst.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies the timestamp operator immediate and clock-backed branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyTimestampBranches()
    {
        var immediateMoments = new RecordingWitness<Moment<int>>();
        Signal.Sequence(One, Three).Timestamp(Sequencer.Immediate).Subscribe(immediateMoments).Dispose();
        IEnumerable<int> expectedImmediateMoments = [One, Two, Three];
        int[] immediateMomentValues = [immediateMoments.Values[0].Value, immediateMoments.Values[1].Value, immediateMoments.Values[Two].Value];
        await Assert.That(immediateMomentValues.SequenceEqual(expectedImmediateMoments)).IsTrue();
        await Assert.That(immediateMoments.Completed).IsEqualTo(1);
        var clockMoments = new List<Moment<int>>();
        var clockMomentCompleted = 0;
        Signal.Sequence(Four, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockMoments.Add, ex => throw ex, () => clockMomentCompleted++);
        IEnumerable<int> expectedClockMoments = [Four, Five];
        int[] clockMomentValues = [clockMoments[0].Value, clockMoments[1].Value];
        await Assert.That(clockMomentValues.SequenceEqual(expectedClockMoments)).IsTrue();
        await Assert.That(clockMomentCompleted).IsEqualTo(1);
        var immediateMomentActions = new List<Moment<int>>();
        var immediateMomentCompleted = 0;
        var immediateTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(Sequencer.Immediate);
        immediateTimestampSignal.Subscribe(immediateMomentActions.Add, ex => throw ex, () => immediateMomentCompleted++).Dispose();
        IEnumerable<int> expectedImmediateMomentActions = [Two, Three];
        int[] immediateMomentActionValues = [immediateMomentActions[0].Value, immediateMomentActions[1].Value];
        await Assert.That(immediateMomentActionValues.SequenceEqual(expectedImmediateMomentActions)).IsTrue();
        await Assert.That(immediateMomentCompleted).IsEqualTo(1);
        var clockMomentObserver = new RecordingWitness<Moment<int>>();
        var clockTimestampSignal = (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(new TestClock(DateTimeOffset.UnixEpoch));
        clockTimestampSignal.Subscribe(clockMomentObserver).Dispose();
        IEnumerable<int> expectedClockMomentObserver = [Two, Three];
        int[] clockMomentObserverValues = [clockMomentObserver.Values[0].Value, clockMomentObserver.Values[1].Value];
        await Assert.That(clockMomentObserverValues.SequenceEqual(expectedClockMomentObserver)).IsTrue();
        await Assert.That(clockMomentObserver.Completed).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((IObserver<Moment<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe(
            (Action<Moment<int>>)null!,
            _ =>
{
},
            () =>
{
}));
    }

    /// <summary>Verifies the time-interval operator immediate and clock-backed branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyTimeIntervalBranches()
    {
        var immediateIntervals = new RecordingWitness<TimeInterval<int>>();
        Signal.Sequence(One, Three).TimeInterval(Sequencer.Immediate).Subscribe(immediateIntervals).Dispose();
        IEnumerable<int> expectedImmediateIntervals = [One, Two, Three];
        int[] immediateIntervalValues = [immediateIntervals.Values[0].Value, immediateIntervals.Values[1].Value, immediateIntervals.Values[Two].Value];
        await Assert.That(immediateIntervalValues.SequenceEqual(expectedImmediateIntervals)).IsTrue();
        await Assert.That(immediateIntervals.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Completed).IsEqualTo(1);
        var clockIntervals = new List<TimeInterval<int>>();
        var clockIntervalCompleted = 0;
        Signal.Sequence(Four, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch)).Subscribe(clockIntervals.Add, ex => throw ex, () => clockIntervalCompleted++);
        IEnumerable<int> expectedClockIntervals = [Four, Five, Six];
        int[] clockIntervalValues = [clockIntervals[0].Value, clockIntervals[1].Value, clockIntervals[Two].Value];
        await Assert.That(clockIntervalValues.SequenceEqual(expectedClockIntervals)).IsTrue();
        await Assert.That(clockIntervals[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalCompleted).IsEqualTo(1);
        var immediateIntervalActions = new List<TimeInterval<int>>();
        var immediateIntervalCompleted = 0;
        var immediateIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Two).TimeInterval(Sequencer.Immediate);
        immediateIntervalSignal.Subscribe(immediateIntervalActions.Add, ex => throw ex, () => immediateIntervalCompleted++).Dispose();
        IEnumerable<int> expectedImmediateIntervalActions = [Two, Three];
        int[] immediateIntervalActionValues = [immediateIntervalActions[0].Value, immediateIntervalActions[1].Value];
        await Assert.That(immediateIntervalActionValues.SequenceEqual(expectedImmediateIntervalActions)).IsTrue();
        await Assert.That(immediateIntervalCompleted).IsEqualTo(1);
        var clockIntervalObserver = new RecordingWitness<TimeInterval<int>>();
        var clockIntervalSignal = (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Three).TimeInterval(new TestClock(DateTimeOffset.UnixEpoch));
        clockIntervalSignal.Subscribe(clockIntervalObserver).Dispose();
        IEnumerable<int> expectedClockIntervalObserver = [Two, Three, Four];
        int[] clockIntervalObserverValues = [clockIntervalObserver.Values[0].Value, clockIntervalObserver.Values[1].Value, clockIntervalObserver.Values[Two].Value];
        await Assert.That(clockIntervalObserverValues.SequenceEqual(expectedClockIntervalObserver)).IsTrue();
        await Assert.That(clockIntervalObserver.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Completed).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe((IObserver<TimeInterval<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => immediateIntervalSignal.Subscribe(
            (Action<TimeInterval<int>>)null!,
            _ =>
{
},
            () =>
{
}));
    }

    /// <summary>Verifies delay-start signal branches, the sequencer work item, and queue guard clauses.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyDelayStartAndWorkItemBranches()
    {
        var shiftedObserver = new RecordingWitness<int>();
        Signal.Sequence(One, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedObserver).Dispose();
        int[] expectedShiftedObserver = [One, Two];
        await Assert.That(shiftedObserver.Values.SequenceEqual(expectedShiftedObserver)).IsTrue();
        await Assert.That(shiftedObserver.Completed).IsEqualTo(1);
        var shiftedActions = new List<int>();
        var shiftedActionCompleted = 0;
        Signal.Sequence(Three, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedActions.Add, ex => throw ex, () => shiftedActionCompleted++);
        int[] expectedShiftedActions = [Three, Four];
        await Assert.That(shiftedActions.SequenceEqual(expectedShiftedActions)).IsTrue();
        await Assert.That(shiftedActionCompleted).IsEqualTo(1);
        var currentThreadShift = (IRequireCurrentThread<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.CurrentThread);
        await Assert.That(currentThreadShift.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        var inlineShift = (IInlineSignal<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate);
        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe(
            (Action<int>)null!,
            _ =>
{
},
            () =>
{
}));
        Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe(
            _ =>
{
},
            _ =>
{
},
            null!));
        var helperValues = new List<int>();
        var helper = new SequencerWorkItem<ISequencer, int>(Sequencer.Immediate, One, (_, state) =>
        {
            helperValues.Add(state);
            return EmptyDisposable.Instance;
        });
        helper.Invoke();
        helper.Dispose();
        helper.Invoke();
        int[] expectedHelperValues = [One];
        await Assert.That(helperValues.SequenceEqual(expectedHelperValues)).IsTrue();
        var unusedScheduled = new ScheduledItem<int, string>(Sequencer.Immediate, "unused", (_, _) => EmptyDisposable.Instance, One);
        await Assert.That(new SequencerQueue<int>().Remove(unusedScheduled)).IsFalse();
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new PriorityQueue<int>(-1));
        var shrink = new PriorityQueue<int>(ThirtyTwo);
        for (var i = 0; i < ThirtyTwo; i++)
        {
            shrink.Enqueue(i);
        }

        for (var i = 0; i < TwentySix; i++)
        {
            await Assert.That(shrink.Dequeue()).IsEqualTo(i);
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
        await Assert.That(await absoluteRan.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false)).IsEqualTo(Five);
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
        await Assert.That(skipped).IsFalse();
        var disposedReturned = 0;
        object?[] holder = [null];
        var selfDisposing = CreateThreadPoolWorkItem(holder, (_, state) =>
        {
            ((IDisposable)state[0]!).Dispose();
            return new ActionDisposable(() => disposedReturned++);
        });
        holder[0] = selfDisposing;
        InvokeThreadPoolWorkItem(selfDisposing);
        await Assert.That(disposedReturned).IsEqualTo(1);
    }

    /// <summary>Creates a thread pool scheduled work item for the given state and action.</summary>
    /// <typeparam name = "TState">The type of the work item state.</typeparam>
    /// <param name = "state">The state passed to the scheduled action.</param>
    /// <param name = "action">The action invoked when the work item runs.</param>
    /// <returns>A new scheduled work item.</returns>
    private static ThreadPoolSequencer.ScheduledWorkItem<TState> CreateThreadPoolWorkItem<TState>(TState state, Func<ISequencer, TState, IDisposable> action) => new(ThreadPoolSequencer.Instance, state, action);

    /// <summary>Executes the supplied thread pool scheduled work item.</summary>
    /// <typeparam name = "TState">The type of the work item state.</typeparam>
    /// <param name = "item">The work item to execute.</param>
    private static void InvokeThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item) => item.Execute();

    /// <summary>Queues the supplied thread pool scheduled work item with the given due time.</summary>
    /// <typeparam name = "TState">The type of the work item state.</typeparam>
    /// <param name = "item">The work item to queue.</param>
    /// <param name = "dueTime">The delay before the work item runs.</param>
    private static void QueueThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item, TimeSpan dueTime) => item.Queue(dueTime);

    /// <summary>Produces an asynchronous sequence of integers from zero to the given count.</summary>
    /// <param name = "count">The number of values to yield.</param>
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
    /// <typeparam name = "T">The type of the observable sequence elements.</typeparam>
    private sealed class ThrowOnSubscribeObservable<T> : IObservable<T>
    {
        /// <summary>The exception thrown on subscription.</summary>
        private readonly Exception _error;

        /// <summary>Initializes a new instance of the <see cref = "ThrowOnSubscribeObservable{T}"/> class.</summary>
        /// <param name = "error">The exception to throw when subscribed to.</param>
        public ThrowOnSubscribeObservable(Exception error) => _error = error;

        /// <summary>Throws the configured exception instead of subscribing.</summary>
        /// <param name = "observer">The observer that would receive notifications.</param>
        /// <returns>This method never returns; it always throws.</returns>
        public IDisposable Subscribe(IObserver<T> observer) => throw _error;
    }

    /// <summary>An equality comparer that compares integers by value without optimization.</summary>
    private sealed class PassthroughComparer : IEqualityComparer<int>
    {
        /// <summary>Determines whether two integers are equal.</summary>
        /// <param name = "x">The first integer to compare.</param>
        /// <param name = "y">The second integer to compare.</param>
        /// <returns><see langword="true"/> when the values are equal; otherwise, <see langword="false"/>.</returns>
        public bool Equals(int x, int y) => x == y;

        /// <summary>Returns the hash code for the supplied integer.</summary>
        /// <param name = "obj">The integer to hash.</param>
        /// <returns>The integer value itself as the hash code.</returns>
        public int GetHashCode(int obj) => obj;
    }

    /// <summary>An observable that replays a scripted sequence of notifications to subscribers.</summary>
    /// <typeparam name = "T">The type of the observable sequence elements.</typeparam>
    private sealed class ScriptedObservable<T> : IObservable<T>
    {
        /// <summary>The script invoked with each subscribing observer.</summary>
        private readonly Action<IObserver<T>> _script;

        /// <summary>Initializes a new instance of the <see cref = "ScriptedObservable{T}"/> class.</summary>
        /// <param name = "script">The script invoked with each subscribing observer.</param>
        public ScriptedObservable(Action<IObserver<T>> script) => _script = script;

        /// <summary>Runs the script against the supplied observer.</summary>
        /// <param name = "observer">The observer to drive with the script.</param>
        /// <returns>An empty disposable.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>An observable that captures the most recent subscribing observer.</summary>
    /// <typeparam name = "T">The type of the observable sequence elements.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the most recently captured observer, if any.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Captures the supplied observer for later use.</summary>
        /// <param name = "observer">The observer to capture.</param>
        /// <returns>An empty disposable.</returns>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>A minimal <see cref = "SignalsBase{T}"/> probe used to exercise base class behavior.</summary>
    /// <typeparam name = "T">The type of the signal sequence elements.</typeparam>
    private sealed class SignalsBaseProbe<T> : SignalsBase<T>
    {
        /// <summary>Initializes a new instance of the <see cref = "SignalsBaseProbe{T}"/> class.</summary>
        /// <param name = "required">Whether subscription must occur on the current thread.</param>
        public SignalsBaseProbe(bool required)
            : base(required)
        {
        }

        /// <summary>Performs the core subscription by returning an empty disposable.</summary>
        /// <param name = "observer">The observer to subscribe.</param>
        /// <param name = "cancel">The disposable used to cancel the subscription.</param>
        /// <returns>An empty disposable.</returns>
        protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) => EmptyDisposable.Instance;
    }

    /// <summary>An observer that throws on selected notifications to exercise failure handling.</summary>
    /// <typeparam name = "T">The type of the observed sequence elements.</typeparam>
    private sealed class ThrowingWitness<T> : IObserver<T>
    {
        /// <summary>Whether to throw when a value is received.</summary>
        private readonly bool _throwOnNext;

        /// <summary>Whether to throw when an error is received.</summary>
        private readonly bool _throwOnError;

        /// <summary>Whether to throw when completion is received.</summary>
        private readonly bool _throwOnCompleted;

        /// <summary>Initializes a new instance of the <see cref = "ThrowingWitness{T}"/> class.</summary>
        /// <param name = "throwOnNext">Whether to throw when a value is received.</param>
        /// <param name = "throwOnError">Whether to throw when an error is received.</param>
        /// <param name = "throwOnCompleted">Whether to throw when completion is received.</param>
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
        /// <param name = "error">The error received.</param>
        public void OnError(Exception error)
        {
            if (!_throwOnError)
            {
                return;
            }

            throw new InvalidOperationException("observer-error");
        }

        /// <summary>Handles a value, throwing when configured to do so.</summary>
        /// <param name = "value">The value received.</param>
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
    /// <typeparam name = "T">The type of the observed sequence elements.</typeparam>
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
        /// <param name = "error">The error received.</param>
        public void OnError(Exception error) => Errors.Add(error);

        /// <summary>Records a received value.</summary>
        /// <param name = "value">The value received.</param>
        public void OnNext(T value) => Values.Add(value);
    }
}
