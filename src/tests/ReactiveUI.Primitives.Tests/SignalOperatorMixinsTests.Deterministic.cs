// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies deterministic operator, factory, coordinator, and timing branch coverage.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>The integer constant ten.</summary>
    private const int Ten = 10;

    /// <summary>The integer constant twelve.</summary>
    private const int Twelve = 12;

    /// <summary>The integer constant fifteen.</summary>
    private const int Fifteen = 15;

    /// <summary>The integer constant sixteen.</summary>
    private const int Sixteen = 16;

    /// <summary>The integer constant seventeen.</summary>
    private const int Seventeen = 17;

    /// <summary>The integer constant twenty-six.</summary>
    private const int TwentySix = 26;

    /// <summary>The integer constant thirty-two.</summary>
    private const int ThirtyTwo = 32;

    /// <summary>The long constant two.</summary>
    private const long TwoLong = 2L;

    /// <summary>The long constant three.</summary>
    private const long ThreeLong = 3L;

    /// <summary>A fixed deterministic timestamp used in place of the current time.</summary>
    private static readonly DateTimeOffset FixedTimestamp = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Expected single string value.</summary>
    private static readonly string[] ExpectedSingleValue = ["value"];

    /// <summary>Expected contains comparer results.</summary>
    private static readonly bool[] ExpectedContainsResults = [true, false, true, false];

    /// <summary>Expected single two-long value.</summary>
    private static readonly long[] ExpectedSingleTwoLong = [TwoLong];

    /// <summary>Expected flat-map null-selector error message.</summary>
    private static readonly string[] ExpectedFlatMapSelectorNull = ["The FlatMap selector returned null."];

    /// <summary>Expected flat-map null-collection-selector error message.</summary>
    private static readonly string[] ExpectedFlatMapCollectionSelectorNull =
        ["The FlatMap collection selector returned null."];

    /// <summary>Expected result-inner error message.</summary>
    private static readonly string[] ExpectedResultInner = ["result-inner"];

    /// <summary>Expected inner-subscribe error message.</summary>
    private static readonly string[] ExpectedInnerSubscribe = ["inner-subscribe"];

    /// <summary>Verifies parity alias operators cover remaining lines.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ParityAliasOperatorsCoverRemainingLines()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        List<int> values = [];
        _ = source.Prepend(Two).Subscribe(values.Add);
        int[] expectedValues = [Two, Three, Four];
        await Assert.That(values.SequenceEqual(expectedValues)).IsTrue();
        var delayedStart = source.DelayStart(TimeSpan.Zero);
        await Assert.That(delayedStart).IsNotNull();
        await Assert.That(source.DelaySubscription(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.DelaySubscription(TimeSpan.Zero, Sequencer.Immediate)).IsNotNull();
        await Assert.That(source.Stabilize(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.Stabilize(TimeSpan.Zero, Sequencer.Immediate)).IsNotNull();
        List<int> fused = [];
        _ = Signal.Emit(One).FuseLatest(Signal.FromEnumerable([Two, Three]), (left, right) => left + right)
            .Subscribe(fused.Add);
        int[] expectedFused = [Three, Four];
        await Assert.That(fused.SequenceEqual(expectedFused)).IsTrue();
        List<string> chainedStrings = [];
        _ = Signal.Chain(Signal.Emit("value")).Subscribe(chainedStrings.Add);
        await Assert.That(chainedStrings.SequenceEqual(ExpectedSingleValue)).IsTrue();
        var ignoredCatchCompleted = 0;
        _ = Signal.Fail<int>(new InvalidOperationException("ignored")).Recover<int, Exception>(Handle.CatchIgnore<int>)
            .Subscribe(_ => { }, ex => throw ex, () => ignoredCatchCompleted++);
        await Assert.That(ignoredCatchCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies range async fast paths and guard clauses cover remaining lines.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification = "Synchronous CollectArray/CollectList operators are deliberately covered.")]
    public async Task RangeAsyncFastPathsAndNullGuardsCoverRemainingLines()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        IEnumerable<IObservable<int>> blendSources = [Signal.Emit(One), Signal.Emit(Two)];
        List<int> blended = [];
        _ = blendSources.Blend().Subscribe(blended.Add);
        await Assert.That(blended.SequenceEqual([One, Two])).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => blendSources.Blend().Subscribe((IObserver<int>)null!));
        List<int[]> rangeArray = [];
        List<IList<int>> rangeList = [];
        _ = Signal.Sequence(Five, Three).CollectArray().Subscribe(rangeArray.Add);
        _ = Signal.Sequence(Five, Three).CollectList().Subscribe(rangeList.Add);
        await Assert.That(rangeArray[0].SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(rangeList[0].SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(await Signal.Sequence(Ten, Three).FirstAsync().ConfigureAwait(false)).IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).FirstOrDefaultAsync().ConfigureAwait(false))
            .IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).FirstOrDefaultAsync(Nine).ConfigureAwait(false))
            .IsEqualTo(Ten);
        await Assert.That(await Signal.Sequence(Ten, Three).LastAsync().ConfigureAwait(false)).IsEqualTo(Twelve);
        await Assert.That(await Signal.Sequence(Ten, Three).LastOrDefaultAsync().ConfigureAwait(false))
            .IsEqualTo(Twelve);
        await Assert.That(await Signal.None<int>().LastOrDefaultAsync(Nine).ConfigureAwait(false)).IsEqualTo(Nine);
        await Assert.That(await Signal.Sequence(One, Three).CountAsync(CancellationToken.None).ConfigureAwait(false))
            .IsEqualTo(Three);
        await Assert.That(
                await Signal.Sequence(One, Three).CountAsync(value => value > One, CancellationToken.None)
                    .ConfigureAwait(false))
            .IsEqualTo(Two);
        await Assert.That(
                await Signal.Sequence(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false))
            .IsEqualTo(ThreeLong);
        await Assert.That(
                await Signal.Sequence(One, Three).LongCount(value => value > One).ToTask(CancellationToken.None)
                    .ConfigureAwait(false))
            .IsEqualTo(TwoLong);
        await Assert.That(await Signal.Sequence(One, Three).AnyAsync(CancellationToken.None).ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).AnyAsync(value => value == Two, CancellationToken.None)
                    .ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).All(value => value < Four).ToTask(CancellationToken.None)
                    .ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).Contains(Three).ToTask(CancellationToken.None).ConfigureAwait(false))
            .IsTrue();
        var collectedArray = await Signal.Sequence(Five, Three).CollectArrayAsync().ConfigureAwait(false);
        var collectedList = await Signal.Sequence(Five, Three).CollectListAsync().ConfigureAwait(false);
        await Assert.That(collectedArray.SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(collectedList.SequenceEqual([Five, Six, Seven])).IsTrue();
        using CancellationTokenSource canceled = new();
        await canceled.CancelAsync().ConfigureAwait(false);
        var canceledTask = Signal.Silent<int>().ToTask(canceled.Token);
        await Assert.That(canceledTask.IsCanceled).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.Count<int>(null!, value => value > 0));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.LongCount<int>(null!, value => value > 0));
        _ = Assert.Throws<ArgumentNullException>(() =>
            LinqExtensions.Blend<int>((IObservable<IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            LinqExtensions.Blend<int>((IEnumerable<IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.Race<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.CollectArray<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(null!, _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => SubscribeExtensions.Subscribe<int>(null!, _ => { }, _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(_ => { }, (Action<Exception>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.None<int>().Recover<int, InvalidOperationException>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => ((IEnumerable<IObservable<int>>)null!).Recover());
        _ = Assert.Throws<ArgumentNullException>(() => Signal.CreateSafe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            StateSignalExtensions.ToReadOnlyState<int, int>(null!, One, value => value));
        _ = Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        _ = Assert.Throws<ArgumentNullException>(() => TaskSignal.Create<int>(null!));
    }

    /// <summary>Verifies non-range task terminal sinks use the observer-backed async paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NonRangeTaskTerminalsUseObserverBackedSinks()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        await Assert.That(await source.CountAsync(CancellationToken.None).ConfigureAwait(false)).IsEqualTo(Two);
        await Assert.That(
                await source.CountAsync(value => value > Three, CancellationToken.None).ConfigureAwait(false))
            .IsEqualTo(One);
        await Assert.That(await source.AnyAsync(CancellationToken.None).ConfigureAwait(false)).IsTrue();
        await Assert.That(
                await source.AnyAsync(value => value == One, CancellationToken.None).ConfigureAwait(false))
            .IsFalse();
        using CancellationTokenSource canceledTerminal = new();
        await canceledTerminal.CancelAsync().ConfigureAwait(false);
        await Assert.That(source.AnyAsync(canceledTerminal.Token).IsCanceled).IsTrue();
        await Assert.That(source.CountAsync(canceledTerminal.Token).IsCanceled).IsTrue();
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

    /// <summary>Verifies optimized coordinator and async enumerable branches cover remaining gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OptimizedCoordinatorAndAsyncEnumerableBranchesCoverRemainingGaps()
    {
        await VerifyAsyncEnumerableShiftAndExpireAsync().ConfigureAwait(false);
        await VerifyRaceSyncLatestAndSwitchBranches();
        await VerifyProbeBranches();
        await VerifyCalmAppendAndForkJoinBranches();
    }

    /// <summary>Verifies range timing, queues, and thread pool cover remaining gaps.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RangeTimingQueuesAndThreadPoolCoverRemainingGaps()
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
        VirtualClock scheduledRangeClock = new(DateTimeOffset.UnixEpoch);
        List<int> scheduledRange = [];
        var scheduledRangeCompleted = 0;
        _ = Signal.Sequence(Three, Three, scheduledRangeClock)
            .Subscribe(scheduledRange.Add, ex => throw ex, () => scheduledRangeCompleted++);
        scheduledRangeClock.Start();
        await Assert.That(scheduledRange.SequenceEqual([Three, Four, Five])).IsTrue();
        await Assert.That(scheduledRangeCompleted).IsEqualTo(1);
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch))).IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.After(FixedTimestamp.AddMilliseconds(1))).IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(
                Signal.PairLatest(
                    Signal.Sequence(One, Two),
                    Signal.Sequence(Three, Two),
                    (left, right) => left + right))
            .IsNotNull();
        List<int> toSignalValues = [];
        _ = new[] { One, Two }.ToSignal().Subscribe(toSignalValues.Add);
        _ = new[] { Three, Four }.ToSignal(CancellationToken.None).Subscribe(toSignalValues.Add);
        int[] expectedToSignalValues = [One, Two, Three, Four];
        await Assert.That(toSignalValues.SequenceEqual(expectedToSignalValues)).IsTrue();
    }

    /// <summary>Verifies task-backed signals, long count, and contains operators.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyTaskSignalsCountAndContainsAsync()
    {
        var firstTaskSignal = await Signal.FromTask(_ => Task.FromResult(Five)).FirstAsync().ConfigureAwait(false);
        var secondTaskSignal = await Signal.FromTask(_ => Task.FromResult(Six), Sequencer.Immediate).FirstAsync()
            .ConfigureAwait(false);
        await Assert.That(firstTaskSignal).IsEqualTo(Five);
        await Assert.That(secondTaskSignal).IsEqualTo(Six);
        await Assert.That(await Task.FromResult(Seven).HandleCancellation().ConfigureAwait(false)).IsEqualTo(Seven);
        await Assert.That(await Task.FromCanceled<int>(new(true)).HandleCancellation().ConfigureAwait(false))
            .IsEqualTo(0);
        List<long> longCount = [];
        _ = Signal.Sequence(One, Four).LongCount(value => value % Two == 0).Subscribe(longCount.Add);
        await Assert.That(longCount.SequenceEqual(ExpectedSingleTwoLong)).IsTrue();
        List<bool> containsWithComparer = [];
        _ = Signal.Sequence(One, Three).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Nine, EqualityComparer<int>.Default).Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        await Assert.That(containsWithComparer.SequenceEqual(ExpectedContainsResults)).IsTrue();
    }

    /// <summary>Verifies alias operators, buffer guard clauses, null-argument guards, and cancellation.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately verifies eager argument validation thrown synchronously, before the awaitable method returns its task.")]
    private static async Task VerifyAliasGuardsAndNullArgumentChecksAsync()
    {
        List<int> startWithAlias = [];
        _ = Signal.Emit(Two).Prepend(One).Subscribe(startWithAlias.Add);
        int[] expectedStartWithAlias = [One, Two];
        await Assert.That(startWithAlias.SequenceEqual(expectedStartWithAlias)).IsTrue();
        await Assert.That(Signal.Emit(One).DelayStart(TimeSpan.Zero)).IsNotNull();
        await Assert.That(await Signal.None<int>().FirstOrDefaultAsync().ConfigureAwait(false)).IsEqualTo(0);
        var noneWitnessCompleted = 0;
        _ = Signal.None(Sequencer.Immediate, One).Subscribe(_ => { }, ex => throw ex, () => noneWitnessCompleted++);
        await Assert.That(noneWitnessCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.Buffer<int>(null!, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(0));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.Buffer<int>(null!, One, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(0, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Emit(One).Buffer(One, 0));
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstAsync());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstOrDefaultAsync());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FirstOrDefaultAsync(One));
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToTask());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).LastOrDefaultAsync(One));
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).AnyAsync());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectArrayAsync());
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectListAsync());
        Signal<int> pending = new();
        using CancellationTokenSource cancelAfterSubscribe = new();
        var pendingTask = pending.ToTask(cancelAfterSubscribe.Token);
        await cancelAfterSubscribe.CancelAsync().ConfigureAwait(false);
        await Assert.That(pendingTask.IsCanceled).IsTrue();
    }

    /// <summary>Verifies immediate signal observer failure branches and the map late-notification branch.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyObserverFailureBranchesAndMap()
    {
        _ = Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => new ReturnSignal<int>(One, Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => new EmptySignal<int>(Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnCompleted: true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(() =>
            new ThrowSignal<int>(new InvalidOperationException("throw-signal"), Sequencer.Immediate)
                .Subscribe(new ThrowingWitness<int>(throwOnError: true)).Dispose());
        ReturnSignal<int>.Return returnWitness = new(new RecordingWitness<int>(), EmptyDisposable.Instance);
        returnWitness.OnError(new InvalidOperationException("return-inner"));
        EmptySignal<int>.Empty emptyWitness = new(new RecordingWitness<int>(), EmptyDisposable.Instance);
        emptyWitness.OnNext(One);
        emptyWitness.OnError(new InvalidOperationException("empty-inner"));
        RecordingWitness<int> mapObserver = new();
        ScriptedObservable<int> badSource = new(observer =>
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
        Signal<int> signal = new();
        _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe<int>(null!));
        List<int> actionValues = [];
        using var actionSubscription = signal.Subscribe(actionValues.Add);
        using var s1 = signal.Subscribe(new RecordingWitness<int>());
        using var s2 = signal.Subscribe(new RecordingWitness<int>());
        using var s3 = signal.Subscribe(new RecordingWitness<int>());
        using var s4 = signal.Subscribe(new RecordingWitness<int>());
        using var s5 = signal.Subscribe(new RecordingWitness<int>());
        _ = Assert.Throws<InvalidOperationException>(() => signal.OnError(new InvalidOperationException("many")));
    }

    /// <summary>Verifies FlatMap terminal completion, disposal, and the null-selector and inner-error branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyFlatMapTerminalAndErrorBranches()
    {
        Signal<IObservable<int>> outer = new();
        Signal<int> firstInner = new();
        Signal<int> secondInner = new();
        List<int> selectManyValues = [];
        var selectManyCompleted = 0;
        using (outer.FlatMap(inner => inner)
                   .Subscribe(selectManyValues.Add, ex => throw ex, () => selectManyCompleted++))
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
        Signal<IObservable<int>> disposedOuter = new();
        Signal<int> disposedInner = new();
        List<int> disposedValues = [];
        var disposedSubscription = disposedOuter.FlatMap(inner => inner).Subscribe(disposedValues.Add);
        disposedOuter.OnNext(disposedInner);
        disposedSubscription.Dispose();
        disposedSubscription.Dispose();
        disposedInner.OnNext(Three);
        await Assert.That(disposedValues.Count).IsEqualTo(0);
        _ = Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner).Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(() => outer.FlatMap(inner => inner, (_, right) => right).Subscribe(null!));
        List<string> nullSelectorErrors = [];
        _ = Signal.Emit(One).FlatMap<int, int>(_ => null!).Subscribe(_ => { }, ex => nullSelectorErrors.Add(ex.Message));
        await Assert.That(nullSelectorErrors.SequenceEqual(ExpectedFlatMapSelectorNull)).IsTrue();
        List<string> nullCollectionErrors = [];
        _ = Signal.Emit(One).FlatMap<int, int, int>(_ => null!, (left, right) => left + right)
            .Subscribe(_ => { }, ex => nullCollectionErrors.Add(ex.Message));
        await Assert.That(nullCollectionErrors.SequenceEqual(ExpectedFlatMapCollectionSelectorNull)).IsTrue();
        List<string> resultInnerErrors = [];
        _ = Signal.Emit(One)
            .FlatMap(
            _ => Signal.Fail<int>(new InvalidOperationException("result-inner")),
            (left, right) => left + right).Subscribe(_ => { }, ex => resultInnerErrors.Add(ex.Message));
        await Assert.That(resultInnerErrors.SequenceEqual(ExpectedResultInner)).IsTrue();
        List<string> subscribeErrors = [];
        _ = Signal.Emit(One)
            .FlatMap(_ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe")))
            .Subscribe(_ => { }, ex => subscribeErrors.Add(ex.Message));
        await Assert.That(subscribeErrors.SequenceEqual(ExpectedInnerSubscribe)).IsTrue();
    }

    /// <summary>Verifies async enumerable subscription, shift timing, and expire timeout branches.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAsyncEnumerableShiftAndExpireAsync()
    {
        _ = Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable(AsyncValues(One)).Subscribe(null!));
        List<int> asyncValues = [];
        TaskCompletionSource<object?> asyncCompleted = new();
        using CancellationTokenSource asyncToken = new();
        _ = Signal.FromAsyncEnumerable(AsyncValues(Three), asyncToken.Token).Subscribe(
            asyncValues.Add,
            ex => asyncCompleted.TrySetException(ex),
            () => asyncCompleted.TrySetResult(null));
        await asyncCompleted.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false);
        int[] expectedAsyncValues = [0, One, Two];
        await Assert.That(asyncValues.SequenceEqual(expectedAsyncValues)).IsTrue();
        var exact = await Signal.FromAsyncEnumerable(AsyncValues(Sixteen)).CollectArrayAsync().ConfigureAwait(false);
        var grown = await Signal.FromAsyncEnumerable(AsyncValues(Seventeen)).CollectArrayAsync().ConfigureAwait(false);
        await Assert.That(exact.Length).IsEqualTo(Sixteen);
        await Assert.That(exact[Fifteen]).IsEqualTo(Fifteen);
        await Assert.That(grown.Length).IsEqualTo(Seventeen);
        await Assert.That(grown[Sixteen]).IsEqualTo(Sixteen);
        VirtualClock shiftedClock = new(DateTimeOffset.UnixEpoch);
        List<int> shifted = [];
        _ = Signal.Sequence(Three, Three).Shift(TimeSpan.FromTicks(Two), shiftedClock).Subscribe(shifted.Add);
        await Assert.That(shifted.Count).IsEqualTo(0);
        shiftedClock.AdvanceBy(TimeSpan.FromTicks(Two));
        int[] expectedShifted = [Three, Four, Five];
        await Assert.That(shifted.SequenceEqual(expectedShifted)).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Expire(TimeSpan.Zero).Subscribe(null!));
        VirtualClock timeoutClock = new(DateTimeOffset.UnixEpoch);
        RecordingWitness<int> timeout = new();
        _ = Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), timeoutClock).Subscribe(timeout);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(timeout.Errors[0] is TimeoutException).IsTrue();
        RecordingWitness<int> expireCompleted = new();
        _ = new ScriptedObservable<int>(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-expire"));
            observer.OnCompleted();
        }).Expire(TimeSpan.FromTicks(Ten), new VirtualClock(DateTimeOffset.UnixEpoch)).Subscribe(expireCompleted);
        int[] expectedExpireCompleted = [One];
        await Assert.That(expireCompleted.Values.SequenceEqual(expectedExpireCompleted)).IsTrue();
        await Assert.That(expireCompleted.Completed).IsEqualTo(1);
        await Assert.That(expireCompleted.Errors.Count).IsEqualTo(0);
        RecordingWitness<int> expireError = new();
        _ = Signal.Fail<int>(new InvalidOperationException("expire-error"))
            .Expire(TimeSpan.FromTicks(Ten), new VirtualClock(DateTimeOffset.UnixEpoch)).Subscribe(expireError);
        await Assert.That(expireError.Errors[0].Message).IsEqualTo("expire-error");
    }

    /// <summary>Verifies the race, synchronized-latest, and switch coordinator branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyRaceSyncLatestAndSwitchBranches()
    {
        Signal<IObservable<int>> raceOuter = new();
        Signal<int> raceWinner = new();
        Signal<int> raceLoser = new();
        RecordingWitness<int> race = new();
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

        TrackingDisposableObservable<int> raceLosing = new();
        TrackingDisposableObservable<int> raceWinning = new();
        RecordingWitness<int> raceWithDisposables = new();
        using (Signal.Race(raceWinning, raceLosing).Subscribe(raceWithDisposables))
        {
            raceWinning.Observer!.OnNext(Three);
            await Assert.That(raceLosing.DisposeCount).IsEqualTo(1);
            await Assert.That(raceWinning.DisposeCount).IsEqualTo(0);
            await Assert.That(raceWithDisposables.Values.SequenceEqual([Three])).IsTrue();
        }

        await Assert.That(raceWinning.DisposeCount).IsEqualTo(1);
        Signal<IObservable<int>> raceCompletionOuter = new();
        Signal<int> raceCompletionWinner = new();
        CapturingObservable<int> raceCompletionLoser = new();
        RecordingWitness<int> raceCompletion = new();
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
        Signal<int> combineLeft = new();
        Signal<int> combineRight = new();
        RecordingWitness<int> combined = new();
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
        Signal<IObservable<int>> switchOuter = new();
        CapturingObservable<int> staleInner = new();
        CapturingObservable<int> currentInner = new();
        RecordingWitness<int> switched = new();
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

        await VerifySwitchTerminalGatingBranches();
    }

    /// <summary>Verifies switch outer-error, deferred-completion, and post-terminal gating branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchTerminalGatingBranches()
    {
        // Outer error is forwarded once and gates all subsequent notifications.
        Signal<IObservable<int>> outerErrorOuter = new();
        CapturingObservable<int> outerErrorInner = new();
        RecordingWitness<int> outerErrored = new();
        using (outerErrorOuter.SwitchTo().Subscribe(outerErrored))
        {
            outerErrorOuter.OnNext(outerErrorInner);
            outerErrorOuter.OnError(new InvalidOperationException("outer-switch"));
            outerErrorInner.Observer!.OnNext(One);
            outerErrorInner.Observer.OnCompleted();
            outerErrorOuter.OnNext(outerErrorInner);
        }

        await Assert.That(outerErrored.Errors[0].Message).IsEqualTo("outer-switch");
        await Assert.That(outerErrored.Errors.Count).IsEqualTo(1);
        await Assert.That(outerErrored.Values.Count).IsEqualTo(0);

        // Outer completes while the inner is still active: completion is deferred until the inner finishes.
        Signal<IObservable<int>> deferredOuter = new();
        CapturingObservable<int> deferredInner = new();
        RecordingWitness<int> deferred = new();
        using (deferredOuter.SwitchTo().Subscribe(deferred))
        {
            deferredOuter.OnNext(deferredInner);
            deferredOuter.OnCompleted();
            await Assert.That(deferred.Completed).IsEqualTo(0);
            deferredInner.Observer!.OnCompleted();
        }

        await Assert.That(deferred.Completed).IsEqualTo(1);

        // A stale inner completion (superseded version) does not complete the observer.
        Signal<IObservable<int>> staleCompleteOuter = new();
        CapturingObservable<int> staleCompleteFirst = new();
        CapturingObservable<int> staleCompleteSecond = new();
        RecordingWitness<int> staleCompleted = new();
        using (staleCompleteOuter.SwitchTo().Subscribe(staleCompleted))
        {
            staleCompleteOuter.OnNext(staleCompleteFirst);
            var staleObserver = staleCompleteFirst.Observer!;
            staleCompleteOuter.OnNext(staleCompleteSecond);
            staleCompleteOuter.OnCompleted();
            staleObserver.OnCompleted();
            await Assert.That(staleCompleted.Completed).IsEqualTo(0);
            staleCompleteSecond.Observer!.OnCompleted();
        }

        await Assert.That(staleCompleted.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies the probe operator error, disposal, and completion branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyProbeBranches()
    {
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Probe(TimeSpan.Zero).Subscribe(null!));
        RecordingWitness<int> probeError = new();
        _ = Signal.Fail<int>(new InvalidOperationException("probe-error"))
            .Probe(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch)).Subscribe(probeError);
        await Assert.That(probeError.Errors[0].Message).IsEqualTo("probe-error");
        Signal<int> probeSource = new();
        var probeSubscription = probeSource.Probe(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch))
            .Subscribe(new RecordingWitness<int>());
        probeSubscription.Dispose();
        probeSubscription.Dispose();
        RecordingWitness<int> completedProbe = new();
        _ = new ScriptedObservable<int>(observer =>
        {
            observer.OnCompleted();
            observer.OnNext(One);
        }).Probe(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch)).Subscribe(completedProbe);
        await Assert.That(completedProbe.Completed).IsEqualTo(1);
        await Assert.That(completedProbe.Values.Count).IsEqualTo(0);
        RecordingWitness<int> directCurrentThreadExpire = new();
        RecordingWitness<int> directCurrentThreadProbe = new();
        _ = Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadExpire);
        _ = Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(directCurrentThreadProbe);
        int[] expectedDirectCurrentThreadExpire = [One];
        await Assert.That(directCurrentThreadExpire.Values.SequenceEqual(expectedDirectCurrentThreadExpire)).IsTrue();
        await Assert.That(directCurrentThreadExpire.Completed).IsEqualTo(1);
        await Assert.That(directCurrentThreadProbe.Values.Count).IsEqualTo(0);
        await Assert.That(directCurrentThreadProbe.Completed).IsEqualTo(1);
        RecordingWitness<int> currentThreadExpire = new();
        RecordingWitness<int> currentThreadProbe = new();
        _ = Sequencer.CurrentThread.Schedule(() =>
        {
            _ = Signal.Emit(One).Expire(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadExpire);
            _ = Signal.Emit(Two).Probe(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(currentThreadProbe);
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
        RecordingWitness<int> calmError = new();
        _ = Signal.Fail<int>(new InvalidOperationException("calm-error"))
            .Calm(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch)).Subscribe(calmError);
        await Assert.That(calmError.Errors[0].Message).IsEqualTo("calm-error");
        VirtualClock calmClock = new(DateTimeOffset.UnixEpoch);
        Signal<int> calmSource = new();
        List<int> calmValues = [];
        _ = calmSource.Calm(TimeSpan.FromTicks(Five), calmClock).Subscribe(calmValues.Add);
        calmSource.OnNext(One);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        calmSource.OnNext(Two);
        calmClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(calmValues.Count).IsEqualTo(0);
        calmClock.AdvanceBy(TimeSpan.FromTicks(Four));
        int[] expectedCalmValues = [Two];
        await Assert.That(calmValues.SequenceEqual(expectedCalmValues)).IsTrue();
        _ = Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Prepend(0).Append(Two).Subscribe(
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
        RecordingWitness<int> appendError = new();
        _ = Signal.Fail<int>(new InvalidOperationException("append-error")).Append(One).Subscribe(appendError);
        await Assert.That(appendError.Errors[0].Message).IsEqualTo("append-error");
        RecordingWitness<int> forkLeftFirst = new();
        Signal<int> forkLeft = new();
        Signal<int> forkRight = new();
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
        RecordingWitness<int> forkRightFirst = new();
        Signal<int> forkOtherLeft = new();
        Signal<int> forkOtherRight = new();
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
        RecordingWitness<Moment<int>> immediateMoments = new();
        Signal.Sequence(One, Three).Timestamp(Sequencer.Immediate).Subscribe(immediateMoments).Dispose();
        IEnumerable<int> expectedImmediateMoments = [One, Two, Three];
        int[] immediateMomentValues =
            [immediateMoments.Values[0].Value, immediateMoments.Values[1].Value, immediateMoments.Values[Two].Value];
        await Assert.That(immediateMomentValues.SequenceEqual(expectedImmediateMoments)).IsTrue();
        await Assert.That(immediateMoments.Completed).IsEqualTo(1);
        List<Moment<int>> clockMoments = [];
        var clockMomentCompleted = 0;
        _ = Signal.Sequence(Four, Two).Timestamp(new VirtualClock(DateTimeOffset.UnixEpoch))
            .Subscribe(clockMoments.Add, ex => throw ex, () => clockMomentCompleted++);
        IEnumerable<int> expectedClockMoments = [Four, Five];
        int[] clockMomentValues = [clockMoments[0].Value, clockMoments[1].Value];
        await Assert.That(clockMomentValues.SequenceEqual(expectedClockMoments)).IsTrue();
        await Assert.That(clockMomentCompleted).IsEqualTo(1);
        List<Moment<int>> immediateMomentActions = [];
        var immediateMomentCompleted = 0;
        var immediateTimestampSignal =
            (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(Sequencer.Immediate);
        immediateTimestampSignal.Subscribe(immediateMomentActions.Add, ex => throw ex, () => immediateMomentCompleted++)
            .Dispose();
        IEnumerable<int> expectedImmediateMomentActions = [Two, Three];
        int[] immediateMomentActionValues = [immediateMomentActions[0].Value, immediateMomentActions[1].Value];
        await Assert.That(immediateMomentActionValues.SequenceEqual(expectedImmediateMomentActions)).IsTrue();
        await Assert.That(immediateMomentCompleted).IsEqualTo(1);
        RecordingWitness<Moment<int>> clockMomentObserver = new();
        var clockTimestampSignal =
            (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(new VirtualClock(DateTimeOffset.UnixEpoch));
        clockTimestampSignal.Subscribe(clockMomentObserver).Dispose();
        IEnumerable<int> expectedClockMomentObserver = [Two, Three];
        int[] clockMomentObserverValues = [clockMomentObserver.Values[0].Value, clockMomentObserver.Values[1].Value];
        await Assert.That(clockMomentObserverValues.SequenceEqual(expectedClockMomentObserver)).IsTrue();
        await Assert.That(clockMomentObserver.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => immediateTimestampSignal.Subscribe((IObserver<Moment<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateTimestampSignal.Subscribe((Action<Moment<int>>)null!, _ => { }, () => { }));
    }

    /// <summary>Verifies the time-interval operator immediate and clock-backed branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyTimeIntervalBranches()
    {
        RecordingWitness<TimeInterval<int>> immediateIntervals = new();
        Signal.Sequence(One, Three).TimeInterval(Sequencer.Immediate).Subscribe(immediateIntervals).Dispose();
        IEnumerable<int> expectedImmediateIntervals = [One, Two, Three];
        int[] immediateIntervalValues =
        [
            immediateIntervals.Values[0].Value, immediateIntervals.Values[1].Value, immediateIntervals.Values[Two].Value
        ];
        await Assert.That(immediateIntervalValues.SequenceEqual(expectedImmediateIntervals)).IsTrue();
        await Assert.That(immediateIntervals.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Completed).IsEqualTo(1);
        List<TimeInterval<int>> clockIntervals = [];
        var clockIntervalCompleted = 0;
        _ = Signal.Sequence(Four, Three).TimeInterval(new VirtualClock(DateTimeOffset.UnixEpoch))
            .Subscribe(clockIntervals.Add, ex => throw ex, () => clockIntervalCompleted++);
        IEnumerable<int> expectedClockIntervals = [Four, Five, Six];
        int[] clockIntervalValues = [clockIntervals[0].Value, clockIntervals[1].Value, clockIntervals[Two].Value];
        await Assert.That(clockIntervalValues.SequenceEqual(expectedClockIntervals)).IsTrue();
        await Assert.That(clockIntervals[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalCompleted).IsEqualTo(1);
        List<TimeInterval<int>> immediateIntervalActions = [];
        var immediateIntervalCompleted = 0;
        var immediateIntervalSignal =
            (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Two).TimeInterval(Sequencer.Immediate);
        immediateIntervalSignal
            .Subscribe(immediateIntervalActions.Add, ex => throw ex, () => immediateIntervalCompleted++)
            .Dispose();
        IEnumerable<int> expectedImmediateIntervalActions = [Two, Three];
        int[] immediateIntervalActionValues = [immediateIntervalActions[0].Value, immediateIntervalActions[1].Value];
        await Assert.That(immediateIntervalActionValues.SequenceEqual(expectedImmediateIntervalActions)).IsTrue();
        await Assert.That(immediateIntervalCompleted).IsEqualTo(1);
        RecordingWitness<TimeInterval<int>> clockIntervalObserver = new();
        var clockIntervalSignal =
            (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Three)
                .TimeInterval(new VirtualClock(DateTimeOffset.UnixEpoch));
        clockIntervalSignal.Subscribe(clockIntervalObserver).Dispose();
        IEnumerable<int> expectedClockIntervalObserver = [Two, Three, Four];
        int[] clockIntervalObserverValues =
        [
            clockIntervalObserver.Values[0].Value, clockIntervalObserver.Values[1].Value,
            clockIntervalObserver.Values[Two].Value
        ];
        await Assert.That(clockIntervalObserverValues.SequenceEqual(expectedClockIntervalObserver)).IsTrue();
        await Assert.That(clockIntervalObserver.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateIntervalSignal.Subscribe((IObserver<TimeInterval<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateIntervalSignal.Subscribe((Action<TimeInterval<int>>)null!, _ => { }, () => { }));
    }

    /// <summary>Verifies delay-start signal branches, the sequencer work item, and queue guard clauses.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyDelayStartAndWorkItemBranches()
    {
        RecordingWitness<int> shiftedObserver = new();
        Signal.Sequence(One, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedObserver).Dispose();
        int[] expectedShiftedObserver = [One, Two];
        await Assert.That(shiftedObserver.Values.SequenceEqual(expectedShiftedObserver)).IsTrue();
        await Assert.That(shiftedObserver.Completed).IsEqualTo(1);
        List<int> shiftedActions = [];
        var shiftedActionCompleted = 0;
        _ = Signal.Sequence(Three, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate)
            .Subscribe(shiftedActions.Add, ex => throw ex, () => shiftedActionCompleted++);
        int[] expectedShiftedActions = [Three, Four];
        await Assert.That(shiftedActions.SequenceEqual(expectedShiftedActions)).IsTrue();
        await Assert.That(shiftedActionCompleted).IsEqualTo(1);
        var currentThreadShift =
            (IRequireCurrentThread<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.CurrentThread);
        await Assert.That(currentThreadShift.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        var inlineShift = (IInlineSignal<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate);
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Sequence(One, One)
            .DelayStart(TimeSpan.Zero, Sequencer.Immediate)
            .Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe((Action<int>)null!, _ => { }, () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe(_ => { }, _ => { }, null!));
        List<int> helperValues = [];
        SequencerWorkItem<ISequencer, int> helper = new(Sequencer.Immediate, One, (_, state) =>
        {
            helperValues.Add(state);
            return EmptyDisposable.Instance;
        });
        helper.Invoke();
        helper.Dispose();
        helper.Invoke();
        int[] expectedHelperValues = [One];
        await Assert.That(helperValues.SequenceEqual(expectedHelperValues)).IsTrue();
        ScheduledItem<int> unusedScheduled =
            ScheduledItem.Create(Sequencer.Immediate, "unused", (_, _) => EmptyDisposable.Instance, One);
        await Assert.That(new SequencerQueue<int>().Remove(unusedScheduled)).IsFalse();
        _ = Assert.Throws<ArgumentOutOfRangeException>(CreatePriorityQueueWithInvalidCapacity);
        PriorityQueue<int> shrink = new(ThirtyTwo);
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
        TaskCompletionSource<int> absoluteRan = new();
        var absolute = ThreadPoolSequencer.Instance.Schedule(Five, FixedTimestamp, (_, state) =>
        {
            if (!absoluteRan.TrySetResult(state))
            {
                throw new InvalidOperationException("Thread pool completion was already set.");
            }

            return EmptyDisposable.Instance;
        });
        await Assert.That(await absoluteRan.Task.WaitAsync(TimeSpan.FromSeconds(Five)).ConfigureAwait(false))
            .IsEqualTo(Five);
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
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="state">The state passed to the scheduled action.</param>
    /// <param name="action">The action invoked when the work item runs.</param>
    /// <returns>A new scheduled work item.</returns>
    private static ThreadPoolSequencer.ScheduledWorkItem<TState> CreateThreadPoolWorkItem<TState>(
        TState state,
        Func<ISequencer, TState, IDisposable> action) => new(ThreadPoolSequencer.Instance, state, action);

    /// <summary>Creates a priority queue with an invalid capacity.</summary>
    private static void CreatePriorityQueueWithInvalidCapacity()
    {
        PriorityQueue<int> invalid = new(-1);
        GC.KeepAlive(invalid);
    }

    /// <summary>Executes the supplied thread pool scheduled work item.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to execute.</param>
    private static void InvokeThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item) =>
        item.Execute();

    /// <summary>Queues the supplied thread pool scheduled work item with the given due time.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to queue.</param>
    /// <param name="dueTime">The delay before the work item runs.</param>
    private static void QueueThreadPoolWorkItem<TState>(
        ThreadPoolSequencer.ScheduledWorkItem<TState> item,
        TimeSpan dueTime) => item.Queue(dueTime);

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

    /// <summary>Observable with a tracked disposable subscription and captured observer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class TrackingDisposableObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets the number of times this subscription was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => DisposeCount++);
        }
    }
}
