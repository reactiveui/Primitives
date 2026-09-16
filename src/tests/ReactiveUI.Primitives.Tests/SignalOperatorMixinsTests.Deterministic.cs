// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests operator and coordinator notification ordering.</summary>
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

    /// <summary>Parity aliases forward prepended, fused, and chained values, and an ignoring recovery completes.</summary>
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
        _ = Signal.Emit(One).FuseLatest(Signal.FromEnumerable([Two, Three]), static (left, right) => left + right)
            .Subscribe(fused.Add);
        int[] expectedFused = [Three, Four];
        await Assert.That(fused.SequenceEqual(expectedFused)).IsTrue();
        List<string> chainedStrings = [];
        _ = Signal.Chain(Signal.Emit("value")).Subscribe(chainedStrings.Add);
        await Assert.That(chainedStrings.SequenceEqual(ExpectedSingleValue)).IsTrue();
        var ignoredCatchCompleted = 0;
        _ = Signal.Fail<int>(new InvalidOperationException("ignored")).Recover<int, Exception>(static _ => Signal.None<int>())
            .Subscribe(static _ => { }, static ex => throw ex, () => ignoredCatchCompleted++);
        await Assert.That(ignoredCatchCompleted).IsEqualTo(1);
    }

    /// <summary>Sequence fast paths collect their values, and a pre-cancelled token cancels the task sink.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [SuppressMessage(
        "Concurrency",
        "PSH1313:Call the async overload from an async method",
        Justification = "The synchronous CollectArray and CollectList operators are the subject under test.")]
    public async Task RangeAsyncFastPathsAndNullGuardsCoverRemainingLines()
    {
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
        await AssertRangeTerminalOperatorsAwaitTheSameResultsAsTheGeneralPaths();
        using CancellationTokenSource canceled = new();
        await canceled.CancelAsync().ConfigureAwait(false);
        var canceledTask = Signal.Silent<int>().ToTask(canceled.Token);
        await Assert.That(canceledTask.IsCanceled).IsTrue();
        AssertOperatorGuardsRejectNullSourcesAndCallbacks();
    }

    /// <summary>Count and any terminals await their results over a plain source, and cancel with a pre-cancelled token.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NonRangeTaskTerminalsUseObserverBackedSinks()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        await Assert.That(await source.CountAsync(CancellationToken.None).ConfigureAwait(false)).IsEqualTo(Two);
        await Assert.That(
                await source.CountAsync(static value => value > Three, CancellationToken.None).ConfigureAwait(false))
            .IsEqualTo(One);
        await Assert.That(await source.AnyAsync(CancellationToken.None).ConfigureAwait(false)).IsTrue();
        await Assert.That(
                await source.AnyAsync(static value => value == One, CancellationToken.None).ConfigureAwait(false))
            .IsFalse();
        using CancellationTokenSource canceledTerminal = new();
        await canceledTerminal.CancelAsync().ConfigureAwait(false);
        await Assert.That(source.AnyAsync(canceledTerminal.Token).IsCanceled).IsTrue();
        await Assert.That(source.CountAsync(canceledTerminal.Token).IsCanceled).IsTrue();
    }

    /// <summary>Scheduled factories, task signals, observer failures, and flat-map faults behave deterministically.</summary>
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

    /// <summary>Async enumerable sources, race, switch, probe, calm, and fork-join coordinators gate their terminals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OptimizedCoordinatorAndAsyncEnumerableBranchesCoverRemainingGaps()
    {
        await VerifyAsyncEnumerableShiftAndExpireAsync().ConfigureAwait(false);
        await VerifyRaceSyncLatestAndSwitchBranches();
        await VerifyProbeBranches();
        await VerifyCalmAppendAndForkJoinBranches();
    }

    /// <summary>Timestamp, time-interval, delay-start, and thread pool work items deliver on their sequencers.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RangeTimingQueuesAndThreadPoolCoverRemainingGaps()
    {
        await VerifyTimestampBranches();
        await VerifyTimeIntervalBranches();
        await VerifyDelayStartAndWorkItemBranches();
        await VerifyThreadPoolWorkItemBranchesAsync().ConfigureAwait(false);
    }

    /// <summary>Asserts the range-specialized terminal operators await the same results as the general paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    private static async Task AssertRangeTerminalOperatorsAwaitTheSameResultsAsTheGeneralPaths()
    {
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
                await Signal.Sequence(One, Three).CountAsync(static value => value > One, CancellationToken.None)
                    .ConfigureAwait(false))
            .IsEqualTo(Two);
        await Assert.That(
                await Signal.Sequence(One, Three).LongCount().ToTask(CancellationToken.None).ConfigureAwait(false))
            .IsEqualTo(ThreeLong);
        await Assert.That(
                await Signal.Sequence(One, Three).LongCount(static value => value > One).ToTask(CancellationToken.None)
                    .ConfigureAwait(false))
            .IsEqualTo(TwoLong);
        await Assert.That(await Signal.Sequence(One, Three).AnyAsync(CancellationToken.None).ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).AnyAsync(static value => value == Two, CancellationToken.None)
                    .ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).All(static value => value < Four).ToTask(CancellationToken.None)
                    .ConfigureAwait(false))
            .IsTrue();
        await Assert.That(
                await Signal.Sequence(One, Three).Contains(Three).ToTask(CancellationToken.None).ConfigureAwait(false))
            .IsTrue();
        var collectedArray = await Signal.Sequence(Five, Three).CollectArrayAsync().ConfigureAwait(false);
        var collectedList = await Signal.Sequence(Five, Three).CollectListAsync().ConfigureAwait(false);
        await Assert.That(collectedArray.SequenceEqual([Five, Six, Seven])).IsTrue();
        await Assert.That(collectedList.SequenceEqual([Five, Six, Seven])).IsTrue();
    }

    /// <summary>Asserts every operator and subscribe overload rejects a null source or callback.</summary>
    private static void AssertOperatorGuardsRejectNullSourcesAndCallbacks()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Count<int>(null!, static value => value > 0));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.LongCount<int>(null!, static value => value > 0));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            LinqExtensions.Blend((IObservable<IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            LinqExtensions.Blend((IEnumerable<IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Race<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.CollectArray<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => SubscribeExtensions.Subscribe<int>(null!, static _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(static _ => { }, static _ => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => SubscribeExtensions.Subscribe<int>(null!, static _ => { }, static _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(null!, static _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Subscribe(static _ => { }, (Action<Exception>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.None<int>().Recover<int, InvalidOperationException>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IEnumerable<IObservable<int>>)null!).Recover());
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.CreateSafe<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            StateSignalExtensions.ToReadOnlyState<int, int>(null!, One, static value => value));
        _ = Assert.Throws<ArgumentNullException>(() => source.ToReadOnlyState(One, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => TaskSignal.Create<int>(null!));
    }

    /// <summary>Verifies the scheduled range fast path and the timing factory aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyScheduledRangeAndTimingFactories()
    {
        VirtualClock scheduledRangeClock = new(DateTimeOffset.UnixEpoch);
        List<int> scheduledRange = [];
        var scheduledRangeCompleted = 0;
        _ = Signal.Sequence(Three, Three, scheduledRangeClock)
            .Subscribe(scheduledRange.Add, static ex => throw ex, () => scheduledRangeCompleted++);
        scheduledRangeClock.Start();
        await Assert.That(scheduledRange.SequenceEqual([Three, Four, Five])).IsTrue();
        await Assert.That(scheduledRangeCompleted).IsEqualTo(1);
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Pulse(TimeSpan.FromTicks(One), new VirtualClock(DateTimeOffset.UnixEpoch)))
            .IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.After(FixedTimestamp.AddMilliseconds(1))).IsNotNull();
        await Assert.That(Signal.After(TimeSpan.FromTicks(One), TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(
                Signal.PairLatest(
                    Signal.Sequence(One, Two),
                    Signal.Sequence(Three, Two),
                    static (left, right) => left + right))
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
        var firstTaskSignal = await Signal.FromTask(static _ => Task.FromResult(Five)).FirstAsync().ConfigureAwait(false);
        var secondTaskSignal = await Signal.FromTask(static _ => Task.FromResult(Six), Sequencer.Immediate).FirstAsync()
            .ConfigureAwait(false);
        await Assert.That(firstTaskSignal).IsEqualTo(Five);
        await Assert.That(secondTaskSignal).IsEqualTo(Six);
        await Assert.That(await Task.FromResult(Seven).HandleCancellation().ConfigureAwait(false)).IsEqualTo(Seven);
        await Assert.That(await Task.FromCanceled<int>(new(true)).HandleCancellation().ConfigureAwait(false))
            .IsEqualTo(0);
        List<long> longCount = [];
        _ = Signal.Sequence(One, Four).LongCount(static value => value % Two == 0).Subscribe(longCount.Add);
        await Assert.That(longCount.SequenceEqual(ExpectedSingleTwoLong)).IsTrue();
        List<bool> containsWithComparer = [];
        _ = Signal.Sequence(One, Three).Contains(Three, EqualityComparer<int>.Default)
            .Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Nine, EqualityComparer<int>.Default)
            .Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Three, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        _ = Signal.Sequence(One, Three).Contains(Nine, new PassthroughComparer()).Subscribe(containsWithComparer.Add);
        await Assert.That(containsWithComparer.SequenceEqual(ExpectedContainsResults)).IsTrue();
    }

    /// <summary>Verifies alias operators, buffer guard clauses, null-argument guards, and cancellation.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    [SuppressMessage(
        "Concurrency",
        "PSH1313:Call the async overload from an async method",
        Justification =
            "The guards under test throw synchronously, before the awaitable method returns its task.")]
    private static async Task VerifyAliasGuardsAndNullArgumentChecksAsync()
    {
        List<int> startWithAlias = [];
        _ = Signal.Emit(Two).Prepend(One).Subscribe(startWithAlias.Add);
        int[] expectedStartWithAlias = [One, Two];
        await Assert.That(startWithAlias.SequenceEqual(expectedStartWithAlias)).IsTrue();
        await Assert.That(Signal.Emit(One).DelayStart(TimeSpan.Zero)).IsNotNull();
        await Assert.That(await Signal.None<int>().FirstOrDefaultAsync().ConfigureAwait(false)).IsEqualTo(0);
        var noneWitnessCompleted = 0;
        _ = Signal.None(Sequencer.Immediate, One).Subscribe(static _ => { }, static ex => throw ex, () => noneWitnessCompleted++);
        await Assert.That(noneWitnessCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Buffer<int>(null!, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Emit(One).Buffer(0));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Buffer<int>(null!, One, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Emit(One).Buffer(0, One));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Emit(One).Buffer(One, 0));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).FirstAsync());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).FirstOrDefaultAsync());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).FirstOrDefaultAsync(One));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).ToTask());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).LastOrDefaultAsync(One));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).AnyAsync());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).CollectArrayAsync());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).CollectListAsync());
        Signal<int> pending = new();
        using CancellationTokenSource cancelAfterSubscribe = new();
        var pendingTask = pending.ToTask(cancelAfterSubscribe.Token);
        await cancelAfterSubscribe.CancelAsync().ConfigureAwait(false);
        await Assert.That(pendingTask.IsCanceled).IsTrue();
    }

    /// <summary>A throwing observer propagates out of an immediate signal, and map drops notifications after its terminal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyObserverFailureBranchesAndMap()
    {
        _ = Assert.Throws<InvalidOperationException>(static () => new ReturnSignal<int>(One, Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(static () => new ReturnSignal<int>(One, Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnCompleted: true)).Dispose());
        _ = Assert.Throws<InvalidOperationException>(static () => new EmptySignal<int>(Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnCompleted: true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(static () =>
            new ThrowSignal<int>(new InvalidOperationException("throw-signal"), Sequencer.Immediate)
                .Subscribe(new ThrowingWitness<int>(throwOnError: true)).Dispose());
        GuardedWitness<int> returnWitness = new(new RecordingWitness<int>(), EmptyDisposable.Instance);
        returnWitness.OnError(new InvalidOperationException("return-inner"));
        GuardedWitness<int> emptyWitness = new(new RecordingWitness<int>(), EmptyDisposable.Instance);
        emptyWitness.OnNext(One);
        emptyWitness.OnError(new InvalidOperationException("empty-inner"));
        RecordingWitness<int> mapObserver = new();
        ScriptedObservable<int> badSource = new(static observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnNext(Two);
            observer.OnError(new InvalidOperationException("late-map"));
            observer.OnCompleted();
        });
        badSource.Map(static value => value).Subscribe(mapObserver).Dispose();
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

    /// <summary>FlatMap completes after its inners, stops on disposal, and faults on a null selector or failing inner.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyFlatMapTerminalAndErrorBranches()
    {
        Signal<IObservable<int>> outer = new();
        Signal<int> firstInner = new();
        Signal<int> secondInner = new();
        List<int> selectManyValues = [];
        var selectManyCompleted = 0;
        using (outer.FlatMap(static inner => inner)
                   .Subscribe(selectManyValues.Add, static ex => throw ex, () => selectManyCompleted++))
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
        var disposedSubscription = disposedOuter.FlatMap(static inner => inner).Subscribe(disposedValues.Add);
        disposedOuter.OnNext(disposedInner);
        disposedSubscription.Dispose();
        disposedSubscription.Dispose();
        disposedInner.OnNext(Three);
        await Assert.That(disposedValues.Count).IsEqualTo(0);
        _ = Assert.Throws<ArgumentNullException>(() => outer.FlatMap(static inner => inner).Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            outer.FlatMap(static inner => inner, static (_, right) => right).Subscribe(null!));
        List<string> nullSelectorErrors = [];
        _ = Signal.Emit(One).FlatMap<int, int>(static _ => null!)
            .Subscribe(static _ => { }, ex => nullSelectorErrors.Add(ex.Message));
        await Assert.That(nullSelectorErrors.SequenceEqual(ExpectedFlatMapSelectorNull)).IsTrue();
        List<string> nullCollectionErrors = [];
        _ = Signal.Emit(One).FlatMap<int, int, int>(static _ => null!, static (left, right) => left + right)
            .Subscribe(static _ => { }, ex => nullCollectionErrors.Add(ex.Message));
        await Assert.That(nullCollectionErrors.SequenceEqual(ExpectedFlatMapCollectionSelectorNull)).IsTrue();
        List<string> resultInnerErrors = [];
        _ = Signal.Emit(One)
            .FlatMap(
                static _ => Signal.Fail<int>(new InvalidOperationException("result-inner")),
                static (left, right) => left + right).Subscribe(static _ => { }, ex => resultInnerErrors.Add(ex.Message));
        await Assert.That(resultInnerErrors.SequenceEqual(ExpectedResultInner)).IsTrue();
        List<string> subscribeErrors = [];
        _ = Signal.Emit(One)
            .FlatMap(static _ => new ThrowOnSubscribeObservable<int>(new InvalidOperationException("inner-subscribe")))
            .Subscribe(static _ => { }, ex => subscribeErrors.Add(ex.Message));
        await Assert.That(subscribeErrors.SequenceEqual(ExpectedInnerSubscribe)).IsTrue();
    }

    /// <summary>Async enumerable sources drain to their subscriber, and shift and expire follow the virtual clock.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyAsyncEnumerableShiftAndExpireAsync()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsyncEnumerable(AsyncValues(One)).Subscribe(null!));
        List<int> asyncValues = [];
        TaskCompletionSource<object?> asyncCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource asyncToken = new();
        _ = Signal.FromAsyncEnumerable(AsyncValues(Three), asyncToken.Token).Subscribe(
            asyncValues.Add,
            ex => asyncCompleted.TrySetException(ex),
            () => asyncCompleted.TrySetResult(null));
        await asyncCompleted.Task.ConfigureAwait(false);
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
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Silent<int>().Expire(TimeSpan.Zero).Subscribe(null!));
        VirtualClock timeoutClock = new(DateTimeOffset.UnixEpoch);
        RecordingWitness<int> timeout = new();
        _ = Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), timeoutClock).Subscribe(timeout);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(One));
        await Assert.That(timeout.Errors[0] is TimeoutException).IsTrue();
        RecordingWitness<int> expireCompleted = new();
        _ = new ScriptedObservable<int>(static observer =>
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

    /// <summary>Race, synchronized-latest, and switch coordinators gate every losing and stale inner.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyRaceSyncLatestAndSwitchBranches()
    {
        await VerifyRaceWinnerGatesTheLosingInners();
        await VerifyRaceDisposesTheLosingSubscriptions();
        await VerifySyncLatestCombinesTheLatestOfBothSides();
        await VerifySwitchToForwardsOnlyTheCurrentInner();
        await VerifySwitchTerminalGatingBranches();
    }

    /// <summary>Verifies the first inner to emit wins the race and every later inner notification is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyRaceWinnerGatesTheLosingInners()
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
    }

    /// <summary>Verifies a race disposes each losing subscription as soon as a winner emerges.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyRaceDisposesTheLosingSubscriptions()
    {
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
    }

    /// <summary>Verifies synchronized-latest emits once both sides have a value and completes with the last side.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySyncLatestCombinesTheLatestOfBothSides()
    {
        Signal<int> combineLeft = new();
        Signal<int> combineRight = new();
        RecordingWitness<int> combined = new();
        using (combineLeft.SyncLatest(combineRight, static (left, right) => left + right).Subscribe(combined))
        {
            combineRight.OnNext(Two);
            combineLeft.OnNext(One);
            combineRight.OnCompleted();
            combineLeft.OnCompleted();
        }

        int[] expectedCombined = [Three];
        await Assert.That(combined.Values.SequenceEqual(expectedCombined)).IsTrue();
        await Assert.That(combined.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies switching forwards nothing from a stale inner, including its error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchToForwardsOnlyTheCurrentInner()
    {
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
    }

    /// <summary>Switch forwards one outer error, defers completion to its current inner, and gates what follows.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchTerminalGatingBranches()
    {
        await VerifySwitchForwardsTheOuterErrorOnceAndGatesWhatFollows();
        await VerifySwitchDefersCompletionUntilTheCurrentInnerFinishes();
        await VerifySwitchGatesEveryNotificationAfterAnInnerError();
    }

    /// <summary>Verifies an outer error is forwarded once and gates every notification that follows it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchForwardsTheOuterErrorOnceAndGatesWhatFollows()
    {
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
    }

    /// <summary>Switch defers its completion to the current inner and ignores a superseded inner's completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchDefersCompletionUntilTheCurrentInnerFinishes()
    {
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

    /// <summary>Checks that an inner error suppresses later outer values and terminal notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySwitchGatesEveryNotificationAfterAnInnerError()
    {
        Signal<IObservable<int>> innerErrorOuter = new();
        CapturingObservable<int> innerErrorFirst = new();
        CapturingObservable<int> innerErrorLate = new();
        RecordingWitness<int> innerErrored = new();
        using (innerErrorOuter.SwitchTo().Subscribe(innerErrored))
        {
            innerErrorOuter.OnNext(innerErrorFirst);
            innerErrorFirst.Observer!.OnError(new InvalidOperationException("inner-switch"));
            innerErrorOuter.OnNext(innerErrorLate);
            innerErrorOuter.OnError(new InvalidOperationException("ignored"));
        }

        await Assert.That(innerErrored.Errors.Count).IsEqualTo(1);
        await Assert.That(innerErrored.Errors[0].Message).IsEqualTo("inner-switch");
        await Assert.That(innerErrorLate.Observer).IsNull();

        Signal<IObservable<int>> innerErrorCompleteOuter = new();
        CapturingObservable<int> innerErrorCompleteInner = new();
        RecordingWitness<int> innerErrorCompleted = new();
        using (innerErrorCompleteOuter.SwitchTo().Subscribe(innerErrorCompleted))
        {
            innerErrorCompleteOuter.OnNext(innerErrorCompleteInner);
            innerErrorCompleteInner.Observer!.OnError(new InvalidOperationException("inner-complete-gate"));
            innerErrorCompleteOuter.OnCompleted();
        }

        await Assert.That(innerErrorCompleted.Errors.Count).IsEqualTo(1);
        await Assert.That(innerErrorCompleted.Completed).IsEqualTo(0);
    }

    /// <summary>Probe forwards a fault, stops after disposal, and completes without a sampled value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyProbeBranches()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Silent<int>().Probe(TimeSpan.Zero).Subscribe(null!));
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
        _ = new ScriptedObservable<int>(static observer =>
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
        int[] expectedDirectCurrentThreadProbe = [Two];
        await Assert.That(directCurrentThreadProbe.Values.SequenceEqual(expectedDirectCurrentThreadProbe)).IsTrue();
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
        int[] expectedCurrentThreadProbe = [Two];
        await Assert.That(currentThreadProbe.Values.SequenceEqual(expectedCurrentThreadProbe)).IsTrue();
        await Assert.That(currentThreadProbe.Completed).IsEqualTo(1);
    }

    /// <summary>Calm emits only the quiet value, append surfaces observer faults, and fork-join waits for both sides.</summary>
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
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Emit(One).Prepend(0).Append(Two).Subscribe(
            static value =>
            {
                if (value != One)
                {
                    return;
                }

                throw new InvalidOperationException("append-next");
            },
            static _ => { },
            static () => { }).Dispose());
        RecordingWitness<int> appendError = new();
        _ = Signal.Fail<int>(new InvalidOperationException("append-error")).Append(One).Subscribe(appendError);
        await Assert.That(appendError.Errors[0].Message).IsEqualTo("append-error");
        RecordingWitness<int> forkLeftFirst = new();
        Signal<int> forkLeft = new();
        Signal<int> forkRight = new();
        using (forkLeft.ForkJoin(forkRight, static (left, right) => left + right).Subscribe(forkLeftFirst))
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
        using (forkOtherLeft.ForkJoin(forkOtherRight, static (left, right) => left + right).Subscribe(forkRightFirst))
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

    /// <summary>Executes the supplied thread pool scheduled work item.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to execute.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void InvokeThreadPoolWorkItem<TState>(ThreadPoolSequencer.ScheduledWorkItem<TState> item) =>
        item.Execute();

    /// <summary>Queues the supplied thread pool scheduled work item with the given due time.</summary>
    /// <typeparam name="TState">The type of the work item state.</typeparam>
    /// <param name="item">The work item to queue.</param>
    /// <param name="dueTime">The delay before the work item runs.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
