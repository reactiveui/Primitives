// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Completes branch and contract coverage for primitive signal operators and aliases.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>The integer constant eight.</summary>
    private const int Eight = 8;

    /// <summary>The integer constant eleven.</summary>
    private const int Eleven = 11;

    /// <summary>The integer constant twenty.</summary>
    private const int Twenty = 20;

    /// <summary>The integer constant twenty-one.</summary>
    private const int TwentyOne = 21;

    /// <summary>The integer constant forty-two used by behavior coverage.</summary>
    private const int BehaviorFortyTwo = 42;

    /// <summary>Expected operator values.</summary>
    private static readonly string[] ExpectedOperatorValues = ["bbb!", "cc!"];

    /// <summary>Expected side-effect values.</summary>
    private static readonly string[] ExpectedSideEffects = ["a!", "bbb!", "cc!"];

    /// <summary>Expected non-null values.</summary>
    private static readonly string[] ExpectedKeepNotNull = ["x", "y"];

    /// <summary>Expected false scalar sequence.</summary>
    private static readonly bool[] ExpectedFalse = [false];

    /// <summary>Expected long-count result.</summary>
    private static readonly long[] ExpectedLongCount = [2L];

    /// <summary>Expected select-many result.</summary>
    private static readonly string[] ExpectedSelectMany = ["1:1", "1:11", "2:2", "2:12"];

    /// <summary>Expected values projected from enumerable collections.</summary>
    private static readonly int[] ExpectedFlatMapValues = [1, Ten, Two, 20];

    /// <summary>Expected spark kind sequence.</summary>
    private static readonly SparkKind[] ExpectedSparkKinds = [SparkKind.OnError];

    /// <summary>Expected spark error messages.</summary>
    private static readonly string[] ExpectedSparkErrors = ["spark"];

    /// <summary>Expected unspark values.</summary>
    private static readonly int[] ExpectedUnsparkValues = [1];

    /// <summary>Expected unspark errors.</summary>
    private static readonly string[] ExpectedUnsparkErrors = ["unspark"];

    /// <summary>Expected rescue values.</summary>
    private static readonly int[] ExpectedRescueValues = [Seven];

    /// <summary>Expected resume values.</summary>
    private static readonly int[] ExpectedResumeValues = [Four, Five];

    /// <summary>Expected final errors.</summary>
    private static readonly string[] ExpectedFinalErrors = ["stop"];

    /// <summary>Expected concat values.</summary>
    private static readonly int[] ExpectedConcatValues = [1, Two, TwentyOne];

    /// <summary>Expected merge values.</summary>
    private static readonly int[] ExpectedMergeValues = [1, Two, Three];

    /// <summary>Expected race values.</summary>
    private static readonly int[] ExpectedRaceValues = [Seven];

    /// <summary>Expected switch values.</summary>
    private static readonly int[] ExpectedSwitchValues = [1, Three];

    /// <summary>Expected latest-combination values.</summary>
    private static readonly string[] ExpectedWithLatestValues = ["2a", "3b"];

    /// <summary>Expected zip values.</summary>
    private static readonly int[] ExpectedZipShortValues = [Eleven];

    /// <summary>Expected delayed scalar values.</summary>
    private static readonly int[] ExpectedDelayedValues = [Three, Four];

    /// <summary>Expected delay-start scalar values.</summary>
    private static readonly int[] ExpectedDelayStartValues = [Two];

    /// <summary>Expected timer values.</summary>
    private static readonly long[] ExpectedTimerValues = [0L, 1L, 2L];

    /// <summary>Expected timestamp values.</summary>
    private static readonly int[] ExpectedTimestampValues = [Eight, Nine];

    /// <summary>Validates null guard coverage across public factories, operators, and observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NullGuardsCoverPublicFactoryOperatorAndObserverContracts()
    {
        var source = Signal.Emit(1);
        var objects = Signal.Emit<object?>("value");
        CoverUnaryOperatorNullGuards(source);
        CoverHigherOrderOperatorNullGuards(source);
        CoverParityOperatorNullGuards(source);
        CoverFactoryAndObserverNullGuards(objects);
        await Task.CompletedTask;
    }

    /// <summary>Exercises successful operator paths and early-termination branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    public async Task OperatorSurfaceCoversSuccessErrorAndEarlyTerminationBranches()
    {
        List<string> values = [];
        List<string> sideEffects = [];
        var terminal = 0;
        Signal.FromEnumerable<object?>(["a", null, Two, "bbb", "cc", Three])
            .KeepType<string>()
            .MapWith("!", (suffix, value) => value + suffix)
            .KeepWith(Two, (min, value) => value.Length >= min)
            .TapWith(sideEffects, (sink, value) => sink.Add(value))
            .CastTo<string>()
            .Skip(1)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Unique(StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add, ex => throw ex, () => terminal++);
        await Assert.That(values.SequenceEqual(ExpectedOperatorValues)).IsTrue();
        await Assert.That(sideEffects.SequenceEqual(ExpectedSideEffects)).IsTrue();
        await Assert.That(terminal).IsEqualTo(1);
        List<string> keepNotNull = [];
        Signal.FromEnumerable([null, "x", null, "y"]).KeepNotNull().Subscribe(keepNotNull.Add);
        await Assert.That(keepNotNull.SequenceEqual(ExpectedKeepNotNull)).IsTrue();
        List<int> emptyTake = [];
        var emptyTakeCompleted = 0;
        Signal.Sequence(1, Three).Take(0).Subscribe(emptyTake.Add, ex => throw ex, () => emptyTakeCompleted++);
        await Assert.That(emptyTake.Count).IsEqualTo(0);
        await Assert.That(emptyTakeCompleted).IsEqualTo(1);
        List<int> skipAll = [];
        Signal.Sequence(1, Three).Skip(Ten).Subscribe(skipAll.Add);
        await Assert.That(skipAll.Count).IsEqualTo(0);
        List<bool> anyFalse = [];
        List<bool> allFalse = [];
        List<bool> containsFalse = [];
        List<long> longCount = [];
        Signal.FromEnumerable([1, Two, Three]).Any(value => value > Nine).Subscribe(anyFalse.Add);
        Signal.FromEnumerable([Two, Four, Five]).All(value => value % Two == 0).Subscribe(allFalse.Add);
        Signal.FromEnumerable([Two, Four, Six]).Contains(Seven).Subscribe(containsFalse.Add);
        Signal.FromEnumerable([1, Two, Three, Four]).LongCount(value => value % Two == 0).Subscribe(longCount.Add);
        await Assert.That(anyFalse.SequenceEqual(ExpectedFalse)).IsTrue();
        await Assert.That(allFalse.SequenceEqual(ExpectedFalse)).IsTrue();
        await Assert.That(containsFalse.SequenceEqual(ExpectedFalse)).IsTrue();
        await Assert.That(longCount.SequenceEqual(ExpectedLongCount)).IsTrue();
        List<string> selectMany = [];
        Signal.FromEnumerable([1, Two])
            .FlatMap(value => Signal.FromEnumerable([value, value + Ten]), (outer, inner) => outer + ":" + inner)
            .Subscribe(selectMany.Add);
        await Assert.That(selectMany.SequenceEqual(ExpectedSelectMany)).IsTrue();
        List<int> flatMapValues = [];
        Signal.FromEnumerable([1, Two]).FlatMapValues<int, int>(value => [value, value * Ten])
            .Subscribe(flatMapValues.Add);
        await Assert.That(flatMapValues.SequenceEqual(ExpectedFlatMapValues)).IsTrue();
    }

    /// <summary>Exercises error materialization, recovery, resume, and retry branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorOperatorsMaterializeRecoverAndResumeDeterministically()
    {
        List<SparkKind> sparkKinds = [];
        List<string> sparkErrors = [];
        List<int> unsparkValues = [];
        List<string> unsparkErrors = [];
        List<int> rescueValues = [];
        List<int> resumeValues = [];
        List<string> finalErrors = [];
        Signal.Fail<int>(new InvalidOperationException("spark")).Spark().Subscribe(spark =>
        {
            sparkKinds.Add(spark.Kind);
            if (spark.Exception is null)
            {
                return;
            }

            sparkErrors.Add(spark.Exception.Message);
        });
        Signal.FromEnumerable(
            [
                Spark.CreateOnNext(1),
                Spark.CreateOnError<int>(new InvalidOperationException("unspark")),
                Spark.CreateOnCompleted<int>()
            ])
            .Unspark()
            .Subscribe(unsparkValues.Add, ex => unsparkErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("recover")).Rescue(error => Signal.Emit(error.Message.Length))
            .Subscribe(rescueValues.Add);
        Signal.Fail<int>(new InvalidOperationException("resume")).Resume(Signal.FromEnumerable([Four, Five]))
            .Subscribe(resumeValues.Add);
        Signal.Lazy(() => Signal.Fail<int>(new InvalidOperationException("stop"))).Reattempt(1)
            .Subscribe(_ => { }, ex => finalErrors.Add(ex.Message));
        await Assert.That(sparkKinds.SequenceEqual(ExpectedSparkKinds)).IsTrue();
        await Assert.That(sparkErrors.SequenceEqual(ExpectedSparkErrors)).IsTrue();
        await Assert.That(unsparkValues.SequenceEqual(ExpectedUnsparkValues)).IsTrue();
        await Assert.That(unsparkErrors.SequenceEqual(ExpectedUnsparkErrors)).IsTrue();
        await Assert.That(rescueValues.SequenceEqual(ExpectedRescueValues)).IsTrue();
        await Assert.That(resumeValues.SequenceEqual(ExpectedResumeValues)).IsTrue();
        await Assert.That(finalErrors.SequenceEqual(ExpectedFinalErrors)).IsTrue();
    }

    /// <summary>Exercises higher-order ordering, racing, switching, and latest-value behavior.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HigherOrderOperatorsHandleAsyncOrderingRacesSwitchingAndLatestValues()
    {
        Signal<int> first = new();
        Signal<int> second = new();
        Signal<IObservable<int>> outer = new();
        List<int> concatValues = [];
        List<int> mergeValues = [];
        List<int> raceValues = [];
        List<int> switchValues = [];
        List<string> withLatestValues = [];
        List<int> zipShortValues = [];
        List<int> forkJoinEmpty = [];
        Dictionary<string, int> completed = [];
        outer.Chain().Subscribe(concatValues.Add, ex => throw ex, () => completed["concat"] = 1);
        outer.OnNext(first);
        outer.OnNext(second);
        first.OnNext(1);
        second.OnNext(Twenty);
        first.OnNext(Two);
        first.OnCompleted();
        second.OnNext(TwentyOne);
        second.OnCompleted();
        outer.OnCompleted();
        Signal.Blend(Signal.FromEnumerable([1, Two]), Signal.FromEnumerable([Three]))
            .Subscribe(mergeValues.Add, ex => throw ex, () => completed["merge"] = 1);
        Signal<int> raceLoser = new();
        Signal<int> raceWinner = new();
        Signal.Race(raceLoser, raceWinner).Subscribe(raceValues.Add, ex => throw ex, () => completed["race"] = 1);
        raceWinner.OnNext(Seven);
        raceLoser.OnNext(NinetyNine);
        raceWinner.OnCompleted();
        Signal<IObservable<int>> switchOuter = new();
        Signal<int> oldInner = new();
        Signal<int> newInner = new();
        switchOuter.SwitchTo().Subscribe(switchValues.Add, ex => throw ex, () => completed["switch"] = 1);
        switchOuter.OnNext(oldInner);
        oldInner.OnNext(1);
        switchOuter.OnNext(newInner);
        oldInner.OnNext(Two);
        newInner.OnNext(Three);
        switchOuter.OnCompleted();
        newInner.OnCompleted();
        Signal<int> left = new();
        Signal<string> right = new();
        left.Latch(right, (l, r) => l + r).Subscribe(withLatestValues.Add);
        left.OnNext(1);
        right.OnNext("a");
        left.OnNext(Two);
        right.OnNext("b");
        left.OnNext(Three);
        left.OnCompleted();
        Signal.FromEnumerable([1, Two, Three])
            .Pair(Signal.Emit(Ten), (l, r) => l + r)
            .Subscribe(zipShortValues.Add, ex => throw ex, () => completed["zip"] = 1);
        Signal.None<int>()
            .ForkJoin(Signal.Emit(1), (l, r) => l + r)
            .Subscribe(forkJoinEmpty.Add, ex => throw ex, () => completed["forkJoinEmpty"] = 1);
        await Assert.That(concatValues.SequenceEqual(ExpectedConcatValues)).IsTrue();
        await Assert.That(mergeValues.Order().SequenceEqual(ExpectedMergeValues)).IsTrue();
        await Assert.That(raceValues.SequenceEqual(ExpectedRaceValues)).IsTrue();
        await Assert.That(switchValues.SequenceEqual(ExpectedSwitchValues)).IsTrue();
        await Assert.That(withLatestValues.SequenceEqual(ExpectedWithLatestValues)).IsTrue();
        await Assert.That(zipShortValues.SequenceEqual(ExpectedZipShortValues)).IsTrue();
        await Assert.That(forkJoinEmpty.Count).IsEqualTo(0);
        await Assert.That(completed["concat"]).IsEqualTo(1);
        await Assert.That(completed["merge"]).IsEqualTo(1);
        await Assert.That(completed["race"]).IsEqualTo(1);
        await Assert.That(completed["switch"]).IsEqualTo(1);
        await Assert.That(completed["zip"]).IsEqualTo(1);
        await Assert.That(completed["forkJoinEmpty"]).IsEqualTo(1);
    }

    /// <summary>Exercises virtual-time operators and aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeOperatorsCoverDelayTimeoutSampleTimerAndTimestampAliases()
    {
        VirtualClock clock = new();
        List<int> delayStartValues = [];
        List<int> delayedValues = [];
        List<int> timeoutValues = [];
        List<string> timeoutErrors = [];
        List<long> pulseValues = [];
        List<long> timerValues = [];
        List<Moment<int>> timestamps = [];
        Signal<int> manual = new();
        manual.DelayStart(TimeSpan.FromTicks(Five), clock).Subscribe(delayStartValues.Add);
        manual.OnNext(1);
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        await Assert.That(delayStartValues.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        manual.OnNext(Two);
        await Assert.That(delayStartValues.SequenceEqual(ExpectedDelayStartValues)).IsTrue();
        Signal.FromEnumerable([Three, Four]).Shift(TimeSpan.FromTicks(Three), clock).Subscribe(delayedValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        await Assert.That(delayedValues.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        await Assert.That(delayedValues.SequenceEqual(ExpectedDelayedValues)).IsTrue();
        Signal<int> never = new();
        never.Expire(TimeSpan.FromTicks(Four), clock)
            .Subscribe(timeoutValues.Add, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        never.OnNext(BehaviorFortyTwo);
        await Assert.That(timeoutValues.Count).IsEqualTo(0);
        await Assert.That(timeoutErrors.SequenceEqual(ExpectedTimeoutErrors)).IsTrue();
        Signal<int> completed = new();
        completed.Expire(TimeSpan.FromTicks(Ten), clock).Subscribe(timeoutValues.Add);
        completed.OnNext(Seven);
        completed.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Ten));
        await Assert.That(timeoutValues.SequenceEqual(ExpectedRaceValues)).IsTrue();
        var pulse = Signal.Pulse(TimeSpan.FromTicks(Two), clock).Subscribe(pulseValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Six));
        pulse.Dispose();
        await Assert.That(pulseValues.SequenceEqual(ExpectedTimerValues)).IsTrue();
        var timer = Signal.After(TimeSpan.FromTicks(Three), TimeSpan.FromTicks(Two), clock).Subscribe(timerValues.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        clock.AdvanceBy(TimeSpan.FromTicks(Four));
        timer.Dispose();
        await Assert.That(timerValues.SequenceEqual(ExpectedTimerValues)).IsTrue();
        Signal.FromEnumerable([Eight, Nine]).Timestamp(clock).Subscribe(timestamps.Add);
        await Assert.That(timestamps.Select(item => item.Value).SequenceEqual(ExpectedTimestampValues)).IsTrue();
        await Assert.That(timestamps.TrueForAll(item => item.Timestamp == clock.Now)).IsTrue();
    }

    /// <summary>Covers null guards for unary operators.</summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverUnaryOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Map(value => value));
        Assert.Throws<ArgumentNullException>(() => source.Map<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).MapWith(1, (_, value) => value));
        Assert.Throws<ArgumentNullException>(() => source.MapWith<int, int, int>(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Keep(value => true));
        Assert.Throws<ArgumentNullException>(() => source.Keep(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).KeepWith(1, (_, _) => true));
        Assert.Throws<ArgumentNullException>(() => source.KeepWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<string?>)null!).KeepNotNull());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object>)null!).KeepType<string>());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<object>)null!).CastTo<string>());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Tap(value => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TapWith(1, (_, _) => { }));
        Assert.Throws<ArgumentNullException>(() => source.TapWith(1, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Fold(0, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Fold(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Reduce(0, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.Reduce(0, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Take(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Skip(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Distinct());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Unique());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Spark());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<Spark<int>>)null!).Unspark());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Shift(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() =>
            ((IObservable<int>)null!).Expire(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectList());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToSignal());
    }

    /// <summary>Covers null guards for higher-order operators.</summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverHigherOrderOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).Chain());
        Assert.Throws<ArgumentNullException>(() => Signal.Chain<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Chain(source, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Blend<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Blend(source, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Race<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Race(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Pair(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Pair<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Pair<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SyncLatest(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.SyncLatest<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.SyncLatest<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Latch(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Latch<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.Latch<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<IObservable<int>>)null!).SwitchTo());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Reattempt(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Resume(source));
        Assert.Throws<ArgumentNullException>(() => source.Resume(null!));
    }

    /// <summary>Covers null guards for parity operators.</summary>
    /// <param name="source">The non-null source used for null argument checks.</param>
    private static void CoverParityOperatorNullGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Prepend(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Append(1));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).IgnoreValues());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DefaultIfEmpty());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).DistinctBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).UniqueBy(value => value));
        Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TakeWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SkipWhile(value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FlatMap(value => source));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).FlatMapValues<int, int>(value => [value]));
        Assert.Throws<ArgumentNullException>(() => source.FlatMapValues<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Count());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).LongCount());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Any());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).All(value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() =>
            ((IObservable<int>)null!).DelayStart(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Calm(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Probe(TimeSpan.Zero, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Timestamp(Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).TimeInterval(Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ForkJoin(source, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(null!, (left, _) => left));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectArrayAsync());
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).CollectListAsync());
    }

    /// <summary>Covers null guards for factories and observers.</summary>
    /// <param name="objects">The non-null object source used for null argument checks.</param>
    private static void CoverFactoryAndObserverNullGuards(IObservable<object?> objects)
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create((Func<IObserver<int>, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Lazy<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromTask((Task<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsyncEnumerable<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Use<IDisposable, int>(null!, resource => Signal.Emit(1)));
        Assert.Throws<ArgumentNullException>(() =>
            Signal.Use<IDisposable, int>(() => EmptyDisposable.Instance, null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).Subscribe(value => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe<int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe(value => { }, (Action<Exception>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(1).Subscribe(value => { }, ex => { }, null!));
        Assert.Throws<ArgumentNullException>(() => objects.CastTo<string>().Subscribe((IObserver<string>)null!));
    }
}
