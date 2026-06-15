// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies signal operator alias, guard, and coordinator contracts.</summary>
public partial class SignalOperatorMixinsTests
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

    /// <summary>The integer constant ninety-nine.</summary>
    private const int NinetyNine = 99;

    /// <summary>The expected side-effect log produced by the tapped source and the faulted tap.</summary>
    private static readonly string[] ExpectedTapSideEffects =
        ["next:1", "next:2", "next:3", "next:4", "completed", "error:do-error"];

    /// <summary>The expected keys retained when distinct-by length is applied.</summary>
    private static readonly string[] ExpectedDistinctKeys = ["aa", "ccc", "dd", "e"];

    /// <summary>The expected emptiness results for the empty and non-empty sources.</summary>
    private static readonly bool[] ExpectedIsEmptyValues = [true, false];

    /// <summary>The expected one-through-four sequence emitted by the four-element source.</summary>
    private static readonly int[] ExpectedOneToFour = [One, Two, Three, Four];

    /// <summary>The expected single null produced by the default-if-empty branch.</summary>
    private static readonly int?[] ExpectedSingleNull = [null];

    /// <summary>The expected one-and-two prefix retained by take-while and distinct branches.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>The expected three-and-four suffix retained by skip-while and delay branches.</summary>
    private static readonly int[] ExpectedThreeFour = [Three, Four];

    /// <summary>The expected single nine produced by the fork-join sum branch.</summary>
    private static readonly int[] ExpectedSingleNine = [Nine];

    /// <summary>The expected two-through-four prefix produced by the single prepend branch.</summary>
    private static readonly int[] ExpectedTwoToFour = [Two, Three, Four];

    /// <summary>The expected message from the single delayed-error branch.</summary>
    private static readonly string[] ExpectedDelayErrors = ["delay-error"];

    /// <summary>The expected error type name from the expire-timeout branch.</summary>
    private static readonly string[] ExpectedTimeoutErrors = [nameof(TimeoutException)];

    /// <summary>The expected single true value emitted by the true signal.</summary>
    private static readonly bool[] ExpectedTrueValues = [true];

    /// <summary>The expected single false value emitted by the false signal.</summary>
    private static readonly bool[] ExpectedFalseValues = [false];

    /// <summary>The expected single seven produced by the scheduled branch.</summary>
    private static readonly int[] ExpectedSingleSeven = [Seven];

    /// <summary>The expected two-and-three sequence produced by the observer prepend branch.</summary>
    private static readonly int[] ExpectedTwoThree = [Two, Three];

    /// <summary>The expected one-through-three sequence produced by the prepend/append branches.</summary>
    private static readonly int[] ExpectedOneToThree = [One, Two, Three];

    /// <summary>The expected three-and-five sequence produced by the combine-latest branch.</summary>
    private static readonly int[] ExpectedThreeFive = [Three, Five];

    /// <summary>The expected single two produced by the switch branch.</summary>
    private static readonly int[] ExpectedSingleTwo = [Two];

    /// <summary>The expected single five produced by the typed catch branch.</summary>
    private static readonly int[] ExpectedSingleFive = [Five];

    /// <summary>The expected message from the keep-predicate fault branch.</summary>
    private static readonly string[] ExpectedKeepErrors = ["keep-predicate"];

    /// <summary>The expected message from the all-predicate fault branch.</summary>
    private static readonly string[] ExpectedAllErrors = ["all-predicate"];

    /// <summary>The expected messages from the recover handler-fault and unmatched branches.</summary>
    private static readonly string[] ExpectedCatchErrors = ["handler-threw", "not-matched"];

    /// <summary>Covers parity operator overloads, aliases, and argument guards that are not hit by scenario tests.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ParityOperatorAliasesAndGuardsCoverRemainingBranches()
    {
        var source = Signal.FromEnumerable([One, Two, Three, Four]);
        List<int> values = [];
        List<string> sideEffects = [];
        var completed = 0;
        List<int?> defaultValue = [];
        var ignoreCompleted = 0;
        List<int> takeWhile = [];
        List<int> skipWhile = [];
        List<string> distinctKeys = [];
        List<bool> isEmptyValues = [];
        source.Tap(
                value => sideEffects.Add("next:" + value),
                error => sideEffects.Add("error:" + error.Message),
                () => sideEffects.Add("completed"))
            .Subscribe(values.Add, ex => throw ex, () => completed++);
        Signal.Fail<int>(new InvalidOperationException("do-error"))
            .Tap(
                value => sideEffects.Add(value.ToString()),
                error => sideEffects.Add("error:" + error.Message),
                () => sideEffects.Add("unused"))
            .Subscribe(_ => { }, _ => { }, () => { });
        Signal.None<int?>().DefaultIfEmpty().Subscribe(defaultValue.Add);
        source.IgnoreValues().Subscribe(_ => values.Add(NinetyNine), ex => throw ex, () => ignoreCompleted++);
        Signal.FromEnumerable([One, Two, Three, Four]).TakeWhile(value => value < Three).Subscribe(takeWhile.Add);
        Signal.FromEnumerable([One, Two, Three, Four]).SkipWhile(value => value < Three).Subscribe(skipWhile.Add);
        Signal.FromEnumerable(["aa", "bb", "ccc", "dd", "e"]).UniqueBy(value => value.Length)
            .Subscribe(distinctKeys.Add);
        Signal.None<int>().IsEmpty().Subscribe(isEmptyValues.Add);
        Signal.Sequence(One, Three).IsEmpty().Subscribe(isEmptyValues.Add);
        List<int> forkJoinRange = [];
        Signal.Sequence(One, Four).ForkJoin(Signal.Sequence(Three, Three), (left, right) => left + right)
            .Subscribe(forkJoinRange.Add);
        await Assert.That(values.SequenceEqual(ExpectedOneToFour)).IsTrue();
        await Assert.That(sideEffects.SequenceEqual(ExpectedTapSideEffects)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(ignoreCompleted).IsEqualTo(1);
        await Assert.That(defaultValue.SequenceEqual(ExpectedSingleNull)).IsTrue();
        await Assert.That(takeWhile.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(skipWhile.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(distinctKeys.SequenceEqual(ExpectedDistinctKeys)).IsTrue();
        await Assert.That(isEmptyValues.SequenceEqual(ExpectedIsEmptyValues)).IsTrue();
        await Assert.That(forkJoinRange.SequenceEqual(ExpectedSingleNine)).IsTrue();
        VerifyCollectOperators(source);
        AssertPrependObserveTapGuards(source);
        AssertAggregateAndTimingGuards(source);
    }

    /// <summary>Covers prepend, observe-on, default-if-empty, and time-shift alias edge branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AliasAndTimeShiftBranchesCoverRemainingEdges()
    {
        var source = Signal.FromEnumerable([Three, Four]);
        List<int> startOne = [];
        List<int> startMany = [];
        List<int> delayed = [];
        List<string> delayErrors = [];
        List<string> timeoutErrors = [];
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        source.Prepend(Two).Subscribe(startOne.Add);
        source.Prepend((IEnumerable<int>)[One, Two]).Subscribe(startMany.Add);
        await Assert.That(source.ObserveOn(Sequencer.Immediate)).IsSameReferenceAs(source);
        var range = Signal.Sequence(One, Three);
        await Assert.That(range.DefaultIfEmpty(NinetyNine)).IsSameReferenceAs(range);
        await Assert.That(source.Shift(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.Expire(TimeSpan.FromTicks(One))).IsNotNull();
        source.DelayStart(TimeSpan.FromTicks(Two), clock).Subscribe(delayed.Add);
        Signal.Fail<int>(new InvalidOperationException("delay-error")).Shift(TimeSpan.FromTicks(Two), clock)
            .Subscribe(_ => { }, ex => delayErrors.Add(ex.Message));
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(Three), clock)
            .Subscribe(_ => { }, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        await Assert.That(startOne.SequenceEqual(ExpectedTwoToFour)).IsTrue();
        await Assert.That(startMany.SequenceEqual(ExpectedOneToFour)).IsTrue();
        await Assert.That(delayed.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(delayErrors.SequenceEqual(ExpectedDelayErrors)).IsTrue();
        await Assert.That(timeoutErrors.SequenceEqual(ExpectedTimeoutErrors)).IsTrue();
    }

    /// <summary>Covers immutable boolean and rx-void return-signal inline subscription branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmutableReturnSignalsCoverInlineBranches()
    {
        List<bool> trueValues = [];
        List<bool> falseValues = [];
        List<RxVoid> voidValues = [];
        var inlineCompleted = 0;
        var trueSignal = Signal.Emit(true);
        var falseSignal = Signal.Emit(false);
        var voidSignal = Signal.EmitRxVoid();
        trueSignal.Subscribe(new RecordingWitness<bool>());
        falseSignal.Subscribe(new RecordingWitness<bool>());
        voidSignal.Subscribe(new RecordingWitness<RxVoid>());
        trueSignal.Subscribe(trueValues.Add, _ => { }, () => inlineCompleted++);
        falseSignal.Subscribe(falseValues.Add, _ => { }, () => inlineCompleted++);
        voidSignal.Subscribe(voidValues.Add, _ => { }, () => inlineCompleted++);
        await Assert.That(((IRequireCurrentThread<bool>)trueSignal).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<bool>)falseSignal).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<RxVoid>)voidSignal).IsRequiredSubscribeOnCurrentThread())
            .IsFalse();
        await Assert.That(trueValues.SequenceEqual(ExpectedTrueValues)).IsTrue();
        await Assert.That(falseValues.SequenceEqual(ExpectedFalseValues)).IsTrue();
        await Assert.That(voidValues.Count).IsEqualTo(1);
        await Assert.That(inlineCompleted).IsEqualTo(Three);
    }

    /// <summary>Covers minimal virtual-clock scheduling guards and dispatch branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MinimalVirtualClockSchedulingCoversGuardsAndDispatch()
    {
        var virtualClock = MinimalVirtualClock.Create();
        List<int> scheduled = [];
        Assert.Throws<ArgumentNullException>(() => _ = MinimalVirtualClock.Create(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceBy(-1));
        virtualClock.AdvanceBy(0);
        Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceTo(-1));
        virtualClock.AdvanceTo(0);
        Assert.Throws<ArgumentNullException>(() =>
            virtualClock.Schedule(One, (Func<ISequencer, int, IDisposable>)null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentNullException>(() => virtualClock.ScheduleRelative(One, 0, null!));
        virtualClock.Schedule(Seven, DateTimeOffset.UnixEpoch.AddTicks(Three), (_, state) =>
        {
            scheduled.Add(state);
            return EmptyDisposable.Instance;
        });
        virtualClock.AdvanceTo(Three);
        await Assert.That(scheduled.SequenceEqual(ExpectedSingleSeven)).IsTrue();
    }

    /// <summary>Covers observer-based inline operator paths and private observer error cleanup paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InlineOperatorObserverAndErrorCleanupPathsCoverRemainingBranches()
    {
        RecordingWitness<int> observerValues = new();
        Signal.Emit(Three).Prepend(Two).Subscribe(observerValues).Dispose();
        await Assert.That(observerValues.Values.SequenceEqual(ExpectedTwoThree)).IsTrue();
        await Assert.That(observerValues.Completed).IsEqualTo(1);
        RecordingWitness<int> enumerableValues = new();
        Signal.Emit(Three).Prepend((IEnumerable<int>)[One, Two]).Subscribe(enumerableValues).Dispose();
        await Assert.That(enumerableValues.Values.SequenceEqual(ExpectedOneToThree)).IsTrue();
        await Assert.That(enumerableValues.Completed).IsEqualTo(1);
        RecordingWitness<int> prependAppendValues = new();
        Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(prependAppendValues).Dispose();
        await Assert.That(prependAppendValues.Values.SequenceEqual(ExpectedOneToThree)).IsTrue();
        await Assert.That(prependAppendValues.Completed).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() =>
            Signal.Emit(One).Prepend((IEnumerable<int>)[Two]).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() =>
            Signal.Emit(One).Prepend(Two).Append(Three).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Append(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Emit(One).Append(Two).Subscribe(new ThrowingWitness<int>(true)).Dispose());
        ThrowingWitness<int> appendErrorObserver = new(throwOnError: true);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("append-error"))
            .Append(Two)
            .Subscribe(appendErrorObserver).Dispose());
        await Assert.That(appendErrorObserver.SeenError).IsTrue();
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).DefaultIfEmpty(Two)
            .Subscribe(new ThrowingWitness<int>(true))
            .Dispose());
        var delegateErrors = 0;
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(Two).Prepend(One).Append(Three)
            .Subscribe(_ => throw new InvalidOperationException("delegate-next"), _ => delegateErrors++, () => { })
            .Dispose());
        Signal.Fail<int>(new InvalidOperationException("delegate-error")).Prepend(One).Append(Three)
            .Subscribe(_ => { }, _ => delegateErrors++, () => { }).Dispose();
        await Assert.That(delegateErrors).IsEqualTo(1);
    }

    /// <summary>Covers coordinator paths where later sources complete or error after another source has won.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HigherOrderCoordinatorRaceCombineSwitchPathsCoverLateBranches()
    {
        RecordingWitness<int> raceErrorWinner = new();
        Signal<int> raceLoser = new();
        Signal<IObservable<int>> raceErrorOuter = new();
        var raceErrorSubscription = raceErrorOuter.Race().Subscribe(raceErrorWinner);
        raceErrorOuter.OnNext(Signal.Fail<int>(new InvalidOperationException("race-error")));
        raceErrorOuter.OnNext(raceLoser);
        raceLoser.OnCompleted();
        raceErrorSubscription.Dispose();
        await Assert.That(raceErrorWinner.Errors[0].Message).IsEqualTo("race-error");
        RecordingWitness<int> raceCompletionWinner = new();
        Signal<IObservable<int>> raceCompletionOuter = new();
        Signal<int> completedWinner = new();
        Signal<int> lateWinner = new();
        var raceCompletionSubscription = raceCompletionOuter.Race().Subscribe(raceCompletionWinner);
        raceCompletionOuter.OnNext(completedWinner);
        completedWinner.OnCompleted();
        raceCompletionOuter.OnNext(lateWinner);
        lateWinner.OnError(new InvalidOperationException("ignored"));
        raceCompletionSubscription.Dispose();
        await Assert.That(raceCompletionWinner.Completed).IsEqualTo(1);
        Signal<int> left = new();
        Signal<int> right = new();
        RecordingWitness<int> combined = new();
        var combineSubscription = left.SyncLatest(right, (l, r) => l + r).Subscribe(combined);
        left.OnNext(One);
        right.OnNext(Two);
        left.OnNext(Three);
        left.OnCompleted();
        await Assert.That(combined.Values.SequenceEqual(ExpectedThreeFive)).IsTrue();
        await Assert.That(combined.Completed).IsEqualTo(0);
        right.OnCompleted();
        combineSubscription.Dispose();
        await Assert.That(combined.Completed).IsEqualTo(1);
        Signal<IObservable<int>> switchOuter = new();
        Signal<int> firstInner = new();
        Signal<int> secondInner = new();
        RecordingWitness<int> switched = new();
        var switchSubscription = switchOuter.SwitchTo().Subscribe(switched);
        switchOuter.OnNext(firstInner);
        switchOuter.OnNext(secondInner);
        firstInner.OnNext(One);
        firstInner.OnCompleted();
        secondInner.OnNext(Two);
        switchOuter.OnCompleted();
        await Assert.That(switched.Values.SequenceEqual(ExpectedSingleTwo)).IsTrue();
        await Assert.That(switched.Completed).IsEqualTo(0);
        secondInner.OnCompleted();
        switchSubscription.Dispose();
        await Assert.That(switched.Completed).IsEqualTo(1);
    }

    /// <summary>Covers observer exception paths and typed catch/finally branches with deterministic synchronous sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObserverExceptionCatchFinallyAndTerminalPredicateBranchesCoverRemainders()
    {
        List<string> keepErrors = [];
        List<string> allErrors = [];
        List<string> distinctErrors = [];
        List<int> catchValues = [];
        List<string> catchErrors = [];
        var finallyCalls = 0;
        Signal.FromEnumerable([One, Two])
            .Keep(value => value == One ? true : throw new InvalidOperationException("keep-predicate"))
            .Subscribe(_ => { }, ex => keepErrors.Add(ex.Message));
        Signal.FromEnumerable([One, Two])
            .All(value => value == One ? true : throw new InvalidOperationException("all-predicate"))
            .Subscribe(_ => { }, ex => allErrors.Add(ex.Message));
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable(["a", "bb"])
            .DistinctBy(value =>
                value.Length == 1 ? value.Length : throw new InvalidOperationException("distinct-key"))
            .Subscribe(_ => { }, ex => distinctErrors.Add(ex.Message)).Dispose());
        Signal.Fail<int>(new InvalidOperationException("typed-catch"))
            .Recover<int, InvalidOperationException>(_ => Signal.Emit(Five))
            .Subscribe(catchValues.Add, ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("handler-fault"))
            .Recover<int, InvalidOperationException>(_ => throw new FormatException("handler-threw"))
            .Subscribe(_ => { }, ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new ArgumentException("not-matched"))
            .Recover<int, InvalidOperationException>(_ => Signal.Emit(Six))
            .Subscribe(_ => { }, ex => catchErrors.Add(ex.Message));
        Signal.Fail<int>(new InvalidOperationException("finally-error")).OnCleanup(() => finallyCalls++)
            .Subscribe(_ => { }, _ => { });
        await Assert.That(keepErrors.SequenceEqual(ExpectedKeepErrors)).IsTrue();
        await Assert.That(allErrors.SequenceEqual(ExpectedAllErrors)).IsTrue();
        await Assert.That(distinctErrors.Count).IsEqualTo(0);
        await Assert.That(catchValues.SequenceEqual(ExpectedSingleFive)).IsTrue();
        await Assert.That(catchErrors.SequenceEqual(ExpectedCatchErrors)).IsTrue();
        await Assert.That(finallyCalls).IsEqualTo(1);
    }

    /// <summary>Covers fused prepend/default-if-empty/append and empty Prepend helpers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PrependAppendDefaultIfEmptyFusionPreservesOrderingAndTerminals()
    {
        List<int> values = [];
        List<int> emptyPrependValues = [];
        var completed = 0;
        Signal.None<int>().DefaultIfEmpty(Two).Prepend(One).Append(Three)
            .Subscribe(values.Add, ex => throw ex, () => completed++);
        Signal.FromEnumerable([One, Two, Three, Four]).Prepend().Append(Four).Subscribe(emptyPrependValues.Add);
        await Assert.That(values.SequenceEqual([One, Two, Three])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(emptyPrependValues.SequenceEqual([One, Two, Three, Four, Four])).IsTrue();
    }

    /// <summary>Covers default-if-empty behavior over hot sources for empty, non-empty, error, and observer-guard branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultIfEmptyCoversHotSourceEmptyNonEmptyErrorAndObserverGuard()
    {
        const string Fallback = "fallback";
        Signal<string?> emptySource = new();
        RecordingWitness<string?> empty = new();
        emptySource.DefaultIfEmpty(Fallback).Subscribe(empty);
        emptySource.OnCompleted();
        Signal<string?> nonEmptySource = new();
        RecordingWitness<string?> nonEmpty = new();
        nonEmptySource.DefaultIfEmpty(Fallback).Subscribe(nonEmpty);
        nonEmptySource.OnNext(null);
        nonEmptySource.OnNext("actual");
        nonEmptySource.OnCompleted();
        Signal<string?> errorSource = new();
        RecordingWitness<string?> errors = new();
        errorSource.DefaultIfEmpty(Fallback).Subscribe(errors);
        errorSource.OnError(new InvalidOperationException("broken"));
        Assert.Throws<ArgumentNullException>(() => emptySource.DefaultIfEmpty("x").Subscribe(null!));
        await Assert.That(empty.Values.SequenceEqual([Fallback])).IsTrue();
        await Assert.That(empty.Completed).IsEqualTo(1);
        string?[] expectedNonEmpty = [null, "actual"];
        await Assert.That(nonEmpty.Values.SequenceEqual(expectedNonEmpty)).IsTrue();
        await Assert.That(nonEmpty.Completed).IsEqualTo(1);
        await Assert.That(errors.Errors.Count).IsEqualTo(1);
        await Assert.That(errors.Errors[0].Message).IsEqualTo("broken");
    }

    /// <summary>Verifies burst telemetry buffering, high-throughput terminal aggregation, and subscriber churn.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task TelemetryBurstBuffersTerminalCountsAndSubscriberChurnAreDeterministic()
    {
        const int BufferSize = 32;
        const int LastBufferIndex = 2;
        const int FirstBurstCount = 64;
        const int TotalCount = 70;
        const double ValueScale = 0.5;
        const int CriticalModulo = 10;
        const double HighValueThreshold = 30;
        const int ExpectedBufferCount = 3;
        const int LastBufferCount = 6;
        const int ExpectedCriticalCount = 7;
        const long ContainsSequence = 20;
        const double ContainsValue = 10;
        Signal<Metric> source = new();
        List<Metric> retained = [];
        List<Metric> churned = [];
        List<IList<Metric>> buffers = [];
        using var retainedSubscription = source.Subscribe(retained.Add);
        var churnedSubscription = source.Subscribe(churned.Add);
        using var bufferedSubscription = source.Buffer(BufferSize).Subscribe(buffers.Add);
        for (var i = 0; i < FirstBurstCount; i++)
        {
            source.OnNext(new(i, i * ValueScale, i % CriticalModulo == 0));
        }

        churnedSubscription.Dispose();
        for (var i = FirstBurstCount; i < TotalCount; i++)
        {
            source.OnNext(new(i, i * ValueScale, i % CriticalModulo == 0));
        }

        source.OnCompleted();
        var terminalSource = Signal.FromEnumerable(retained);
        var count = terminalSource.Count(metric => metric.IsCritical);
        var anyHigh = terminalSource.Any(metric => metric.Value > HighValueThreshold);
        var allNonNegative = terminalSource.All(metric => metric.Sequence >= 0);
        var contains = terminalSource.Contains(new(ContainsSequence, ContainsValue, true));
        await Assert.That(retained.Count).IsEqualTo(TotalCount);
        await Assert.That(churned.Count).IsEqualTo(FirstBurstCount);
        await Assert.That(buffers.Count).IsEqualTo(ExpectedBufferCount);
        await Assert.That(buffers[0].Count).IsEqualTo(BufferSize);
        await Assert.That(buffers[1].Count).IsEqualTo(BufferSize);
        await Assert.That(buffers[LastBufferIndex].Count).IsEqualTo(LastBufferCount);
        int[] expectedCriticalCounts = [ExpectedCriticalCount];
        await Assert.That(Capture(count).SequenceEqual(expectedCriticalCounts)).IsTrue();
        await Assert.That(Capture(anyHigh).SequenceEqual([true])).IsTrue();
        await Assert.That(Capture(allNonNegative).SequenceEqual([true])).IsTrue();
        await Assert.That(Capture(contains).SequenceEqual([true])).IsTrue();
        await Assert.That(retainedSubscription is not null).IsTrue();
    }

    /// <summary>Captures values emitted by a synchronous signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="source">The source to observe.</param>
    /// <returns>The captured values.</returns>
    private static List<T> Capture<T>(IObservable<T> source)
    {
        List<T> values = [];
        source.Subscribe(values.Add);
        return values;
    }

    /// <summary>Covers the synchronous collect-list and collect-array operator branches.</summary>
    /// <param name="source">A four-element integer source.</param>
    private static void VerifyCollectOperators(IObservable<int> source)
    {
        List<IList<int>> listValues = [];
        List<int[]> arrayValues = [];
        List<IList<int>> rangeListValues = [];
        List<int[]> rangeArrayValues = [];
        source.CollectList().Subscribe(listValues.Add);
        source.CollectArray().Subscribe(arrayValues.Add);
        Signal.Sequence(Three, Three).CollectList().Subscribe(rangeListValues.Add);
        Signal.Sequence(Three, Three).CollectArray().Subscribe(rangeArrayValues.Add);
        if (listValues[0].SequenceEqual([One, Two, Three, Four])
            && arrayValues[0].SequenceEqual([One, Two, Three, Four])
            && rangeListValues[0].SequenceEqual([Three, Four, Five])
            && rangeArrayValues[0].SequenceEqual([Three, Four, Five]))
        {
            return;
        }

        throw new InvalidOperationException("Collect operators produced unexpected sequences.");
    }

    /// <summary>Asserts the prepend, observe-on, subscribe-on, and tap argument guards.</summary>
    /// <param name="source">A non-null source used to exercise instance guards.</param>
    private static void AssertPrependObserveTapGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Prepend(null!, One, Two));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((int[])null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Prepend<int>(null!, (IEnumerable<int>)[One]));
        Assert.Throws<ArgumentNullException>(() => source.Prepend((IEnumerable<int>)null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.ObserveOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.ObserveOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.SubscribeOn<int>(null!, Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => source.SubscribeOn(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Tap<int>(null!, _ => { }, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => source.Tap(_ => { }, _ => { }, null!));
    }

    /// <summary>Asserts the aggregate, flat-map, and timing operator argument guards.</summary>
    /// <param name="source">A non-null source used to exercise instance guards.</param>
    private static void AssertAggregateAndTimingGuards(IObservable<int> source)
    {
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.IgnoreValues<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.DistinctBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.UniqueBy<int, int>(null!, value => value));
        Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.TakeWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.SkipWhile<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        Assert.Throws<ArgumentNullException>(() =>
            LinqExtensions.FlatMap<int, int>(null!, value => Signal.Emit(value)));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.FlatMapValues<int, int>(null!, value => [value]));
        Assert.Throws<ArgumentNullException>(() => source.FlatMapValues<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() =>
            source.FlatMap<int, int, int>(null!, (outer, inner) => outer + inner));
        Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int, int>(value => Signal.Emit(value), null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Count<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.Count(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.LongCount<int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.LongCount(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Any<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Any<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.Any(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.All<int>(null!, value => true));
        Assert.Throws<ArgumentNullException>(() => source.All(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Contains(null!, One));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.DelayStart<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Calm<int>(null!, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Probe(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.Timestamp<int>(null!));
        Assert.Throws<ArgumentNullException>(() => LinqExtensions.TimeInterval<int>(null!));
        Assert.Throws<ArgumentNullException>(() =>
            LinqExtensions.ForkJoin<int, int, int>(null!, Signal.Emit(One), (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() =>
            source.ForkJoin<int, int, int>(null!, (left, right) => left + right));
        Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(Signal.Emit(One), null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).AsObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable());
        Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable(CancellationToken.None));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Take(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Skip(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Reattempt(-1));
    }

    /// <summary>Telemetry metric value type used by high-throughput scenarios.</summary>
    /// <param name="Sequence">The sequence number.</param>
    /// <param name="Value">The metric value.</param>
    /// <param name="IsCritical">A value indicating whether the metric is critical.</param>
    private readonly record struct Metric(long Sequence, double Value, bool IsCritical);
}
