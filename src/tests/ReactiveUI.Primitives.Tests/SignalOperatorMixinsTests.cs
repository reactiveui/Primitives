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

    /// <summary>How long a disposed subscription is given to prove it forwards nothing after disposal.</summary>
    private static readonly TimeSpan PostDisposalSettleDelay = TimeSpan.FromMilliseconds(50);

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
        _ = source.Tap(
                value => sideEffects.Add("next:" + value),
                error => sideEffects.Add("error:" + error.Message),
                () => sideEffects.Add("completed"))
            .Subscribe(values.Add, static ex => throw ex, () => completed++);
        _ = Signal.Fail<int>(new InvalidOperationException("do-error"))
            .Tap(
                value => sideEffects.Add(value.ToString()),
                error => sideEffects.Add("error:" + error.Message),
                () => sideEffects.Add("unused"))
            .Subscribe(static _ => { }, static _ => { }, static () => { });
        _ = Signal.None<int?>().DefaultIfEmpty().Subscribe(defaultValue.Add);
        _ = source.IgnoreValues().Subscribe(_ => values.Add(NinetyNine), static ex => throw ex, () => ignoreCompleted++);
        _ = Signal.FromEnumerable([One, Two, Three, Four]).TakeWhile(static value => value < Three).Subscribe(takeWhile.Add);
        _ = Signal.FromEnumerable([One, Two, Three, Four]).SkipWhile(static value => value < Three).Subscribe(skipWhile.Add);
        _ = Signal.FromEnumerable(["aa", "bb", "ccc", "dd", "e"]).UniqueBy(static value => value.Length)
            .Subscribe(distinctKeys.Add);
        _ = Signal.None<int>().IsEmpty().Subscribe(isEmptyValues.Add);
        _ = Signal.Sequence(One, Three).IsEmpty().Subscribe(isEmptyValues.Add);
        List<int> forkJoinRange = [];
        _ = Signal.Sequence(One, Four).ForkJoin(Signal.Sequence(Three, Three), static (left, right) => left + right)
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
        _ = source.Prepend(Two).Subscribe(startOne.Add);
        _ = source.Prepend((IEnumerable<int>)[One, Two]).Subscribe(startMany.Add);
        await Assert.That(source.ObserveOn(Sequencer.Immediate)).IsSameReferenceAs(source);
        var range = Signal.Sequence(One, Three);
        await Assert.That(range.DefaultIfEmpty(NinetyNine)).IsSameReferenceAs(range);
        await Assert.That(source.Shift(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.Expire(TimeSpan.FromTicks(One))).IsNotNull();
        _ = source.DelayStart(TimeSpan.FromTicks(Two), clock).Subscribe(delayed.Add);
        _ = Signal.Fail<int>(new InvalidOperationException("delay-error")).Shift(TimeSpan.FromTicks(Two), clock)
            .Subscribe(static _ => { }, ex => delayErrors.Add(ex.Message));
        _ = Signal.Silent<int>().Expire(TimeSpan.FromTicks(Three), clock)
            .Subscribe(static _ => { }, ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        await Assert.That(startOne.SequenceEqual(ExpectedTwoToFour)).IsTrue();
        await Assert.That(startMany.SequenceEqual(ExpectedOneToFour)).IsTrue();
        await Assert.That(delayed.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(delayErrors.SequenceEqual(ExpectedDelayErrors)).IsTrue();
        await Assert.That(timeoutErrors.SequenceEqual(ExpectedTimeoutErrors)).IsTrue();
    }

    /// <summary>Covers deterministic shortcut branches in primitive-vocabulary operator wrappers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PrimitiveShortcutBranchesCoverCancelableFallbackAndPendingTaskPaths()
    {
        using CancellationTokenSource conversion = new();
        List<int> cancelableToSignal = [];
        _ = new[] { One, Two }.ToSignal(conversion.Token).Subscribe(cancelableToSignal.Add);

        List<int> emptyLoopTake = [];
        var emptyLoopCompleted = 0;
        _ = Signal.Loop(One).Take(0).Subscribe(emptyLoopTake.Add, static ex => throw ex, () => emptyLoopCompleted++);

        List<int> uniqueValues = [];
        _ = Signal.FromEnumerable([One, One, Two]).Unique(null).Subscribe(uniqueValues.Add);

        await Assert.That(cancelableToSignal.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(emptyLoopTake.Count).IsEqualTo(0);
        await Assert.That(emptyLoopCompleted).IsEqualTo(1);
        await Assert.That(uniqueValues.SequenceEqual(ExpectedOneTwo)).IsTrue();

        await Assert.That(Signal.Sequence(One, Two).Shift(TimeSpan.Zero)).IsNotNull();
        await Assert.That(Signal.FromEnumerable([One]).Shift(TimeSpan.Zero, null)).IsNotNull();
        await Assert.That(Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), null)).IsNotNull();
        await Assert.That(Signal.Emit(One).ToSignal()).IsNotNull();

        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit(One).Rescue(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).ToSignal());

        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingWitness<int> pendingSignal = new();
        using var pendingSubscription = pending.Task.ToSignal().Subscribe(pendingSignal);
        pending.SetResult(Three);
        await TestPolling.SpinUntil(() => pendingSignal.Values.Count == 1, TimeSpan.FromSeconds(One));
        await Assert.That(pendingSignal.Values.SequenceEqual([Three])).IsTrue();

        RecordingWitness<int> emptySwitch = new();
        _ = Signal.FromEnumerable<IObservable<int>>([]).SwitchTo().Subscribe(emptySwitch);
        await Assert.That(emptySwitch.Values.Count).IsEqualTo(0);
        await Assert.That(emptySwitch.Completed).IsEqualTo(1);

        List<int> iteratorSwitch = [];
        _ = Signal.FromEnumerable(YieldInners()).SwitchTo().Subscribe(iteratorSwitch.Add);
        await Assert.That(iteratorSwitch.SequenceEqual(ExpectedOneTwo)).IsTrue();

        List<string> stringSwitch = [];
        _ = Signal.FromEnumerable<IObservable<string>>([Signal.Emit("value")]).SwitchTo().Subscribe(stringSwitch.Add);
        await Assert.That(stringSwitch.SequenceEqual(ExpectedSingleValue)).IsTrue();
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
        _ = trueSignal.Subscribe(new RecordingWitness<bool>());
        _ = falseSignal.Subscribe(new RecordingWitness<bool>());
        _ = voidSignal.Subscribe(new RecordingWitness<RxVoid>());
        _ = trueSignal.Subscribe(trueValues.Add, static _ => { }, () => inlineCompleted++);
        _ = falseSignal.Subscribe(falseValues.Add, static _ => { }, () => inlineCompleted++);
        _ = voidSignal.Subscribe(voidValues.Add, static _ => { }, () => inlineCompleted++);
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
        _ = Assert.Throws<ArgumentNullException>(static () => _ = MinimalVirtualClock.Create(null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceBy(-1));
        virtualClock.AdvanceBy(0);
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => virtualClock.AdvanceTo(-1));
        virtualClock.AdvanceTo(0);
        _ = Assert.Throws<ArgumentNullException>(() =>
            virtualClock.Schedule(One, (Func<ISequencer, int, IDisposable>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, TimeSpan.Zero, null!));
        _ = Assert.Throws<ArgumentNullException>(() => virtualClock.Schedule(One, DateTimeOffset.UnixEpoch, null!));
        _ = Assert.Throws<ArgumentNullException>(() => virtualClock.ScheduleRelative(One, 0, null!));
        _ = virtualClock.Schedule(Seven, DateTimeOffset.UnixEpoch.AddTicks(Three), (_, state) =>
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
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit(One).Prepend(Two).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Emit(One).Prepend((IEnumerable<int>)[Two]).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Emit(One).Prepend(Two).Append(Three).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit(One).Append(Two).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<InvalidOperationException>(static () =>
            Signal.Emit(One).Append(Two).Subscribe(new ThrowingWitness<int>(true)).Dispose());
        ThrowingWitness<int> appendErrorObserver = new(throwOnError: true);
        _ = Assert.Throws<InvalidOperationException>(() => Signal
            .Fail<int>(new InvalidOperationException("append-error"))
            .Append(Two)
            .Subscribe(appendErrorObserver).Dispose());
        await Assert.That(appendErrorObserver.SeenError).IsTrue();
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Emit(One).DefaultIfEmpty(Two)
            .Subscribe(new ThrowingWitness<int>(true))
            .Dispose());
        var delegateErrors = 0;
        _ = Assert.Throws<InvalidOperationException>(() => Signal.Emit(Two).Prepend(One).Append(Three)
            .Subscribe(static _ => throw new InvalidOperationException("delegate-next"), _ => delegateErrors++, static () => { })
            .Dispose());
        Signal.Fail<int>(new InvalidOperationException("delegate-error")).Prepend(One).Append(Three)
            .Subscribe(static _ => { }, _ => delegateErrors++, static () => { }).Dispose();
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
        var combineSubscription = left.SyncLatest(right, static (l, r) => l + r).Subscribe(combined);
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
        _ = Signal.FromEnumerable([One, Two])
            .Keep(static value => value == One ? true : throw new InvalidOperationException("keep-predicate"))
            .Subscribe(static _ => { }, ex => keepErrors.Add(ex.Message));
        _ = Signal.FromEnumerable([One, Two])
            .All(static value => value == One ? true : throw new InvalidOperationException("all-predicate"))
            .Subscribe(static _ => { }, ex => allErrors.Add(ex.Message));
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable(["a", "bb"])
            .DistinctBy(static value =>
                value.Length == 1 ? value.Length : throw new InvalidOperationException("distinct-key"))
            .Subscribe(static _ => { }, ex => distinctErrors.Add(ex.Message)).Dispose());
        _ = Signal.Fail<int>(new InvalidOperationException("typed-catch"))
            .Recover<int, InvalidOperationException>(static _ => Signal.Emit(Five))
            .Subscribe(catchValues.Add, ex => catchErrors.Add(ex.Message));
        _ = Signal.Fail<int>(new InvalidOperationException("handler-fault"))
            .Recover<int, InvalidOperationException>(static _ => throw new FormatException("handler-threw"))
            .Subscribe(static _ => { }, ex => catchErrors.Add(ex.Message));
        _ = Signal.Fail<int>(new ArgumentException("not-matched"))
            .Recover<int, InvalidOperationException>(static _ => Signal.Emit(Six))
            .Subscribe(static _ => { }, ex => catchErrors.Add(ex.Message));
        _ = Signal.Fail<int>(new InvalidOperationException("finally-error")).OnCleanup(() => finallyCalls++)
            .Subscribe(static _ => { }, static _ => { });
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
        _ = Signal.None<int>().DefaultIfEmpty(Two).Prepend(One).Append(Three)
            .Subscribe(values.Add, static ex => throw ex, () => completed++);
        _ = Signal.FromEnumerable([One, Two, Three, Four]).Prepend().Append(Four).Subscribe(emptyPrependValues.Add);
        await Assert.That(values.SequenceEqual([One, Two, Three])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(emptyPrependValues.SequenceEqual([One, Two, Three, Four, Four])).IsTrue();
    }

    /// <summary>Verifies indexed mapping, task chaining, and task conversion compatibility aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IndexedMappingTaskChainAndTaskSignalAliasesCoverCompatibilityPaths()
    {
        Signal<int> source = new();
        RecordingWitness<int> mapped = new();
        using var subscription = LinqExtensions.MapIndexed(source, static (value, index) => value + index)
            .Subscribe(mapped);

        source.OnNext(One);
        source.OnNext(Three);
        source.OnCompleted();
        source.OnNext(Five);
        source.OnError(new InvalidOperationException("late"));

        await Assert.That(mapped.Values.SequenceEqual([One, Four])).IsTrue();
        await Assert.That(mapped.Completed).IsEqualTo(1);
        await Assert.That(mapped.Errors.Count).IsEqualTo(0);

        RecordingWitness<int> failed = new();
        Signal<int> failing = new();
        using var failingSubscription = LinqExtensions
            .MapIndexed<int, int>(failing, static (_, _) => throw new InvalidOperationException("indexed"))
            .Subscribe(failed);
        failing.OnNext(One);
        failing.OnNext(Two);

        await Assert.That(failed.Values.Count).IsEqualTo(0);
        await Assert.That(failed.Errors.Count).IsEqualTo(1);
        await Assert.That(failed.Errors[0].Message).IsEqualTo("indexed");

        await AssertIndexedMappingKeepsOnlyTheFirstTerminalNotification();
        await AssertTaskSignalAliasesForwardResultsCancellationAndFaults();

        _ = Assert.Throws<ArgumentNullException>(static () =>
            LinqExtensions.MapIndexed<int, int>(null!, static (value, _) => value));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.MapIndexed<int, int>(source, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<Task<int>>)null!).Chain());
    }

    /// <summary>Verifies direct task-chain sequencing without the map adapter.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskChainDirectSignalKeepsPendingTasksInSourceOrder()
    {
        TaskCompletionSource<int> first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Signal<Task<int>> source = new();
        RecordingWitness<int> chained = new();

        using var subscription = source.Chain().Subscribe(chained);
        source.OnNext(first.Task);
        source.OnNext(second.Task);
        source.OnCompleted();

        second.SetResult(Two);
        await Task.Yield();
        await Assert.That(chained.Values.Count).IsEqualTo(0);

        first.SetResult(One);
        await TestPolling.SpinUntil(
            () => chained.Values.Count == Two && chained.Completed == One,
            TimeSpan.FromSeconds(One));

        await Assert.That(chained.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(chained.Errors.Count).IsEqualTo(0);
        await Assert.That(chained.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies direct task-chain terminal and disposal paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskChainDirectSignalHandlesErrorsAndDisposal()
    {
        InvalidOperationException sourceError = new("task-source");
        RecordingWitness<int> sourceFailure = new();
        _ = new ScriptedObservable<Task<int>>(observer =>
        {
            observer.OnError(sourceError);
            observer.OnNext(Task.FromResult(One));
            observer.OnCompleted();
        }).Chain().Subscribe(sourceFailure);

        await Assert.That(sourceFailure.Errors.Count).IsEqualTo(One);
        await Assert.That(sourceFailure.Errors[0]).IsSameReferenceAs(sourceError);
        await Assert.That(sourceFailure.Values.Count).IsEqualTo(0);
        await Assert.That(sourceFailure.Completed).IsEqualTo(0);

        RecordingWitness<int> nullTaskFailure = new();
        _ = new ScriptedObservable<Task<int>>(static observer =>
        {
            observer.OnNext(null!);
            observer.OnCompleted();
        }).Chain().Subscribe(nullTaskFailure);

        await Assert.That(nullTaskFailure.Errors.Count).IsEqualTo(One);
        await Assert.That(nullTaskFailure.Errors[0]).IsTypeOf<ArgumentNullException>();
        await Assert.That(nullTaskFailure.Completed).IsEqualTo(0);

        InvalidOperationException taskError = new("task");
        RecordingWitness<int> taskFailure = new();
        _ = Signal.FromEnumerable([Task.FromException<int>(taskError), Task.FromResult(One)]).Chain()
            .Subscribe(taskFailure);

        await Assert.That(taskFailure.Errors.Count).IsEqualTo(One);
        await Assert.That(taskFailure.Errors[0]).IsSameReferenceAs(taskError);
        await Assert.That(taskFailure.Values.Count).IsEqualTo(0);
        await Assert.That(taskFailure.Completed).IsEqualTo(0);

        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Signal<Task<int>> disposableSource = new();
        RecordingWitness<int> disposed = new();
        using (var disposable = disposableSource.Chain().Subscribe(disposed))
        {
            disposableSource.OnNext(pending.Task);
            disposable.Dispose();
            pending.SetResult(Five);
        }

        await Task.Delay(PostDisposalSettleDelay);
        await Assert.That(disposed.Values.Count).IsEqualTo(0);
        await Assert.That(disposed.Errors.Count).IsEqualTo(0);
        await Assert.That(disposed.Completed).IsEqualTo(0);
    }

    /// <summary>Covers default-if-empty behavior over hot sources for empty, non-empty, error, and observer-guard branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultIfEmptyCoversHotSourceEmptyNonEmptyErrorAndObserverGuard()
    {
        const string Fallback = "fallback";
        Signal<string?> emptySource = new();
        RecordingWitness<string?> empty = new();
        _ = emptySource.DefaultIfEmpty(Fallback).Subscribe(empty);
        emptySource.OnCompleted();
        Signal<string?> nonEmptySource = new();
        RecordingWitness<string?> nonEmpty = new();
        _ = nonEmptySource.DefaultIfEmpty(Fallback).Subscribe(nonEmpty);
        nonEmptySource.OnNext(null);
        nonEmptySource.OnNext("actual");
        nonEmptySource.OnCompleted();
        Signal<string?> errorSource = new();
        RecordingWitness<string?> errors = new();
        _ = errorSource.DefaultIfEmpty(Fallback).Subscribe(errors);
        errorSource.OnError(new InvalidOperationException("broken"));
        _ = Assert.Throws<ArgumentNullException>(() => emptySource.DefaultIfEmpty("x").Subscribe(null!));
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
        var count = terminalSource.Count(static metric => metric.IsCritical);
        var anyHigh = terminalSource.Any(static metric => metric.Value > HighValueThreshold);
        var allNonNegative = terminalSource.All(static metric => metric.Sequence >= 0);
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
        _ = source.Subscribe(values.Add);
        return values;
    }

    /// <summary>Asserts indexed mapping drops every notification that follows the first terminal one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertIndexedMappingKeepsOnlyTheFirstTerminalNotification()
    {
        RecordingWitness<int> duplicateTerminal = new();
        _ = LinqExtensions.MapIndexed(
            new ScriptedObservable<int>(static observer =>
            {
                observer.OnNext(One);
                observer.OnError(new InvalidOperationException("first"));
                observer.OnError(new InvalidOperationException("late"));
                observer.OnCompleted();
            }),
            static (value, index) => value + index).Subscribe(duplicateTerminal);

        await Assert.That(duplicateTerminal.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(duplicateTerminal.Errors.Count).IsEqualTo(1);
        await Assert.That(duplicateTerminal.Errors[0].Message).IsEqualTo("first");

        RecordingWitness<int> duplicateCompletion = new();
        _ = LinqExtensions.MapIndexed(
            new ScriptedObservable<int>(static observer =>
            {
                observer.OnCompleted();
                observer.OnCompleted();
            }),
            static (value, index) => value + index).Subscribe(duplicateCompletion);

        await Assert.That(duplicateCompletion.Completed).IsEqualTo(1);

        var currentThread = (IRequireCurrentThread<int>)LinqExtensions
            .MapIndexed(new CurrentThreadObservable<int>(), static (value, index) => value + index);
        await Assert.That(currentThread.IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Asserts the task-to-signal aliases forward a result, a cancellation, and a fault.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertTaskSignalAliasesForwardResultsCancellationAndFaults()
    {
        List<int> chained = [];
        _ = Signal.FromEnumerable([Task.FromResult(One), Task.FromResult(Two)]).Chain().Subscribe(chained.Add);
        await Task.Yield();

        await Assert.That(chained.SequenceEqual(ExpectedOneTwo)).IsTrue();

        var taskValue = await Task.FromResult(Three).ToSignal().FirstAsync().ConfigureAwait(false);
        await Assert.That(taskValue).IsEqualTo(Three);

        RecordingWitness<int> canceledTaskSignal = new();
        _ = Task.FromCanceled<int>(new(true)).ToSignal().Subscribe(canceledTaskSignal);
        await Assert.That(canceledTaskSignal.Errors[0]).IsTypeOf<TaskCanceledException>();

        InvalidOperationException taskError = new("task-signal");
        RecordingWitness<int> faultedTaskSignal = new();
        _ = Task.FromException<int>(taskError).ToSignal().Subscribe(faultedTaskSignal);
        await Assert.That(faultedTaskSignal.Errors[0]).IsSameReferenceAs(taskError);
    }

    /// <summary>Covers the synchronous collect-list and collect-array operator branches.</summary>
    /// <param name="source">A four-element integer source.</param>
    private static void VerifyCollectOperators(IObservable<int> source)
    {
        List<IList<int>> listValues = [];
        List<int[]> arrayValues = [];
        List<IList<int>> rangeListValues = [];
        List<int[]> rangeArrayValues = [];
        _ = source.CollectList().Subscribe(listValues.Add);
        _ = source.CollectArray().Subscribe(arrayValues.Add);
        _ = Signal.Sequence(Three, Three).CollectList().Subscribe(rangeListValues.Add);
        _ = Signal.Sequence(Three, Three).CollectArray().Subscribe(rangeArrayValues.Add);
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
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Prepend(null!, One, Two));
        _ = Assert.Throws<ArgumentNullException>(() => source.Prepend((int[])null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Prepend<int>(null!, (IEnumerable<int>)[One]));
        _ = Assert.Throws<ArgumentNullException>(() => source.Prepend((IEnumerable<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.ObserveOn<int>(null!, Sequencer.Immediate));
        _ = Assert.Throws<ArgumentNullException>(() => source.ObserveOn(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.SubscribeOn<int>(null!, Sequencer.Immediate));
        _ = Assert.Throws<ArgumentNullException>(() => source.SubscribeOn(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Tap<int>(null!, static _ => { }, static _ => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Tap(null!, static _ => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Tap(static _ => { }, null!, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => source.Tap(static _ => { }, static _ => { }, null!));
    }

    /// <summary>Asserts the aggregate, flat-map, and timing operator argument guards.</summary>
    /// <param name="source">A non-null source used to exercise instance guards.</param>
    private static void AssertAggregateAndTimingGuards(IObservable<int> source)
    {
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.IgnoreValues<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.DistinctBy<int, int>(null!, static value => value));
        _ = Assert.Throws<ArgumentNullException>(() => source.DistinctBy<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.UniqueBy<int, int>(null!, static value => value));
        _ = Assert.Throws<ArgumentNullException>(() => source.UniqueBy<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.TakeWhile<int>(null!, static value => true));
        _ = Assert.Throws<ArgumentNullException>(() => source.TakeWhile(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.SkipWhile<int>(null!, static value => true));
        _ = Assert.Throws<ArgumentNullException>(() => source.SkipWhile(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            LinqExtensions.FlatMap<int, int>(null!, Signal.Emit));
        _ = Assert.Throws<ArgumentNullException>(() => source.FlatMap<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.FlatMapValues<int, int>(null!, static value => [value]));
        _ = Assert.Throws<ArgumentNullException>(() => source.FlatMapValues<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.FlatMap<int, int, int>(null!, static (outer, inner) => outer + inner));
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.FlatMap<int, int, int>(Signal.Emit, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Count<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Count(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.LongCount<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.LongCount(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Any<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Any<int>(null!, static value => true));
        _ = Assert.Throws<ArgumentNullException>(() => source.Any(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.All<int>(null!, static value => true));
        _ = Assert.Throws<ArgumentNullException>(() => source.All(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Contains(null!, One));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.DelayStart<int>(null!, TimeSpan.Zero));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Calm<int>(null!, TimeSpan.Zero));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Probe(TimeSpan.FromTicks(-1)));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.Timestamp<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.TimeInterval<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            LinqExtensions.ForkJoin<int, int, int>(null!, Signal.Emit(One), static (left, right) => left + right));
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.ForkJoin<int, int, int>(null!, static (left, right) => left + right));
        _ = Assert.Throws<ArgumentNullException>(() => source.ForkJoin<int, int, int>(Signal.Emit(One), null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).AsObservable());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IEnumerable<int>)null!).ToObservable());
        _ = Assert.Throws<ArgumentNullException>(static () => ((IEnumerable<int>)null!).ToObservable(CancellationToken.None));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Take(-1));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Skip(-1));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Reattempt(-1));
    }

    /// <summary>Yields inner signals without exposing an indexable backing collection.</summary>
    /// <returns>The yielded inner signals.</returns>
    private static IEnumerable<IObservable<int>> YieldInners()
    {
        yield return Signal.Emit(One);
        yield return Signal.Emit(Two);
    }

    /// <summary>Telemetry metric value type used by high-throughput scenarios.</summary>
    /// <param name="Sequence">The sequence number.</param>
    /// <param name="Value">The metric value.</param>
    /// <param name="IsCritical">A value indicating whether the metric is critical.</param>
    private readonly record struct Metric(long Sequence, double Value, bool IsCritical);

    /// <summary>A source that reports current-thread subscription requirements.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CurrentThreadObservable<T> : IRequireCurrentThread<T>
    {
        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => true;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }
}
