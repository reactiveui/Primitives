// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal infrastructure coverage.</summary>
public partial class InternalInfrastructureCoverageTests
{
    /// <summary>Covers parity operator overloads, aliases, and argument guards that are not hit by scenario tests.</summary>
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

        Assert.Equal(ExpectedOneToFour, values);
        Assert.Equal(ExpectedTapSideEffects, sideEffects);
        Assert.Equal(1, completed);
        Assert.Equal(1, ignoreCompleted);
        Assert.Equal(ExpectedSingleNull, defaultValue);
        Assert.Equal(ExpectedOneTwo, takeWhile);
        Assert.Equal(ExpectedThreeFour, skipWhile);
        Assert.Equal(ExpectedDistinctKeys, distinctKeys);
        Assert.Equal(ExpectedIsEmptyValues, isEmptyValues);
        Assert.Equal<int>([One, Two, Three, Four], listValues[0]);
        Assert.Equal<int>([One, Two, Three, Four], arrayValues[0]);
        Assert.Equal<int>([Three, Four, Five], rangeListValues[0]);
        Assert.Equal<int>([Three, Four, Five], rangeArrayValues[0]);
        Assert.Equal(ExpectedSingleNine, forkJoinRange);

        AssertParityOperatorArgumentGuards(source);
    }

    /// <summary>Covers operator Subscribe(null) and current-thread propagation for internal optimized signal classes.</summary>
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

    /// <summary>Covers remaining public alias, immutable-return, and virtual-time edge branches.</summary>
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

        Assert.Equal(ExpectedTwoToFour, startOne);
        Assert.Equal(ExpectedOneToFour, startMany);
        Assert.Equal(ExpectedThreeFour, delayed);
        Assert.Equal(ExpectedDelayErrors, delayErrors);
        Assert.Equal(ExpectedTimeoutErrors, timeoutErrors);

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
        Assert.Equal(ExpectedTrueValues, trueValues);
        Assert.Equal(ExpectedFalseValues, falseValues);
        Assert.Equal(1, rxVoidValues.Count);
        Assert.Equal(Three, inlineCompleted);

        var virtualClock = new MinimalVirtualClock();
        var scheduled = new List<int>();
        Assert.Throws<ArgumentNullException>(() => _ = new MinimalVirtualClock(null!));
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
            return EmptyDisposable.Instance;
        });
        virtualClock.AdvanceTo(Three);
        Assert.Equal(ExpectedSingleSeven, scheduled);
    }

    /// <summary>
    /// Covers observer-based inline operator paths and private observer error cleanup paths left by action-subscribe scenarios.
    /// </summary>
    [Test]
    public void InlineOperatorObserverAndErrorCleanupPathsCoverRemainingBranches()
    {
        var observerValues = new RecordingObserver<int>();
        Signal.Emit(Three).Prepend(Two).Subscribe(observerValues).Dispose();
        Assert.Equal(ExpectedTwoThree, observerValues.Values);
        Assert.Equal(1, observerValues.Completed);

        var enumerableValues = new RecordingObserver<int>();
        Signal.Emit(Three).Prepend((IEnumerable<int>)[One, Two]).Subscribe(enumerableValues).Dispose();
        Assert.Equal(ExpectedOneToThree, enumerableValues.Values);
        Assert.Equal(1, enumerableValues.Completed);

        var prependAppendValues = new RecordingObserver<int>();
        Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(prependAppendValues).Dispose();
        Assert.Equal(ExpectedOneToThree, prependAppendValues.Values);
        Assert.Equal(1, prependAppendValues.Completed);

        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend((IEnumerable<int>)[Two]).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Append(Three).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Append(Two).Subscribe((IObserver<int>)null!));

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Append(Two).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());
        var appendErrorObserver = new ThrowingObserver<int>(throwOnError: true);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("append-error")).Append(Two).Subscribe(appendErrorObserver).Dispose());
        Assert.True(appendErrorObserver.SeenError);

        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).DefaultIfEmpty(Two).Subscribe(new ThrowingObserver<int>(throwOnNext: true)).Dispose());

        var delegateErrors = 0;
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(Two).Prepend(One).Append(Three)
            .Subscribe(_ => throw new InvalidOperationException("delegate-next"), _ => delegateErrors++, () => { }).Dispose());
        Signal.Fail<int>(new InvalidOperationException("delegate-error")).Prepend(One).Append(Three).Subscribe(_ => { }, _ => delegateErrors++, () => { }).Dispose();
        Assert.Equal(1, delegateErrors);
    }

    /// <summary>Covers optimized aggregate helper paths with custom comparers, selector exceptions, and range-backed count aliases.</summary>
    [Test]
    public void AggregateOptimizedSignalsCoverComparerAndExceptionPaths()
    {
        var distinctValues = new RecordingObserver<int>();
        Signal.FromEnumerable([One, Two, Three, Four])
            .DistinctBy(value => value % Two, EqualityComparer<int>.Default)
            .Subscribe(distinctValues)
            .Dispose();
        Assert.Equal(ExpectedOneTwo, distinctValues.Values);

        var rangeDistinctCount = new RecordingObserver<int>();
        var rangeDistinctLongCount = new RecordingObserver<long>();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).Count().Subscribe(rangeDistinctCount).Dispose();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).LongCount().Subscribe(rangeDistinctLongCount).Dispose();
        Assert.Equal(ExpectedSingleTwo, rangeDistinctCount.Values);
        Assert.Equal(ExpectedRangeDistinctLongCount, rangeDistinctLongCount.Values);

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
        Assert.Equal(ExpectedTrueValues, containsRange.Values);
    }

    /// <summary>Covers coordinator paths where later sources complete or error after another source has won or supplied both values.</summary>
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
        Assert.Equal(ExpectedThreeFive, combined.Values);
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
        Assert.Equal(ExpectedSingleTwo, switched.Values);
        Assert.Equal(0, switched.Completed);
        secondInner.OnCompleted();
        switchSubscription.Dispose();
        Assert.Equal(1, switched.Completed);
    }
}
