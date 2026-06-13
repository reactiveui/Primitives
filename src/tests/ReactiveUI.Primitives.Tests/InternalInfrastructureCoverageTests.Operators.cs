// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103, S138, S6966 // Coverage tests intentionally group branch-heavy scenarios.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal infrastructure coverage.</summary>
public partial class InternalInfrastructureCoverageTests
{
    /// <summary>Covers parity operator overloads, aliases, and argument guards that are not hit by scenario tests.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ParityOperatorAliasesAndGuardsCoverRemainingBranches()
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
        source.Tap(value => sideEffects.Add("next:" + value), error => sideEffects.Add("error:" + error.Message), () => sideEffects.Add("completed")).Subscribe(values.Add, ex => throw ex, () => completed++);
        Signal.Fail<int>(new InvalidOperationException("do-error")).Tap(value => sideEffects.Add(value.ToString()), error => sideEffects.Add("error:" + error.Message), () => sideEffects.Add("unused")).Subscribe(
            _ =>
        {
        },
            _ =>
        {
        },
            () =>
        {
        });
        Signal.None<int?>().DefaultIfEmpty().Subscribe(defaultValue.Add);
        source.IgnoreValues().Subscribe(_ => values.Add(NinetyNine), ex => throw ex, () => ignoreCompleted++);
        Signal.FromEnumerable([One, Two, Three, Four]).TakeWhile(value => value < Three).Subscribe(takeWhile.Add);
        Signal.FromEnumerable([One, Two, Three, Four]).SkipWhile(value => value < Three).Subscribe(skipWhile.Add);
        Signal.FromEnumerable(["aa", "bb", "ccc", "dd", "e"]).UniqueBy(value => value.Length).Subscribe(distinctKeys.Add);
        Signal.None<int>().IsEmpty().Subscribe(isEmptyValues.Add);
        Signal.Sequence(One, Three).IsEmpty().Subscribe(isEmptyValues.Add);
        source.CollectList().Subscribe(listValues.Add);
        source.CollectArray().Subscribe(arrayValues.Add);
        Signal.Sequence(Three, Three).CollectList().Subscribe(rangeListValues.Add);
        Signal.Sequence(Three, Three).CollectArray().Subscribe(rangeArrayValues.Add);
        Signal.Sequence(One, Four).ForkJoin(Signal.Sequence(Three, Three), (left, right) => left + right).Subscribe(forkJoinRange.Add);
        await Assert.That(values.SequenceEqual(ExpectedOneToFour)).IsTrue();
        await Assert.That(sideEffects.SequenceEqual(ExpectedTapSideEffects)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(ignoreCompleted).IsEqualTo(1);
        await Assert.That(defaultValue.SequenceEqual(ExpectedSingleNull)).IsTrue();
        await Assert.That(takeWhile.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(skipWhile.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(distinctKeys.SequenceEqual(ExpectedDistinctKeys)).IsTrue();
        await Assert.That(isEmptyValues.SequenceEqual(ExpectedIsEmptyValues)).IsTrue();
        await Assert.That(listValues[0].SequenceEqual([One, Two, Three, Four])).IsTrue();
        await Assert.That(arrayValues[0].SequenceEqual([One, Two, Three, Four])).IsTrue();
        await Assert.That(rangeListValues[0].SequenceEqual([Three, Four, Five])).IsTrue();
        await Assert.That(rangeArrayValues[0].SequenceEqual([Three, Four, Five])).IsTrue();
        await Assert.That(forkJoinRange.SequenceEqual(ExpectedSingleNine)).IsTrue();
        AssertParityOperatorArgumentGuards(source);
    }

    /// <summary>Covers operator Subscribe(null) and current-thread propagation for internal optimized signal classes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InternalOptimizedOperatorSignalsValidateObserversAndThreadRequirements()
    {
        IObservable<int>[] intSignals = [Signal.FromEnumerable([One, Two, Three]).DistinctBy(value => value), Signal.FromEnumerable([One, Two, Three]).Count(), Signal.FromEnumerable([One, Two, Three]).Count(value => value > One),];
        IObservable<long>[] longSignals = [Signal.FromEnumerable([One, Two, Three]).LongCount(), Signal.FromEnumerable([One, Two, Three]).LongCount(value => value > One),];
        IObservable<bool>[] boolSignals = [Signal.FromEnumerable([One, Two, Three]).All(value => value > 0), Signal.FromEnumerable([One, Two, Three]).Contains(Two), Signal.FromEnumerable([One, Two, Three]).Any(), Signal.FromEnumerable([One, Two, Three]).Any(value => value > Two),];
        for (var i = 0; i < intSignals.Length; i++)
        {
            var signal = intSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<int>)null!));
            if (signal is IRequireCurrentThread<int> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }

        for (var i = 0; i < longSignals.Length; i++)
        {
            var signal = longSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<long>)null!));
            if (signal is IRequireCurrentThread<long> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }

        for (var i = 0; i < boolSignals.Length; i++)
        {
            var signal = boolSignals[i];
            Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<bool>)null!));
            if (signal is IRequireCurrentThread<bool> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }
    }

    /// <summary>Covers remaining public alias, immutable-return, and virtual-time edge branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AliasRangeImmutableAndVirtualTimeBranchesCoverRemainingEdges()
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
        await Assert.That(source.ObserveOn(Sequencer.Immediate)).IsSameReferenceAs(source);
        var range = Signal.Sequence(One, Three);
        await Assert.That(range.DefaultIfEmpty(NinetyNine)).IsSameReferenceAs(range);
        await Assert.That(source.Shift(TimeSpan.Zero)).IsNotNull();
        await Assert.That(source.Expire(TimeSpan.FromTicks(One))).IsNotNull();
        source.DelayStart(TimeSpan.FromTicks(Two), clock).Subscribe(delayed.Add);
        Signal.Fail<int>(new InvalidOperationException("delay-error")).Shift(TimeSpan.FromTicks(Two), clock).Subscribe(
            _ =>
        {
        },
            ex => delayErrors.Add(ex.Message));
        Signal.Silent<int>().Expire(TimeSpan.FromTicks(Three), clock).Subscribe(
            _ =>
        {
        },
            ex => timeoutErrors.Add(ex.GetType().Name));
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        await Assert.That(startOne.SequenceEqual(ExpectedTwoToFour)).IsTrue();
        await Assert.That(startMany.SequenceEqual(ExpectedOneToFour)).IsTrue();
        await Assert.That(delayed.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(delayErrors.SequenceEqual(ExpectedDelayErrors)).IsTrue();
        await Assert.That(timeoutErrors.SequenceEqual(ExpectedTimeoutErrors)).IsTrue();
        var trueValues = new List<bool>();
        var falseValues = new List<bool>();
        var rxVoidValues = new List<RxVoid>();
        var inlineCompleted = 0;
        var trueSignal = Signal.Emit(true);
        var falseSignal = Signal.Emit(false);
        var rxVoidSignal = Signal.EmitRxVoid();
        trueSignal.Subscribe(new RecordingWitness<bool>());
        falseSignal.Subscribe(new RecordingWitness<bool>());
        rxVoidSignal.Subscribe(new RecordingWitness<RxVoid>());
        trueSignal.Subscribe(
            trueValues.Add,
            _ =>
        {
        },
            () => inlineCompleted++);
        falseSignal.Subscribe(
            falseValues.Add,
            _ =>
        {
        },
            () => inlineCompleted++);
        rxVoidSignal.Subscribe(
            rxVoidValues.Add,
            _ =>
        {
        },
            () => inlineCompleted++);
        await Assert.That(((IRequireCurrentThread<bool>)trueSignal).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<bool>)falseSignal).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<RxVoid>)rxVoidSignal).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(trueValues.SequenceEqual(ExpectedTrueValues)).IsTrue();
        await Assert.That(falseValues.SequenceEqual(ExpectedFalseValues)).IsTrue();
        await Assert.That(rxVoidValues.Count).IsEqualTo(1);
        await Assert.That(inlineCompleted).IsEqualTo(Three);
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
        await Assert.That(scheduled.SequenceEqual(ExpectedSingleSeven)).IsTrue();
    }

    /// <summary>
    /// Covers observer-based inline operator paths and private observer error cleanup paths left by action-subscribe scenarios.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InlineOperatorObserverAndErrorCleanupPathsCoverRemainingBranches()
    {
        var observerValues = new RecordingWitness<int>();
        Signal.Emit(Three).Prepend(Two).Subscribe(observerValues).Dispose();
        await Assert.That(observerValues.Values.SequenceEqual(ExpectedTwoThree)).IsTrue();
        await Assert.That(observerValues.Completed).IsEqualTo(1);
        var enumerableValues = new RecordingWitness<int>();
        Signal.Emit(Three).Prepend((IEnumerable<int>)[One, Two]).Subscribe(enumerableValues).Dispose();
        await Assert.That(enumerableValues.Values.SequenceEqual(ExpectedOneToThree)).IsTrue();
        await Assert.That(enumerableValues.Completed).IsEqualTo(1);
        var prependAppendValues = new RecordingWitness<int>();
        Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(prependAppendValues).Dispose();
        await Assert.That(prependAppendValues.Values.SequenceEqual(ExpectedOneToThree)).IsTrue();
        await Assert.That(prependAppendValues.Completed).IsEqualTo(1);
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend((IEnumerable<int>)[Two]).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Prepend(Two).Append(Three).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Emit(One).Append(Two).Subscribe((IObserver<int>)null!));
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).Append(Two).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        var appendErrorObserver = new ThrowingWitness<int>(throwOnError: true);
        Assert.Throws<InvalidOperationException>(() => Signal.Fail<int>(new InvalidOperationException("append-error")).Append(Two).Subscribe(appendErrorObserver).Dispose());
        await Assert.That(appendErrorObserver.SeenError).IsTrue();
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(One).DefaultIfEmpty(Two).Subscribe(new ThrowingWitness<int>(throwOnNext: true)).Dispose());
        var delegateErrors = 0;
        Assert.Throws<InvalidOperationException>(() => Signal.Emit(Two).Prepend(One).Append(Three).Subscribe(_ => throw new InvalidOperationException("delegate-next"), _ => delegateErrors++, () =>
{
}).Dispose());
        Signal.Fail<int>(new InvalidOperationException("delegate-error")).Prepend(One).Append(Three).Subscribe(
            _ =>
        {
        },
            _ => delegateErrors++,
            () =>
        {
        }).Dispose();
        await Assert.That(delegateErrors).IsEqualTo(1);
    }

    /// <summary>Covers optimized aggregate helper paths with custom comparers, selector exceptions, and range-backed count aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AggregateOptimizedSignalsCoverComparerAndExceptionPaths()
    {
        var distinctValues = new RecordingWitness<int>();
        Signal.FromEnumerable([One, Two, Three, Four]).DistinctBy(value => value % Two, EqualityComparer<int>.Default).Subscribe(distinctValues).Dispose();
        await Assert.That(distinctValues.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        var rangeDistinctCount = new RecordingWitness<int>();
        var rangeDistinctLongCount = new RecordingWitness<long>();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).Count().Subscribe(rangeDistinctCount).Dispose();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).LongCount().Subscribe(rangeDistinctLongCount).Dispose();
        await Assert.That(rangeDistinctCount.Values.SequenceEqual(ExpectedSingleTwo)).IsTrue();
        await Assert.That(rangeDistinctLongCount.Values.SequenceEqual(ExpectedRangeDistinctLongCount)).IsTrue();
        var distinctErrors = new RecordingWitness<int>();
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).DistinctBy<int, int>(_ => throw new InvalidOperationException("distinct-key")).Subscribe(distinctErrors).Dispose());
        await Assert.That(distinctErrors.Values.Count).IsEqualTo(0);
        var countErrors = new RecordingWitness<int>();
        var longCountErrors = new RecordingWitness<long>();
        var anyErrors = new RecordingWitness<bool>();
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).Count(_ => throw new InvalidOperationException("count-predicate")).Subscribe(countErrors).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).LongCount(_ => throw new InvalidOperationException("long-count-predicate")).Subscribe(longCountErrors).Dispose());
        Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One]).Any(_ => throw new InvalidOperationException("any-predicate")).Subscribe(anyErrors).Dispose());
        await Assert.That(countErrors.Values.Count).IsEqualTo(0);
        await Assert.That(longCountErrors.Values.Count).IsEqualTo(0);
        await Assert.That(anyErrors.Values.Count).IsEqualTo(0);
        var containsRange = new RecordingWitness<bool>();
        Signal.Sequence(One, Four).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsRange).Dispose();
        await Assert.That(containsRange.Values.SequenceEqual(ExpectedTrueValues)).IsTrue();
    }

    /// <summary>Covers coordinator paths where later sources complete or error after another source has won or supplied both values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HigherOrderCoordinatorRaceCombineSwitchPathsCoverLateBranches()
    {
        var raceErrorWinner = new RecordingWitness<int>();
        var raceLoser = new Signal<int>();
        var raceErrorOuter = new Signal<IObservable<int>>();
        var raceErrorSubscription = raceErrorOuter.Race().Subscribe(raceErrorWinner);
        raceErrorOuter.OnNext(Signal.Fail<int>(new InvalidOperationException("race-error")));
        raceErrorOuter.OnNext(raceLoser);
        raceLoser.OnCompleted();
        raceErrorSubscription.Dispose();
        await Assert.That(raceErrorWinner.Errors[0].Message).IsEqualTo("race-error");
        var raceCompletionWinner = new RecordingWitness<int>();
        var raceCompletionOuter = new Signal<IObservable<int>>();
        var completedWinner = new Signal<int>();
        var lateWinner = new Signal<int>();
        var raceCompletionSubscription = raceCompletionOuter.Race().Subscribe(raceCompletionWinner);
        raceCompletionOuter.OnNext(completedWinner);
        completedWinner.OnCompleted();
        raceCompletionOuter.OnNext(lateWinner);
        lateWinner.OnError(new InvalidOperationException("ignored"));
        raceCompletionSubscription.Dispose();
        await Assert.That(raceCompletionWinner.Completed).IsEqualTo(1);
        var left = new Signal<int>();
        var right = new Signal<int>();
        var combined = new RecordingWitness<int>();
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
        var switchOuter = new Signal<IObservable<int>>();
        var firstInner = new Signal<int>();
        var secondInner = new Signal<int>();
        var switched = new RecordingWitness<int>();
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
}
