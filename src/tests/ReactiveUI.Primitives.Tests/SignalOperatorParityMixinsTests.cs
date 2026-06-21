// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies aggregate, distinct, and terminal operator contracts.</summary>
public partial class SignalOperatorParityMixinsTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The long constant two.</summary>
    private const long TwoLong = 2L;

    /// <summary>The expected one-and-two prefix retained by distinct branches.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>The expected single two produced by the distinct count alias.</summary>
    private static readonly int[] ExpectedSingleTwo = [Two];

    /// <summary>The expected long count produced by the range-backed distinct long-count alias.</summary>
    private static readonly long[] ExpectedRangeDistinctLongCount = [TwoLong];

    /// <summary>The expected single true value emitted by a true signal.</summary>
    private static readonly bool[] ExpectedTrueValues = [true];

    /// <summary>Covers operator Subscribe(null) and current-thread propagation for internal optimized signal classes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task InternalOptimizedOperatorSignalsValidateObserversAndThreadRequirements()
    {
        IObservable<int>[] intSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).DistinctBy(value => value),
            Signal.FromEnumerable([One, Two, Three]).Count(),
            Signal.FromEnumerable([One, Two, Three]).Count(value => value > One)
        ];
        IObservable<long>[] longSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).LongCount(),
            Signal.FromEnumerable([One, Two, Three]).LongCount(value => value > One)
        ];
        IObservable<bool>[] boolSignals =
        [
            Signal.FromEnumerable([One, Two, Three]).All(value => value > 0),
            Signal.FromEnumerable([One, Two, Three]).Contains(Two),
            Signal.FromEnumerable([One, Two, Three]).Any(),
            Signal.FromEnumerable([One, Two, Three]).Any(value => value > Two)
        ];
        for (var i = 0; i < intSignals.Length; i++)
        {
            var signal = intSignals[i];
            _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<int>)null!));
            if (signal is IRequireCurrentThread<int> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }

        for (var i = 0; i < longSignals.Length; i++)
        {
            var signal = longSignals[i];
            _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<long>)null!));
            if (signal is IRequireCurrentThread<long> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }

        for (var i = 0; i < boolSignals.Length; i++)
        {
            var signal = boolSignals[i];
            _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe((IObserver<bool>)null!));
            if (signal is IRequireCurrentThread<bool> required)
            {
                await Assert.That(required.IsRequiredSubscribeOnCurrentThread()).IsFalse();
            }
        }
    }

    /// <summary>Covers optimized aggregate helper paths with custom comparers, selector exceptions, and range-backed count aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task AggregateOptimizedSignalsCoverComparerAndExceptionPaths()
    {
        RecordingWitness<int> distinctValues = new();
        Signal.FromEnumerable([One, Two, Three, Four])
            .DistinctBy(value => value % Two, EqualityComparer<int>.Default).Subscribe(distinctValues).Dispose();
        await Assert.That(distinctValues.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        RecordingWitness<int> rangeDistinctCount = new();
        RecordingWitness<long> rangeDistinctLongCount = new();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).Count()
            .Subscribe(rangeDistinctCount).Dispose();
        Signal.Sequence(One, Four).DistinctBy(value => value % Two, EqualityComparer<int>.Default).LongCount()
            .Subscribe(rangeDistinctLongCount).Dispose();
        await Assert.That(rangeDistinctCount.Values.SequenceEqual(ExpectedSingleTwo)).IsTrue();
        await Assert.That(rangeDistinctLongCount.Values.SequenceEqual(ExpectedRangeDistinctLongCount)).IsTrue();
        RecordingWitness<int> distinctErrors = new();
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One])
            .DistinctBy<int, int>(_ => throw new InvalidOperationException("distinct-key"))
            .Subscribe(distinctErrors).Dispose());
        await Assert.That(distinctErrors.Values.Count).IsEqualTo(0);
        RecordingWitness<int> countErrors = new();
        RecordingWitness<long> longCountErrors = new();
        RecordingWitness<bool> anyErrors = new();
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One])
            .Count(_ => throw new InvalidOperationException("count-predicate"))
            .Subscribe(countErrors).Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One])
            .LongCount(_ => throw new InvalidOperationException("long-count-predicate"))
            .Subscribe(longCountErrors).Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One])
            .Any(_ => throw new InvalidOperationException("any-predicate"))
            .Subscribe(anyErrors).Dispose());
        await Assert.That(countErrors.Values.Count).IsEqualTo(0);
        await Assert.That(longCountErrors.Values.Count).IsEqualTo(0);
        await Assert.That(anyErrors.Values.Count).IsEqualTo(0);
        RecordingWitness<bool> containsRange = new();
        Signal.Sequence(One, Four).Contains(Three, EqualityComparer<int>.Default).Subscribe(containsRange).Dispose();
        await Assert.That(containsRange.Values.SequenceEqual(ExpectedTrueValues)).IsTrue();
    }

    /// <summary>Covers terminal observers that must ignore protocol violations after their first terminal signal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task TerminalObserversIgnoreLateSignalsAndForwardPredicateFailures()
    {
        List<string> errors = [];
        List<object> values = [];
        ScriptedObservable<int> badIntegers = new(observer =>
        {
            observer.OnNext(One);
            observer.OnError(new InvalidOperationException("first-terminal"));
            observer.OnNext(Two);
            observer.OnCompleted();
        });
        ScriptedObservable<int> lateCompletionIntegers = new(observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
            observer.OnError(new InvalidOperationException("late-error"));
            observer.OnNext(Two);
        });
        _ = badIntegers.Count().Subscribe(value => values.Add(value), ex => errors.Add("count:" + ex.Message));
        _ = badIntegers.LongCount().Subscribe(value => values.Add(value), ex => errors.Add("long-count:" + ex.Message));
        _ = badIntegers.Count(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("count-predicate:" + ex.Message));
        _ = badIntegers.LongCount(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("long-count-predicate:" + ex.Message));
        _ = badIntegers.Any().Subscribe(value => values.Add(value), ex => errors.Add("any:" + ex.Message));
        _ = badIntegers.Any(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("any-predicate:" + ex.Message));
        _ = badIntegers.All(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("all:" + ex.Message));
        _ = badIntegers.Contains(Two).Subscribe(value => values.Add(value), ex => errors.Add("contains:" + ex.Message));
        _ = lateCompletionIntegers.Count()
            .Subscribe(value => values.Add(value), ex => errors.Add("late-count:" + ex.Message));
        _ = lateCompletionIntegers.LongCount()
            .Subscribe(value => values.Add(value), ex => errors.Add("late-long-count:" + ex.Message));
        _ = lateCompletionIntegers.Any(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("late-any:" + ex.Message));
        _ = lateCompletionIntegers.All(value => value > 0)
            .Subscribe(value => values.Add(value), ex => errors.Add("late-all:" + ex.Message));
        _ = lateCompletionIntegers.Contains(One)
            .Subscribe(value => values.Add(value), ex => errors.Add("late-contains:" + ex.Message));
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .Count(value => value == One ? true : throw new InvalidOperationException("count-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .LongCount(value =>
                value == One ? true : throw new InvalidOperationException("long-count-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        _ = Assert.Throws<InvalidOperationException>(() => Signal.FromEnumerable([One, Two])
            .Any(value => value == One ? false : throw new InvalidOperationException("any-predicate-fault"))
            .Subscribe(_ => { }, ex => errors.Add(ex.Message)).Dispose());
        _ = Signal.FromEnumerable([One, Two]).Contains(Two, new ThrowingComparer())
            .Subscribe(_ => { }, ex => errors.Add(ex.Message));
        await Assert.That(errors).Contains("count:first-terminal");
        await Assert.That(errors).Contains("long-count:first-terminal");
        await Assert.That(errors).Contains("count-predicate:first-terminal");
        await Assert.That(errors).Contains("long-count-predicate:first-terminal");
        await Assert.That(errors).Contains("all:first-terminal");
        await Assert.That(errors).Contains("contains:first-terminal");
        await Assert.That(errors).Contains("comparer-fault");
        await Assert.That(values).Contains(1);
        await Assert.That(values).Contains(1L);
        await Assert.That(values).Contains(true);
    }

    /// <summary>Verifies scheduler-based ToObservable conversion and task conversion aliases.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ToObservableSchedulerAndTaskAliasesEmitAndHonorCancellation()
    {
        List<int> immediate = [];
        _ = new[] { One, Two }.ToObservable(Sequencer.Immediate).Subscribe(immediate.Add);

        await Assert.That(immediate.SequenceEqual(ExpectedOneTwo)).IsTrue();

        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        List<int> scheduled = [];
        var completed = 0;
        _ = new[] { One, Two }.ToObservable(clock).Subscribe(scheduled.Add, ex => throw ex, () => completed++);
        clock.Start();

        await Assert.That(scheduled.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);

        VirtualClock cancelDuringLoopClock = new(DateTimeOffset.UnixEpoch);
        List<int> cancelledDuringLoop = [];
        IDisposable? cancelDuringLoopSubscription = null;
        cancelDuringLoopSubscription = new[] { One, Two }.ToObservable(cancelDuringLoopClock)
            .Subscribe(value =>
            {
                cancelledDuringLoop.Add(value);
                cancelDuringLoopSubscription?.Dispose();
            });
        cancelDuringLoopClock.Start();

        await Assert.That(cancelledDuringLoop.SequenceEqual([One])).IsTrue();

        VirtualClock cancelBeforeCompletionClock = new(DateTimeOffset.UnixEpoch);
        List<int> cancelledBeforeCompletion = [];
        var cancelBeforeCompletionCompleted = 0;
        IDisposable? cancelBeforeCompletionSubscription = null;
        cancelBeforeCompletionSubscription = new[] { One }.ToObservable(cancelBeforeCompletionClock)
            .Subscribe(
                value =>
                {
                    cancelledBeforeCompletion.Add(value);
                    cancelBeforeCompletionSubscription?.Dispose();
                },
                ex => throw ex,
                () => cancelBeforeCompletionCompleted++);
        cancelBeforeCompletionClock.Start();

        await Assert.That(cancelledBeforeCompletion.SequenceEqual([One])).IsTrue();
        await Assert.That(cancelBeforeCompletionCompleted).IsEqualTo(0);

        var taskValue = await Task.FromResult(Three).ToObservable().FirstAsync().ConfigureAwait(false);
        await Assert.That(taskValue).IsEqualTo(Three);

        _ = Assert.Throws<ArgumentNullException>(() => ((IEnumerable<int>)null!).ToObservable(Sequencer.Immediate));
        _ = Assert.Throws<ArgumentNullException>(() => new[] { One }.ToObservable(null!));
        _ = Assert.Throws<ArgumentNullException>(() => ((Task<int>)null!).ToObservable());
    }
}
