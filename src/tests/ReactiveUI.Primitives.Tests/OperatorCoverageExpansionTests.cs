// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S6966 // Coverage tests intentionally group branch-heavy scenarios.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Covers optimized operator helper implementations that are not exercised by broad contract tests.</summary>
public class OperatorCoverageExpansionTests
{
    /// <summary>The first value.</summary>
    private const int First = 1;

    /// <summary>The second value.</summary>
    private const int Second = 2;

    /// <summary>The third value.</summary>
    private const int Third = 3;

    /// <summary>The fourth value.</summary>
    private const int Fourth = 4;

    /// <summary>A value outside the tested ranges.</summary>
    private const int MissingRangeValue = 99;

    /// <summary>Observed error index for the any error.</summary>
    private const int AnyErrorIndex = 2;

    /// <summary>Observed error index for the distinct error.</summary>
    private const int DistinctErrorIndex = 3;

    /// <summary>Observed error index for the outer FlatMap error.</summary>
    private const int OuterErrorIndex = 2;

    /// <summary>The first inner value.</summary>
    private const int FirstInner = 10;

    /// <summary>The second inner value.</summary>
    private const int SecondInner = 20;

    /// <summary>Selector failure message.</summary>
    private const string SelectorMessage = "selector";

    /// <summary>Inner failure message.</summary>
    private const string InnerMessage = "inner";

    /// <summary>Outer failure message.</summary>
    private const string OuterMessage = "outer";

    /// <summary>All predicate failure message.</summary>
    private const string AllMessage = "all";

    /// <summary>Source values for aggregate operator tests.</summary>
    private static readonly int[] AggregateSource = [First, Second, Third, Fourth];

    /// <summary>Source values with duplicate keys.</summary>
    private static readonly int[] DuplicateKeySource = [First, First, Second, Second, Third];

    /// <summary>Single first-value source.</summary>
    private static readonly int[] SingleFirstSource = [First];

    /// <summary>Two-value source for FlatMap projection.</summary>
    private static readonly int[] FirstSecondSource = [First, Second];

    /// <summary>Single inner-value source.</summary>
    private static readonly int[] SingleInnerSource = [FirstInner];

    /// <summary>Expected count result for two matching values.</summary>
    private static readonly int[] CountTwoExpected = [Second];

    /// <summary>Expected distinct count result.</summary>
    private static readonly int[] DistinctCountExpected = [Third];

    /// <summary>Expected long count result.</summary>
    private static readonly long[] LongCountFourExpected = [Fourth];

    /// <summary>Expected long predicate count result.</summary>
    private static readonly long[] LongCountTwoExpected = [Second];

    /// <summary>Expected distinct long count result.</summary>
    private static readonly long[] DistinctLongCountExpected = [Third];

    /// <summary>Expected true boolean result.</summary>
    private static readonly bool[] TrueExpected = [true];

    /// <summary>Expected false boolean result.</summary>
    private static readonly bool[] FalseExpected = [false];

    /// <summary>Expected queued FlatMap values.</summary>
    private static readonly int[] QueuedFlatMapExpected = [FirstInner, SecondInner];

    /// <summary>Expected result-selector FlatMap values.</summary>
    private static readonly int[] ResultFlatMapExpected = [First + FirstInner, Second + FirstInner];

    /// <summary>Covers count, long-count, distinct fast count, and any helper branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AggregateHelpersCoverPredicateDistinctAndAnyPaths()
    {
        var countPredicate = new List<int>();
        var distinctCount = new List<int>();
        var longCount = new List<long>();
        var longCountPredicate = new List<long>();
        var distinctLongCount = new List<long>();
        var anyTrue = new List<bool>();
        var anyFalse = new List<bool>();
        var rangeDistinctCount = new List<int>();
        var rangeDistinctLongCount = new List<long>();
        var rangeAnyTrue = new List<bool>();
        var rangeAnyFalse = new List<bool>();
        var rangeAllTrue = new List<bool>();
        var rangeAllFalse = new List<bool>();
        var rangeContainsTrue = new List<bool>();
        var rangeContainsFalse = new List<bool>();
        Signal.FromEnumerable(AggregateSource).Count(value => value % Second == 0).Subscribe(countPredicate.Add);
        Signal.FromEnumerable(DuplicateKeySource).DistinctBy(value => value).Count().Subscribe(distinctCount.Add);
        Signal.FromEnumerable(AggregateSource).LongCount().Subscribe(longCount.Add);
        Signal.FromEnumerable(AggregateSource).LongCount(value => value > Second).Subscribe(longCountPredicate.Add);
        Signal.FromEnumerable(DuplicateKeySource).DistinctBy(value => value).LongCount().Subscribe(distinctLongCount.Add);
        Signal.FromEnumerable(AggregateSource).Any().Subscribe(anyTrue.Add);
        Signal.None<int>().Any().Subscribe(anyFalse.Add);
        Signal.Sequence(First, Fourth).DistinctBy(value => value / Second).Count().Subscribe(rangeDistinctCount.Add);
        Signal.Sequence(First, Fourth).DistinctBy(value => value / Second).LongCount().Subscribe(rangeDistinctLongCount.Add);
        Signal.Sequence(First, Fourth).Any(value => value == Third).Subscribe(rangeAnyTrue.Add);
        Signal.Sequence(First, Fourth).Any(value => value == MissingRangeValue).Subscribe(rangeAnyFalse.Add);
        Signal.Sequence(First, Fourth).All(value => value > 0).Subscribe(rangeAllTrue.Add);
        Signal.Sequence(First, Fourth).All(value => value < Fourth).Subscribe(rangeAllFalse.Add);
        Signal.Sequence(First, Fourth).Contains(Third).Subscribe(rangeContainsTrue.Add);
        Signal.Sequence(First, Fourth).Contains(MissingRangeValue).Subscribe(rangeContainsFalse.Add);
        await Assert.That(countPredicate.SequenceEqual(CountTwoExpected)).IsTrue();
        await Assert.That(distinctCount.SequenceEqual(DistinctCountExpected)).IsTrue();
        await Assert.That(longCount.SequenceEqual(LongCountFourExpected)).IsTrue();
        await Assert.That(longCountPredicate.SequenceEqual(LongCountTwoExpected)).IsTrue();
        await Assert.That(distinctLongCount.SequenceEqual(DistinctLongCountExpected)).IsTrue();
        await Assert.That(anyTrue.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(anyFalse.SequenceEqual(FalseExpected)).IsTrue();
        int[] expectedRangeDistinctCount = [Third];
        await Assert.That(rangeDistinctCount.SequenceEqual(expectedRangeDistinctCount)).IsTrue();
        long[] expectedRangeDistinctLongCount = [Third];
        await Assert.That(rangeDistinctLongCount.SequenceEqual(expectedRangeDistinctLongCount)).IsTrue();
        await Assert.That(rangeAnyTrue.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(rangeAnyFalse.SequenceEqual(FalseExpected)).IsTrue();
        await Assert.That(rangeAllTrue.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(rangeAllFalse.SequenceEqual(FalseExpected)).IsTrue();
        await Assert.That(rangeContainsTrue.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(rangeContainsFalse.SequenceEqual(FalseExpected)).IsTrue();
    }

    /// <summary>Covers optimized aggregate observer error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AggregateHelpersForwardSourceErrors()
    {
        var countError = new InvalidOperationException("count");
        var longCountError = new InvalidOperationException("long-count");
        var anyError = new InvalidOperationException("any");
        var distinctError = new InvalidOperationException("distinct");
        var observed = new List<Exception>();
        Signal.Fail<int>(countError).Count().Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        Signal.Fail<int>(longCountError).LongCount().Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        Signal.Fail<int>(anyError).Any().Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        Signal.Fail<int>(distinctError).DistinctBy(value => value).Count().Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        await Assert.That(observed[0]).IsSameReferenceAs(countError);
        await Assert.That(observed[1]).IsSameReferenceAs(longCountError);
        await Assert.That(observed[AnyErrorIndex]).IsSameReferenceAs(anyError);
        await Assert.That(observed[DistinctErrorIndex]).IsSameReferenceAs(distinctError);
    }

    /// <summary>Covers predicate exceptions for aggregate boolean terminals.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AggregateBooleanTerminalsForwardPredicateErrors()
    {
        var allError = new InvalidOperationException(AllMessage);
        var observed = new List<Exception>();
        Signal.Sequence(First, Fourth).All(_ => throw allError).Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        await Assert.That(observed[0]).IsSameReferenceAs(allError);
    }

    /// <summary>Covers fused prepend/default-if-empty/append and empty Prepend helpers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PrependAppendDefaultIfEmptyFusionPreservesOrderingAndTerminals()
    {
        var values = new List<int>();
        var emptyPrependValues = new List<int>();
        var completed = 0;
        Signal.None<int>().DefaultIfEmpty(Second).Prepend(First).Append(Third).Subscribe(values.Add, ex => throw ex, () => completed++);
        Signal.FromEnumerable(AggregateSource).Prepend().Append(Fourth).Subscribe(emptyPrependValues.Add);
        int[] expectedValues = [First, Second, Third];
        await Assert.That(values.SequenceEqual(expectedValues)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        int[] expectedEmptyPrependValues = [First, Second, Third, Fourth, Fourth];
        await Assert.That(emptyPrependValues.SequenceEqual(expectedEmptyPrependValues)).IsTrue();
    }

    /// <summary>Covers FlatMap queuing while an inner signal is active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapQueuesInnerSignalsUntilActiveInnerCompletes()
    {
        var outer = new Signal<int>();
        var firstInner = new Signal<int>();
        var secondInner = new Signal<int>();
        var values = new List<int>();
        var completed = 0;
        using var subscription = outer.FlatMap(value => value == First ? firstInner : secondInner).Subscribe(values.Add, ex => throw ex, () => completed++);
        outer.OnNext(First);
        outer.OnNext(Second);
        outer.OnCompleted();
        firstInner.OnNext(FirstInner);
        firstInner.OnCompleted();
        secondInner.OnNext(SecondInner);
        secondInner.OnCompleted();
        await Assert.That(values.SequenceEqual(QueuedFlatMapExpected)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
    }

    /// <summary>Covers the FlatMap overload with an outer and inner result selector.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapResultSelectorProjectsOuterAndInnerValues()
    {
        var values = new List<int>();
        Signal.FromEnumerable(FirstSecondSource).FlatMap(value => Signal.FromEnumerable(SingleInnerSource), (outer, inner) => outer + inner).Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(ResultFlatMapExpected)).IsTrue();
    }

    /// <summary>Covers FlatMap selector, inner, and outer error forwarding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapForwardsSelectorInnerAndOuterErrors()
    {
        var selectorError = new InvalidOperationException(SelectorMessage);
        var innerError = new InvalidOperationException(InnerMessage);
        var outerError = new InvalidOperationException(OuterMessage);
        var observed = new List<Exception>();
        Signal.FromEnumerable(SingleFirstSource).FlatMap<int, int>(_ => throw selectorError).Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        Signal.FromEnumerable(SingleFirstSource).FlatMap(_ => Signal.Fail<int>(innerError)).Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        Signal.Fail<int>(outerError).FlatMap(ReturnValue).Subscribe(
            _ =>
        {
        },
            observed.Add,
            () =>
        {
        });
        await Assert.That(observed[0]).IsSameReferenceAs(selectorError);
        await Assert.That(observed[1]).IsSameReferenceAs(innerError);
        await Assert.That(observed[OuterErrorIndex]).IsSameReferenceAs(outerError);
    }

    /// <summary>Returns a scalar signal for the supplied value.</summary>
    /// <param name = "value">The value to emit.</param>
    /// <returns>A scalar signal.</returns>
    private static IObservable<int> ReturnValue(int value) => Signal.Emit(value);
}
