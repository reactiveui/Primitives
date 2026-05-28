// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Covers optimized operator helper implementations that are not exercised by broad contract tests.
/// </summary>
public class OperatorCoverageExpansionTests
{
    /// <summary>
    /// The first value.
    /// </summary>
    private const int First = 1;

    /// <summary>
    /// The second value.
    /// </summary>
    private const int Second = 2;

    /// <summary>
    /// The third value.
    /// </summary>
    private const int Third = 3;

    /// <summary>
    /// The fourth value.
    /// </summary>
    private const int Fourth = 4;

    /// <summary>
    /// A value outside the tested ranges.
    /// </summary>
    private const int MissingRangeValue = 99;

    /// <summary>
    /// Observed error index for the any error.
    /// </summary>
    private const int AnyErrorIndex = 2;

    /// <summary>
    /// Observed error index for the distinct error.
    /// </summary>
    private const int DistinctErrorIndex = 3;

    /// <summary>
    /// Observed error index for the outer SelectMany error.
    /// </summary>
    private const int OuterErrorIndex = 2;

    /// <summary>
    /// The first inner value.
    /// </summary>
    private const int FirstInner = 10;

    /// <summary>
    /// The second inner value.
    /// </summary>
    private const int SecondInner = 20;

    /// <summary>
    /// Selector failure message.
    /// </summary>
    private const string SelectorMessage = "selector";

    /// <summary>
    /// Inner failure message.
    /// </summary>
    private const string InnerMessage = "inner";

    /// <summary>
    /// Outer failure message.
    /// </summary>
    private const string OuterMessage = "outer";

    /// <summary>
    /// All predicate failure message.
    /// </summary>
    private const string AllMessage = "all";

    /// <summary>
    /// Source values for aggregate operator tests.
    /// </summary>
    private static readonly int[] AggregateSource = [First, Second, Third, Fourth];

    /// <summary>
    /// Source values with duplicate keys.
    /// </summary>
    private static readonly int[] DuplicateKeySource = [First, First, Second, Second, Third];

    /// <summary>
    /// Single first-value source.
    /// </summary>
    private static readonly int[] SingleFirstSource = [First];

    /// <summary>
    /// Two-value source for SelectMany projection.
    /// </summary>
    private static readonly int[] FirstSecondSource = [First, Second];

    /// <summary>
    /// Single inner-value source.
    /// </summary>
    private static readonly int[] SingleInnerSource = [FirstInner];

    /// <summary>
    /// Expected count result for two matching values.
    /// </summary>
    private static readonly int[] CountTwoExpected = [Second];

    /// <summary>
    /// Expected distinct count result.
    /// </summary>
    private static readonly int[] DistinctCountExpected = [Third];

    /// <summary>
    /// Expected long count result.
    /// </summary>
    private static readonly long[] LongCountFourExpected = [Fourth];

    /// <summary>
    /// Expected long predicate count result.
    /// </summary>
    private static readonly long[] LongCountTwoExpected = [Second];

    /// <summary>
    /// Expected distinct long count result.
    /// </summary>
    private static readonly long[] DistinctLongCountExpected = [Third];

    /// <summary>
    /// Expected true boolean result.
    /// </summary>
    private static readonly bool[] TrueExpected = [true];

    /// <summary>
    /// Expected false boolean result.
    /// </summary>
    private static readonly bool[] FalseExpected = [false];

    /// <summary>
    /// Expected queued SelectMany values.
    /// </summary>
    private static readonly int[] QueuedSelectManyExpected = [FirstInner, SecondInner];

    /// <summary>
    /// Expected result-selector SelectMany values.
    /// </summary>
    private static readonly int[] ResultSelectManyExpected = [First + FirstInner, Second + FirstInner];

    /// <summary>
    /// Covers count, long-count, distinct fast count, and any helper branches.
    /// </summary>
    [Test]
    public void AggregateHelpersCoverPredicateDistinctAndAnyPaths()
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
        Signal.Empty<int>().Any().Subscribe(anyFalse.Add);
        Signal.Range(First, Fourth).DistinctBy(value => value / Second).Count().Subscribe(rangeDistinctCount.Add);
        Signal.Range(First, Fourth).DistinctBy(value => value / Second).LongCount().Subscribe(rangeDistinctLongCount.Add);
        Signal.Range(First, Fourth).Any(value => value == Third).Subscribe(rangeAnyTrue.Add);
        Signal.Range(First, Fourth).Any(value => value == MissingRangeValue).Subscribe(rangeAnyFalse.Add);
        Signal.Range(First, Fourth).All(value => value > 0).Subscribe(rangeAllTrue.Add);
        Signal.Range(First, Fourth).All(value => value < Fourth).Subscribe(rangeAllFalse.Add);
        Signal.Range(First, Fourth).Contains(Third).Subscribe(rangeContainsTrue.Add);
        Signal.Range(First, Fourth).Contains(MissingRangeValue).Subscribe(rangeContainsFalse.Add);

        Assert.Equal(CountTwoExpected, countPredicate);
        Assert.Equal(DistinctCountExpected, distinctCount);
        Assert.Equal(LongCountFourExpected, longCount);
        Assert.Equal(LongCountTwoExpected, longCountPredicate);
        Assert.Equal(DistinctLongCountExpected, distinctLongCount);
        Assert.Equal(TrueExpected, anyTrue);
        Assert.Equal(FalseExpected, anyFalse);
        Assert.Equal(new[] { Third }, rangeDistinctCount);
        Assert.Equal(new long[] { Third }, rangeDistinctLongCount);
        Assert.Equal(TrueExpected, rangeAnyTrue);
        Assert.Equal(FalseExpected, rangeAnyFalse);
        Assert.Equal(TrueExpected, rangeAllTrue);
        Assert.Equal(FalseExpected, rangeAllFalse);
        Assert.Equal(TrueExpected, rangeContainsTrue);
        Assert.Equal(FalseExpected, rangeContainsFalse);
    }

    /// <summary>
    /// Covers optimized aggregate observer error paths.
    /// </summary>
    [Test]
    public void AggregateHelpersForwardSourceErrors()
    {
        var countError = new InvalidOperationException("count");
        var longCountError = new InvalidOperationException("long-count");
        var anyError = new InvalidOperationException("any");
        var distinctError = new InvalidOperationException("distinct");
        var observed = new List<Exception>();

        Signal.Throw<int>(countError).Count().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Throw<int>(longCountError).LongCount().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Throw<int>(anyError).Any().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Throw<int>(distinctError).DistinctBy(value => value).Count().Subscribe(_ => { }, observed.Add, () => { });

        Assert.Same(countError, observed[0]);
        Assert.Same(longCountError, observed[1]);
        Assert.Same(anyError, observed[AnyErrorIndex]);
        Assert.Same(distinctError, observed[DistinctErrorIndex]);
    }

    /// <summary>
    /// Covers predicate exceptions for aggregate boolean terminals.
    /// </summary>
    [Test]
    public void AggregateBooleanTerminalsForwardPredicateErrors()
    {
        var allError = new InvalidOperationException(AllMessage);
        var observed = new List<Exception>();

        Signal.Range(First, Fourth).All(_ => throw allError).Subscribe(_ => { }, observed.Add, () => { });

        Assert.Same(allError, observed[0]);
    }

    /// <summary>
    /// Covers fused prepend/default-if-empty/append and empty StartWith helpers.
    /// </summary>
    [Test]
    public void StartWithAppendDefaultIfEmptyFusionPreservesOrderingAndTerminals()
    {
        var values = new List<int>();
        var emptyStartWithValues = new List<int>();
        var completed = 0;

        Signal.Empty<int>()
            .DefaultIfEmpty(Second)
            .StartWith(First)
            .Append(Third)
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        Signal.FromEnumerable(AggregateSource)
            .StartWith()
            .Append(Fourth)
            .Subscribe(emptyStartWithValues.Add);

        Assert.Equal(new[] { First, Second, Third }, values);
        Assert.Equal(1, completed);
        Assert.Equal(new[] { First, Second, Third, Fourth, Fourth }, emptyStartWithValues);
    }

    /// <summary>
    /// Covers SelectMany queuing while an inner signal is active.
    /// </summary>
    [Test]
    public void SelectManyQueuesInnerSignalsUntilActiveInnerCompletes()
    {
        var outer = new Signal<int>();
        var firstInner = new Signal<int>();
        var secondInner = new Signal<int>();
        var values = new List<int>();
        var completed = 0;

        using var subscription = outer.SelectMany(value => value == First ? firstInner : secondInner)
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        outer.OnNext(First);
        outer.OnNext(Second);
        outer.OnCompleted();
        firstInner.OnNext(FirstInner);
        firstInner.OnCompleted();
        secondInner.OnNext(SecondInner);
        secondInner.OnCompleted();

        Assert.Equal(QueuedSelectManyExpected, values);
        Assert.Equal(1, completed);
    }

    /// <summary>
    /// Covers the SelectMany overload with an outer and inner result selector.
    /// </summary>
    [Test]
    public void SelectManyResultSelectorProjectsOuterAndInnerValues()
    {
        var values = new List<int>();

        Signal.FromEnumerable(FirstSecondSource)
            .SelectMany(value => Signal.FromEnumerable(SingleInnerSource), (outer, inner) => outer + inner)
            .Subscribe(values.Add);

        Assert.Equal(ResultSelectManyExpected, values);
    }

    /// <summary>
    /// Covers SelectMany selector, inner, and outer error forwarding.
    /// </summary>
    [Test]
    public void SelectManyForwardsSelectorInnerAndOuterErrors()
    {
        var selectorError = new InvalidOperationException(SelectorMessage);
        var innerError = new InvalidOperationException(InnerMessage);
        var outerError = new InvalidOperationException(OuterMessage);
        var observed = new List<Exception>();

        Signal.FromEnumerable(SingleFirstSource).SelectMany<int, int>(_ => throw selectorError).Subscribe(_ => { }, observed.Add, () => { });
        Signal.FromEnumerable(SingleFirstSource).SelectMany(_ => Signal.Throw<int>(innerError)).Subscribe(_ => { }, observed.Add, () => { });
        Signal.Throw<int>(outerError).SelectMany(ReturnValue).Subscribe(_ => { }, observed.Add, () => { });

        Assert.Same(selectorError, observed[0]);
        Assert.Same(innerError, observed[1]);
        Assert.Same(outerError, observed[OuterErrorIndex]);
    }

    /// <summary>
    /// Returns a scalar signal for the supplied value.
    /// </summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>A scalar signal.</returns>
    private static IObservable<int> ReturnValue(int value) => Signal.Return(value);
}
