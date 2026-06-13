// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies optimized aggregate and flat-map helper contracts.</summary>
[SuppressMessage("Major Code Smell", "S6966", Justification = "Coverage tests intentionally group branch-heavy scenarios.")]
public partial class SignalOperatorParityMixinsTests
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
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task AggregateHelpersCoverPredicateDistinctAndAnyPaths()
    {
        List<int> countPredicate = [];
        List<int> distinctCount = [];
        List<long> longCount = [];
        List<long> longCountPredicate = [];
        List<long> distinctLongCount = [];
        List<bool> anyTrue = [];
        List<bool> anyFalse = [];
        List<int> rangeDistinctCount = [];
        List<long> rangeDistinctLongCount = [];
        List<bool> rangeAnyTrue = [];
        List<bool> rangeAnyFalse = [];
        List<bool> rangeAllTrue = [];
        List<bool> rangeAllFalse = [];
        List<bool> rangeContainsTrue = [];
        List<bool> rangeContainsFalse = [];
        Signal.FromEnumerable(AggregateSource).Count(value => value % Second == 0).Subscribe(countPredicate.Add);
        Signal.FromEnumerable(DuplicateKeySource).DistinctBy(value => value).Count().Subscribe(distinctCount.Add);
        Signal.FromEnumerable(AggregateSource).LongCount().Subscribe(longCount.Add);
        Signal.FromEnumerable(AggregateSource).LongCount(value => value > Second).Subscribe(longCountPredicate.Add);
        Signal.FromEnumerable(DuplicateKeySource).DistinctBy(value => value).LongCount()
            .Subscribe(distinctLongCount.Add);
        Signal.FromEnumerable(AggregateSource).Any().Subscribe(anyTrue.Add);
        Signal.None<int>().Any().Subscribe(anyFalse.Add);
        Signal.Sequence(First, Fourth).DistinctBy(value => value / Second).Count().Subscribe(rangeDistinctCount.Add);
        Signal.Sequence(First, Fourth).DistinctBy(value => value / Second).LongCount()
            .Subscribe(rangeDistinctLongCount.Add);
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
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task AggregateHelpersForwardSourceErrors()
    {
        InvalidOperationException countError = new("count");
        InvalidOperationException longCountError = new("long-count");
        InvalidOperationException anyError = new("any");
        InvalidOperationException distinctError = new("distinct");
        List<Exception> observed = [];
        Signal.Fail<int>(countError).Count().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Fail<int>(longCountError).LongCount().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Fail<int>(anyError).Any().Subscribe(_ => { }, observed.Add, () => { });
        Signal.Fail<int>(distinctError).DistinctBy(value => value).Count()
            .Subscribe(_ => { }, observed.Add, () => { });
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
        InvalidOperationException allError = new(AllMessage);
        List<Exception> observed = [];
        Signal.Sequence(First, Fourth).All(_ => throw allError).Subscribe(_ => { }, observed.Add, () => { });
        await Assert.That(observed[0]).IsSameReferenceAs(allError);
    }

    /// <summary>Covers FlatMap queuing while an inner signal is active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapQueuesInnerSignalsUntilActiveInnerCompletes()
    {
        Signal<int> outer = new();
        Signal<int> firstInner = new();
        Signal<int> secondInner = new();
        List<int> values = [];
        var completed = 0;
        using var subscription = outer.FlatMap(value => value == First ? firstInner : secondInner)
            .Subscribe(values.Add, ex => throw ex, () => completed++);
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
        List<int> values = [];
        Signal.FromEnumerable(FirstSecondSource)
            .FlatMap(value => Signal.FromEnumerable(SingleInnerSource), (outer, inner) => outer + inner)
            .Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(ResultFlatMapExpected)).IsTrue();
    }

    /// <summary>Covers FlatMap selector, inner, and outer error forwarding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapForwardsSelectorInnerAndOuterErrors()
    {
        InvalidOperationException selectorError = new(SelectorMessage);
        InvalidOperationException innerError = new(InnerMessage);
        InvalidOperationException outerError = new(OuterMessage);
        List<Exception> observed = [];
        Signal.FromEnumerable(SingleFirstSource).FlatMap<int, int>(_ => throw selectorError)
            .Subscribe(_ => { }, observed.Add, () => { });
        Signal.FromEnumerable(SingleFirstSource).FlatMap(_ => Signal.Fail<int>(innerError))
            .Subscribe(_ => { }, observed.Add, () => { });
        Signal.Fail<int>(outerError).FlatMap(ReturnValue).Subscribe(_ => { }, observed.Add, () => { });
        await Assert.That(observed[0]).IsSameReferenceAs(selectorError);
        await Assert.That(observed[1]).IsSameReferenceAs(innerError);
        await Assert.That(observed[OuterErrorIndex]).IsSameReferenceAs(outerError);
    }

    /// <summary>Verifies collection and async terminal operators with reference, record, and nullable values.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task CollectionAndAsyncTerminalsHandleReferenceRecordAndNullableValues()
    {
        const int ExpectedLastNameCount = 2;
        Contact[] contacts =
        [
            new("Ada", "Lovelace"), new("Grace", null), new("Katherine", "Johnson")
        ];
        var source = Signal.FromEnumerable(contacts);
        var collectedArray = await source.CollectArrayAsync();
        var collectedList = await source.CollectListAsync();
        var firstDefault = await Signal.None<Contact>().FirstOrDefaultAsync(new("empty", null));
        var last = await source.LastAsync();
        var anyNullLastName = await source.AnyAsync(contact => contact.LastName is null);
        var countWithLastName = await source.CountAsync(contact => contact.LastName is not null);
        await Assert.That(collectedArray.SequenceEqual(contacts)).IsTrue();
        await Assert.That(collectedList.SequenceEqual(contacts)).IsTrue();
        await Assert.That(firstDefault).IsEqualTo(new("empty", null));
        await Assert.That(last).IsEqualTo(new("Katherine", "Johnson"));
        await Assert.That(anyNullLastName).IsTrue();
        await Assert.That(countWithLastName).IsEqualTo(ExpectedLastNameCount);
    }

    /// <summary>Returns a scalar signal for the supplied value.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>A scalar signal.</returns>
    private static IObservable<int> ReturnValue(int value) => Signal.Emit(value);

    /// <summary>Contact reference record with nullable fields.</summary>
    /// <param name="FirstName">The first name.</param>
    /// <param name="LastName">The optional last name.</param>
    private sealed record Contact(string FirstName, string? LastName);
}
