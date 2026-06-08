// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Behavioural tests for the public observer "sink" types that back the operator implementations.</summary>
public class SinkObserverTests
{
    /// <summary>The literal two, used for counts, thresholds, and skip windows.</summary>
    private const int Two = 2;

    /// <summary>The literal three.</summary>
    private const int Three = 3;

    /// <summary>The literal five.</summary>
    private const int Five = 5;

    /// <summary>The sum of <see cref="SourceTriple"/>.</summary>
    private const int Six = 6;

    /// <summary>The literal ten, used as an appended/default value.</summary>
    private const int Ten = 10;

    /// <summary>Source values 1..4.</summary>
    private static readonly int[] Source = [1, 2, 3, 4];

    /// <summary>Source values 1..3.</summary>
    private static readonly int[] SourceTriple = [1, 2, 3];

    /// <summary>Source values containing duplicates.</summary>
    private static readonly int[] Duplicates = [1, 1, 2, 2, 3];

    /// <summary>Source values with adjacent duplicates.</summary>
    private static readonly int[] Adjacent = [1, 1, 2, 1];

    /// <summary>Expected values [3, 4].</summary>
    private static readonly int[] ExpectedThreeFour = [3, 4];

    /// <summary>Expected values [1, 2].</summary>
    private static readonly int[] ExpectedOneTwo = [1, 2];

    /// <summary>Expected unique values [1, 2, 1].</summary>
    private static readonly int[] ExpectedUnique = [1, 2, 1];

    /// <summary>Expected running fold [1, 3, 6].</summary>
    private static readonly int[] ExpectedFold = [1, 3, 6];

    /// <summary>Expected strings ["a", "b"].</summary>
    private static readonly string[] ExpectedStrings = ["a", "b"];

    /// <summary>Verifies <see cref="SkipObserver{T}"/> drops the leading values then forwards the rest.</summary>
    [Test]
    public void SkipForwardsAfterCount()
    {
        var r = new Recorder<int>();
        Feed(new SkipObserver<int>(r, Two), Source);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="DistinctObserver{T}"/> forwards only the first occurrence of each value.</summary>
    [Test]
    public void DistinctForwardsFirstOccurrenceOnly()
    {
        var r = new Recorder<int>();
        Feed(new DistinctObserver<int>(r, []), Duplicates);
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="UniqueObserver{T}"/> suppresses adjacent duplicates only.</summary>
    [Test]
    public void UniqueSuppressesAdjacentDuplicates()
    {
        var r = new Recorder<int>();
        Feed(new UniqueObserver<int>(r, EqualityComparer<int>.Default), Adjacent);
        Assert.Equal(ExpectedUnique.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="FoldObserver{TSource, TAccumulate}"/> emits a running accumulation.</summary>
    [Test]
    public void FoldEmitsRunningAccumulation()
    {
        var r = new Recorder<int>();
        Feed(new FoldObserver<int, int>(r, 0, static (a, b) => a + b), SourceTriple);
        Assert.Equal(ExpectedFold.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="ReduceObserver{TSource, TAccumulate}"/> emits the final accumulation on completion.</summary>
    [Test]
    public void ReduceEmitsFinalOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new ReduceObserver<int, int>(r, 0, static (a, b) => a + b);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Six, r.Values[0]);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="KeepNotNullObserver{T}"/> drops null values.</summary>
    [Test]
    public void KeepNotNullDropsNulls()
    {
        var r = new Recorder<string>();
        var sink = new KeepNotNullObserver<string>(r);
        sink.OnNext("a");
        sink.OnNext(null);
        sink.OnNext("b");
        Assert.Equal(ExpectedStrings.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="KeepTypeObserver{TResult}"/> forwards only assignable values.</summary>
    [Test]
    public void KeepTypeForwardsAssignableValues()
    {
        var r = new Recorder<string>();
        var sink = new KeepTypeObserver<string>(r);
        sink.OnNext("a");
        sink.OnNext(1);
        sink.OnNext("b");
        Assert.Equal(ExpectedStrings.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="TapObserver{T}"/> runs the side effect and forwards the value.</summary>
    [Test]
    public void TapRunsSideEffectAndForwards()
    {
        var r = new Recorder<int>();
        var tapped = new List<int>();
        Feed(new TapObserver<int>(r, tapped.Add, static _ => { }, static () => { }), SourceTriple);
        Assert.Equal(SourceTriple.AsEnumerable(), tapped);
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="IgnoreValuesObserver{T}"/> drops values but forwards completion.</summary>
    [Test]
    public void IgnoreValuesDropsValues()
    {
        var r = new Recorder<int>();
        var sink = new IgnoreValuesObserver<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(0, r.Values.Count);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="SparkObserver{T}"/> materializes values into sparks.</summary>
    [Test]
    public void SparkMaterializesValues()
    {
        var r = new Recorder<Spark<int>>();
        new SparkObserver<int>(r).OnNext(Five);
        Assert.True(r.Values[0].HasValue);
        Assert.Equal(Five, r.Values[0].Value);
    }

    /// <summary>Verifies <see cref="UnsparkObserver{T}"/> unwraps on-next sparks.</summary>
    [Test]
    public void UnsparkUnwrapsValues()
    {
        var r = new Recorder<int>();
        new UnsparkObserver<int>(r).OnNext(Spark.CreateOnNext(Five));
        Assert.Equal(Five, r.Values[0]);
    }

    /// <summary>Verifies <see cref="TimeIntervalObserver{T}"/> annotates values with an interval.</summary>
    [Test]
    public void TimeIntervalAnnotatesValues()
    {
        var r = new Recorder<TimeInterval<int>>();
        new TimeIntervalObserver<int>(r, Sequencer.Immediate).OnNext(Five);
        Assert.Equal(Five, r.Values[0].Value);
    }

    /// <summary>Verifies <see cref="BufferObserver{T}"/> emits non-overlapping windows.</summary>
    [Test]
    public void BufferEmitsWindows()
    {
        var r = new Recorder<IList<int>>();
        Feed(new BufferObserver<int>(r, Two, 0), Source);
        Assert.Equal(Two, r.Values.Count);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values[0]);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values[1]);
    }

    /// <summary>Verifies <see cref="CollectListObserver{T}"/> emits a single list on completion.</summary>
    [Test]
    public void CollectListEmitsOnCompletion()
    {
        var r = new Recorder<IList<int>>();
        var sink = new CollectListObserver<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values[0]);
    }

    /// <summary>Verifies <see cref="CollectArrayObserver{T}"/> emits a single array on completion.</summary>
    [Test]
    public void CollectArrayEmitsOnCompletion()
    {
        var r = new Recorder<int[]>();
        var sink = new CollectArrayObserver<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values[0]);
    }

    /// <summary>Verifies <see cref="AnyObserver{T}"/> emits true when a value arrives.</summary>
    [Test]
    public void AnyEmitsTrueOnValue()
    {
        var r = new Recorder<bool>();
        new AnyObserver<int>(r).OnNext(1);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="AnyPredicateObserver{T}"/> emits true on the first match.</summary>
    [Test]
    public void AnyPredicateEmitsTrueOnMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AnyPredicateObserver<int>(r, static x => x > Two), SourceTriple);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="CountObserver{T}"/> emits the element count on completion.</summary>
    [Test]
    public void CountEmitsCount()
    {
        var r = new Recorder<int>();
        var sink = new CountObserver<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Three, r.Values[0]);
    }

    /// <summary>Verifies <see cref="CountPredicateObserver{T}"/> counts only matching values.</summary>
    [Test]
    public void CountPredicateCountsMatches()
    {
        var r = new Recorder<int>();
        var sink = new CountPredicateObserver<int>(r, static x => x > 1);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="LongCountObserver{T}"/> emits the element count on completion.</summary>
    [Test]
    public void LongCountEmitsCount()
    {
        var r = new Recorder<long>();
        var sink = new LongCountObserver<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal((long)Three, r.Values[0]);
    }

    /// <summary>Verifies <see cref="LongCountPredicateObserver{T}"/> counts only matching values.</summary>
    [Test]
    public void LongCountPredicateCountsMatches()
    {
        var r = new Recorder<long>();
        var sink = new LongCountPredicateObserver<int>(r, static x => x > 1);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal((long)Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="DistinctByObserver{T, TKey}"/> forwards the first value per key.</summary>
    [Test]
    public void DistinctByForwardsFirstPerKey()
    {
        var r = new Recorder<int>();
        Feed(new DistinctByObserver<int, int>(r, static x => x % Two, null), Source);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="DistinctByCountObserver{T, TKey}"/> counts distinct keys.</summary>
    [Test]
    public void DistinctByCountCountsKeys()
    {
        var r = new Recorder<int>();
        var sink = new DistinctByCountObserver<int, int>(r, static x => x % Two, null);
        Feed(sink, Source);
        sink.OnCompleted();
        Assert.Equal(Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="DistinctByLongCountObserver{T, TKey}"/> counts distinct keys.</summary>
    [Test]
    public void DistinctByLongCountCountsKeys()
    {
        var r = new Recorder<long>();
        var sink = new DistinctByLongCountObserver<int, int>(r, static x => x % Two, null);
        Feed(sink, Source);
        sink.OnCompleted();
        Assert.Equal((long)Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="UniqueByObserver{T, TKey}"/> suppresses adjacent duplicates by key.</summary>
    [Test]
    public void UniqueBySuppressesAdjacentByKey()
    {
        var r = new Recorder<int>();
        Feed(new UniqueByObserver<int, int>(r, static x => x % Two, EqualityComparer<int>.Default), Adjacent);
        Assert.Equal(ExpectedUnique.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="SkipWhileObserver{T}"/> drops the leading matching values.</summary>
    [Test]
    public void SkipWhileDropsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new SkipWhileObserver<int>(r, static x => x < Three), Source);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="TakeWhileObserver{T}"/> forwards leading matches then completes.</summary>
    [Test]
    public void TakeWhileForwardsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new TakeWhileObserver<int>(r, static x => x < Three), Source);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="AppendObserver{T}"/> appends a value after completion.</summary>
    [Test]
    public void AppendAddsValueOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new AppendObserver<int>(r, Ten);
        Feed(sink, ExpectedOneTwo);
        sink.OnCompleted();
        Assert.Equal(Ten, r.Values[^1]);
    }

    /// <summary>Verifies <see cref="DefaultIfEmptyObserver{T}"/> emits the default when the source is empty.</summary>
    [Test]
    public void DefaultIfEmptyEmitsDefaultWhenEmpty()
    {
        var r = new Recorder<int>();
        new DefaultIfEmptyObserver<int>(r, Ten).OnCompleted();
        Assert.Equal(Ten, r.Values[0]);
    }

    /// <summary>Verifies <see cref="AllPredicateObserver{T}"/> emits false on the first non-match.</summary>
    [Test]
    public void AllPredicateEmitsFalseOnNonMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AllPredicateObserver<int>(r, static x => x < Three), Source);
        Assert.False(r.Values[0]);
    }

    /// <summary>Verifies <see cref="ContainsObserver{T}"/> emits true when the value is found.</summary>
    [Test]
    public void ContainsEmitsTrueWhenFound()
    {
        var r = new Recorder<bool>();
        Feed(new ContainsObserver<int>(r, Two, EqualityComparer<int>.Default), SourceTriple);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="TakeObserver{T}"/> forwards the requested count then completes.</summary>
    [Test]
    public void TakeForwardsRequestedCount()
    {
        var r = new Recorder<int>();
        Feed(new TakeObserver<int>(r, Two), SourceTriple);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values);
        Assert.True(r.Completed);
    }

    /// <summary>Pushes each value through the sink via <see cref="IObserver{T}.OnNext"/>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="sink">The sink under test.</param>
    /// <param name="values">The values to push.</param>
    private static void Feed<T>(IObserver<T> sink, IEnumerable<T> values)
    {
        foreach (var value in values)
        {
            sink.OnNext(value);
        }
    }

    /// <summary>Observer that records the notifications it receives.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets the recorded values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the recorded error, if any.</summary>
        public Exception? Error { get; private set; }

        /// <summary>Gets a value indicating whether completion was recorded.</summary>
        public bool Completed { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => Error = error;

        /// <inheritdoc/>
        public void OnCompleted() => Completed = true;
    }
}
