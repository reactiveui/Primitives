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

    /// <summary>Verifies <see cref="SkipWitness{T}"/> drops the leading values then forwards the rest.</summary>
    [Test]
    public void SkipForwardsAfterCount()
    {
        var r = new Recorder<int>();
        Feed(new SkipWitness<int>(r, Two), Source);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="DistinctWitness{T}"/> forwards only the first occurrence of each value.</summary>
    [Test]
    public void DistinctForwardsFirstOccurrenceOnly()
    {
        var r = new Recorder<int>();
        Feed(new DistinctWitness<int>(r, []), Duplicates);
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="UniqueWitness{T}"/> suppresses adjacent duplicates only.</summary>
    [Test]
    public void UniqueSuppressesAdjacentDuplicates()
    {
        var r = new Recorder<int>();
        Feed(new UniqueWitness<int>(r, EqualityComparer<int>.Default), Adjacent);
        Assert.Equal(ExpectedUnique.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="FoldWitness{TSource, TAccumulate}"/> emits a running accumulation.</summary>
    [Test]
    public void FoldEmitsRunningAccumulation()
    {
        var r = new Recorder<int>();
        Feed(new FoldWitness<int, int>(r, 0, static (a, b) => a + b), SourceTriple);
        Assert.Equal(ExpectedFold.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="ReduceWitness{TSource, TAccumulate}"/> emits the final accumulation on completion.</summary>
    [Test]
    public void ReduceEmitsFinalOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new ReduceWitness<int, int>(r, 0, static (a, b) => a + b);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Six, r.Values[0]);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="KeepNotNullWitness{T}"/> drops null values.</summary>
    [Test]
    public void KeepNotNullDropsNulls()
    {
        var r = new Recorder<string>();
        var sink = new KeepNotNullWitness<string>(r);
        sink.OnNext("a");
        sink.OnNext(null);
        sink.OnNext("b");
        Assert.Equal(ExpectedStrings.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="KeepTypeWitness{TResult}"/> forwards only assignable values.</summary>
    [Test]
    public void KeepTypeForwardsAssignableValues()
    {
        var r = new Recorder<string>();
        var sink = new KeepTypeWitness<string>(r);
        sink.OnNext("a");
        sink.OnNext(1);
        sink.OnNext("b");
        Assert.Equal(ExpectedStrings.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="TapWitness{T}"/> runs the side effect and forwards the value.</summary>
    [Test]
    public void TapRunsSideEffectAndForwards()
    {
        var r = new Recorder<int>();
        var tapped = new List<int>();
        Feed(new TapWitness<int>(r, tapped.Add, static _ => { }, static () => { }), SourceTriple);
        Assert.Equal(SourceTriple.AsEnumerable(), tapped);
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="IgnoreValuesWitness{T}"/> drops values but forwards completion.</summary>
    [Test]
    public void IgnoreValuesDropsValues()
    {
        var r = new Recorder<int>();
        var sink = new IgnoreValuesWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(0, r.Values.Count);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="SparkWitness{T}"/> materializes values into sparks.</summary>
    [Test]
    public void SparkMaterializesValues()
    {
        var r = new Recorder<Spark<int>>();
        new SparkWitness<int>(r).OnNext(Five);
        Assert.True(r.Values[0].HasValue);
        Assert.Equal(Five, r.Values[0].Value);
    }

    /// <summary>Verifies <see cref="UnsparkWitness{T}"/> unwraps on-next sparks.</summary>
    [Test]
    public void UnsparkUnwrapsValues()
    {
        var r = new Recorder<int>();
        new UnsparkWitness<int>(r).OnNext(Spark.CreateOnNext(Five));
        Assert.Equal(Five, r.Values[0]);
    }

    /// <summary>Verifies <see cref="TimeIntervalWitness{T}"/> annotates values with an interval.</summary>
    [Test]
    public void TimeIntervalAnnotatesValues()
    {
        var r = new Recorder<TimeInterval<int>>();
        new TimeIntervalWitness<int>(r, Sequencer.Immediate).OnNext(Five);
        Assert.Equal(Five, r.Values[0].Value);
    }

    /// <summary>Verifies <see cref="BufferWitness{T}"/> emits non-overlapping windows.</summary>
    [Test]
    public void BufferEmitsWindows()
    {
        var r = new Recorder<IList<int>>();
        Feed(new BufferWitness<int>(r, Two, 0), Source);
        Assert.Equal(Two, r.Values.Count);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values[0]);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values[1]);
    }

    /// <summary>Verifies <see cref="CollectListWitness{T}"/> emits a single list on completion.</summary>
    [Test]
    public void CollectListEmitsOnCompletion()
    {
        var r = new Recorder<IList<int>>();
        var sink = new CollectListWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values[0]);
    }

    /// <summary>Verifies <see cref="CollectArrayWitness{T}"/> emits a single array on completion.</summary>
    [Test]
    public void CollectArrayEmitsOnCompletion()
    {
        var r = new Recorder<int[]>();
        var sink = new CollectArrayWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(SourceTriple.AsEnumerable(), r.Values[0]);
    }

    /// <summary>Verifies <see cref="AnyWitness{T}"/> emits true when a value arrives.</summary>
    [Test]
    public void AnyEmitsTrueOnValue()
    {
        var r = new Recorder<bool>();
        new AnyWitness<int>(r).OnNext(1);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="AnyPredicateWitness{T}"/> emits true on the first match.</summary>
    [Test]
    public void AnyPredicateEmitsTrueOnMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AnyPredicateWitness<int>(r, static x => x > Two), SourceTriple);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="CountAggregator{T}"/> emits the element count on completion.</summary>
    [Test]
    public void CountEmitsCount()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, CountAggregator<int>>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Three, r.Values[0]);
    }

    /// <summary>Verifies <see cref="CountPredicateAggregator{T}"/> counts only matching values.</summary>
    [Test]
    public void CountPredicateCountsMatches()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, CountPredicateAggregator<int>>(r, new CountPredicateAggregator<int>(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="LongCountAggregator{T}"/> emits the element count on completion.</summary>
    [Test]
    public void LongCountEmitsCount()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, LongCountAggregator<int>>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal((long)Three, r.Values[0]);
    }

    /// <summary>Verifies <see cref="LongCountPredicateAggregator{T}"/> counts only matching values.</summary>
    [Test]
    public void LongCountPredicateCountsMatches()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, LongCountPredicateAggregator<int>>(r, new LongCountPredicateAggregator<int>(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal((long)Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="DistinctByWitness{T, TKey}"/> forwards the first value per key.</summary>
    [Test]
    public void DistinctByForwardsFirstPerKey()
    {
        var r = new Recorder<int>();
        Feed(new DistinctByWitness<int, int>(r, static x => x % Two, null), Source);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="DistinctByCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    [Test]
    public void DistinctByCountCountsKeys()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, DistinctByCountAggregator<int, int>>(r, new DistinctByCountAggregator<int, int>(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        Assert.Equal(Two, r.Values[0]);
    }

    /// <summary>Verifies <see cref="DistinctByLongCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    [Test]
    public void DistinctByLongCountCountsKeys()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, DistinctByLongCountAggregator<int, int>>(r, new DistinctByLongCountAggregator<int, int>(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        Assert.Equal((long)Two, r.Values[0]);
    }

    /// <summary>Verifies a user-defined <see cref="IAggregator{T,TResult,TSelf}"/> drives <see cref="AggregateWitness{T,TResult,TAggregator}"/>.</summary>
    [Test]
    public void CustomAggregatorIsDrivenByAggregateWitness()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, SumAggregator>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        Assert.Equal(Six, r.Values[0]);
    }

    /// <summary>Verifies <see cref="UniqueByWitness{T, TKey}"/> suppresses adjacent duplicates by key.</summary>
    [Test]
    public void UniqueBySuppressesAdjacentByKey()
    {
        var r = new Recorder<int>();
        Feed(new UniqueByWitness<int, int>(r, static x => x % Two, EqualityComparer<int>.Default), Adjacent);
        Assert.Equal(ExpectedUnique.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="SkipWhileWitness{T}"/> drops the leading matching values.</summary>
    [Test]
    public void SkipWhileDropsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new SkipWhileWitness<int>(r, static x => x < Three), Source);
        Assert.Equal(ExpectedThreeFour.AsEnumerable(), r.Values);
    }

    /// <summary>Verifies <see cref="TakeWhileWitness{T}"/> forwards leading matches then completes.</summary>
    [Test]
    public void TakeWhileForwardsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new TakeWhileWitness<int>(r, static x => x < Three), Source);
        Assert.Equal(ExpectedOneTwo.AsEnumerable(), r.Values);
        Assert.True(r.Completed);
    }

    /// <summary>Verifies <see cref="AppendWitness{T}"/> appends a value after completion.</summary>
    [Test]
    public void AppendAddsValueOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new AppendWitness<int>(r, Ten);
        Feed(sink, ExpectedOneTwo);
        sink.OnCompleted();
        Assert.Equal(Ten, r.Values[^1]);
    }

    /// <summary>Verifies <see cref="DefaultIfEmptyWitness{T}"/> emits the default when the source is empty.</summary>
    [Test]
    public void DefaultIfEmptyEmitsDefaultWhenEmpty()
    {
        var r = new Recorder<int>();
        new DefaultIfEmptyWitness<int>(r, Ten).OnCompleted();
        Assert.Equal(Ten, r.Values[0]);
    }

    /// <summary>Verifies <see cref="AllPredicateWitness{T}"/> emits false on the first non-match.</summary>
    [Test]
    public void AllPredicateEmitsFalseOnNonMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AllPredicateWitness<int>(r, static x => x < Three), Source);
        Assert.False(r.Values[0]);
    }

    /// <summary>Verifies <see cref="ContainsWitness{T}"/> emits true when the value is found.</summary>
    [Test]
    public void ContainsEmitsTrueWhenFound()
    {
        var r = new Recorder<bool>();
        Feed(new ContainsWitness<int>(r, Two, EqualityComparer<int>.Default), SourceTriple);
        Assert.True(r.Values[0]);
    }

    /// <summary>Verifies <see cref="TakeWitness{T}"/> forwards the requested count then completes.</summary>
    [Test]
    public void TakeForwardsRequestedCount()
    {
        var r = new Recorder<int>();
        Feed(new TakeWitness<int>(r, Two), SourceTriple);
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

    /// <summary>A user-defined accumulator that sums observed values, exercising the public <see cref="IAggregator{T,TResult,TSelf}"/> contract.</summary>
    private readonly record struct SumAggregator : IAggregator<int, int, SumAggregator>
    {
        private SumAggregator(int result) => Result = result;

        /// <inheritdoc/>
        public int Result { get; }

        /// <inheritdoc/>
        public SumAggregator Add(int value) => new(Result + value);
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
