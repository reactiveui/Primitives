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

    /// <summary>The sum of <see cref = "SourceTriple"/>.</summary>
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

    /// <summary>Verifies <see cref = "SkipWitness{T}"/> drops the leading values then forwards the rest.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SkipForwardsAfterCount()
    {
        var r = new Recorder<int>();
        Feed(new SkipWitness<int>(r, Two), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedThreeFour)).IsTrue();
    }

    /// <summary>Verifies <see cref = "DistinctWitness{T}"/> forwards only the first occurrence of each value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctForwardsFirstOccurrenceOnly()
    {
        var r = new Recorder<int>();
        Feed(new DistinctWitness<int>(r, []), Duplicates);
        await Assert.That(r.Values.SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "UniqueWitness{T}"/> suppresses adjacent duplicates only.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UniqueSuppressesAdjacentDuplicates()
    {
        var r = new Recorder<int>();
        Feed(new UniqueWitness<int>(r, EqualityComparer<int>.Default), Adjacent);
        await Assert.That(r.Values.SequenceEqual(ExpectedUnique)).IsTrue();
    }

    /// <summary>Verifies <see cref = "FoldWitness{TSource, TAccumulate}"/> emits a running accumulation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FoldEmitsRunningAccumulation()
    {
        var r = new Recorder<int>();
        Feed(new FoldWitness<int, int>(r, 0, static (a, b) => a + b), SourceTriple);
        await Assert.That(r.Values.SequenceEqual(ExpectedFold)).IsTrue();
    }

    /// <summary>Verifies <see cref = "ReduceWitness{TSource, TAccumulate}"/> emits the final accumulation on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReduceEmitsFinalOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new ReduceWitness<int, int>(r, 0, static (a, b) => a + b);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Six);
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>Verifies <see cref = "KeepNotNullWitness{T}"/> drops null values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task KeepNotNullDropsNulls()
    {
        var r = new Recorder<string>();
        var sink = new KeepNotNullWitness<string>(r);
        sink.OnNext("a");
        sink.OnNext(null);
        sink.OnNext("b");
        await Assert.That(r.Values.SequenceEqual(ExpectedStrings)).IsTrue();
    }

    /// <summary>Verifies <see cref = "KeepTypeWitness{TResult}"/> forwards only assignable values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task KeepTypeForwardsAssignableValues()
    {
        var r = new Recorder<string>();
        var sink = new KeepTypeWitness<string>(r);
        sink.OnNext("a");
        sink.OnNext(1);
        sink.OnNext("b");
        await Assert.That(r.Values.SequenceEqual(ExpectedStrings)).IsTrue();
    }

    /// <summary>Verifies <see cref = "TapWitness{T}"/> runs the side effect and forwards the value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TapRunsSideEffectAndForwards()
    {
        var r = new Recorder<int>();
        var tapped = new List<int>();
        Feed(
            new TapWitness<int>(
                r,
                tapped.Add,
                static _ =>
                {
                },
                static () =>
                {
                }),
            SourceTriple);
        await Assert.That(tapped.SequenceEqual(SourceTriple)).IsTrue();
        await Assert.That(r.Values.SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "IgnoreValuesWitness{T}"/> drops values but forwards completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IgnoreValuesDropsValues()
    {
        var r = new Recorder<int>();
        var sink = new IgnoreValuesWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values.Count).IsEqualTo(0);
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>Verifies <see cref = "SparkWitness{T}"/> materializes values into sparks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SparkMaterializesValues()
    {
        var r = new Recorder<Spark<int>>();
        new SparkWitness<int>(r).OnNext(Five);
        await Assert.That(r.Values[0].HasValue).IsTrue();
        await Assert.That(r.Values[0].Value).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "UnsparkWitness{T}"/> unwraps on-next sparks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnsparkUnwrapsValues()
    {
        var r = new Recorder<int>();
        new UnsparkWitness<int>(r).OnNext(Spark.CreateOnNext(Five));
        await Assert.That(r.Values[0]).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "TimeIntervalWitness{T}"/> annotates values with an interval.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeIntervalAnnotatesValues()
    {
        var r = new Recorder<TimeInterval<int>>();
        new TimeIntervalWitness<int>(r, Sequencer.Immediate).OnNext(Five);
        await Assert.That(r.Values[0].Value).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "BufferWitness{T}"/> emits non-overlapping windows.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferEmitsWindows()
    {
        var r = new Recorder<IList<int>>();
        Feed(new BufferWitness<int>(r, Two, 0), Source);
        await Assert.That(r.Values.Count).IsEqualTo(Two);
        await Assert.That(r.Values[0].SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(r.Values[1].SequenceEqual(ExpectedThreeFour)).IsTrue();
    }

    /// <summary>Verifies <see cref = "CollectListWitness{T}"/> emits a single list on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectListEmitsOnCompletion()
    {
        var r = new Recorder<IList<int>>();
        var sink = new CollectListWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0].SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "CollectArrayWitness{T}"/> emits a single array on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectArrayEmitsOnCompletion()
    {
        var r = new Recorder<int[]>();
        var sink = new CollectArrayWitness<int>(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0].SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "AnyWitness{T}"/> emits true when a value arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyEmitsTrueOnValue()
    {
        var r = new Recorder<bool>();
        new AnyWitness<int>(r).OnNext(1);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "AnyPredicateWitness{T}"/> emits true on the first match.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyPredicateEmitsTrueOnMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AnyPredicateWitness<int>(r, static x => x > Two), SourceTriple);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "CountAggregator{T}"/> emits the element count on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountEmitsCount()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, CountAggregator<int>>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Three);
    }

    /// <summary>Verifies <see cref = "CountPredicateAggregator{T}"/> counts only matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountPredicateCountsMatches()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, CountPredicateAggregator<int>>(r, new CountPredicateAggregator<int>(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies <see cref = "LongCountAggregator{T}"/> emits the element count on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LongCountEmitsCount()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, LongCountAggregator<int>>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Three);
    }

    /// <summary>Verifies <see cref = "LongCountPredicateAggregator{T}"/> counts only matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LongCountPredicateCountsMatches()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, LongCountPredicateAggregator<int>>(r, new LongCountPredicateAggregator<int>(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Two);
    }

    /// <summary>Verifies <see cref = "DistinctByWitness{T, TKey}"/> forwards the first value per key.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByForwardsFirstPerKey()
    {
        var r = new Recorder<int>();
        Feed(new DistinctByWitness<int, int>(r, static x => x % Two, null), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>Verifies <see cref = "DistinctByCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByCountCountsKeys()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, DistinctByCountAggregator<int, int>>(r, new DistinctByCountAggregator<int, int>(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies <see cref = "DistinctByLongCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByLongCountCountsKeys()
    {
        var r = new Recorder<long>();
        var sink = new AggregateWitness<int, long, DistinctByLongCountAggregator<int, int>>(r, new DistinctByLongCountAggregator<int, int>(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Two);
    }

    /// <summary>Verifies a user-defined <see cref = "IAggregator{T, TResult, TSelf}"/> drives <see cref = "AggregateWitness{T, TResult, TAggregator}"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CustomAggregatorIsDrivenByAggregateWitness()
    {
        var r = new Recorder<int>();
        var sink = new AggregateWitness<int, int, SumAggregator>(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Six);
    }

    /// <summary>Verifies <see cref = "UniqueByWitness{T, TKey}"/> suppresses adjacent duplicates by key.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UniqueBySuppressesAdjacentByKey()
    {
        var r = new Recorder<int>();
        Feed(new UniqueByWitness<int, int>(r, static x => x % Two, EqualityComparer<int>.Default), Adjacent);
        await Assert.That(r.Values.SequenceEqual(ExpectedUnique)).IsTrue();
    }

    /// <summary>Verifies <see cref = "SkipWhileWitness{T}"/> drops the leading matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SkipWhileDropsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new SkipWhileWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedThreeFour)).IsTrue();
    }

    /// <summary>Verifies <see cref = "TakeWhileWitness{T}"/> forwards leading matches then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeWhileForwardsLeadingMatches()
    {
        var r = new Recorder<int>();
        Feed(new TakeWhileWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>Verifies <see cref = "AppendWitness{T}"/> appends a value after completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AppendAddsValueOnCompletion()
    {
        var r = new Recorder<int>();
        var sink = new AppendWitness<int>(r, Ten);
        Feed(sink, ExpectedOneTwo);
        sink.OnCompleted();
        await Assert.That(r.Values[^1]).IsEqualTo(Ten);
    }

    /// <summary>Verifies <see cref = "DefaultIfEmptyWitness{T}"/> emits the default when the source is empty.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultIfEmptyEmitsDefaultWhenEmpty()
    {
        var r = new Recorder<int>();
        new DefaultIfEmptyWitness<int>(r, Ten).OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Ten);
    }

    /// <summary>Verifies <see cref = "AllPredicateWitness{T}"/> emits false on the first non-match.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AllPredicateEmitsFalseOnNonMatch()
    {
        var r = new Recorder<bool>();
        Feed(new AllPredicateWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values[0]).IsFalse();
    }

    /// <summary>Verifies <see cref = "ContainsWitness{T}"/> emits true when the value is found.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ContainsEmitsTrueWhenFound()
    {
        var r = new Recorder<bool>();
        Feed(new ContainsWitness<int>(r, Two, EqualityComparer<int>.Default), SourceTriple);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "TakeWitness{T}"/> forwards the requested count then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeForwardsRequestedCount()
    {
        var r = new Recorder<int>();
        Feed(new TakeWitness<int>(r, Two), SourceTriple);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>Pushes each value through the sink via <see cref = "IObserver{T}.OnNext"/>.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
    /// <param name = "sink">The sink under test.</param>
    /// <param name = "values">The values to push.</param>
    private static void Feed<T>(IObserver<T> sink, IEnumerable<T> values)
    {
        foreach (var value in values)
        {
            sink.OnNext(value);
        }
    }

    /// <summary>A user-defined accumulator that sums observed values, exercising the public <see cref = "IAggregator{T, TResult, TSelf}"/> contract.</summary>
    private readonly record struct SumAggregator : IAggregator<int, int, SumAggregator>
    {
        /// <summary>Initializes a new instance of the <see cref = "SumAggregator"/> struct.</summary>
        /// <param name = "result">The current accumulated sum.</param>
        private SumAggregator(int result) => Result = result;

        /// <inheritdoc/>
        public int Result { get; }

        /// <inheritdoc/>
        public SumAggregator Add(int value) => new(Result + value);
    }

    /// <summary>Observer that records the notifications it receives.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
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
