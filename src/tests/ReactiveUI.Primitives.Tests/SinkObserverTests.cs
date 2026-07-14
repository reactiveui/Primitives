// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
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
        Recorder<int> r = new();
        Feed(new SkipWitness<int>(r, Two), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedThreeFour)).IsTrue();
    }

    /// <summary>Verifies <see cref = "DistinctWitness{T}"/> forwards only the first occurrence of each value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctForwardsFirstOccurrenceOnly()
    {
        Recorder<int> r = new();
        Feed(new DistinctWitness<int>(r, []), Duplicates);
        await Assert.That(r.Values.SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "UniqueWitness{T}"/> suppresses adjacent duplicates only.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UniqueSuppressesAdjacentDuplicates()
    {
        Recorder<int> r = new();
        Feed(new UniqueWitness<int>(r, EqualityComparer<int>.Default), Adjacent);
        await Assert.That(r.Values.SequenceEqual(ExpectedUnique)).IsTrue();
    }

    /// <summary>Verifies <see cref = "FoldWitness{TSource, TAccumulate}"/> emits a running accumulation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FoldEmitsRunningAccumulation()
    {
        Recorder<int> r = new();
        Feed(new FoldWitness<int, int>(r, 0, static (a, b) => a + b), SourceTriple);
        await Assert.That(r.Values.SequenceEqual(ExpectedFold)).IsTrue();
    }

    /// <summary>Verifies <see cref = "ReduceWitness{TSource, TAccumulate}"/> emits the final accumulation on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReduceEmitsFinalOnCompletion()
    {
        Recorder<int> r = new();
        ReduceWitness<int, int> sink = new(r, 0, static (a, b) => a + b);
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
        Recorder<string> r = new();
        KeepNotNullWitness<string> sink = new(r);
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
        Recorder<string> r = new();
        KeepTypeWitness<string> sink = new(r);
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
        Recorder<int> r = new();
        List<int> tapped = [];
        Feed(
            new TapWitness<int>(
                r,
                tapped.Add,
                static _ => { },
                static () => { }),
            SourceTriple);
        await Assert.That(tapped.SequenceEqual(SourceTriple)).IsTrue();
        await Assert.That(r.Values.SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "IgnoreValuesWitness{T}"/> drops values but forwards completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IgnoreValuesDropsValues()
    {
        Recorder<int> r = new();
        IgnoreValuesWitness<int> sink = new(r);
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
        Recorder<Spark<int>> r = new();
        new SparkWitness<int>(r).OnNext(Five);
        await Assert.That(r.Values[0].HasValue).IsTrue();
        await Assert.That(r.Values[0].Value).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "UnsparkWitness{T}"/> unwraps on-next sparks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnsparkUnwrapsValues()
    {
        Recorder<int> r = new();
        new UnsparkWitness<int>(r).OnNext(Spark.CreateOnNext(Five));
        await Assert.That(r.Values[0]).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "TimeIntervalWitness{T}"/> annotates values with an interval.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeIntervalAnnotatesValues()
    {
        Recorder<TimeInterval<int>> r = new();
        new TimeIntervalWitness<int>(r, Sequencer.Immediate).OnNext(Five);
        await Assert.That(r.Values[0].Value).IsEqualTo(Five);
    }

    /// <summary>Verifies <see cref = "BufferWitness{T}"/> emits non-overlapping windows.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferEmitsWindows()
    {
        Recorder<IList<int>> r = new();
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
        Recorder<IList<int>> r = new();
        CollectListWitness<int> sink = new(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0].SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "CollectArrayWitness{T}"/> emits a single array on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectArrayEmitsOnCompletion()
    {
        Recorder<int[]> r = new();
        CollectArrayWitness<int> sink = new(r);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0].SequenceEqual(SourceTriple)).IsTrue();
    }

    /// <summary>Verifies <see cref = "AnyWitness{T}"/> emits true when a value arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyEmitsTrueOnValue()
    {
        Recorder<bool> r = new();
        new AnyWitness<int>(r).OnNext(1);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "AnyPredicateWitness{T}"/> emits true on the first match.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyPredicateEmitsTrueOnMatch()
    {
        Recorder<bool> r = new();
        Feed(new AnyPredicateWitness<int>(r, static x => x > Two), SourceTriple);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "CountAggregator{T}"/> emits the element count on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountEmitsCount()
    {
        Recorder<int> r = new();
        AggregateWitness<int, int, CountAggregator<int>> sink = new(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Three);
    }

    /// <summary>Verifies <see cref = "CountPredicateAggregator{T}"/> counts only matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountPredicateCountsMatches()
    {
        Recorder<int> r = new();
        AggregateWitness<int, int, CountPredicateAggregator<int>> sink = new(r, new(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies <see cref = "LongCountAggregator{T}"/> emits the element count on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LongCountEmitsCount()
    {
        Recorder<long> r = new();
        AggregateWitness<int, long, LongCountAggregator<int>> sink = new(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Three);
    }

    /// <summary>Verifies <see cref = "LongCountPredicateAggregator{T}"/> counts only matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LongCountPredicateCountsMatches()
    {
        Recorder<long> r = new();
        AggregateWitness<int, long, LongCountPredicateAggregator<int>> sink = new(r, new(static x => x > 1));
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Two);
    }

    /// <summary>Verifies <see cref = "DistinctByWitness{T, TKey}"/> forwards the first value per key.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByForwardsFirstPerKey()
    {
        Recorder<int> r = new();
        Feed(new DistinctByWitness<int, int>(r, static x => x % Two, null), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>Verifies <see cref = "DistinctByCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByCountCountsKeys()
    {
        Recorder<int> r = new();
        AggregateWitness<int, int, DistinctByCountAggregator<int, int>> sink = new(
            r,
            new(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies <see cref = "DistinctByLongCountAggregator{T, TKey}"/> counts distinct keys.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctByLongCountCountsKeys()
    {
        Recorder<long> r = new();
        AggregateWitness<int, long, DistinctByLongCountAggregator<int, int>> sink = new(
            r,
            new(static x => x % Two, null));
        Feed(sink, Source);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo((long)Two);
    }

    /// <summary>Verifies a user-defined <see cref = "IAggregator{T, TResult, TSelf}"/> drives <see cref = "AggregateWitness{T, TResult, TAggregator}"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CustomAggregatorIsDrivenByAggregateWitness()
    {
        Recorder<int> r = new();
        AggregateWitness<int, int, SumAggregator> sink = new(r, default);
        Feed(sink, SourceTriple);
        sink.OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Six);
    }

    /// <summary>Verifies <see cref = "UniqueByWitness{T, TKey}"/> suppresses adjacent duplicates by key.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UniqueBySuppressesAdjacentByKey()
    {
        Recorder<int> r = new();
        Feed(new UniqueByWitness<int, int>(r, static x => x % Two, EqualityComparer<int>.Default), Adjacent);
        await Assert.That(r.Values.SequenceEqual(ExpectedUnique)).IsTrue();
    }

    /// <summary>Verifies <see cref = "SkipWhileWitness{T}"/> drops the leading matching values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SkipWhileDropsLeadingMatches()
    {
        Recorder<int> r = new();
        Feed(new SkipWhileWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedThreeFour)).IsTrue();
    }

    /// <summary>Verifies <see cref = "TakeWhileWitness{T}"/> forwards leading matches then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeWhileForwardsLeadingMatches()
    {
        Recorder<int> r = new();
        Feed(new TakeWhileWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>Verifies <see cref = "AppendWitness{T}"/> appends a value after completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AppendAddsValueOnCompletion()
    {
        Recorder<int> r = new();
        AppendWitness<int> sink = new(r, Ten);
        Feed(sink, ExpectedOneTwo);
        sink.OnCompleted();
        await Assert.That(r.Values[^1]).IsEqualTo(Ten);
    }

    /// <summary>Verifies <see cref = "DefaultIfEmptyWitness{T}"/> emits the default when the source is empty.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultIfEmptyEmitsDefaultWhenEmpty()
    {
        Recorder<int> r = new();
        new DefaultIfEmptyWitness<int>(r, Ten).OnCompleted();
        await Assert.That(r.Values[0]).IsEqualTo(Ten);
    }

    /// <summary>Verifies <see cref = "AllPredicateWitness{T}"/> emits false on the first non-match.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AllPredicateEmitsFalseOnNonMatch()
    {
        Recorder<bool> r = new();
        Feed(new AllPredicateWitness<int>(r, static x => x < Three), Source);
        await Assert.That(r.Values[0]).IsFalse();
    }

    /// <summary>Verifies <see cref = "ContainsWitness{T}"/> emits true when the value is found.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ContainsEmitsTrueWhenFound()
    {
        Recorder<bool> r = new();
        Feed(new ContainsWitness<int>(r, Two, EqualityComparer<int>.Default), SourceTriple);
        await Assert.That(r.Values[0]).IsTrue();
    }

    /// <summary>Verifies <see cref = "TakeWitness{T}"/> forwards the requested count then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeForwardsRequestedCount()
    {
        Recorder<int> r = new();
        Feed(new TakeWitness<int>(r, Two), SourceTriple);
        await Assert.That(r.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(r.Completed).IsTrue();
    }

    /// <summary>
    /// A filtering sink that hands a value to a throwing observer must unsubscribe upstream before it rethrows.
    /// Without that teardown the source keeps producing into an observer that has already failed.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FilteringSinksReleaseTheUpstreamWhenTheObserverThrows()
    {
        RecordingDisposable skip = new();
        SkipWitness<int> skipSink = new(new ThrowingWitness<int>(throwOnNext: true), 0);
        skipSink.SetSubscription(skip);
        await AssertObserverFailureReleasesUpstream(skipSink, skip);

        RecordingDisposable skipWhile = new();
        SkipWhileWitness<int> skipWhileSink = new(new ThrowingWitness<int>(throwOnNext: true), static _ => false);
        skipWhileSink.SetSubscription(skipWhile);
        await AssertObserverFailureReleasesUpstream(skipWhileSink, skipWhile);

        RecordingDisposable distinct = new();
        DistinctWitness<int> distinctSink = new(new ThrowingWitness<int>(throwOnNext: true), []);
        distinctSink.SetSubscription(distinct);
        await AssertObserverFailureReleasesUpstream(distinctSink, distinct);

        RecordingDisposable unique = new();
        UniqueWitness<int> uniqueSink =
            new(new ThrowingWitness<int>(throwOnNext: true), EqualityComparer<int>.Default);
        uniqueSink.SetSubscription(unique);
        await AssertObserverFailureReleasesUpstream(uniqueSink, unique);

        RecordingDisposable uniqueBy = new();
        UniqueByWitness<int, int> uniqueBySink = new(
            new ThrowingWitness<int>(throwOnNext: true),
            static value => value,
            EqualityComparer<int>.Default);
        uniqueBySink.SetSubscription(uniqueBy);
        await AssertObserverFailureReleasesUpstream(uniqueBySink, uniqueBy);
    }

    /// <summary>
    /// The accumulating and counting sinks make the same promise: a throwing observer tears the sink down and
    /// unsubscribes upstream rather than leaving a half-live pipeline behind.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AccumulatingAndCountingSinksReleaseTheUpstreamWhenTheObserverThrows()
    {
        RecordingDisposable fold = new();
        FoldWitness<int, int> foldSink =
            new(new ThrowingWitness<int>(throwOnNext: true), 0, static (a, b) => a + b);
        foldSink.SetSubscription(fold);
        await AssertObserverFailureReleasesUpstream(foldSink, fold);

        RecordingDisposable take = new();
        TakeWitness<int> takeSink = new(new ThrowingWitness<int>(throwOnNext: true), Two);
        takeSink.SetSubscription(take);
        await AssertObserverFailureReleasesUpstream(takeSink, take);

        RecordingDisposable takeWhile = new();
        TakeWhileWitness<int> takeWhileSink = new(new ThrowingWitness<int>(throwOnNext: true), static _ => true);
        takeWhileSink.SetSubscription(takeWhile);
        await AssertObserverFailureReleasesUpstream(takeWhileSink, takeWhile);
    }

    /// <summary>
    /// A <see cref = "BufferWitness{T}"/> hands a window to the observer only when the window fills, so its
    /// teardown-on-throw path is reached on the value that closes the window, not on the ones that fill it.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferSinkReleasesTheUpstreamWhenTheObserverThrowsOnACompletedWindow()
    {
        RecordingDisposable subscription = new();
        BufferWitness<int> sink = new(new ThrowingWitness<IList<int>>(throwOnNext: true), Two, 0);
        sink.SetSubscription(subscription);

        // The first value only fills the window; nothing is emitted, so nothing can throw yet.
        sink.OnNext(1);
        await Assert.That(subscription.DisposeCount).IsEqualTo(0);

        var thrown = Assert.Throws<InvalidOperationException>(() => sink.OnNext(Two));

        await Assert.That(thrown!.Message).IsEqualTo("observer-next");
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>
    /// Every single-source sink forwards a fault to its observer exactly once and releases the upstream
    /// subscription as it goes; none of them completes the observer as well.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FilteringSinksForwardFaultsAndReleaseTheUpstream()
    {
        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                SkipWitness<int> sink = new(observer, Two);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                SkipWhileWitness<int> sink = new(observer, static _ => true);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                DistinctWitness<int> sink = new(observer, []);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                UniqueWitness<int> sink = new(observer, EqualityComparer<int>.Default);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                UniqueByWitness<int, int> sink =
                    new(observer, static value => value, EqualityComparer<int>.Default);
                sink.SetSubscription(subscription);
                return sink;
            });
    }

    /// <summary>The accumulating and counting sinks forward a fault and release the upstream subscription too.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AccumulatingAndCountingSinksForwardFaultsAndReleaseTheUpstream()
    {
        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                FoldWitness<int, int> sink = new(observer, 0, static (a, b) => a + b);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                ReduceWitness<int, int> sink = new(observer, 0, static (a, b) => a + b);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                TakeWhileWitness<int> sink = new(observer, static _ => true);
                sink.SetSubscription(subscription);
                return sink;
            });

        await AssertFaultIsForwardedAndUpstreamReleased(
            static (observer, subscription) =>
            {
                TakeWitness<int> sink = new(observer, Two);
                sink.SetSubscription(subscription);
                return sink;
            });
    }

    /// <summary>
    /// The take sinks complete early, so a source that has not noticed yet can still push a fault at them. That
    /// fault must be dropped: the observer has already been completed and must not then be told it failed.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeSinksDropFaultsThatArriveAfterTheirTerminal()
    {
        Recorder<int> takeObserver = new();
        TakeWitness<int> take = new(takeObserver, 1);
        take.OnNext(1);
        take.OnError(new InvalidOperationException("late"));

        await Assert.That(takeObserver.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(takeObserver.Completed).IsTrue();
        await Assert.That(takeObserver.Error).IsNull();

        Recorder<int> takeWhileObserver = new();
        TakeWhileWitness<int> takeWhile = new(takeWhileObserver, static value => value < Two);
        takeWhile.OnNext(1);
        takeWhile.OnNext(Two);
        takeWhile.OnError(new InvalidOperationException("late"));

        await Assert.That(takeWhileObserver.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(takeWhileObserver.Completed).IsTrue();
        await Assert.That(takeWhileObserver.Error).IsNull();
    }

    /// <summary>Asserts a sink rethrows the observer's failure and releases its upstream subscription.</summary>
    /// <param name = "sink">The sink under test, already holding <paramref name = "subscription"/>.</param>
    /// <param name = "subscription">The upstream subscription the sink must release.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertObserverFailureReleasesUpstream(
        IObserver<int> sink,
        RecordingDisposable subscription)
    {
        var thrown = Assert.Throws<InvalidOperationException>(() => sink.OnNext(1));

        await Assert.That(thrown!.Message).IsEqualTo("observer-next");
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Asserts a sink forwards a fault to its observer once and releases its upstream subscription.</summary>
    /// <param name = "create">Builds the sink over the supplied observer and upstream subscription.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertFaultIsForwardedAndUpstreamReleased(
        Func<IObserver<int>, RecordingDisposable, IObserver<int>> create)
    {
        Recorder<int> observer = new();
        RecordingDisposable subscription = new();
        var sink = create(observer, subscription);
        InvalidOperationException error = new("sink-fault");

        sink.OnError(error);

        await Assert.That(observer.Error).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsFalse();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
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
