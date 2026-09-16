// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using R3Subject = R3.Subject<int>;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures terminal and filtering operators composed from the public sink witnesses over a hot source.</summary>
[MemoryDiagnoser]
public class AdvancedSinkWitnessBenchmarks
{
    /// <summary>The number of values pushed through each pipeline.</summary>
    private const int Count = 1000;

    /// <summary>The number of distinct keys the key selector produces.</summary>
    private const int Keys = 16;

    /// <summary>The threshold the predicate first matches near the end of the stream.</summary>
    private const int Threshold = Count - 2;

    /// <summary>Selects the distinctness key for a value.</summary>
    private static readonly Func<int, int> KeySelector = static value => value % Keys;

    /// <summary>Matches values near the end of the stream.</summary>
    private static readonly Func<int, bool> LateMatch = static value => value > Threshold;

    /// <summary>Benchmarks an any-value sink that completes on the first value.</summary>
    /// <returns>One when the sink reported a value, plus its completion count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesAnyWitness()
    {
        BooleanSignalWitness result = new();
        using Signal<int> source = new();
        using AnyWitness<int> sink = new(result);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return (result.Value ? 1 : 0) + result.CompletionCount;
    }

    /// <summary>Benchmarks an any-value operator using System.Reactive.</summary>
    /// <returns>One when the operator reported a value, plus its completion count.</returns>
    [Benchmark]
    public int SystemReactiveAny()
    {
        BooleanSignalWitness result = new();
        using RxSubject source = new();
        using var subscription = Observable.Any(source).Subscribe(result);
        Push(source);
        return (result.Value ? 1 : 0) + result.CompletionCount;
    }

    /// <summary>Benchmarks a predicate sink that scans until a late value matches.</summary>
    /// <returns>One when the sink matched, plus its completion count.</returns>
    [Benchmark]
    public int PrimitivesAnyPredicateWitness()
    {
        BooleanSignalWitness result = new();
        using Signal<int> source = new();
        using AnyPredicateWitness<int> sink = new(result, LateMatch);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return (result.Value ? 1 : 0) + result.CompletionCount;
    }

    /// <summary>Benchmarks a predicate any operator using System.Reactive.</summary>
    /// <returns>One when the operator matched, plus its completion count.</returns>
    [Benchmark]
    public int SystemReactiveAnyPredicate()
    {
        BooleanSignalWitness result = new();
        using RxSubject source = new();
        using var subscription = Observable.Any(source, LateMatch).Subscribe(result);
        Push(source);
        return (result.Value ? 1 : 0) + result.CompletionCount;
    }

    /// <summary>Benchmarks a sink forwarding the first value per key.</summary>
    /// <returns>The sum of forwarded values.</returns>
    [Benchmark]
    public int PrimitivesDistinctByWitness()
    {
        IntSignalWitness result = new();
        using Signal<int> source = new();
        using DistinctByWitness<int, int> sink = new(result, KeySelector, null);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks a key-distinct operator using System.Reactive.</summary>
    /// <returns>The sum of forwarded values.</returns>
    [Benchmark]
    public int SystemReactiveDistinctBy()
    {
        IntSignalWitness result = new();
        using RxSubject source = new();
        using var subscription = Observable.Distinct(source, KeySelector).Subscribe(result);
        Push(source);
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks a key-distinct operator using R3.</summary>
    /// <returns>The sum of forwarded values.</returns>
    [Benchmark]
    public int R3DistinctBy()
    {
        IntR3Witness result = new();
        using R3Subject source = new();
        using var subscription = R3.ObservableExtensions.DistinctBy(source, KeySelector).Subscribe(result);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted(R3.Result.Success);
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks counting distinct keys through an aggregate sink.</summary>
    /// <returns>The distinct key count.</returns>
    [Benchmark]
    public int PrimitivesDistinctByCountAggregate()
    {
        IntSignalWitness result = new();
        using Signal<int> source = new();
        using AggregateWitness<int, int, DistinctByCountAggregator<int, int>> sink =
            new(result, new(KeySelector, EqualityComparer<int>.Default));
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return result.LastValue;
    }

    /// <summary>Benchmarks counting distinct keys using System.Reactive.</summary>
    /// <returns>The distinct key count.</returns>
    [Benchmark]
    public int SystemReactiveDistinctByCount()
    {
        IntSignalWitness result = new();
        using RxSubject source = new();
        using var subscription = Observable.Count(Observable.Distinct(source, KeySelector)).Subscribe(result);
        Push(source);
        return result.LastValue;
    }

    /// <summary>Benchmarks counting distinct keys as a 64-bit count through an aggregate sink.</summary>
    /// <returns>The distinct key count.</returns>
    [Benchmark]
    public long PrimitivesDistinctByLongCountAggregate()
    {
        long count = 0;
        using Signal<int> source = new();
        using AggregateWitness<int, long, DistinctByLongCountAggregator<int, int>> sink =
            new(Witness.Create<long>(value => count = value), new(KeySelector, null));
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return count;
    }

    /// <summary>Benchmarks counting distinct keys as a 64-bit count using System.Reactive.</summary>
    /// <returns>The distinct key count.</returns>
    [Benchmark]
    public long SystemReactiveDistinctByLongCount()
    {
        long count = 0;
        using RxSubject source = new();
        using var subscription = Observable.LongCount(Observable.Distinct(source, KeySelector))
            .Subscribe(Witness.Create<long>(value => count = value));
        Push(source);
        return count;
    }

    /// <summary>Pushes the value range and completes a Primitives source.</summary>
    /// <param name="source">The source to drive.</param>
    private static void Push(Signal<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }

    /// <summary>Pushes the value range and completes a System.Reactive source.</summary>
    /// <param name="source">The source to drive.</param>
    private static void Push(RxSubject source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }
}
