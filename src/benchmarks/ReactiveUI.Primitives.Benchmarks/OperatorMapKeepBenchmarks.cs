// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Operator benchmarks for mapping, filtering, and aggregate predicates.</summary>
[MemoryDiagnoser]
public class OperatorMapKeepBenchmarks
{
    /// <summary>The starting value of each benchmarked sequence.</summary>
    private const int StartValue = 0;

    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int RangeCount = 32;

    /// <summary>The divisor used by the key-selector benchmarks.</summary>
    private const int KeyDivisor = 2;

    /// <summary>The value matched by the any-predicate benchmarks.</summary>
    private const int MatchValue = 31;

    /// <summary>Baseline map/where chain using primitives.</summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesRangeMapKeep()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Map(static x => x + 1)
            .Keep(static x => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Map/where chain using System.Reactive.</summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark]
    public int SystemReactiveRangeSelectWhere()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .Select(static x => x + 1)
            .Where(static x => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Map/where chain using R3.</summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark]
    public int R3RangeSelectWhere()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Where(
                R3.ObservableExtensions.Select(
                    R3.Observable.Range(StartValue, RangeCount),
                    static x => x + 1),
                static x => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Baseline aggregate/count with predicate sequence.</summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public int PrimitivesAggregateAnyCount()
    {
        IntSignalWitness count = new();
        BooleanSignalWitness any = new();
        using var countSubscription = Signal.Sequence(StartValue, RangeCount)
            .DistinctBy(static x => x / KeyDivisor)
            .Count()
            .Subscribe(count);
        using var anySubscription = Signal.Sequence(StartValue, RangeCount)
            .Any(static x => x == MatchValue)
            .Subscribe(any);
        return any.Value ? count.Total : -count.Total;
    }

    /// <summary>Aggregate/count with predicate sequence using System.Reactive.</summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public int SystemReactiveAggregateAnyCount()
    {
        IntSignalWitness count = new();
        BooleanSignalWitness any = new();
        using var countSubscription = RxObservable.Range(StartValue, RangeCount)
            .Select(static x => x / KeyDivisor)
            .Distinct()
            .Count()
            .Subscribe(count);
        using var anySubscription = RxObservable.Any(RxObservable.Range(StartValue, RangeCount), static x => x == MatchValue)
            .Subscribe(any);
        return any.Value ? count.Total : -count.Total;
    }

    /// <summary>Aggregate/count with predicate sequence using R3.</summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public async Task<int> R3AggregateAnyCount()
    {
        var count = await R3.ObservableExtensions.CountAsync(
                R3.ObservableExtensions.Distinct(
                    R3.ObservableExtensions.Select(
                        R3.Observable.Range(StartValue, RangeCount),
                        static x => x / KeyDivisor)),
                CancellationToken.None)
            .ConfigureAwait(false);

        var any = await R3.ObservableExtensions.AnyAsync(
                R3.Observable.Range(StartValue, RangeCount),
                static x => x == MatchValue,
                CancellationToken.None)
            .ConfigureAwait(false);

        return any ? count : -count;
    }
}
