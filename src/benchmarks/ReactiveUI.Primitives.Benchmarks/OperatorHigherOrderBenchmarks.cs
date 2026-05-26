// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for higher-order combinators.
/// </summary>
[MemoryDiagnoser]
public class OperatorHigherOrderBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks concatenating inner ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesConcatRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Concat(Signal.Range(1, Count), Signal.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks merging inner ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergeRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Merge(Signal.Range(1, Count), Signal.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks racing two sources.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRaceRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Race(Signal.Range(1, Count), Signal.Range(100, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks switching to the latest inner source.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSwitchRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.FromEnumerable([Signal.Range(1, Count), Signal.Range(100, Count)])
            .Switch()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks combine-latest over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesCombineLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.CombineLatest(
            Signal.Range(1, Count),
            Signal.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks with-latest over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesWithLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count)
            .WithLatest(Signal.Range(10, Count), static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fork-join over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesForkJoinRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.ForkJoin(
            Signal.Range(1, Count),
            Signal.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }
}
