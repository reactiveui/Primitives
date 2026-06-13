// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Subscribe-and-drain benchmarks for the stateful single-source operators that were converted
/// from per-subscription closures to dedicated SingleSourceWitness sinks (Skip, Unique, Fold,
/// Reduce, TakeWhile, SkipWhile, UniqueBy). The Allocated column reflects the closure-to-sink
/// reduction; System.Reactive is the comparison baseline.
/// </summary>
[MemoryDiagnoser]
public class OperatorStatefulFilterBenchmarks
{
    /// <summary>The starting value of each benchmarked sequence.</summary>
    private const int StartValue = 0;

    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int RangeCount = 32;

    /// <summary>The number of leading values skipped or compared by the benchmarks.</summary>
    private const int SkipCount = 8;

    /// <summary>The exclusive upper bound used by the take/skip-while benchmarks.</summary>
    private const int TakeWhileLimit = 24;

    /// <summary>The divisor used by the key-selector benchmarks.</summary>
    private const int KeyDivisor = 2;

    /// <summary>Primitives Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSkip()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Skip(SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSkip()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .Skip(SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives Unique (adjacent distinct).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUnique()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Unique()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive DistinctUntilChanged.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDistinctUntilChanged()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .DistinctUntilChanged()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives Fold (running accumulation).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesFold()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Fold(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive Scan.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveScan()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .Scan(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives Reduce (final accumulation).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesReduce()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Reduce(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive Aggregate.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveAggregate()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .Aggregate(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives TakeWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesTakeWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .TakeWhile(static x => x < TakeWhileLimit)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive TakeWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTakeWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .TakeWhile(static x => x < TakeWhileLimit)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSkipWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .SkipWhile(static x => x < SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSkipWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .SkipWhile(static x => x < SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives UniqueBy (adjacent distinct by key).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUniqueBy()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .UniqueBy(static x => x / KeyDivisor)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive DistinctUntilChanged with key selector.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDistinctUntilChangedKey()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .DistinctUntilChanged(static x => x / KeyDivisor)
            .Subscribe(observer);
        return observer.Total;
    }
}
