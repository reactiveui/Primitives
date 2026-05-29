// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Subscribe-and-drain benchmarks for the stateful single-source operators that were converted
/// from per-subscription closures to dedicated SingleSourceObserver sinks (Skip, Unique, Fold,
/// Reduce, TakeWhile, SkipWhile, UniqueBy). The Allocated column reflects the closure-to-sink
/// reduction; System.Reactive is the comparison baseline.
/// </summary>
[MemoryDiagnoser]
public class OperatorStatefulFilterBenchmarks
{
    private const int StartValue = 0;
    private const int RangeCount = 32;

    /// <summary>Primitives Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSkip()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Skip(8)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSkip()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .Skip(8)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives Unique (adjacent distinct).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUnique()
    {
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .TakeWhile(static x => x < 24)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive TakeWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTakeWhile()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .TakeWhile(static x => x < 24)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSkipWhile()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .SkipWhile(static x => x < 8)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSkipWhile()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .SkipWhile(static x => x < 8)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Primitives UniqueBy (adjacent distinct by key).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUniqueBy()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .UniqueBy(static x => x / 2)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>System.Reactive DistinctUntilChanged with key selector.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDistinctUntilChangedKey()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(StartValue, RangeCount)
            .DistinctUntilChanged(static x => x / 2)
            .Subscribe(observer);
        return observer.Total;
    }
}
