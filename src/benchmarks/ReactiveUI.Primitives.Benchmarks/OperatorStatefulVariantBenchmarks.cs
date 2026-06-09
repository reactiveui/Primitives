// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the state-passing operator variants (MapWith / KeepWith / TapWith) against the
/// closure-capturing equivalents in System.Reactive and R3. The Primitives variants pass runtime
/// state explicitly with a cached static delegate, so they allocate no per-subscription closure;
/// the comparison frameworks must capture the same runtime value in a closure.
/// </summary>
[MemoryDiagnoser]
public class OperatorStatefulVariantBenchmarks
{
    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int Count = 16;

    /// <summary>The multiplier passed as explicit state to the projection benchmarks.</summary>
    private readonly int _factor = 3;

    /// <summary>The threshold passed as explicit state to the filter benchmarks.</summary>
    private readonly int _threshold = 8;

    /// <summary>Benchmarks a stateful projection without a per-subscription closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesMapWith()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(1, Count).MapWith(_factor, static (factor, x) => x * factor).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful projection using a System.Reactive closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSelectClosure()
    {
        var factor = _factor;
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.Select(RxObservable.Range(1, Count), x => x * factor).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful projection using an R3 closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3SelectClosure()
    {
        var factor = _factor;
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), x => x * factor).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful filter without a per-subscription closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesKeepWith()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(1, Count).KeepWith(_threshold, static (threshold, x) => x > threshold).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful filter using a System.Reactive closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveWhereClosure()
    {
        var threshold = _threshold;
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.Where(RxObservable.Range(1, Count), x => x > threshold).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful filter using an R3 closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3WhereClosure()
    {
        var threshold = _threshold;
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.Where(R3.Observable.Range(1, Count), x => x > threshold).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful side-effect without a per-subscription closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesTapWith()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(1, Count).TapWith(_factor, static (factor, x) => _ = x * factor).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful side-effect using a System.Reactive closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDoClosure()
    {
        var factor = _factor;
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.Do(RxObservable.Range(1, Count), x => _ = x * factor).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a stateful side-effect using an R3 closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DoClosure()
    {
        var factor = _factor;
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.Do(R3.Observable.Range(1, Count), onNext: x => _ = x * factor).Subscribe(observer);
        return observer.Total;
    }
}
