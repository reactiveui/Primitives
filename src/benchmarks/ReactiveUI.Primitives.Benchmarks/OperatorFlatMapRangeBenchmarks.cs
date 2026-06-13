// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for flattening selectors.</summary>
[MemoryDiagnoser]
public class OperatorFlatMapRangeBenchmarks
{
    /// <summary>Baseline flatten and map chain using primitives.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesFlatMapRange()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(1, 8).FlatMap(static x => Signal.Sequence(x * 10, 2))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Flatten and map chain using System.Reactive.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int SystemReactiveSelectManyRange()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.SelectMany(RxObservable.Range(1, 8), static x => RxObservable.Range(x * 10, 2))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Flatten and map chain using R3.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int R3SelectManyRange()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.SelectMany(
                R3.Observable.Range(1, 8),
                static x => R3.Observable.Range(x * 10, 2))
            .Subscribe(observer);
        return observer.Total;
    }
}
