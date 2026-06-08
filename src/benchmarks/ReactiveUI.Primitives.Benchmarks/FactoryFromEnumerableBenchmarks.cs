// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for enumerable-to-observable adapter factories.</summary>
[MemoryDiagnoser]
public class FactoryFromEnumerableBenchmarks
{
    /// <summary>The source values streamed through each enumerable adapter under test.</summary>
    private static readonly int[] Values =
    [
        0, 1, 2, 3, 4, 5, 6, 7,
        8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31,
    ];

    /// <summary>Baseline enumerable adapter using ReactiveUI.Primitives.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesFromEnumerableSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.FromEnumerable(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Enumerable adapter using System.Reactive.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int SystemReactiveToObservableSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.ToObservable(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Enumerable adapter using R3.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int R3ToObservableSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.ToObservable(Values, CancellationToken.None).Subscribe(observer);
        return observer.Total;
    }
}
