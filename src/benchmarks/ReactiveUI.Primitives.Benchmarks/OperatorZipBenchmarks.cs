// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for pairwise zip composition.</summary>
[MemoryDiagnoser]
public class OperatorZipBenchmarks
{
    /// <summary>The starting value of the left-hand sequence.</summary>
    private const int LeftStart = 1;

    /// <summary>The starting value of the right-hand sequence.</summary>
    private const int RightStart = 10;

    /// <summary>The number of values produced by each zipped sequence.</summary>
    private const int Count = 16;

    /// <summary>Baseline zip using ReactiveUI.Primitives.</summary>
    /// <returns>The sum of zipped values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesZip()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Pair(
                Signal.Sequence(LeftStart, Count),
                Signal.Sequence(RightStart, Count),
                static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Zip using System.Reactive.</summary>
    /// <returns>The sum of zipped values.</returns>
    [Benchmark]
    public int SystemReactiveZip()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Zip(RxObservable.Range(LeftStart, Count), RxObservable.Range(RightStart, Count), static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Zip using R3.</summary>
    /// <returns>The sum of zipped values.</returns>
    [Benchmark]
    public int R3Zip()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Zip(
                R3.Observable.Range(LeftStart, Count),
                R3.Observable.Range(RightStart, Count),
                static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }
}
