// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for single value signal construction and observation.
/// </summary>
[MemoryDiagnoser]
public class ScalarSignalBenchmarks
{
    /// <summary>
    /// The single value emitted by each scalar signal under test.
    /// </summary>
    private const int ScalarValue = 42;

    /// <summary>
    /// Baseline single-value sequence with ReactiveUI.Primitives.
    /// </summary>
    /// <returns>The observed value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesReturnSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Emit(ScalarValue).Subscribe(observer);
        return observer.LastValue;
    }

    /// <summary>
    /// Single-value sequence using System.Reactive.
    /// </summary>
    /// <returns>The observed value.</returns>
    [Benchmark]
    public int SystemReactiveReturnSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Return(ScalarValue).Subscribe(observer);
        return observer.LastValue;
    }

    /// <summary>
    /// Single-value sequence using R3.
    /// </summary>
    /// <returns>The observed value.</returns>
    [Benchmark]
    public int R3ReturnSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Return(ScalarValue).Subscribe(observer);
        return observer.Total;
    }
}
