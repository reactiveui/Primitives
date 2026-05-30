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
/// Benchmarks the cleanup/finalizer operator (OnCleanup) against System.Reactive Finally and
/// R3 Do(onDisposed).
/// </summary>
[MemoryDiagnoser]
public class OperatorCleanupBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks running a cleanup action on termination.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesOnCleanup()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).OnCleanup(static () => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks running a cleanup action on termination using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveFinally()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Finally(static () => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks running a cleanup action on termination using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DoOnDisposed()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Do(R3.Observable.Range(1, Count), onDispose: static () => { }).Subscribe(observer);
        return observer.Total;
    }
}
