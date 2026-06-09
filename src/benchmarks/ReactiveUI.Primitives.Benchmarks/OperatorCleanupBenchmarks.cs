// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the cleanup/finalizer operator (OnCleanup) against System.Reactive Finally and R3 Do(onDisposed).</summary>
[MemoryDiagnoser]
public class OperatorCleanupBenchmarks
{
    /// <summary>The inclusive start value of the range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The number of elements produced by the range used by each benchmark.</summary>
    private const int Count = 16;

    /// <summary>Benchmarks running a cleanup action on termination.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesOnCleanup()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(Start, Count).OnCleanup(static () => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks running a cleanup action on termination using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveFinally()
    {
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.Range(Start, Count).Finally(static () => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks running a cleanup action on termination using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DoOnDisposed()
    {
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.Do(R3.Observable.Range(Start, Count), onDispose: static () => { }).Subscribe(observer);
        return observer.Total;
    }
}
