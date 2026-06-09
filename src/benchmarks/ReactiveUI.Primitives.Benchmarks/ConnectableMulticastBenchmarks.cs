// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the general multicast primitive (a connectable backed by a caller-supplied hub),
/// which underlies the Publish/Share family.
/// </summary>
[MemoryDiagnoser]
public class ConnectableMulticastBenchmarks
{
    /// <summary>The inclusive start value of the range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The number of elements produced by the range used by each benchmark.</summary>
    private const int Count = 32;

    /// <summary>Benchmarks multicasting through a caller-supplied hub.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesMulticastConnect()
    {
        var observer = new IntSignalWitness();
        var connectable = Signal.Sequence(Start, Count).Multicast(new Signal<int>());
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>Benchmarks multicasting through a caller-supplied subject using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMulticastConnect()
    {
        var observer = new IntSignalWitness();
        var connectable = RxObservable.Range(Start, Count).Multicast(new Subject<int>());
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>Benchmarks multicasting through a caller-supplied subject using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3MulticastConnect()
    {
        var observer = new IntR3Witness();
        var connectable = R3.ObservableExtensions.Multicast(R3.Observable.Range(Start, Count), new R3.Subject<int>());
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }
}
