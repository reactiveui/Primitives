// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures error recovery and observer fault isolation.</summary>
[MemoryDiagnoser]
public class FaultIsolationRecoveryBenchmarks
{
    /// <summary>The number of values in each fallback or source range.</summary>
    private const int Count = 64;

    /// <summary>The failure raised by the failing sources.</summary>
    private static readonly InvalidOperationException Failure = new("failure");

    /// <summary>Recovers from a failing source by switching to a fallback range.</summary>
    /// <returns>The total observed from the fallback.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesRecoverToFallback()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Throw<int>(Failure)
            .Recover(static _ => Signal.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Recovers from a failing source by switching to a fallback range using System.Reactive.</summary>
    /// <returns>The total observed from the fallback.</returns>
    [Benchmark]
    public int SystemReactiveCatchToFallback()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Catch<int, Exception>(
                RxObservable.Throw<int>(Failure),
                static _ => RxObservable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribes an observer through the fault-isolating wrapper and pushes a range.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesSubscribeSafe()
    {
        IntSignalWitness observer = new();
        using var subscription = LinqExtensions.SubscribeSafe(Signal.Range(1, Count), observer);
        return observer.Total;
    }

    /// <summary>Subscribes an observer through the fault-isolating wrapper and pushes a range using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveSubscribeSafe()
    {
        IntSignalWitness observer = new();
        using var subscription = System.ObservableExtensions.SubscribeSafe(RxObservable.Range(1, Count), observer);
        return observer.Total;
    }
}
