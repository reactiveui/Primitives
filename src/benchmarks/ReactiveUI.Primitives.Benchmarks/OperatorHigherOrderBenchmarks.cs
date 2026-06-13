// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for higher-order combinators.</summary>
[MemoryDiagnoser]
public class OperatorHigherOrderBenchmarks
{
    /// <summary>The inclusive start value of the primary range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The inclusive start value of the alternate range used by racing/switching benchmarks.</summary>
    private const int AltStart = 100;

    /// <summary>The inclusive start value of the right-hand range used by combining benchmarks.</summary>
    private const int RightStart = 10;

    /// <summary>The number of trailing elements retained by the fork-join-equivalent benchmarks.</summary>
    private const int TakeLastCount = 1;

    /// <summary>The number of elements produced by each range used by the benchmarks.</summary>
    private const int Count = 16;

    /// <summary>Benchmarks concatenating inner ranges.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesConcatRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Chain(Signal.Sequence(Start, Count), Signal.Sequence(Start, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks concatenating ranges using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveConcatRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Concat(RxObservable.Range(Start, Count), RxObservable.Range(Start, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks concatenating ranges using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ConcatRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.Concat(R3.Observable.Range(Start, Count), R3.Observable.Range(Start, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging inner ranges.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergeRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Blend(Signal.Sequence(Start, Count), Signal.Sequence(Start, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging ranges using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMergeRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Merge(RxObservable.Range(Start, Count), RxObservable.Range(Start, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging ranges using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3MergeRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.Merge(R3.Observable.Range(Start, Count), R3.Observable.Range(Start, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks racing two sources.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRaceRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Race(Signal.Sequence(Start, Count), Signal.Sequence(AltStart, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks racing two sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRaceRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Amb(RxObservable.Range(Start, Count), RxObservable.Range(AltStart, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks racing two sources using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3RaceRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.Race(R3.Observable.Range(Start, Count), R3.Observable.Range(AltStart, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks switching to the latest inner source.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSwitchRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable([Signal.Sequence(Start, Count), Signal.Sequence(AltStart, Count)])
            .SwitchTo()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks switching to the latest inner source using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSwitchRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Switch(
                RxObservable.ToObservable([RxObservable.Range(Start, Count), RxObservable.Range(AltStart, Count)]))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks switching to the latest inner source using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3SwitchRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Switch(
                R3.Observable.ToObservable(
                    [R3.Observable.Range(Start, Count), R3.Observable.Range(AltStart, Count)],
                    CancellationToken.None))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks combine-latest over ranges.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesCombineLatestRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.SyncLatest(
            Signal.Sequence(Start, Count),
            Signal.Sequence(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks combine-latest over ranges using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.CombineLatest(
            RxObservable.Range(Start, Count),
            RxObservable.Range(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks combine-latest over ranges using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CombineLatestRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.CombineLatest(
            R3.Observable.Range(Start, Count),
            R3.Observable.Range(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks with-latest over ranges.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesWithLatestRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(Start, Count)
            .Latch(Signal.Sequence(RightStart, Count), static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks with-latest over ranges using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveWithLatestRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.WithLatestFrom(
            RxObservable.Range(Start, Count),
            RxObservable.Range(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks with-latest over ranges using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3WithLatestRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.WithLatestFrom(
            R3.Observable.Range(Start, Count),
            R3.Observable.Range(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks fork-join over ranges.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesForkJoinRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.ForkJoin(
            Signal.Sequence(Start, Count),
            Signal.Sequence(RightStart, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks fork-join over ranges using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveForkJoinRanges()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.TakeLast(
                RxObservable.CombineLatest(
                    RxObservable.Range(Start, Count),
                    RxObservable.Range(RightStart, Count),
                    static (left, right) => left + right),
                TakeLastCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks fork-join-equivalent last combined value over ranges using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ForkJoinRanges()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.TakeLast(
                R3.Observable.CombineLatest(
                    R3.Observable.Range(Start, Count),
                    R3.Observable.Range(RightStart, Count),
                    static (left, right) => left + right),
                TakeLastCount)
            .Subscribe(observer);
        return observer.Total;
    }
}
