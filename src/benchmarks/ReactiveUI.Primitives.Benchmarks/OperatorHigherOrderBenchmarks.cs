// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Threading;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for higher-order combinators.
/// </summary>
[MemoryDiagnoser]
public class OperatorHigherOrderBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks concatenating inner ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesConcatRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Concat(Signal.Range(1, Count), Signal.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks concatenating ranges using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveConcatRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Concat(RxObservable.Range(1, Count), RxObservable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks concatenating ranges using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ConcatRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Concat(R3.Observable.Range(1, Count), R3.Observable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks merging inner ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergeRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Merge(Signal.Range(1, Count), Signal.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks merging ranges using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMergeRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Merge(RxObservable.Range(1, Count), RxObservable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks merging ranges using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3MergeRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Merge(R3.Observable.Range(1, Count), R3.Observable.Range(1, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks racing two sources.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRaceRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Race(Signal.Range(1, Count), Signal.Range(100, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks racing two sources using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRaceRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Amb(RxObservable.Range(1, Count), RxObservable.Range(100, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks racing two sources using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3RaceRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Race(R3.Observable.Range(1, Count), R3.Observable.Range(100, Count))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks switching to the latest inner source.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSwitchRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.FromEnumerable([Signal.Range(1, Count), Signal.Range(100, Count)])
            .Switch()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks switching to the latest inner source using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSwitchRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Switch(
                RxObservable.ToObservable(new[] { RxObservable.Range(1, Count), RxObservable.Range(100, Count) }))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks switching to the latest inner source using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3SwitchRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Switch(
                R3.Observable.ToObservable(
                    new[] { R3.Observable.Range(1, Count), R3.Observable.Range(100, Count) },
                    CancellationToken.None))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks combine-latest over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesCombineLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.CombineLatest(
            Signal.Range(1, Count),
            Signal.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks combine-latest over ranges using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.CombineLatest(
            RxObservable.Range(1, Count),
            RxObservable.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks combine-latest over ranges using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CombineLatestRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.CombineLatest(
            R3.Observable.Range(1, Count),
            R3.Observable.Range(10, Count),
            static (int left, int right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks with-latest over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesWithLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count)
            .WithLatest(Signal.Range(10, Count), static (left, right) => left + right)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks with-latest over ranges using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveWithLatestRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.WithLatestFrom(
            RxObservable.Range(1, Count),
            RxObservable.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks with-latest over ranges using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3WithLatestRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.WithLatestFrom(
            R3.Observable.Range(1, Count),
            R3.Observable.Range(10, Count),
            static (int left, int right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fork-join over ranges.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesForkJoinRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.ForkJoin(
            Signal.Range(1, Count),
            Signal.Range(10, Count),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fork-join over ranges using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveForkJoinRanges()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.TakeLast(
                RxObservable.CombineLatest(
                    RxObservable.Range(1, Count),
                    RxObservable.Range(10, Count),
                    static (left, right) => left + right),
                1)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fork-join-equivalent last combined value over ranges using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ForkJoinRanges()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.TakeLast(
                R3.Observable.CombineLatest(
                    R3.Observable.Range(1, Count),
                    R3.Observable.Range(10, Count),
                    static (int left, int right) => left + right),
                1)
            .Subscribe(observer);
        return observer.Total;
    }
}
