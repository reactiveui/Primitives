// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;
using System.Threading;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Operator benchmarks for mapping, filtering, and aggregate predicates.
/// </summary>
[MemoryDiagnoser]
public class OperatorMapKeepBenchmarks
{
    private const int StartValue = 0;
    private const int RangeCount = 32;

    /// <summary>
    /// Baseline map/where chain using primitives.
    /// </summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesRangeMapKeep()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(StartValue, RangeCount)
            .Map(static x => x + 1)
            .Keep(static x => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Map/where chain using System.Reactive.
    /// </summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark]
    public int SystemReactiveRangeSelectWhere()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Where(
                RxObservable.Select(RxObservable.Range(StartValue, RangeCount), static x => x + 1),
                static x => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Map/where chain using R3.
    /// </summary>
    /// <returns>The aggregate total.</returns>
    [Benchmark]
    public int R3RangeSelectWhere()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Where(
                R3.ObservableExtensions.Select(
                    R3.Observable.Range(StartValue, RangeCount),
                    static (int x) => x + 1),
                static (int x) => (x & 1) == 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Baseline aggregate/count with predicate sequence.
    /// </summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public int PrimitivesAggregateAnyCount()
    {
        var count = new IntSignalObserver();
        var any = new BooleanSignalObserver();
        using var countSubscription = Signal.Range(StartValue, RangeCount)
            .DistinctBy(static x => x / 2)
            .Count()
            .Subscribe(count);
        using var anySubscription = Signal.Range(StartValue, RangeCount)
            .Any(static x => x == 31)
            .Subscribe(any);
        return any.Value ? count.Total : -count.Total;
    }

    /// <summary>
    /// Aggregate/count with predicate sequence using System.Reactive.
    /// </summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public int SystemReactiveAggregateAnyCount()
    {
        var count = new IntSignalObserver();
        var any = new BooleanSignalObserver();
        using var countSubscription = RxObservable.Count(
                RxObservable.Distinct(
                    RxObservable.Select(RxObservable.Range(StartValue, RangeCount), static x => x / 2)))
            .Subscribe(count);
        using var anySubscription = RxObservable.Any(RxObservable.Range(StartValue, RangeCount), static x => x == 31)
            .Subscribe(any);
        return any.Value ? count.Total : -count.Total;
    }

    /// <summary>
    /// Aggregate/count with predicate sequence using R3.
    /// </summary>
    /// <returns>Negated count if predicate matched; otherwise positive count.</returns>
    [Benchmark]
    public async Task<int> R3AggregateAnyCount()
    {
        var count = await R3.ObservableExtensions.CountAsync(
                R3.ObservableExtensions.Distinct(
                    R3.ObservableExtensions.Select(
                        R3.Observable.Range(StartValue, RangeCount),
                        static (int x) => x / 2)),
                CancellationToken.None)
            .ConfigureAwait(false);

        var any = await R3.ObservableExtensions.AnyAsync(
                R3.Observable.Range(StartValue, RangeCount),
                static (int x) => x == 31,
                CancellationToken.None)
            .ConfigureAwait(false);

        return any ? count : -count;
    }
}
