// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Reactive.Concurrency;
using ReactiveUI.Primitives.Signals;
using R3Observable = R3.Observable;
using R3Subscribe = R3.ObservableSubscribeExtensions;
using RxDisposable = System.Reactive.Disposables.Disposable;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;
using ShimLinq = ReactiveUI.Primitives.Reactive.LinqExtensions;
using ShimSignal = ReactiveUI.Primitives.Reactive.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures Primitives operators scheduled through System.Reactive schedulers: trampolined ranges, virtual time and cancelled delayed work.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class RxSchedulerInteropBenchmarks
{
    /// <summary>The number of values, ticks or scheduled items in each case.</summary>
    private const int Count = 256;

    /// <summary>The virtual-time tick interval and quiet period.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(1);

    /// <summary>A due time far enough away that cancelled work never fires.</summary>
    private static readonly TimeSpan DistantDueTime = TimeSpan.FromHours(1);

    /// <summary>Subscribes to a range that checks whether the current-thread trampoline must schedule it.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("CurrentThreadRange")]
    public int PrimitivesSequenceSubscribe()
    {
        CountingSignalWitness<int> observer = new();
        using var subscription = ShimSignal.Sequence(0, Count).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Subscribes to a range using System.Reactive.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("CurrentThreadRange")]
    public int SystemReactiveRangeSubscribe()
    {
        CountingSignalWitness<int> observer = new();
        using var subscription = RxObservable.Range(0, Count).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Subscribes to a range using R3.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("CurrentThreadRange")]
    public int R3RangeSubscribe()
    {
        var count = 0;
        using var subscription = R3Subscribe.Subscribe(R3Observable.Range(0, Count), _ => count++);
        return count;
    }

    /// <summary>Observes a range on a historical scheduler and drains the queued deliveries.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ObserveOnVirtualTime")]
    public int PrimitivesObserveOnHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<int> observer = new();
        using var subscription = ShimLinq.ObserveOn(ShimSignal.Sequence(0, Count), scheduler).Subscribe(observer);
        scheduler.Start();
        return observer.Count;
    }

    /// <summary>Observes a range on a historical scheduler using System.Reactive.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("ObserveOnVirtualTime")]
    public int SystemReactiveObserveOnHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<int> observer = new();
        using var subscription = RxObservable.ObserveOn(RxObservable.Range(0, Count), scheduler).Subscribe(observer);
        scheduler.Start();
        return observer.Count;
    }

    /// <summary>Advances a historical scheduler through one periodic tick at a time.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("PeriodicVirtualTime")]
    public int PrimitivesEveryHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = ShimSignal.Every(Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Advances a historical scheduler through one interval tick at a time using System.Reactive.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    [BenchmarkCategory("PeriodicVirtualTime")]
    public int SystemReactiveIntervalHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = RxObservable.Interval(Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Rearms a debounce window on a historical scheduler for every value.</summary>
    /// <returns>The last value forwarded.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DebounceVirtualTime")]
    public int PrimitivesCalmHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = ShimLinq.Calm(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.LastValue;
    }

    /// <summary>Rearms a throttle window on a historical scheduler for every value using System.Reactive.</summary>
    /// <returns>The last value forwarded.</returns>
    [Benchmark]
    [BenchmarkCategory("DebounceVirtualTime")]
    public int SystemReactiveThrottleHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Throttle(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.LastValue;
    }

    /// <summary>Schedules delayed work on the WebAssembly event-loop scheduler and cancels it before it is due.</summary>
    /// <returns>The number of items scheduled and cancelled.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("CancelDelayedWork")]
    public int PrimitivesWasmScheduleCancelDelayed()
    {
        var scheduler = WasmScheduler.Default;
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            scheduler.Schedule(i, DistantDueTime, static (_, _) => RxDisposable.Empty).Dispose();
            cancelled++;
        }

        return cancelled;
    }

    /// <summary>Schedules delayed work on System.Reactive's default scheduler and cancels it before it is due.</summary>
    /// <returns>The number of items scheduled and cancelled.</returns>
    [Benchmark]
    [BenchmarkCategory("CancelDelayedWork")]
    public int SystemReactiveDefaultScheduleCancelDelayed()
    {
        var scheduler = DefaultScheduler.Instance;
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            scheduler.Schedule(i, DistantDueTime, static (_, _) => RxDisposable.Empty).Dispose();
            cancelled++;
        }

        return cancelled;
    }
}
