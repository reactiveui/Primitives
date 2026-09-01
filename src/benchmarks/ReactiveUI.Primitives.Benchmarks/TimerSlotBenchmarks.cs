// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the timer slot every repeating operator arms once per window. The burst cases elsewhere schedule
/// a single timer for the whole run and so never show what re-arming costs; these advance the clock between
/// values, which closes each window and forces a fresh arm, which is the rate that matters for a debounce over
/// a slow-moving source or for a periodic tick.
/// </summary>
[MemoryDiagnoser]
public class TimerSlotBenchmarks
{
    /// <summary>The number of timer windows each case arms.</summary>
    private const int Count = 1000;

    /// <summary>The quiet period and tick interval used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(1);

    /// <summary>Benchmarks one debounce window per value, so every value arms its own timer.</summary>
    /// <returns>The last observed value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesThrottleRearmPerValue()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Calm(Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.LastValue;
    }

    /// <summary>Benchmarks one debounce window per value using System.Reactive.</summary>
    /// <returns>The last observed value.</returns>
    [Benchmark]
    public int SystemReactiveThrottleRearmPerValue()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.Throttle(Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.LastValue;
    }

    /// <summary>Benchmarks a periodic tick, which arms its successor from inside every tick.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int PrimitivesIntervalRearmPerTick()
    {
        VirtualClock clock = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = Signal.Every(Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            clock.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks a periodic tick using System.Reactive.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int SystemReactiveIntervalRearmPerTick()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = Observable.Interval(Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }
}
