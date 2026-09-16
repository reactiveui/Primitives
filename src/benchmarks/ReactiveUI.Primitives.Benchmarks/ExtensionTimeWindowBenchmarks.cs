// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the throttle and debounce extension operators by pushing values across advancing virtual time windows.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ExtensionTimeWindowBenchmarks
{
    /// <summary>The number of time windows each case drives.</summary>
    private const int Count = 1000;

    /// <summary>The number of consecutive windows that repeat a value in the distinct cases.</summary>
    private const int RepeatsPerValue = 2;

    /// <summary>The throttle or debounce window used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(1);

    /// <summary>Benchmarks ThrottleFirst with two values per window, keeping the leading one.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ThrottleFirst")]
    public int PrimitivesThrottleFirst()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ThrottleFirst(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            source.OnNext(-i);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleFirst with two values per window using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("ThrottleFirst")]
    public int PackageThrottleFirst()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ThrottleFirst(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            source.OnNext(-i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleDistinct over a sequence that repeats each value across two windows.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ThrottleDistinct")]
    public int PrimitivesThrottleDistinct()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ThrottleDistinct(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i / RepeatsPerValue);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleDistinct over repeating values using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("ThrottleDistinct")]
    public int PackageThrottleDistinct()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ThrottleDistinct(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i / RepeatsPerValue);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleOnScheduler with a burst per window that collapses to its latest value.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ThrottleOnScheduler")]
    public int PrimitivesThrottleOnScheduler()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ThrottleOnScheduler(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(-i);
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleOnScheduler with a burst per window using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("ThrottleOnScheduler")]
    public int PackageThrottleOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ThrottleOnScheduler(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(-i);
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleUntilTrue when every value satisfies the predicate and passes straight through.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ThrottleUntilTrue")]
    public int PrimitivesThrottleUntilTrue()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ThrottleUntilTrue(source, Window, static value => value >= 0)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ThrottleUntilTrue pass-through using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("ThrottleUntilTrue")]
    public int PackageThrottleUntilTrue()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ThrottleUntilTrue(source, Window, static value => value >= 0)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks DebounceImmediate, which emits the first value inline and debounces the rest.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DebounceImmediate")]
    public int PrimitivesDebounceImmediate()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.DebounceImmediate(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(-i);
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks DebounceImmediate using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("DebounceImmediate")]
    public int PackageDebounceImmediate()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.DebounceImmediate(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(-i);
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks DebounceUntil where even values pass inline and odd values wait out the window.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DebounceUntil")]
    public int PrimitivesDebounceUntil()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.DebounceUntil(
                source,
                Window,
                static value => (value & 1) == 0,
                clock)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks DebounceUntil with alternating inline and debounced values using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("DebounceUntil")]
    public int PackageDebounceUntil()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.DebounceUntil(
                source,
                Window,
                static value => (value & 1) == 0,
                scheduler)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }
}
