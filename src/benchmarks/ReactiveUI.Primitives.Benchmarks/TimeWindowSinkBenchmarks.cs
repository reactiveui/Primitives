// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures sinks that batch, debounce, time-stamp or reschedule values as virtual time advances.</summary>
[MemoryDiagnoser]
public class TimeWindowSinkBenchmarks
{
    /// <summary>The number of values pushed through each case.</summary>
    private const int Count = 256;

    /// <summary>The number of values pushed in a burst before a quiet period.</summary>
    private const int Burst = 4;

    /// <summary>The mask that selects the last value of each burst.</summary>
    private const int BurstMask = Burst - 1;

    /// <summary>The virtual time advanced after each value.</summary>
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    /// <summary>The batching and quiet-period window.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(Burst * 2);

    /// <summary>Benchmarks collecting values into time windows using the System.Reactive operator name.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesBufferOnClock()
    {
        VirtualClock clock = new();
        using Signal<int> source = new();
        return CountOnClock(source.Buffer(Window, clock), source, clock);
    }

    /// <summary>Benchmarks collecting values into time windows using System.Reactive.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int SystemReactiveBufferOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        using RxSubject source = new();
        return CountOnScheduler(RxObservable.Buffer(source, Window, scheduler), source, scheduler);
    }

    /// <summary>Benchmarks collecting values into time windows using the Primitives operator name.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int PrimitivesCollectOnClock()
    {
        VirtualClock clock = new();
        using Signal<int> source = new();
        return CountOnClock(source.Collect(Window, clock), source, clock);
    }

    /// <summary>Benchmarks emitting the latest value once a burst goes quiet.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int PrimitivesEmitIfQuietOnClock()
    {
        VirtualClock clock = new();
        using Signal<int> source = new();
        return CountOnClock(source.EmitIfQuiet(Window, clock), source, clock);
    }

    /// <summary>Benchmarks emitting the latest value once a burst goes quiet using System.Reactive.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int SystemReactiveThrottleOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        using RxSubject source = new();
        return CountOnScheduler(RxObservable.Throttle(source, Window, scheduler), source, scheduler);
    }

    /// <summary>Benchmarks annotating each value with the interval since the previous one.</summary>
    /// <returns>The number of annotated values observed.</returns>
    [Benchmark]
    public int PrimitivesTimeIntervalOnClock()
    {
        VirtualClock clock = new();
        using Signal<int> source = new();
        return CountOnClock(source.TimeInterval(clock), source, clock);
    }

    /// <summary>Benchmarks annotating each value with its interval using System.Reactive.</summary>
    /// <returns>The number of annotated values observed.</returns>
    [Benchmark]
    public int SystemReactiveTimeIntervalOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        using RxSubject source = new();
        return CountOnScheduler(RxObservable.TimeInterval(source, scheduler), source, scheduler);
    }

    /// <summary>Benchmarks delivering each value through a sequencer.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int PrimitivesObserveOnClock()
    {
        VirtualClock clock = new();
        using Signal<int> source = new();
        return CountOnClock(source.ObserveOn(clock), source, clock);
    }

    /// <summary>Benchmarks delivering each value through a scheduler using System.Reactive.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int SystemReactiveObserveOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        using RxSubject source = new();
        return CountOnScheduler(RxObservable.ObserveOn(source, scheduler), source, scheduler);
    }

    /// <summary>Subscribes to a Primitives pipeline, drives its source through virtual time and counts the output.</summary>
    /// <typeparam name="T">The pipeline output type.</typeparam>
    /// <param name="pipeline">The pipeline under measurement.</param>
    /// <param name="source">The source feeding the pipeline.</param>
    /// <param name="clock">The virtual clock the pipeline schedules on.</param>
    /// <returns>The number of values the pipeline emitted.</returns>
    private static int CountOnClock<T>(IObservable<T> pipeline, Signal<int> source, VirtualClock clock)
    {
        CountingSignalWitness<T> observer = new();
        using var subscription = pipeline.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            clock.AdvanceBy((i & BurstMask) == BurstMask ? Window : Tick);
        }

        source.OnCompleted();
        clock.AdvanceBy(Window);
        return observer.Count;
    }

    /// <summary>Subscribes to a System.Reactive pipeline, drives its source through virtual time and counts the output.</summary>
    /// <typeparam name="T">The pipeline output type.</typeparam>
    /// <param name="pipeline">The pipeline under measurement.</param>
    /// <param name="source">The source feeding the pipeline.</param>
    /// <param name="scheduler">The historical scheduler the pipeline schedules on.</param>
    /// <returns>The number of values the pipeline emitted.</returns>
    private static int CountOnScheduler<T>(IObservable<T> pipeline, RxSubject source, HistoricalScheduler scheduler)
    {
        CountingSignalWitness<T> observer = new();
        using var subscription = pipeline.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy((i & BurstMask) == BurstMask ? Window : Tick);
        }

        source.OnCompleted();
        scheduler.AdvanceBy(Window);
        return observer.Count;
    }
}
