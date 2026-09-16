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

/// <summary>Measures time-windowed buffering and sequencer hand-off, advancing virtual time.</summary>
[MemoryDiagnoser]
public class VirtualTimeBufferObserveOnBenchmarks
{
    /// <summary>The number of windows each case advances through.</summary>
    private const int Windows = 32;

    /// <summary>The number of values pushed per window.</summary>
    private const int ValuesPerWindow = 4;

    /// <summary>The buffer window and tick interval.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(10);

    /// <summary>Buffers values into time windows on a virtual clock.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesTimeWindowBuffer()
    {
        VirtualClock clock = new();
        CountingSignalWitness<IList<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = source.Buffer(Window, clock).Subscribe(observer);
        for (var w = 0; w < Windows; w++)
        {
            for (var i = 0; i < ValuesPerWindow; i++)
            {
                source.OnNext(i);
            }

            clock.AdvanceBy(Window);
        }

        source.OnNext(Windows);
        source.OnCompleted();
        return observer.Count;
    }

    /// <summary>Buffers values into time windows on a historical scheduler using System.Reactive.</summary>
    /// <returns>The number of batches observed.</returns>
    [Benchmark]
    public int SystemReactiveTimeWindowBuffer()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<IList<int>> observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Buffer(source, Window, scheduler).Subscribe(observer);
        for (var w = 0; w < Windows; w++)
        {
            for (var i = 0; i < ValuesPerWindow; i++)
            {
                source.OnNext(i);
            }

            scheduler.AdvanceBy(Window);
        }

        source.OnNext(Windows);
        source.OnCompleted();
        return observer.Count;
    }

    /// <summary>Delivers source values on a virtual clock sequencer.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesWitnessOnVirtualClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.WitnessOn(clock).Subscribe(observer);
        for (var w = 0; w < Windows; w++)
        {
            for (var i = 0; i < ValuesPerWindow; i++)
            {
                source.OnNext(i);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Delivers source values on a historical scheduler using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveObserveOnHistoricalScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.ObserveOn(source, scheduler).Subscribe(observer);
        for (var w = 0; w < Windows; w++)
        {
            for (var i = 0; i < ValuesPerWindow; i++)
            {
                source.OnNext(i);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }
}
