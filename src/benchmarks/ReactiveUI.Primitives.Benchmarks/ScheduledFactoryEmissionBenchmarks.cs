// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures factory signals that emit through a sequencer, advancing virtual time once per subscription.</summary>
[MemoryDiagnoser]
public class ScheduledFactoryEmissionBenchmarks
{
    /// <summary>The number of subscription cycles each case runs, and the length of emitted ranges.</summary>
    private const int Count = 16;

    /// <summary>The virtual time advanced after each subscription so scheduled work runs.</summary>
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    /// <summary>The error scheduled failures terminate with.</summary>
    private static readonly InvalidOperationException Failure = new("Scheduled failure.");

    /// <summary>The values emitted by scheduled enumerable conversions.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>Benchmarks a single scheduled value per subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesReturnOnClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Return(i, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks a single scheduled value per subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveReturnOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Return(i, scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks a scheduled completion per subscription.</summary>
    /// <returns>The number of completions observed.</returns>
    [Benchmark]
    public int PrimitivesEmptyOnClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Empty<int>(clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.CompletionCount;
    }

    /// <summary>Benchmarks a scheduled completion per subscription using System.Reactive.</summary>
    /// <returns>The number of completions observed.</returns>
    [Benchmark]
    public int SystemReactiveEmptyOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Empty<int>(scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.CompletionCount;
    }

    /// <summary>Benchmarks a scheduled error per subscription.</summary>
    /// <returns>The number of errors observed.</returns>
    [Benchmark]
    public int PrimitivesThrowOnClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Throw<int>(Failure, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.ErrorCount;
    }

    /// <summary>Benchmarks a scheduled error per subscription using System.Reactive.</summary>
    /// <returns>The number of errors observed.</returns>
    [Benchmark]
    public int SystemReactiveThrowOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Throw<int>(Failure, scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.ErrorCount;
    }

    /// <summary>Benchmarks a scheduled integer range per subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRangeOnClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Range(0, Count, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks a scheduled integer range per subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRangeOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Range(0, Count, scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks converting an enumerable to a signal that emits on a sequencer.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesEnumerableOnClock()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Values.ToObservable(clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks converting an enumerable to a scheduled observable using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveEnumerableOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.ToObservable(Values, scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks running an action on a sequencer and emitting the unit value.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark]
    public int PrimitivesStartActionOnClock()
    {
        VirtualClock clock = new();
        CountingSignalWitness<RxVoid> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Start(static () => { }, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks running an action on a scheduler using System.Reactive.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark]
    public int SystemReactiveStartActionOnScheduler()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<RxUnit> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Start(static () => { }, scheduler).Subscribe(observer);
            scheduler.AdvanceBy(Tick);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks subscribing to the shared unit-value signal.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark]
    public int PrimitivesEmitRxVoid()
    {
        CountingSignalWitness<RxVoid> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.EmitRxVoid().Subscribe(observer);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks subscribing to a unit-value return using System.Reactive.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark]
    public int SystemReactiveReturnUnit()
    {
        CountingSignalWitness<RxUnit> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Return(RxUnit.Default).Subscribe(observer);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks an endless loop bounded downstream, subscribed inside the current-thread trampoline.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesLoopBoundedDownstream()
    {
        IntSignalWitness observer = new();
        IDisposable? subscription = null;
        using var scheduled = Sequencer.CurrentThread.Schedule(() =>
            subscription = Signal.Loop(1).Map(static value => value + 1).Take(Count).Subscribe(observer));
        subscription?.Dispose();
        return observer.Total;
    }

    /// <summary>Benchmarks an endless repeat bounded downstream using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRepeatValueBoundedDownstream()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Take(RxObservable.Repeat(1), Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Creates the values emitted by scheduled enumerable conversions.</summary>
    /// <returns>The values.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }
}
