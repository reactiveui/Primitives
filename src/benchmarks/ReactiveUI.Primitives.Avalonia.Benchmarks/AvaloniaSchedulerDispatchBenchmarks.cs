// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using Avalonia.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using PrimitivesAvaloniaScheduler = ReactiveUI.Primitives.Reactive.Concurrency.AvaloniaScheduler;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures System.Reactive scheduling onto an Avalonia dispatcher, drained with RunJobs.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class AvaloniaSchedulerDispatchBenchmarks
{
    /// <summary>The number of actions or values dispatched per case.</summary>
    private const int Count = 1000;

    /// <summary>The category for bursts of scheduled actions.</summary>
    private const string ScheduleBurst = "ScheduleBurst";

    /// <summary>The category for values observed on the dispatcher.</summary>
    private const string ObserveOnBurst = "ObserveOnBurst";

    /// <summary>Benchmarks scheduling actions through the Avalonia synchronization context.</summary>
    /// <returns>The number of actions executed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(ScheduleBurst)]
    public int SynchronizationContextScheduleBurst()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        SynchronizationContextScheduler scheduler = new(new AvaloniaSynchronizationContext(dispatcher, DispatcherPriority.Background));
        return ScheduleAndDrain(dispatcher, scheduler);
    }

    /// <summary>Benchmarks scheduling actions through the coalescing Avalonia scheduler.</summary>
    /// <returns>The number of actions executed.</returns>
    [Benchmark]
    [BenchmarkCategory(ScheduleBurst)]
    public int PrimitivesSchedulerScheduleBurst()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        PrimitivesAvaloniaScheduler scheduler = new(dispatcher);
        return ScheduleAndDrain(dispatcher, scheduler);
    }

    /// <summary>Benchmarks observing values on the dispatcher through the Avalonia synchronization context.</summary>
    /// <returns>The sum of values delivered on the dispatcher.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(ObserveOnBurst)]
    public int SynchronizationContextObserveOn()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        SynchronizationContextScheduler scheduler = new(new AvaloniaSynchronizationContext(dispatcher, DispatcherPriority.Background));
        return ObserveOnAndDrain(dispatcher, scheduler);
    }

    /// <summary>Benchmarks observing values on the dispatcher through the coalescing Avalonia scheduler.</summary>
    /// <returns>The sum of values delivered on the dispatcher.</returns>
    [Benchmark]
    [BenchmarkCategory(ObserveOnBurst)]
    public int PrimitivesSchedulerObserveOn()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        PrimitivesAvaloniaScheduler scheduler = new(dispatcher);
        return ObserveOnAndDrain(dispatcher, scheduler);
    }

    /// <summary>Schedules a burst of counting actions and drains the dispatcher.</summary>
    /// <param name="dispatcher">The dispatcher to drain.</param>
    /// <param name="scheduler">The scheduler under measurement.</param>
    /// <returns>The number of actions executed.</returns>
    private static int ScheduleAndDrain(Dispatcher dispatcher, IScheduler scheduler)
    {
        ScheduleTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            _ = scheduler.Schedule(tally, static (_, state) =>
            {
                state.Increment();
                return Disposable.Empty;
            });
        }

        dispatcher.RunJobs();
        return tally.Executions;
    }

    /// <summary>Pushes values through ObserveOn and drains the dispatcher.</summary>
    /// <param name="dispatcher">The dispatcher to drain.</param>
    /// <param name="scheduler">The scheduler under measurement.</param>
    /// <returns>The sum of values delivered on the dispatcher.</returns>
    private static int ObserveOnAndDrain(Dispatcher dispatcher, IScheduler scheduler)
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.ObserveOn(scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        dispatcher.RunJobs();
        return observer.Total;
    }

    /// <summary>Counts executed actions.</summary>
    private sealed class ScheduleTally
    {
        /// <summary>Gets the number of executions.</summary>
        public int Executions { get; private set; }

        /// <summary>Records one execution.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Increment() => Executions++;
    }
}
