// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using PrimitivesRendererScheduler = ReactiveUI.Primitives.Blazor.Reactive.Concurrency.BlazorRendererSequencer;
using RendererDispatcher = Microsoft.AspNetCore.Components.Dispatcher;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures System.Reactive scheduling through a Blazor renderer dispatcher with the renderer scheduler.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class BlazorRendererSchedulerDispatchBenchmarks
{
    /// <summary>The number of actions or values dispatched per case.</summary>
    private const int Count = 1000;

    /// <summary>The category for bursts of scheduled actions.</summary>
    private const string ScheduleBurst = "ScheduleBurst";

    /// <summary>The category for values observed on the renderer.</summary>
    private const string ObserveOnBurst = "ObserveOnBurst";

    /// <summary>The renderer dispatcher shared by every case.</summary>
    private readonly RendererDispatcher _dispatcher = RendererDispatcher.CreateDefault();

    /// <summary>Benchmarks invoking each action on the renderer dispatcher directly.</summary>
    /// <returns>The number of actions executed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(ScheduleBurst)]
    public int DispatcherInvokeBurst()
    {
        ScheduleTally tally = new();
        Action work = tally.Increment;
        for (var i = 0; i < Count; i++)
        {
            _ = _dispatcher.InvokeAsync(work);
        }

        return tally.Executions;
    }

    /// <summary>Benchmarks scheduling each action through the renderer scheduler.</summary>
    /// <returns>The number of actions executed.</returns>
    [Benchmark]
    [BenchmarkCategory(ScheduleBurst)]
    public int PrimitivesSchedulerScheduleBurst()
    {
        PrimitivesRendererScheduler scheduler = new(_dispatcher);
        ScheduleTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            _ = scheduler.Schedule(tally, static (_, state) =>
            {
                state.Increment();
                return Disposable.Empty;
            });
        }

        return tally.Executions;
    }

    /// <summary>Benchmarks an observer that invokes every value on the renderer dispatcher itself.</summary>
    /// <returns>The sum of values delivered on the renderer.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(ObserveOnBurst)]
    public int DispatcherInvokeObserver()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.Subscribe(new RendererInvokeWitness(_dispatcher, observer));
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks observing values on the renderer through the renderer scheduler.</summary>
    /// <returns>The sum of values delivered on the renderer.</returns>
    [Benchmark]
    [BenchmarkCategory(ObserveOnBurst)]
    public int PrimitivesSchedulerObserveOn()
    {
        PrimitivesRendererScheduler scheduler = new(_dispatcher);
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.ObserveOn(scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

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

    /// <summary>Invokes every value on a renderer dispatcher before forwarding it.</summary>
    /// <param name="dispatcher">The renderer dispatcher that delivers values.</param>
    /// <param name="inner">The observer receiving values on the renderer.</param>
    private sealed class RendererInvokeWitness(RendererDispatcher dispatcher, IObserver<int> inner) : IObserver<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value) => _ = dispatcher.InvokeAsync(() => inner.OnNext(value));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _ = dispatcher.InvokeAsync(() => inner.OnError(error));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _ = dispatcher.InvokeAsync(inner.OnCompleted);
    }
}
