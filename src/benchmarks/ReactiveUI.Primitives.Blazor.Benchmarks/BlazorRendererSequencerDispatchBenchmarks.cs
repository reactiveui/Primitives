// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Blazor.Concurrency;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RendererDispatcher = Microsoft.AspNetCore.Components.Dispatcher;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures marshalling work through a Blazor renderer dispatcher with the renderer sequencer.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class BlazorRendererSequencerDispatchBenchmarks
{
    /// <summary>The number of work items or values dispatched per case.</summary>
    private const int Count = 1000;

    /// <summary>The category for bursts of scheduled work.</summary>
    private const string WorkBurst = "WorkBurst";

    /// <summary>The category for values observed on the renderer.</summary>
    private const string ObserveOnBurst = "ObserveOnBurst";

    /// <summary>The renderer dispatcher shared by every case.</summary>
    private readonly RendererDispatcher _dispatcher = RendererDispatcher.CreateDefault();

    /// <summary>Benchmarks invoking each unit of work on the renderer dispatcher directly.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(WorkBurst)]
    public int DispatcherInvokeBurst()
    {
        DispatchTally tally = new();
        Action work = tally.Execute;
        for (var i = 0; i < Count; i++)
        {
            _ = _dispatcher.InvokeAsync(work);
        }

        return tally.Executions;
    }

    /// <summary>Benchmarks scheduling each unit of work through the renderer sequencer.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark]
    [BenchmarkCategory(WorkBurst)]
    public int PrimitivesSequencerBurst()
    {
        var sequencer = _dispatcher.ToSequencer();
        DispatchTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            sequencer.Schedule(tally);
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
        using Signal<int> source = new();
        using var subscription = source.Subscribe(new RendererInvokeWitness(_dispatcher, observer));
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks observing values on the renderer through the sequencer.</summary>
    /// <returns>The sum of values delivered on the renderer.</returns>
    [Benchmark]
    [BenchmarkCategory(ObserveOnBurst)]
    public int PrimitivesSequencerObserveOn()
    {
        BlazorRendererSequencer sequencer = new(_dispatcher);
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.ObserveOn(sequencer).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Counts executions of dispatched work.</summary>
    private sealed class DispatchTally : IWorkItem
    {
        /// <summary>Gets the number of executions.</summary>
        public int Executions { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => Executions++;
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
