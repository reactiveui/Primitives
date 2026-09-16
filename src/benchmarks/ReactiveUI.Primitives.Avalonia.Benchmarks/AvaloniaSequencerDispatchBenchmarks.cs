// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Avalonia.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures marshalling work onto an Avalonia dispatcher through the coalescing sequencer, drained with RunJobs.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class AvaloniaSequencerDispatchBenchmarks
{
    /// <summary>The number of work items or values dispatched per case.</summary>
    private const int Count = 1000;

    /// <summary>The category for bursts of scheduled work.</summary>
    private const string WorkBurst = "WorkBurst";

    /// <summary>The category for values observed on the dispatcher.</summary>
    private const string ObserveOnBurst = "ObserveOnBurst";

    /// <summary>Benchmarks posting each unit of work straight to the dispatcher.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(WorkBurst)]
    public int DispatcherPostBurst()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        DispatchTally tally = new();
        Action work = tally.Execute;
        for (var i = 0; i < Count; i++)
        {
            dispatcher.Post(work, DispatcherPriority.Background);
        }

        dispatcher.RunJobs();
        return tally.Executions;
    }

    /// <summary>Benchmarks scheduling each unit of work through the coalescing sequencer.</summary>
    /// <returns>The number of work items executed.</returns>
    [Benchmark]
    [BenchmarkCategory(WorkBurst)]
    public int PrimitivesSequencerBurst()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        AvaloniaScheduler scheduler = new(dispatcher);
        DispatchTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            scheduler.Schedule(tally);
        }

        dispatcher.RunJobs();
        return tally.Executions;
    }

    /// <summary>Benchmarks an observer that posts every value to the dispatcher itself.</summary>
    /// <returns>The sum of values delivered on the dispatcher.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory(ObserveOnBurst)]
    public int DispatcherPostObserver()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Subscribe(new DispatcherPostWitness(dispatcher, observer));
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        dispatcher.RunJobs();
        return observer.Total;
    }

    /// <summary>Benchmarks observing values on the dispatcher through the sequencer.</summary>
    /// <returns>The sum of values delivered on the dispatcher.</returns>
    [Benchmark]
    [BenchmarkCategory(ObserveOnBurst)]
    public int PrimitivesSequencerObserveOn()
    {
        var dispatcher = Dispatcher.CurrentDispatcher;
        AvaloniaScheduler scheduler = new(dispatcher);
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.ObserveOn(scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        dispatcher.RunJobs();
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

    /// <summary>Posts every value to a dispatcher before forwarding it.</summary>
    /// <param name="dispatcher">The dispatcher that delivers values.</param>
    /// <param name="inner">The observer receiving values on the dispatcher.</param>
    private sealed class DispatcherPostWitness(Dispatcher dispatcher, IObserver<int> inner) : IObserver<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value) =>
            dispatcher.Post(
                static state =>
                {
                    var (observer, item) = ((IObserver<int>, int))state!;
                    observer.OnNext(item);
                },
                (inner, value),
                DispatcherPriority.Background);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) =>
            dispatcher.Post(
                static state =>
                {
                    var (observer, failure) = ((IObserver<int>, Exception))state!;
                    observer.OnError(failure);
                },
                (inner, error),
                DispatcherPriority.Background);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() =>
            dispatcher.Post(static state => ((IObserver<int>)state!).OnCompleted(), inner, DispatcherPriority.Background);
    }
}
