// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures merging many live sources, which is how a view model joins everything a view reacts to into one signal.</summary>
[MemoryDiagnoser]
public class OperatorMergeManyBenchmarks
{
    /// <summary>The number of sources merged.</summary>
    private const int Sources = 24;

    /// <summary>Merges live sources, gives each one value and completes them.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesMergeLiveSources()
    {
        IntSignalWitness observer = new();
        var inputs = new Signal<int>[Sources];
        var sources = new IObservable<int>[Sources];
        for (var i = 0; i < Sources; i++)
        {
            inputs[i] = new();
            sources[i] = inputs[i];
        }

        using (Signal.Merge(sources).Subscribe(observer))
        {
            for (var i = 0; i < Sources; i++)
            {
                inputs[i].OnNext(i);
                inputs[i].OnCompleted();
            }
        }

        return observer.Total;
    }

    /// <summary>Merges live sources, gives each one value and completes them, using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveMergeLiveSources()
    {
        IntSignalWitness observer = new();
        var inputs = new System.Reactive.Subjects.Subject<int>[Sources];
        var sources = new IObservable<int>[Sources];
        for (var i = 0; i < Sources; i++)
        {
            inputs[i] = new();
            sources[i] = inputs[i];
        }

        using (RxObservable.Merge(sources).Subscribe(observer))
        {
            for (var i = 0; i < Sources; i++)
            {
                inputs[i].OnNext(i);
                inputs[i].OnCompleted();
            }
        }

        return observer.Total;
    }

    /// <summary>Merges live sources and subscribes to the merge without giving any source a value, which is the cost of setting the merge up and tearing it down.</summary>
    /// <returns>The number of sources.</returns>
    [Benchmark]
    public int PrimitivesMergeSubscribeOnly()
    {
        var inputs = new Signal<int>[Sources];
        var sources = new IObservable<int>[Sources];
        for (var i = 0; i < Sources; i++)
        {
            inputs[i] = new();
            sources[i] = inputs[i];
        }

        IntSignalWitness observer = new();
        using var subscription = Signal.Merge(sources).Subscribe(observer);
        return Sources;
    }
}
