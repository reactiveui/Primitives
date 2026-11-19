// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures switching to the newest of a stream of live inner sources, which is how a view follows a property of whatever object is current.</summary>
[MemoryDiagnoser]
public class OperatorSwitchBenchmarks
{
    /// <summary>The inner sources the outer stream gives.</summary>
    private const int Inners = 16;

    /// <summary>Switches between live inner sources, each giving one value and completing.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSwitchLiveInners()
    {
        IntSignalWitness observer = new();
        using Signal<IObservable<int>> outer = new();
        using var switched = outer.Switch().Subscribe(observer);
        for (var i = 0; i < Inners; i++)
        {
            using Signal<int> inner = new();
            outer.OnNext(inner);
            inner.OnNext(i);
            inner.OnCompleted();
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Switches between live inner sources, each giving one value and completing, using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveSwitchLiveInners()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<IObservable<int>> outer = new();
        using var switched = RxObservable.Switch(outer).Subscribe(observer);
        for (var i = 0; i < Inners; i++)
        {
            using System.Reactive.Subjects.Subject<int> inner = new();
            outer.OnNext(inner);
            inner.OnNext(i);
            inner.OnCompleted();
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Maps each value of an outer stream to a live inner source and follows the newest.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesSwitchMapLiveInners()
    {
        IntSignalWitness observer = new();
        using Signal<int> outer = new();
        using var switched = outer.SwitchMap(Signal.Emit<int>).Subscribe(observer);
        for (var i = 0; i < Inners; i++)
        {
            outer.OnNext(i);
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Maps each value of an outer stream to an inner source and follows the newest, using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveSelectSwitchLiveInners()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<int> outer = new();
        using var switched = RxObservable.Switch(RxObservable.Select(outer, RxObservable.Return<int>)).Subscribe(observer);
        for (var i = 0; i < Inners; i++)
        {
            outer.OnNext(i);
        }

        outer.OnCompleted();
        return observer.Total;
    }
}
