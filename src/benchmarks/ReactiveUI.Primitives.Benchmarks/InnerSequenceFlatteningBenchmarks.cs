// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the operators that flatten inner sequences: Chain/Concat, Switch, SwitchMap, SwitchSelect and Choose.</summary>
[MemoryDiagnoser]
public class InnerSequenceFlatteningBenchmarks
{
    /// <summary>The number of inner sequences or outer values per case.</summary>
    private const int Count = 32;

    /// <summary>The number of values produced by each inner sequence.</summary>
    private const int InnerCount = 4;

    /// <summary>The factor applied by the projection cases.</summary>
    private const int Factor = 2;

    /// <summary>Chains live inner sequences one after another, then two fixed sources.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesChainInnerSequences()
    {
        IntSignalWitness observer = new();
        using Signal<IObservable<int>> outer = new();
        using var chained = outer.Chain().Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            outer.OnNext(Signal.Range(i, InnerCount));
        }

        outer.OnCompleted();
        using var pair = Signal.Range(1, Count).Chain(Signal.Range(1, Count)).Subscribe(observer);
        using var concat = outer.Concat().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Concatenates live inner sequences one after another, then two fixed sources, using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveConcatInnerSequences()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<IObservable<int>> outer = new();
        using var chained = RxObservable.Concat(outer).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            outer.OnNext(RxObservable.Range(i, InnerCount));
        }

        outer.OnCompleted();
        using var pair = RxObservable.Concat(RxObservable.Range(1, Count), RxObservable.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Switches through live inner subjects, displacing each one with the next.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesSwitchLiveInners()
    {
        IntSignalWitness observer = new();
        using Signal<IObservable<int>> outer = new();
        using var switched = outer.Switch().Subscribe(observer);
        using var switchedTo = outer.SwitchTo().Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            using Signal<int> inner = new();
            outer.OnNext(inner);
            inner.OnNext(i);
            inner.OnCompleted();
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Switches through live inner subjects, displacing each one with the next, using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveSwitchLiveInners()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<IObservable<int>> outer = new();
        using var switched = RxObservable.Switch(outer).Subscribe(observer);
        using var switchedTo = RxObservable.Switch(outer).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            using System.Reactive.Subjects.Subject<int> inner = new();
            outer.OnNext(inner);
            inner.OnNext(i);
            inner.OnCompleted();
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Projects each value to an inner range and mirrors the latest, including the null-skipping variant.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesSwitchMap()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using Signal<string?> keys = new();
        using var mapped = source.SwitchMap(static value => Signal.Range(value, InnerCount)).Subscribe(observer);
        using var selected = keys.SwitchSelect(static key => Signal.Range(key.Length, InnerCount)).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            keys.OnNext((i & 1) == 0 ? null : "key");
        }

        return observer.Total;
    }

    /// <summary>Projects each value to an inner range and mirrors the latest, including a null filter, using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveSelectSwitch()
    {
        IntSignalWitness observer = new();
        using System.Reactive.Subjects.Subject<int> source = new();
        using System.Reactive.Subjects.Subject<string?> keys = new();
        using var mapped = RxObservable.Switch(RxObservable.Select(source, static value => RxObservable.Range(value, InnerCount)))
            .Subscribe(observer);
        using var selected = RxObservable.Switch(
                RxObservable.Select(
                    RxObservable.Where(keys, static key => key is not null),
                    static key => RxObservable.Range(key!.Length, InnerCount)))
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            keys.OnNext((i & 1) == 0 ? null : "key");
        }

        return observer.Total;
    }

    /// <summary>Filters and projects in one fused sink.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesChoose()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Range(1, Count)
            .Choose(static value => ((value & 1) == 0, value * Factor))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Filters and projects with a filter followed by a map using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveWhereSelect()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Select(
                RxObservable.Where(RxObservable.Range(1, Count), static value => (value & 1) == 0),
                static value => value * Factor)
            .Subscribe(observer);
        return observer.Total;
    }
}
