// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures projections that flatten inner sources or carry an index: select-many, switch-map and indexed map.</summary>
[MemoryDiagnoser]
public class ProjectionFlatteningBenchmarks
{
    /// <summary>The number of source values each case projects.</summary>
    private const int Count = 64;

    /// <summary>The source values.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>Benchmarks projecting each value to an inner source and merging the inner values.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSelectMany()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable(Values)
            .SelectMany(Signal.Emit)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks projecting each value to an inner source using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSelectMany()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.SelectMany(
                RxObservable.ToObservable(Values),
                RxReturn)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks projecting each value to an inner source and combining each outer and inner pair.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSelectManyWithResult()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable(Values)
            .SelectMany(Signal.Emit, static (value, inner) => value + inner)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks projecting and combining outer and inner pairs using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSelectManyWithResult()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.SelectMany(
                RxObservable.ToObservable(Values),
                RxReturn,
                static (value, inner) => value + inner)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks the fused switch-map that mirrors only the latest projected inner source.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSwitchMapSubject()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.SwitchMap(Signal.Emit).Subscribe(observer);
        EmitAll(source);
        return observer.Total;
    }

    /// <summary>Benchmarks a projected switch using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSelectSwitchSubject()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Switch(RxObservable.Select(source, RxReturn))
            .Subscribe(observer);
        EmitAll(source);
        return observer.Total;
    }

    /// <summary>Benchmarks projecting each value together with its zero-based index.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMapIndexed()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable(Values)
            .MapIndexed(static (value, index) => value + index)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks an indexed projection using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSelectIndexed()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Select(RxObservable.ToObservable(Values), static (value, index) => value + index)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Creates a System.Reactive inner source that emits one value.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>The inner source.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> RxReturn(int value) => RxObservable.Return(value);

    /// <summary>Creates the source values.</summary>
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

    /// <summary>Emits every value on the source and completes it.</summary>
    /// <param name="source">The source to drive.</param>
    private static void EmitAll(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }
}
