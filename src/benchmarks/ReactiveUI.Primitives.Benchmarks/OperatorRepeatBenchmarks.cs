// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures repeating a source, which is how a sequence of signals is walked one after another.</summary>
[MemoryDiagnoser]
public class OperatorRepeatBenchmarks
{
    /// <summary>The value each run gives.</summary>
    private const int Value = 7;

    /// <summary>The times the source is repeated.</summary>
    private const int Count = 64;

    /// <summary>Repeats a one value source a fixed number of times.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesRepeatCount()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Emit(Value).Repeat(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Repeats a one value source a fixed number of times using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveRepeatCount()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Return(Value).Repeat(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Repeats a one value source without end and stops it with <c>Take</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesRepeatTake()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Emit(Value).Repeat().Take(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Repeats a one value source without end and stops it with <c>Take</c>, using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveRepeatTake()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Return(Value).Repeat().Take(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Walks a sequence by hand: a deferred step repeated until it says there is no more, the shape of a lazy concatenation.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesDeferRepeatTakeWhile()
    {
        IntSignalWitness observer = new();
        var remaining = Count;
        using var subscription = Signal.Defer(() => Interlocked.Decrement(ref remaining) >= 0 ? Signal.Emit(Value) : Signal.Emit(0))
            .Repeat()
            .TakeWhile(static value => value != 0)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Concatenates a sequence of one value sources, which is what the walk above stands in for.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesConcatEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Concat(Enumerable.Repeat(Signal.Emit(Value), Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Concatenates a sequence of one value sources using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveConcatEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Concat(Enumerable.Repeat(RxObservable.Return(Value), Count)).Subscribe(observer);
        return observer.Total;
    }
}
