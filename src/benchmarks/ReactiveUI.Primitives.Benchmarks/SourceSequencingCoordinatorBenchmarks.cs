// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSourceSubject = System.Reactive.Subjects.Subject<System.IObservable<int>>;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the coordinators that subscribe several inner sources: merge, bounded merge, concat, race, switch, catch and repeat.</summary>
[MemoryDiagnoser]
public class SourceSequencingCoordinatorBenchmarks
{
    /// <summary>The number of inner sources each case subscribes.</summary>
    private const int SourceCount = 8;

    /// <summary>The number of values each inner source emits.</summary>
    private const int Count = 16;

    /// <summary>The maximum number of inner sources a bounded merge keeps active.</summary>
    private const int MaxConcurrent = 2;

    /// <summary>The number of times a repeated source is resubscribed.</summary>
    private const int RepeatCount = 8;

    /// <summary>The values every inner source emits.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>The error the first source of a catch chain fails with.</summary>
    private static readonly InvalidOperationException Failure = new("The first source failed.");

    /// <summary>Benchmarks merging an enumerable of sources through the enumerable blend coordinator.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesMergeEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Merge(CreateSignalSources()).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging an enumerable of sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMergeEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Merge(CreateRxSources()).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks the System.Reactive-named enumerable merge, which runs the merge coordinator.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergeCoordinatorEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = CreateSignalSources().Merge().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging two sources through the pairwise merge coordinator.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergePair()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable(Values).Merge(Signal.FromEnumerable(Values)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks merging two sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMergePair()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Merge(RxObservable.ToObservable(Values), RxObservable.ToObservable(Values))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a merge that keeps at most two inner sources active.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesMergeBounded()
    {
        IntSignalWitness observer = new();
        using var subscription = CreateSignalSources().Merge(MaxConcurrent).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks a bounded merge using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveMergeBounded()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Merge(CreateRxSources(), MaxConcurrent).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks concatenating an enumerable of sources through the chain coordinator.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesConcatEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Concat(CreateSignalSources()).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks concatenating an enumerable of sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveConcatEnumerable()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Concat(CreateRxSources()).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks racing two hot sources where the first one to emit wins.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRaceSubjects()
    {
        IntSignalWitness observer = new();
        using Signal<int> first = new();
        using Signal<int> second = new();
        using var subscription = Signal.Race(first, second).Subscribe(observer);
        EmitAll(first, second);
        return observer.Total;
    }

    /// <summary>Benchmarks racing two hot sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveAmbSubjects()
    {
        IntSignalWitness observer = new();
        using RxSubject first = new();
        using RxSubject second = new();
        using var subscription = RxObservable.Amb(first, second).Subscribe(observer);
        EmitAll(first, second);
        return observer.Total;
    }

    /// <summary>Benchmarks switching across inner sources pushed through a hot outer source.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSwitchOuterSubject()
    {
        IntSignalWitness observer = new();
        using Signal<IObservable<int>> outer = new();
        using var subscription = Signal.Switch(outer).Subscribe(observer);
        for (var i = 0; i < SourceCount; i++)
        {
            outer.OnNext(Signal.FromEnumerable(Values));
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Benchmarks switching across inner sources using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSwitchOuterSubject()
    {
        IntSignalWitness observer = new();
        using RxSourceSubject outer = new();
        using var subscription = RxObservable.Switch(outer).Subscribe(observer);
        for (var i = 0; i < SourceCount; i++)
        {
            outer.OnNext(RxObservable.ToObservable(Values));
        }

        outer.OnCompleted();
        return observer.Total;
    }

    /// <summary>Benchmarks moving past a failed source to the next source of a catch chain.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRecoverEnumerable()
    {
        IntSignalWitness observer = new();
        IObservable<int>[] sources = [Signal.Fail<int>(Failure), Signal.FromEnumerable(Values)];
        using var subscription = sources.Recover().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks moving past a failed source using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCatchEnumerable()
    {
        IntSignalWitness observer = new();
        IEnumerable<IObservable<int>> sources = [RxObservable.Throw<int>(Failure), RxObservable.ToObservable(Values)];
        using var subscription = RxObservable.Catch(sources).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks resubscribing a completing source a fixed number of times.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRepeatSource()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.FromEnumerable(Values).Repeat(RepeatCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks resubscribing a completing source using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRepeatSource()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Repeat(RxObservable.ToObservable(Values), RepeatCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Creates the values every inner source emits.</summary>
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

    /// <summary>Creates the Primitives inner sources.</summary>
    /// <returns>The inner sources.</returns>
    private static IObservable<int>[] CreateSignalSources()
    {
        var sources = new IObservable<int>[SourceCount];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = Signal.FromEnumerable(Values);
        }

        return sources;
    }

    /// <summary>Creates the System.Reactive inner sources.</summary>
    /// <returns>The inner sources.</returns>
    private static IObservable<int>[] CreateRxSources()
    {
        var sources = new IObservable<int>[SourceCount];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = RxObservable.ToObservable(Values);
        }

        return sources;
    }

    /// <summary>Emits every value on the first source, then on the second, and completes both.</summary>
    /// <param name="first">The source that emits first.</param>
    /// <param name="second">The source that emits second.</param>
    private static void EmitAll(IObserver<int> first, IObserver<int> second)
    {
        for (var i = 0; i < Count; i++)
        {
            first.OnNext(i);
            second.OnNext(i);
        }

        first.OnCompleted();
        second.OnCompleted();
    }
}
