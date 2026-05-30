// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the dedicated null-filtering and type-filtering/cast operators (KeepNotNull,
/// KeepType, CastTo) against the equivalent System.Reactive and R3 pipelines. Each pipeline first
/// projects ints into a nullable/boxed reference so all three frameworks pay the same projection
/// cost; the measured difference is the filter/cast sink itself.
/// </summary>
[MemoryDiagnoser]
public class OperatorFilterCastBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks filtering out nulls from a nullable reference sequence.
    /// </summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesKeepNotNull()
    {
        var observer = new CountingSignalObserver<string>();
        using var subscription = Signal.Sequence(1, Count).Map(static x => (string?)x.ToString()).KeepNotNull().Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks filtering out nulls from a nullable reference sequence using System.Reactive.
    /// </summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark]
    public int SystemReactiveKeepNotNull()
    {
        var observer = new CountingSignalObserver<string>();
        using var subscription = RxObservable.Range(1, Count)
            .Select(static x => (string?)x.ToString())
            .Where(static x => x is not null)
            .Select(static x => x!)
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks filtering out nulls from a nullable reference sequence using R3.
    /// </summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark]
    public int R3KeepNotNull()
    {
        var observer = new CountingR3Observer<string>();
        var source = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static x => (string?)x.ToString());
        var filtered = R3.ObservableExtensions.Where(source, static x => x is not null);
        using var subscription = R3.ObservableExtensions.Select(filtered, static x => x!).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks filtering a boxed sequence by element type.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesKeepType()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Map(static x => (object?)x).KeepType<int>().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks filtering a boxed sequence by element type using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveKeepType()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Select(static x => (object)x).OfType<int>().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks filtering a boxed sequence by element type using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3KeepType()
    {
        var observer = new IntR3Observer();
        var boxed = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static x => (object)x);
        using var subscription = R3.ObservableExtensions.OfType<object, int>(boxed).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks casting a boxed sequence to a value type.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesCastTo()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Map(static x => (object?)x).CastTo<int>().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks casting a boxed sequence to a value type using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCastTo()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Select(static x => (object)x).Cast<int>().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks casting a boxed sequence to a value type using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CastTo()
    {
        var observer = new IntR3Observer();
        var boxed = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static x => (object)x);
        using var subscription = R3.ObservableExtensions.Cast<object, int>(boxed).Subscribe(observer);
        return observer.Total;
    }
}
