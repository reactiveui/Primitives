// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
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
    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int Count = 16;

    /// <summary>The shared reference projected into each benchmarked sequence.</summary>
    private const string Shared = "x";

    /// <summary>Benchmarks filtering out nulls from a nullable reference sequence.</summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesKeepNotNull()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = Signal.Sequence(1, Count).Map(static x => (string?)x.ToString()).KeepNotNull()
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks filtering out nulls from a nullable reference sequence using System.Reactive.</summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark]
    public int SystemReactiveKeepNotNull()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = RxObservable.Range(1, Count)
            .Select(static x => (string?)x.ToString())
            .Where(static x => x is not null)
            .Select(static x => x!)
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks filtering out nulls from a nullable reference sequence using R3.</summary>
    /// <returns>The number of non-null values observed.</returns>
    [Benchmark]
    public int R3KeepNotNull()
    {
        CountingR3Witness<string> observer = new();
        var source = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static x => (string?)x.ToString());
        var filtered = R3.ObservableExtensions.Where(source, static x => x is not null);
        using var subscription = R3.ObservableExtensions.Select(filtered, static x => x!).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks filtering a reference sequence by element type.</summary>
    /// <returns>The number of matching values observed.</returns>
    [Benchmark]
    public int PrimitivesKeepType()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = Signal.Sequence(1, Count).Map(static _ => (object?)Shared).KeepType<string>()
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks filtering a reference sequence by element type using System.Reactive.</summary>
    /// <returns>The number of matching values observed.</returns>
    [Benchmark]
    public int SystemReactiveKeepType()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = RxObservable.Select(RxObservable.Range(1, Count), static _ => (object)Shared)
            .OfType<string>().Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks filtering a reference sequence by element type using R3.</summary>
    /// <returns>The number of matching values observed.</returns>
    [Benchmark]
    public int R3KeepType()
    {
        CountingR3Witness<string> observer = new();
        var typed = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static _ => (object)Shared);
        using var subscription = R3.ObservableExtensions.OfType<object, string>(typed).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks casting a reference sequence to a target type.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int PrimitivesCastTo()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = Signal.Sequence(1, Count).Map(static _ => (object?)Shared).CastTo<string>()
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks casting a reference sequence to a target type using System.Reactive.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int SystemReactiveCastTo()
    {
        CountingSignalWitness<string> observer = new();
        using var subscription = RxObservable.Select(RxObservable.Range(1, Count), static _ => (object)Shared)
            .Cast<string>().Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks casting a reference sequence to a target type using R3.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public int R3CastTo()
    {
        CountingR3Witness<string> observer = new();
        var typed = R3.ObservableExtensions.Select(R3.Observable.Range(1, Count), static _ => (object)Shared);
        using var subscription = R3.ObservableExtensions.Cast<object, string>(typed).Subscribe(observer);
        return observer.Count;
    }
}
