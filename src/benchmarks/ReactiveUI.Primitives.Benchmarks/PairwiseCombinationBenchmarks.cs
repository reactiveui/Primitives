// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures two-source combinators driven by hot sources: positional pairing, latest-value fusion and fork-join.</summary>
[MemoryDiagnoser]
public class PairwiseCombinationBenchmarks
{
    /// <summary>The number of values each side emits.</summary>
    private const int Count = 256;

    /// <summary>Benchmarks pairing values from two hot sources by position.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPairSubjects()
    {
        IntSignalWitness observer = new();
        using Signal<int> left = new();
        using Signal<int> right = new();
        using var subscription = Signal.Pair(left, right, static (l, r) => l + r).Subscribe(observer);
        EmitAll(left, right);
        return observer.Total;
    }

    /// <summary>Benchmarks pairing values by position using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveZipSubjects()
    {
        IntSignalWitness observer = new();
        using RxSubject left = new();
        using RxSubject right = new();
        using var subscription = RxObservable.Zip(left, right, static (l, r) => l + r).Subscribe(observer);
        EmitAll(left, right);
        return observer.Total;
    }

    /// <summary>Benchmarks combining the latest values of two hot sources.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSyncLatestSubjects()
    {
        IntSignalWitness observer = new();
        using Signal<int> left = new();
        using Signal<int> right = new();
        using var subscription = Signal.SyncLatest(left, right, static (l, r) => l + r).Subscribe(observer);
        EmitAll(left, right);
        return observer.Total;
    }

    /// <summary>Benchmarks combining the latest values using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestSubjects()
    {
        IntSignalWitness observer = new();
        using RxSubject left = new();
        using RxSubject right = new();
        using var subscription = RxObservable.CombineLatest(left, right, static (l, r) => l + r).Subscribe(observer);
        EmitAll(left, right);
        return observer.Total;
    }

    /// <summary>Benchmarks combining the final values of two hot sources once both complete.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesForkJoinSubjects()
    {
        IntSignalWitness observer = new();
        using Signal<int> left = new();
        using Signal<int> right = new();
        using var subscription = Signal.ForkJoin(left, right, static (l, r) => l + r).Subscribe(observer);
        EmitAll(left, right);
        return observer.Total;
    }

    /// <summary>Emits every value on both sides, alternating, and completes both.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    private static void EmitAll(IObserver<int> left, IObserver<int> right)
    {
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i);
            right.OnNext(i);
        }

        left.OnCompleted();
        right.OnCompleted();
    }
}
