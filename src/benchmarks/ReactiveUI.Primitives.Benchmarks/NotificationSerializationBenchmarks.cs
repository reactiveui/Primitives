// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the per-value cost of serializing notifications from a hot source.</summary>
[MemoryDiagnoser]
public class NotificationSerializationBenchmarks
{
    /// <summary>The number of values pushed through each case.</summary>
    private const int Count = 1024;

    /// <summary>Benchmarks lock-free serialized delivery.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSerializeSubject()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Serialize().Subscribe(observer);
        EmitAll(source);
        return observer.Total;
    }

    /// <summary>Benchmarks serialized delivery using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSynchronizeSubject()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Synchronize(source).Subscribe(observer);
        EmitAll(source);
        return observer.Total;
    }

#if NET9_0_OR_GREATER
    /// <summary>Benchmarks delivery serialized behind a caller-supplied object gate.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSynchronizeObjectGate()
    {
        IntSignalWitness observer = new();
        object gate = new();
        using Signal<int> source = new();
        using var subscription = source.Synchronize(gate).Subscribe(observer);
        EmitAll(source);
        return observer.Total;
    }
#endif

    /// <summary>Benchmarks delivery serialized behind a caller-supplied gate using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSynchronizeObjectGate()
    {
        IntSignalWitness observer = new();
        object gate = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Synchronize(source, gate).Subscribe(observer);
        EmitAll(source);
        return observer.Total;
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
