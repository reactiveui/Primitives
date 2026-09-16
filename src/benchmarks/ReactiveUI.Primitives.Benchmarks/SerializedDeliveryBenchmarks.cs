// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;
using RxSubjectFactory = System.Reactive.Subjects.Subject;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the uncontended cost of observers and signals that serialize notifications from many producers.</summary>
[MemoryDiagnoser]
public class SerializedDeliveryBenchmarks
{
    /// <summary>The number of values pushed through each pipeline.</summary>
    private const int Count = 1000;

    /// <summary>Benchmarks a lock-free serializing sink over a hot source.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSerializeWitness()
    {
        IntSignalWitness result = new();
        using Signal<int> source = new();
        using SerializeWitness<int> sink = new(result);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks a lock-based synchronizing sink over a hot source.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesSynchronizeWitness()
    {
        IntSignalWitness result = new();
        using Signal<int> source = new();
        using SynchronizeWitness<int> sink = new(result);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks a synchronizing operator using System.Reactive.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int SystemReactiveSynchronize()
    {
        IntSignalWitness result = new();
        using RxSubject source = new();
        using var subscription = System.Reactive.Linq.Observable.Synchronize(source).Subscribe(result);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
        return result.Total + result.CompletionCount;
    }

    /// <summary>Benchmarks a serialized signal wrapping a new signal.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesSerializedSignal()
    {
        IntSignalWitness result = new();
        using SerializedSignal<int> signal = new();
        using var subscription = signal.Subscribe(result);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i);
        }

        signal.OnCompleted();
        return result.Total + result.CompletionCount + (signal.HasObservers ? 1 : 0) + (signal.IsDisposed ? 1 : 0);
    }

    /// <summary>Benchmarks a serialized signal wrapping a caller-supplied signal that ends in an error.</summary>
    /// <returns>The sum of observed values plus the error count.</returns>
    [Benchmark]
    public int PrimitivesSerializedSignalOverExisting()
    {
        IntSignalWitness result = new();
        using SerializedSignal<int> signal = new(new Signal<int>());
        using var subscription = signal.Subscribe(result);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i);
        }

        signal.OnError(new InvalidOperationException());
        return result.Total + result.ErrorCount;
    }

    /// <summary>Benchmarks a synchronized subject using System.Reactive.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int SystemReactiveSynchronizedSubject()
    {
        IntSignalWitness result = new();
        using RxSubject inner = new();
        var signal = RxSubjectFactory.Synchronize(inner);
        using var subscription = signal.Subscribe(result);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i);
        }

        signal.OnCompleted();
        return result.Total + result.CompletionCount;
    }

    /// <summary>Pushes the value range and completes a Primitives source.</summary>
    /// <param name="source">The source to drive.</param>
    private static void Push(Signal<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }
}
