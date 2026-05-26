// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for factory and adapter creation surfaces.
/// </summary>
[MemoryDiagnoser]
public class FactoryAdapterExpansionBenchmarks
{
    private const int Count = 16;
    private const int Value = 42;

    /// <summary>
    /// Benchmarks a custom create signal.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCreateSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Create<int>(target =>
        {
            target.OnNext(Value);
            target.OnCompleted();
            return Disposable.Empty;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a custom safe-create signal.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesCreateSafeSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.CreateSafe<int>(target =>
        {
            target.OnNext(Value);
            target.OnCompleted();
            return Disposable.Empty;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferred factory creation.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesDeferSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Defer(() => Signal.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks starting work on the immediate scheduler.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesStartSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Start(() => Value, Sequencer.Immediate).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks finite unfold generation.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUnfoldSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Unfold(0, static state => state < Count, static state => state + 1, static state => state)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks resource-scoped signal creation.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesUseSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Use(static () => Disposable.Empty, static _ => Signal.Return(Value)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks async-enumerable adaptation.
    /// </summary>
    /// <returns>The number of values collected.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFromAsyncEnumerableSubscribeAsync()
    {
        return (await Signal.FromAsyncEnumerable(ValuesAsync()).CollectArrayAsync().ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks subscribing and disposing a never-ending signal.
    /// </summary>
    /// <returns>The observed notification count.</returns>
    [Benchmark]
    public int PrimitivesNeverSubscribeDispose()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Never<int>().Subscribe(observer);
        return observer.NextCount + observer.CompletionCount + observer.ErrorCount;
    }

    private static async IAsyncEnumerable<int> ValuesAsync()
    {
        await Task.Yield();
        for (var i = 0; i < Count; i++)
        {
            yield return i;
        }
    }
}
