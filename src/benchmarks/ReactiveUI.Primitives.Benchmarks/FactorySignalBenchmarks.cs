// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;

using RxObservable = System.Reactive.Linq.Observable;
using RxSystemActionObserver = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for factory-style signal constructors.
/// </summary>
[MemoryDiagnoser]
public class FactorySignalBenchmarks
{
    private const int RangeStart = 4;
    private const int RangeCount = 32;
    private const int RepeatCount = 32;
    private const int ThrowValue = 42;

    /// <summary>
    /// Baseline benchmark for empty completion with the primitives implementation.
    /// </summary>
    /// <returns>The number of completion notifications observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesEmptySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Empty<int>().Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks System.Reactive empty completion path.
    /// </summary>
    /// <returns>The number of completion notifications observed.</returns>
    [Benchmark]
    public int SystemReactiveEmptySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Empty<int>().Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks R3 empty completion path.
    /// </summary>
    /// <returns>The number of completion notifications observed.</returns>
    [Benchmark]
    public int R3EmptySubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Empty<int>().Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks range generation and subscription.
    /// </summary>
    /// <returns>The sum of the received integer range.</returns>
    [Benchmark]
    public int PrimitivesRangeSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(RangeStart, RangeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks range generation and subscription using System.Reactive.
    /// </summary>
    /// <returns>The sum of the received integer range.</returns>
    [Benchmark]
    public int SystemReactiveRangeSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(RangeStart, RangeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks range generation and subscription using R3.
    /// </summary>
    /// <returns>The sum of the received integer range.</returns>
    [Benchmark]
    public int R3RangeSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Range(RangeStart, RangeCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fixed repeat sequence generation.
    /// </summary>
    /// <returns>The sum of the received repeated values.</returns>
    [Benchmark]
    public int PrimitivesRepeatSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Repeat(ThrowValue, RepeatCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fixed repeat sequence generation in System.Reactive.
    /// </summary>
    /// <returns>The sum of the received repeated values.</returns>
    [Benchmark]
    public int SystemReactiveRepeatSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Repeat(ThrowValue, RepeatCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks fixed repeat sequence generation in R3.
    /// </summary>
    /// <returns>The sum of the received repeated values.</returns>
    [Benchmark]
    public int R3RepeatSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Repeat(ThrowValue, RepeatCount).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks terminal error completion for primitives.
    /// </summary>
    /// <returns>The number of errors observed.</returns>
    [Benchmark]
    public int PrimitivesThrowSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Throw<int>(new InvalidOperationException()).Subscribe(observer);
        return observer.ErrorCount;
    }

    /// <summary>
    /// Benchmarks terminal error completion for System.Reactive.
    /// </summary>
    /// <returns>The number of errors observed.</returns>
    [Benchmark]
    public int SystemReactiveThrowSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Throw<int>(new InvalidOperationException()).Subscribe(observer);
        return observer.ErrorCount;
    }

    /// <summary>
    /// Benchmarks terminal error completion for R3.
    /// </summary>
    /// <returns>The number of errors observed.</returns>
    [Benchmark]
    public int R3ThrowSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Throw<int>(new InvalidOperationException()).Subscribe(observer);
        return observer.ErrorCount;
    }
}
