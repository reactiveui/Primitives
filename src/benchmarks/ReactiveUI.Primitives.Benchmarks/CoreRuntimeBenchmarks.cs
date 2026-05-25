// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using System.Reactive.Concurrency;
using RxDisposable = System.Reactive.Disposables.Disposable;
using RxCompositeDisposable = System.Reactive.Disposables.CompositeDisposable;

using RxCurrentThreadScheduler = System.Reactive.Concurrency.CurrentThreadScheduler;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for low-level runtime and sequencing primitives.
/// </summary>
[MemoryDiagnoser]
public class CoreRuntimeBenchmarks
{
    /// <summary>
    /// Baseline multi-action dispose path for <see cref="Pocket"/>.
    /// </summary>
    /// <returns>The number of disposal callbacks executed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPocketDispose()
    {
        var disposed = 0;
        var pocket = new Pocket(
            Disposable.Create(() => disposed++),
            Disposable.Create(() => disposed++),
            Disposable.Create(() => disposed++));

        pocket.Dispose();
        return disposed;
    }

    /// <summary>
    /// Composite disposable dispose path in System.Reactive.
    /// </summary>
    /// <returns>The number of disposal callbacks executed.</returns>
    [Benchmark]
    public int SystemReactiveCompositeDispose()
    {
        var disposed = 0;
        var pocket = new RxCompositeDisposable(
            RxDisposable.Create(() => disposed++),
            RxDisposable.Create(() => disposed++),
            RxDisposable.Create(() => disposed++));

        pocket.Dispose();
        return disposed;
    }

    /// <summary>
    /// Schedule and execute one action on current-thread sequencer.
    /// </summary>
    /// <returns>The executed marker value.</returns>
    [Benchmark]
    public int PrimitivesCurrentThreadSchedule()
    {
        var value = 0;
        using var scheduled = Sequencer.CurrentThread.Schedule(() => value = 1);
        return value;
    }

    /// <summary>
    /// Schedule and execute one action on System.Reactive current-thread scheduler.
    /// </summary>
    /// <returns>The executed marker value.</returns>
    [Benchmark]
    public int SystemReactiveCurrentThreadSchedule()
    {
        var value = 0;
        using var scheduled = RxCurrentThreadScheduler.Instance.Schedule(() => value = 1);
        return value;
    }

    /// <summary>
    /// Wrap a witness with the safe witness helper.
    /// </summary>
    /// <returns>The forwarded value.</returns>
    [Benchmark]
    public int PrimitivesSafeWitness()
    {
        var value = 0;
        var witness = Witness.Safe(Witness.Create<int>(x => value = x));
        witness.OnNext(42);
        witness.OnCompleted();
        return value;
    }

    /// <summary>
    /// Allocating a completed spark should remain allocation efficient.
    /// </summary>
    /// <returns>An integer marker extracted from kind.</returns>
    [Benchmark]
    public int PrimitivesCompletedSpark()
    {
        var spark = Spark.CreateOnCompleted<int>();
        return (int)spark.Kind;
    }
}
