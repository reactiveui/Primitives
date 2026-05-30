// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using RxCompositeDisposable = System.Reactive.Disposables.CompositeDisposable;
using RxCurrentThreadScheduler = System.Reactive.Concurrency.CurrentThreadScheduler;
using RxDisposable = System.Reactive.Disposables.Disposable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for low-level runtime and sequencing primitives.
/// </summary>
[MemoryDiagnoser]
public class CoreRuntimeBenchmarks
{
    /// <summary>
    /// The value forwarded through the witness and observer benchmarks.
    /// </summary>
    private const int ForwardedValue = 42;

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
    /// Composite disposable dispose path in R3.
    /// </summary>
    /// <returns>The number of disposal callbacks executed.</returns>
    [Benchmark]
    public int R3CompositeDispose()
    {
        var disposed = 0;
        var pocket = new R3.CompositeDisposable(
            R3.Disposable.Create(() => disposed++),
            R3.Disposable.Create(() => disposed++),
            R3.Disposable.Create(() => disposed++));

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
    /// Immediate dispatch through R3 return subscription.
    /// </summary>
    /// <returns>The executed marker value.</returns>
    [Benchmark]
    public int R3CurrentThreadSchedule()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Return(1).Subscribe(observer);
        return observer.LastValue;
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
        witness.OnNext(ForwardedValue);
        witness.OnCompleted();
        return value;
    }

    /// <summary>
    /// Notify a System.Reactive observer created from delegates.
    /// </summary>
    /// <returns>The forwarded value.</returns>
    [Benchmark]
    public int SystemReactiveSafeWitness()
    {
        var value = 0;
        var observer = System.Reactive.Observer.Create<int>(x => value = x, _ => { }, () => { });
        observer.OnNext(ForwardedValue);
        observer.OnCompleted();
        return value;
    }

    /// <summary>
    /// Notify an R3 observer created from delegates.
    /// </summary>
    /// <returns>The forwarded value.</returns>
    [Benchmark]
    public int R3SafeWitness()
    {
        var value = 0;
        var observer = new IntR3ActionObserver(x => value = x);
        observer.OnNext(ForwardedValue);
        observer.OnCompleted(R3.Result.Success);
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

    /// <summary>
    /// Allocating a completed notification with System.Reactive.
    /// </summary>
    /// <returns>An integer marker extracted from kind.</returns>
    [Benchmark]
    public int SystemReactiveCompletedSpark()
    {
        var notification = System.Reactive.Notification.CreateOnCompleted<int>();
        return (int)notification.Kind;
    }

    /// <summary>
    /// Allocating a completed notification with R3.
    /// </summary>
    /// <returns>An integer marker extracted from kind.</returns>
    [Benchmark]
    public int R3CompletedSpark()
    {
        var notification = new R3.Notification<int>(R3.Result.Success);
        return (int)notification.Kind;
    }
}
