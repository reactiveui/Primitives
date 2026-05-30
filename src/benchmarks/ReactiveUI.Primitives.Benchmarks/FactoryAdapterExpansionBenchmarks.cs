// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

using RxDisposable = System.Reactive.Disposables.Disposable;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for factory and adapter creation surfaces.
/// </summary>
[MemoryDiagnoser]
public class FactoryAdapterExpansionBenchmarks
{
    /// <summary>
    /// The number of values produced by each benchmarked sequence.
    /// </summary>
    private const int Count = 16;

    /// <summary>
    /// The scalar value emitted by the single-value benchmarks.
    /// </summary>
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
    /// Benchmarks a custom create observable using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCreateSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Create<int>(target =>
        {
            target.OnNext(Value);
            target.OnCompleted();
            return RxDisposable.Empty;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a custom create observable using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CreateSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Create<int>(static target =>
        {
            target.OnNext(Value);
            target.OnCompleted(R3.Result.Success);
            return R3.Disposable.Empty;
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
        using var subscription = Signal.Lazy(() => Signal.Sequence(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferred factory creation using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDeferSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Defer(static () => RxObservable.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferred factory creation using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DeferSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Defer(static () => R3.Observable.Range(1, Count)).Subscribe(observer);
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
    /// Benchmarks starting work on the immediate scheduler using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveStartSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Start(static () => Value, ImmediateScheduler.Instance).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks starting completed work using R3 async factory semantics.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3StartSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.FromAsync(static _ => new ValueTask<int>(Value), configureAwait: false)
            .Subscribe(observer);
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
    /// Benchmarks finite generation using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveUnfoldSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Generate(
                0,
                static state => state < Count,
                static state => state + 1,
                static state => state)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks finite generation using an R3 create loop.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3UnfoldSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Create<int>(static target =>
        {
            for (var state = 0; state < Count; state++)
            {
                target.OnNext(state);
            }

            target.OnCompleted(R3.Result.Success);
            return R3.Disposable.Empty;
        }).Subscribe(observer);
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
        using var subscription = Signal.Use(static () => Disposable.Empty, static _ => Signal.Emit(Value)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks resource-scoped observable creation using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveUseSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Using(static () => RxDisposable.Empty, static _ => RxObservable.Return(Value))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks resource-scoped observable creation using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3UseSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Create<int>(static target =>
        {
            using var resource = R3.Disposable.Empty;
            target.OnNext(Value);
            target.OnCompleted(R3.Result.Success);
            return R3.Disposable.Empty;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks async-enumerable adaptation.
    /// </summary>
    /// <returns>The number of values collected.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFromAsyncEnumerableSubscribeAsync() => (await Signal.FromAsyncEnumerable(ValuesAsync()).CollectArrayAsync().ConfigureAwait(false)).Length;

    /// <summary>
    /// Benchmarks async-enumerable adaptation using System.Reactive create semantics.
    /// </summary>
    /// <returns>The number of values collected.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveFromAsyncEnumerableSubscribeAsync()
    {
        var values = await RxObservable.Create<int>(static async (target, cancellationToken) =>
            {
                await foreach (var value in ValuesAsync().WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    target.OnNext(value);
                }

                target.OnCompleted();
            })
            .ToArrayAsync()
            .ConfigureAwait(false);
        return values.Length;
    }

    /// <summary>
    /// Benchmarks async-enumerable adaptation using R3.
    /// </summary>
    /// <returns>The number of values collected.</returns>
    [Benchmark]
    public async Task<int> R3FromAsyncEnumerableSubscribeAsync() =>
        (await R3.ObservableExtensions.ToArrayAsync(
                R3.Observable.ToObservable(ValuesAsync()),
                CancellationToken.None)
            .ConfigureAwait(false)).Length;

    /// <summary>
    /// Benchmarks subscribing and disposing a never-ending signal.
    /// </summary>
    /// <returns>The observed notification count.</returns>
    [Benchmark]
    public int PrimitivesNeverSubscribeDispose()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Silent<int>().Subscribe(observer);
        return observer.NextCount + observer.CompletionCount + observer.ErrorCount;
    }

    /// <summary>
    /// Benchmarks subscribing and disposing a never-ending System.Reactive observable.
    /// </summary>
    /// <returns>The observed notification count.</returns>
    [Benchmark]
    public int SystemReactiveNeverSubscribeDispose()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Never<int>().Subscribe(observer);
        return observer.NextCount + observer.CompletionCount + observer.ErrorCount;
    }

    /// <summary>
    /// Benchmarks subscribing and disposing a never-ending R3 observable.
    /// </summary>
    /// <returns>The observed notification count.</returns>
    [Benchmark]
    public int R3NeverSubscribeDispose()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.Never<int>().Subscribe(observer);
        return observer.NextCount + observer.CompletionCount + observer.ErrorCount;
    }

    /// <summary>
    /// Gets the values.
    /// </summary>
    /// <returns>The enumerable of values.</returns>
    private static async IAsyncEnumerable<int> ValuesAsync()
    {
        await Task.Yield();
        for (var i = 0; i < Count; i++)
        {
            yield return i;
        }
    }
}
