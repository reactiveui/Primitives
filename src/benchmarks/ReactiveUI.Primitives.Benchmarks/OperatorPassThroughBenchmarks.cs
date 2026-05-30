// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for the pass-through and materialization operators converted to dedicated signals
/// (Tap, IgnoreValues, Spark/Unspark materialize round-trip, SubscribeOn, Reattempt).
/// </summary>
[MemoryDiagnoser]
public class OperatorPassThroughBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks a side-effecting pass-through over a range.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesTapRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Tap(static _ => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a side-effecting pass-through over a range using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveTapRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Do(static _ => { }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a side-effecting pass-through over a range using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3TapRange()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Do(R3.Observable.Range(1, Count), onNext: static (int _) => { })
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks dropping values while forwarding completion.
    /// </summary>
    /// <returns>The number of completions observed.</returns>
    [Benchmark]
    public int PrimitivesIgnoreValuesRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).IgnoreValues().Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks dropping values while forwarding completion using System.Reactive.
    /// </summary>
    /// <returns>The number of completions observed.</returns>
    [Benchmark]
    public int SystemReactiveIgnoreValuesRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).IgnoreElements().Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks dropping values while forwarding completion using R3.
    /// </summary>
    /// <returns>The number of completions observed.</returns>
    [Benchmark]
    public int R3IgnoreValuesRange()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.IgnoreElements(R3.Observable.Range(1, Count)).Subscribe(observer);
        return observer.CompletionCount;
    }

    /// <summary>
    /// Benchmarks materializing notifications over a range.
    /// </summary>
    /// <returns>The number of materialized notifications observed.</returns>
    [Benchmark]
    public int PrimitivesMaterializeRange()
    {
        var observer = new CountingSignalObserver<Spark<int>>();
        using var subscription = Signal.Sequence(1, Count).Spark().Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks materializing notifications over a range using System.Reactive.
    /// </summary>
    /// <returns>The number of materialized notifications observed.</returns>
    [Benchmark]
    public int SystemReactiveMaterializeRange()
    {
        var observer = new CountingSignalObserver<System.Reactive.Notification<int>>();
        using var subscription = RxObservable.Range(1, Count).Materialize().Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks materializing notifications over a range using R3.
    /// </summary>
    /// <returns>The number of materialized notifications observed.</returns>
    [Benchmark]
    public int R3MaterializeRange()
    {
        var observer = new CountingR3Observer<R3.Notification<int>>();
        using var subscription = R3.ObservableExtensions.Materialize(R3.Observable.Range(1, Count)).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>
    /// Benchmarks a materialize/dematerialize round-trip over a range.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesDematerializeRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Spark().Unspark().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a materialize/dematerialize round-trip over a range using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDematerializeRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Materialize().Dematerialize().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks a materialize/dematerialize round-trip over a range using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DematerializeRange()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions
            .Dematerialize(R3.ObservableExtensions.Materialize(R3.Observable.Range(1, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferring subscription onto an immediate scheduler.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSubscribeOnImmediate()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).SubscribeOn(Sequencer.Immediate).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferring subscription onto an immediate scheduler using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSubscribeOnImmediate()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).SubscribeOn(ImmediateScheduler.Instance).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks deferring subscription onto an immediate synchronization context using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3SubscribeOnImmediate()
    {
        var observer = new IntR3Observer();
        using var context = new ImmediateSynchronizationContext();
        using var subscription = R3.ObservableExtensions
            .SubscribeOn(R3.Observable.Range(1, Count), context)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks retry-on-error over a non-erroring range (no retries taken).
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesReattemptRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Reattempt(2).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks retry-on-error over a non-erroring range using System.Reactive (no R3 equivalent).
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveReattemptRange()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Retry(2).Subscribe(observer);
        return observer.Total;
    }

    private sealed class ImmediateSynchronizationContext : SynchronizationContext, IDisposable
    {
        public override void Post(SendOrPostCallback d, object? state) => d(state);

        public override void Send(SendOrPostCallback d, object? state) => d(state);

        public void Dispose()
        {
        }
    }
}
