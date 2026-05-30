// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for async-to-stream adapters.
/// </summary>
[MemoryDiagnoser]
public class AsyncBridgeBenchmarks
{
    /// <summary>
    /// The value produced by the pre-completed task used as the bridge source.
    /// </summary>
    private const int CompletedTaskValue = 42;

    /// <summary>
    /// A pre-completed task that yields <see cref="CompletedTaskValue"/>.
    /// </summary>
    private static readonly Task<int> CompletedTask = Task.FromResult(CompletedTaskValue);

    /// <summary>
    /// Baseline conversion from completed task in primitives.
    /// </summary>
    /// <returns>The emitted value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCompletedTaskBridge()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.FromTask(CompletedTask).Subscribe(observer);
        return observer.LastValue;
    }

    /// <summary>
    /// Completed task conversion in System.Reactive.
    /// </summary>
    /// <returns>The emitted value.</returns>
    [Benchmark]
    public int SystemReactiveCompletedTaskBridge()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.FromAsync(() => CompletedTask).Subscribe(observer);
        return observer.LastValue;
    }

    /// <summary>
    /// Completed task conversion in R3.
    /// </summary>
    /// <returns>The emitted value.</returns>
    [Benchmark]
    public int R3CompletedTaskBridge()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.Observable.ToObservable(CompletedTask, configureAwait: false).Subscribe(observer);
        return observer.LastValue;
    }
}
