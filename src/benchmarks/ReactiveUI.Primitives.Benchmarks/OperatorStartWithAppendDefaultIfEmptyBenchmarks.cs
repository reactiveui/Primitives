// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for default-if-empty, prepend/append, and equivalent operators.
/// </summary>
[MemoryDiagnoser]
public class OperatorStartWithAppendDefaultIfEmptyBenchmarks
{
    /// <summary>
    /// Baseline start-with / default-if-empty / append chain using primitives.
    /// </summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesStartWithAppendDefaultIfEmpty()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.None<int>()
            .DefaultIfEmpty(2)
            .Prepend(1)
            .Append(3)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Default-if-empty over an immediate empty primitives source.
    /// </summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int PrimitivesDefaultIfEmptyEmpty()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.None<int>()
            .DefaultIfEmpty(2)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Equivalent composition using System.Reactive.
    /// </summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int SystemReactiveStartWithAppendDefaultIfEmpty()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Empty<int>()
            .DefaultIfEmpty(2)
            .StartWith(1)
            .Append(3)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Default-if-empty over an immediate empty System.Reactive source.
    /// </summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int SystemReactiveDefaultIfEmptyEmpty()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.DefaultIfEmpty(RxObservable.Empty<int>(), 2)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Equivalent chain using R3.
    /// </summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int R3PrependAppendDefaultIfEmpty()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Append(
                R3.ObservableExtensions.Prepend(
                    R3.ObservableExtensions.DefaultIfEmpty(
                        R3.Observable.Empty<int>(),
                        2),
                    1),
                3)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Default-if-empty over an immediate empty R3 source.
    /// </summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int R3DefaultIfEmptyEmpty()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.DefaultIfEmpty(R3.Observable.Empty<int>(), 2)
            .Subscribe(observer);
        return observer.Total;
    }
}
