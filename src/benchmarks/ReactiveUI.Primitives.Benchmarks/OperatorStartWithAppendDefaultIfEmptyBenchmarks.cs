// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for default-if-empty, prepend/append, and equivalent operators.</summary>
[MemoryDiagnoser]
public class OperatorStartWithAppendDefaultIfEmptyBenchmarks
{
    /// <summary>The value substituted when the source completes without emitting.</summary>
    private const int DefaultValue = 2;

    /// <summary>The value prepended ahead of the source.</summary>
    private const int PrependedValue = 1;

    /// <summary>The value appended after the source.</summary>
    private const int AppendedValue = 3;

    /// <summary>Baseline start-with / default-if-empty / append chain using primitives.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesStartWithAppendDefaultIfEmpty()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.None<int>()
            .DefaultIfEmpty(DefaultValue)
            .Prepend(PrependedValue)
            .Append(AppendedValue)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Default-if-empty over an immediate empty primitives source.</summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int PrimitivesDefaultIfEmptyEmpty()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.None<int>()
            .DefaultIfEmpty(DefaultValue)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Equivalent composition using System.Reactive.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int SystemReactiveStartWithAppendDefaultIfEmpty()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Empty<int>()
            .DefaultIfEmpty(DefaultValue)
            .StartWith(PrependedValue)
            .Append(AppendedValue)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Default-if-empty over an immediate empty System.Reactive source.</summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int SystemReactiveDefaultIfEmptyEmpty()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.DefaultIfEmpty(RxObservable.Empty<int>(), DefaultValue)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Equivalent chain using R3.</summary>
    /// <returns>The sum of emitted values.</returns>
    [Benchmark]
    public int R3PrependAppendDefaultIfEmpty()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Append(
                R3.ObservableExtensions.Prepend(
                    R3.ObservableExtensions.DefaultIfEmpty(
                        R3.Observable.Empty<int>(),
                        DefaultValue),
                    PrependedValue),
                AppendedValue)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Default-if-empty over an immediate empty R3 source.</summary>
    /// <returns>The emitted default value.</returns>
    [Benchmark]
    public int R3DefaultIfEmptyEmpty()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.DefaultIfEmpty(R3.Observable.Empty<int>(), DefaultValue)
            .Subscribe(observer);
        return observer.Total;
    }
}
