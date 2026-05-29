// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the core projection/combination operators across Primitives,
/// System.Reactive, and R3. Delegates to the comparison benchmark methods. Opt in with
/// <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class OperatorCoreGcProfileBenchmarks
{
    private readonly OperatorMapKeepBenchmarks _mapKeep = new();
    private readonly OperatorFlatMapRangeBenchmarks _flatMap = new();
    private readonly OperatorZipBenchmarks _zip = new();
    private readonly OperatorStartWithAppendDefaultIfEmptyBenchmarks _startWith = new();

    /// <summary>Map+Keep chain (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_MapKeep() => _mapKeep.PrimitivesRangeMapKeep();

    /// <summary>Select+Where chain (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_MapKeep() => _mapKeep.SystemReactiveRangeSelectWhere();

    /// <summary>Select+Where chain (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_MapKeep() => _mapKeep.R3RangeSelectWhere();

    /// <summary>DistinctBy+Count and Any (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_AggregateAnyCount() => _mapKeep.PrimitivesAggregateAnyCount();

    /// <summary>Distinct+Count and Any (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_AggregateAnyCount() => _mapKeep.SystemReactiveAggregateAnyCount();

    /// <summary>Distinct+Count and Any (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public Task<int> R3_AggregateAnyCount() => _mapKeep.R3AggregateAnyCount();

    /// <summary>FlatMap over ranges (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_FlatMap() => _flatMap.PrimitivesFlatMapRange();

    /// <summary>SelectMany over ranges (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_FlatMap() => _flatMap.SystemReactiveSelectManyRange();

    /// <summary>SelectMany over ranges (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_FlatMap() => _flatMap.R3SelectManyRange();

    /// <summary>Zip (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Zip() => _zip.PrimitivesZip();

    /// <summary>Zip (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Zip() => _zip.SystemReactiveZip();

    /// <summary>Zip (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Zip() => _zip.R3Zip();

    /// <summary>Prepend+Append+DefaultIfEmpty (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_StartWithAppend() => _startWith.PrimitivesStartWithAppendDefaultIfEmpty();

    /// <summary>StartWith+Append+DefaultIfEmpty (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_StartWithAppend() => _startWith.SystemReactiveStartWithAppendDefaultIfEmpty();

    /// <summary>Prepend+Append+DefaultIfEmpty (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_StartWithAppend() => _startWith.R3PrependAppendDefaultIfEmpty();
}
