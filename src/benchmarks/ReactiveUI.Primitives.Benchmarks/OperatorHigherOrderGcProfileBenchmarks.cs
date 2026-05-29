// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the higher-order/combining operators across Primitives,
/// System.Reactive, and R3. Delegates to <see cref="OperatorHigherOrderBenchmarks"/>. Opt in with
/// <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class OperatorHigherOrderGcProfileBenchmarks
{
    private readonly OperatorHigherOrderBenchmarks _b = new();

    /// <summary>Concat (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Concat() => _b.PrimitivesConcatRanges();

    /// <summary>Concat (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Concat() => _b.SystemReactiveConcatRanges();

    /// <summary>Concat (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Concat() => _b.R3ConcatRanges();

    /// <summary>Merge/Blend (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Merge() => _b.PrimitivesMergeRanges();

    /// <summary>Merge (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Merge() => _b.SystemReactiveMergeRanges();

    /// <summary>Merge (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Merge() => _b.R3MergeRanges();

    /// <summary>Race (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Race() => _b.PrimitivesRaceRanges();

    /// <summary>Race/Amb (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Race() => _b.SystemReactiveRaceRanges();

    /// <summary>Race (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Race() => _b.R3RaceRanges();

    /// <summary>Switch (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_Switch() => _b.PrimitivesSwitchRanges();

    /// <summary>Switch (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_Switch() => _b.SystemReactiveSwitchRanges();

    /// <summary>Switch (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_Switch() => _b.R3SwitchRanges();

    /// <summary>CombineLatest (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_CombineLatest() => _b.PrimitivesCombineLatestRanges();

    /// <summary>CombineLatest (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_CombineLatest() => _b.SystemReactiveCombineLatestRanges();

    /// <summary>CombineLatest (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_CombineLatest() => _b.R3CombineLatestRanges();

    /// <summary>WithLatestFrom (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_WithLatest() => _b.PrimitivesWithLatestRanges();

    /// <summary>WithLatestFrom (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_WithLatest() => _b.SystemReactiveWithLatestRanges();

    /// <summary>WithLatestFrom (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_WithLatest() => _b.R3WithLatestRanges();

    /// <summary>ForkJoin (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Primitives_ForkJoin() => _b.PrimitivesForkJoinRanges();

    /// <summary>ForkJoin/Zip-final (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Rx_ForkJoin() => _b.SystemReactiveForkJoinRanges();

    /// <summary>ForkJoin (R3).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3_ForkJoin() => _b.R3ForkJoinRanges();
}
