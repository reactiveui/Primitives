// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the factory operators, across Primitives, System.Reactive,
/// and R3. Delegates to the comparison benchmark methods so the scenarios stay in one place.
/// Opt in with <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
[System.Diagnostics.DebuggerDisplay("FactoryGcProfileBenchmarks: {nameof(FactoryGcProfileBenchmarks),nq}")]
public class FactoryGcProfileBenchmarks
{
    /// <summary>The scalar factory benchmark scenarios delegated to by this profile.</summary>
    private readonly ScalarSignalBenchmarks _scalar = new();

    /// <summary>The general factory benchmark scenarios delegated to by this profile.</summary>
    private readonly FactorySignalBenchmarks _factory = new();

    /// <summary>The enumerable-adaptation benchmark scenarios delegated to by this profile.</summary>
    private readonly FactoryFromEnumerableBenchmarks _fromEnumerable = new();

    /// <summary>Return subscribe (Primitives).</summary>
    /// <returns>The observed value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Return() => _scalar.PrimitivesReturnSubscribe();

    /// <summary>Return subscribe (System.Reactive).</summary>
    /// <returns>The observed value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Return() => _scalar.SystemReactiveReturnSubscribe();

    /// <summary>Return subscribe (R3).</summary>
    /// <returns>The observed value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Return() => _scalar.R3ReturnSubscribe();

    /// <summary>Empty subscribe (Primitives).</summary>
    /// <returns>The completion count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Empty() => _factory.PrimitivesEmptySubscribe();

    /// <summary>Empty subscribe (System.Reactive).</summary>
    /// <returns>The completion count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Empty() => _factory.SystemReactiveEmptySubscribe();

    /// <summary>Empty subscribe (R3).</summary>
    /// <returns>The completion count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Empty() => _factory.R3EmptySubscribe();

    /// <summary>Range subscribe (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Range() => _factory.PrimitivesRangeSubscribe();

    /// <summary>Range subscribe (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Range() => _factory.SystemReactiveRangeSubscribe();

    /// <summary>Range subscribe (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Range() => _factory.R3RangeSubscribe();

    /// <summary>Repeat/Loop subscribe (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Repeat() => _factory.PrimitivesRepeatSubscribe();

    /// <summary>Repeat subscribe (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Repeat() => _factory.SystemReactiveRepeatSubscribe();

    /// <summary>Repeat subscribe (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Repeat() => _factory.R3RepeatSubscribe();

    /// <summary>Throw subscribe (Primitives).</summary>
    /// <returns>The error count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_Throw() => _factory.PrimitivesThrowSubscribe();

    /// <summary>Throw subscribe (System.Reactive).</summary>
    /// <returns>The error count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_Throw() => _factory.SystemReactiveThrowSubscribe();

    /// <summary>Throw subscribe (R3).</summary>
    /// <returns>The error count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_Throw() => _factory.R3ThrowSubscribe();

    /// <summary>FromEnumerable subscribe (Primitives).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Primitives_FromEnumerable() => _fromEnumerable.PrimitivesFromEnumerableSubscribe();

    /// <summary>ToObservable subscribe (System.Reactive).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int Rx_FromEnumerable() => _fromEnumerable.SystemReactiveToObservableSubscribe();

    /// <summary>ToObservable subscribe (R3).</summary>
    /// <returns>The observed total.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Benchmark]
    public int R3_FromEnumerable() => _fromEnumerable.R3ToObservableSubscribe();
}
