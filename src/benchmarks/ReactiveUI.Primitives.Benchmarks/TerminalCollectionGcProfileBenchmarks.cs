// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation baselines for the terminal/collection operators across Primitives,
/// System.Reactive, and R3. Delegates to <see cref="TerminalCollectionBenchmarks"/>. Opt in with
/// <c>--filter "*GcProfile*"</c>.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class TerminalCollectionGcProfileBenchmarks
{
    /// <summary>
    /// The delegate benchmark instance that performs the measured work.
    /// </summary>
    private readonly TerminalCollectionBenchmarks _b = new();

    /// <summary>ToList (Primitives).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int Primitives_CollectList() => _b.PrimitivesCollectList();

    /// <summary>ToList (System.Reactive).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int Rx_CollectList() => _b.SystemReactiveCollectList();

    /// <summary>ToList (R3).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public Task<int> R3_CollectList() => _b.R3CollectList();

    /// <summary>ToArray (Primitives).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int Primitives_CollectArray() => _b.PrimitivesCollectArray();

    /// <summary>ToArray (System.Reactive).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int Rx_CollectArray() => _b.SystemReactiveCollectArray();

    /// <summary>ToArray (R3).</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public Task<int> R3_CollectArray() => _b.R3CollectArray();

    /// <summary>Count(predicate) (Primitives).</summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public int Primitives_CountPredicate() => _b.PrimitivesCountPredicate();

    /// <summary>Count(predicate) (System.Reactive).</summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public int Rx_CountPredicate() => _b.SystemReactiveCountPredicate();

    /// <summary>Count(predicate) (R3).</summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public Task<int> R3_CountPredicate() => _b.R3CountPredicate();

    /// <summary>All (Primitives).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public int Primitives_All() => _b.PrimitivesAllRange();

    /// <summary>All (System.Reactive).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public int Rx_All() => _b.SystemReactiveAllRange();

    /// <summary>All (R3).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public Task<int> R3_All() => _b.R3AllRange();

    /// <summary>Contains (Primitives).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public int Primitives_Contains() => _b.PrimitivesContainsRange();

    /// <summary>Contains (System.Reactive).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public int Rx_Contains() => _b.SystemReactiveContainsRange();

    /// <summary>Contains (R3).</summary>
    /// <returns>The boolean result as an int.</returns>
    [Benchmark]
    public Task<int> R3_Contains() => _b.R3ContainsRange();
}
