// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for terminal and collection APIs.
/// </summary>
[MemoryDiagnoser]
public class TerminalCollectionBenchmarks
{
    private const int Count = 32;

    /// <summary>
    /// Benchmarks collecting into a list signal.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCollectList()
    {
        var result = 0;
        using var subscription = Signal.Range(1, Count).CollectList().Subscribe(values => result = values.Count);
        return result;
    }

    /// <summary>
    /// Benchmarks collecting into an array signal.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int PrimitivesCollectArray()
    {
        var result = 0;
        using var subscription = Signal.Range(1, Count).CollectArray().Subscribe(values => result = values.Length);
        return result;
    }

    /// <summary>
    /// Benchmarks asynchronous array collection.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCollectArrayAsync()
    {
        return (await Signal.Range(1, Count).CollectArrayAsync().ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks first-value task conversion.
    /// </summary>
    /// <returns>The first value.</returns>
    [Benchmark]
    public Task<int> PrimitivesFirstAsync() =>
        Signal.Range(1, Count).FirstAsync();

    /// <summary>
    /// Benchmarks last-value task conversion.
    /// </summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public Task<int> PrimitivesToTask() =>
        Signal.Range(1, Count).ToTask();

    /// <summary>
    /// Benchmarks predicate count.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public int PrimitivesCountPredicate()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).Count(static value => value % 2 == 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks all and contains terminal predicates.
    /// </summary>
    /// <returns>The number of true results.</returns>
    [Benchmark]
    public int PrimitivesAllContains()
    {
        var result = 0;
        using var all = Signal.Range(1, Count).All(static value => value > 0).Subscribe(value => result += value ? 1 : 0);
        using var contains = Signal.Range(1, Count).Contains(Count).Subscribe(value => result += value ? 1 : 0);
        return result;
    }
}
