// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation profile for the stateful single-source operators converted to dedicated
/// single-source sinks. Pairs with <see cref="OperatorStatefulFilterBenchmarks"/>; the
/// EventPipe trace captures per-subscription allocations on the subscribe-and-drain path.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class OperatorStatefulFilterGcProfileBenchmarks
{
    /// <summary>The starting value of each benchmarked sequence.</summary>
    private const int StartValue = 0;

    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int RangeCount = 1024;

    /// <summary>The number of leading values skipped or compared by the benchmarks.</summary>
    private const int SkipCount = 8;

    /// <summary>The divisor used by the key-selector benchmarks.</summary>
    private const int KeyDivisor = 2;

    /// <summary>Subscribe-and-drain through Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Skip()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Skip(SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through Unique.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Unique()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Unique()
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through UniqueBy.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int UniqueBy()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .UniqueBy(static x => x / KeyDivisor)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through Fold.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Fold()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Fold(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through Reduce.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Reduce()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Reduce(0, static (acc, x) => acc + x)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through TakeWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int TakeWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .TakeWhile(static x => x < (RangeCount - SkipCount))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SkipWhile()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .SkipWhile(static x => x < SkipCount)
            .Subscribe(observer);
        return observer.Total;
    }
}
