// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose allocation profile for the stateful single-source operators converted to dedicated
/// SingleSourceObserver sinks. Pairs with <see cref="OperatorStatefulFilterBenchmarks"/>; the
/// EventPipe trace captures per-subscription allocations on the subscribe-and-drain path.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class OperatorStatefulFilterGcProfileBenchmarks
{
    private const int StartValue = 0;
    private const int RangeCount = 1024;

    /// <summary>Subscribe-and-drain through Skip.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Skip()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .Skip(8)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through Unique.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Unique()
    {
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .UniqueBy(static x => x / 2)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through Fold.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int Fold()
    {
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
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
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .TakeWhile(static x => x < (RangeCount - 8))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribe-and-drain through SkipWhile.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SkipWhile()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(StartValue, RangeCount)
            .SkipWhile(static x => x < 8)
            .Subscribe(observer);
        return observer.Total;
    }
}
