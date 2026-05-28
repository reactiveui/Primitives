// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using R3;

using R3ReplaySubject = R3.ReplaySubject<int>;
using RxReplaySubject = System.Reactive.Subjects.ReplaySubject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks history/snapshot behavior for bounded replay buffers.
/// </summary>
[MemoryDiagnoser]
public class HistorySignalBenchmarks
{
    /// <summary>
    /// Baseline bounded replay subscription benchmark for primitives.
    /// </summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesHistorySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subject = new HistorySignal<int>(16);
        PopulateHistorySignal(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Bounded replay subscription benchmark for System.Reactive.
    /// </summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark]
    public int SystemReactiveReplaySubscribe()
    {
        var observer = new IntSignalObserver();
        using var subject = new RxReplaySubject(16);
        PopulateReplaySubject(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Bounded replay subscription benchmark for R3.
    /// </summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark]
    public int R3ReplaySubscribe()
    {
        var observer = new IntR3Observer();
        using var subject = new R3ReplaySubject(16);
        PopulateReplaySubject(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    private static void PopulateHistorySignal(HistorySignal<int> subject)
    {
        for (var i = 0; i < 16; i++)
        {
            subject.OnNext(i);
        }
    }

    private static void PopulateReplaySubject(RxReplaySubject subject)
    {
        for (var i = 0; i < 16; i++)
        {
            subject.OnNext(i);
        }
    }

    private static void PopulateReplaySubject(R3ReplaySubject subject)
    {
        for (var i = 0; i < 16; i++)
        {
            subject.OnNext(i);
        }
    }
}
