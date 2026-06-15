// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using R3ReplaySubject = R3.ReplaySubject<int>;
using RxReplaySubject = System.Reactive.Subjects.ReplaySubject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks history/snapshot behavior for bounded replay buffers.</summary>
[MemoryDiagnoser]
public class ReplaySignalBenchmarks
{
    /// <summary>The bounded replay buffer size and the number of values populated into each subject.</summary>
    private const int BufferSize = 16;

    /// <summary>Baseline bounded replay subscription benchmark for primitives.</summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesHistorySubscribe()
    {
        IntSignalWitness observer = new();
        using ReplaySignal<int> subject = new(BufferSize);
        PopulateReplaySignal(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Bounded replay subscription benchmark for System.Reactive.</summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark]
    public int SystemReactiveReplaySubscribe()
    {
        IntSignalWitness observer = new();
        using RxReplaySubject subject = new(BufferSize);
        PopulateReplaySubject(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Bounded replay subscription benchmark for R3.</summary>
    /// <returns>The sum replayed to a late subscriber.</returns>
    [Benchmark]
    public int R3ReplaySubscribe()
    {
        IntR3Witness observer = new();
        using R3ReplaySubject subject = new(BufferSize);
        PopulateReplaySubject(subject);
        using var subscription = subject.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Populates the bounded primitives history signal with the buffered values.</summary>
    /// <param name="subject">The history signal to populate.</param>
    private static void PopulateReplaySignal(ReplaySignal<int> subject)
    {
        for (var i = 0; i < BufferSize; i++)
        {
            subject.OnNext(i);
        }
    }

    /// <summary>Populates the System.Reactive replay subject with the buffered values.</summary>
    /// <param name="subject">The replay subject to populate.</param>
    private static void PopulateReplaySubject(RxReplaySubject subject)
    {
        for (var i = 0; i < BufferSize; i++)
        {
            subject.OnNext(i);
        }
    }

    /// <summary>Populates the R3 replay subject with the buffered values.</summary>
    /// <param name="subject">The replay subject to populate.</param>
    private static void PopulateReplaySubject(R3ReplaySubject subject)
    {
        for (var i = 0; i < BufferSize; i++)
        {
            subject.OnNext(i);
        }
    }
}
