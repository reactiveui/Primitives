// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the stream-health extension operators (heartbeat, staleness, conflation, idle buffering) over virtual time.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ExtensionStreamHealthBenchmarks
{
    /// <summary>The number of time windows each case drives.</summary>
    private const int Count = 1000;

    /// <summary>The number of values pushed into each idle buffer burst.</summary>
    private const int BurstSize = 4;

    /// <summary>The number of windows between source updates in the staleness cases.</summary>
    private const int UpdateInterval = 3;

    /// <summary>The heartbeat, staleness, conflation or idle period used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(1);

    /// <summary>Benchmarks Heartbeat on a source that updates every other window, so quiet windows emit heartbeats.</summary>
    /// <returns>The number of updates and heartbeats observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Heartbeat")]
    public int PrimitivesHeartbeat()
    {
        VirtualClock clock = new();
        CountingSignalWitness<Heartbeat<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.Heartbeat(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            if ((i & 1) == 0)
            {
                source.OnNext(i);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks Heartbeat on a source that updates every other window using ReactiveUI.Extensions.</summary>
    /// <returns>The number of updates and heartbeats observed.</returns>
    [Benchmark]
    [BenchmarkCategory("Heartbeat")]
    public int PackageHeartbeat()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Heartbeat<int>> observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.Heartbeat(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            if ((i & 1) == 0)
            {
                source.OnNext(i);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks DetectStale on a source that updates every third window, so the gaps emit stale markers.</summary>
    /// <returns>The number of updates and stale markers observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DetectStale")]
    public int PrimitivesDetectStale()
    {
        VirtualClock clock = new();
        CountingSignalWitness<Stale<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.DetectStale(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            if (i % UpdateInterval == 0)
            {
                source.OnNext(i);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks DetectStale on a source that updates every third window using ReactiveUI.Extensions.</summary>
    /// <returns>The number of updates and stale markers observed.</returns>
    [Benchmark]
    [BenchmarkCategory("DetectStale")]
    public int PackageDetectStale()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Stale<int>> observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.DetectStale(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            if (i % UpdateInterval == 0)
            {
                source.OnNext(i);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks Conflate with two values per window, deferring the second to the end of the interval.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Conflate")]
    public int PrimitivesConflate()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.Conflate(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            source.OnNext(i + 1);
            clock.AdvanceBy(Window);
        }

        source.OnCompleted();
        clock.AdvanceBy(Window);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks Conflate with two values per window using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("Conflate")]
    public int PackageConflate()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.Conflate(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            source.OnNext(i + 1);
            scheduler.AdvanceBy(Window);
        }

        source.OnCompleted();
        scheduler.AdvanceBy(Window);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks BufferUntilIdle, collecting a burst of values into one list per quiet window.</summary>
    /// <returns>The number of buffered lists emitted.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("BufferUntilIdle")]
    public int PrimitivesBufferUntilIdle()
    {
        VirtualClock clock = new();
        CountingSignalWitness<IList<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.BufferUntilIdle(source, Window, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks BufferUntilIdle burst collection using ReactiveUI.Extensions.</summary>
    /// <returns>The number of buffered lists emitted.</returns>
    [Benchmark]
    [BenchmarkCategory("BufferUntilIdle")]
    public int PackageBufferUntilIdle()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<IList<int>> observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.BufferUntilIdle(source, Window, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Count;
    }
}
