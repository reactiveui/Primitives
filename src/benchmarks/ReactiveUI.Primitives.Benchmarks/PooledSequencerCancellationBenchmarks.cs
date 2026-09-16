// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using RxDisposable = System.Reactive.Disposables.Disposable;
using RxThreadPoolScheduler = System.Reactive.Concurrency.ThreadPoolScheduler;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures scheduling work on shared background sequencers and cancelling it before it runs.</summary>
[MemoryDiagnoser]
public class PooledSequencerCancellationBenchmarks
{
    /// <summary>The number of work items scheduled and cancelled per case.</summary>
    private const int Count = 256;

    /// <summary>A delay long enough that no scheduled item becomes due during a run.</summary>
    private static readonly TimeSpan FarFuture = TimeSpan.FromHours(1);

    /// <summary>Benchmarks arming delayed thread-pool work and cancelling it, as a timeout that never fires.</summary>
    /// <returns>The number of cancelled work items.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesThreadPoolDelayedCancel()
    {
        StrongBox<int> executed = new();
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            ThreadPoolSequencer.Instance.Schedule(executed, FarFuture, static box => box.Value++).Dispose();
            cancelled++;
        }

        return cancelled - executed.Value;
    }

    /// <summary>Benchmarks arming delayed thread-pool work and cancelling it using System.Reactive.</summary>
    /// <returns>The number of cancelled work items.</returns>
    [Benchmark]
    public int SystemReactiveThreadPoolDelayedCancel()
    {
        StrongBox<int> executed = new();
        var cancelled = 0;
        for (var i = 0; i < Count; i++)
        {
            RxThreadPoolScheduler.Instance.Schedule(executed, FarFuture, static (_, box) =>
            {
                box.Value++;
                return RxDisposable.Empty;
            }).Dispose();
            cancelled++;
        }

        return cancelled - executed.Value;
    }

    /// <summary>Benchmarks queueing event-loop work on the WebAssembly sequencer and cancelling it before its drain.</summary>
    /// <returns>The number of cancelled work items.</returns>
    [Benchmark]
    public int PrimitivesWasmReadyCancel()
    {
        StrongBox<int> executed = new();
        var cancelled = 0;
        var sequencer = WasmSequencer.Default;
        for (var i = 0; i < Count; i++)
        {
            sequencer.Schedule(executed, static box => box.Value++).Dispose();
            cancelled++;
        }

        return cancelled + (sequencer.Now > DateTimeOffset.MinValue ? 1 : 0) + (sequencer.Timestamp > 0 ? 1 : 0);
    }
}
