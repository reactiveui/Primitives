// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for time and scheduler operators.
/// </summary>
[MemoryDiagnoser]
public class OperatorTimeSchedulerBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks delayed range delivery.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDelayRange()
    {
        var clock = new TestClock();
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).Delay(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks delayed subscription.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesDelayStartRange()
    {
        var clock = new TestClock();
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).DelayStart(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks throttle over a burst.
    /// </summary>
    /// <returns>The last observed value.</returns>
    [Benchmark]
    public int PrimitivesThrottleBurst()
    {
        var clock = new TestClock();
        var observer = new IntSignalObserver();
        using var source = new Signal<int>();
        using var subscription = source.Throttle(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.LastValue;
    }

    /// <summary>
    /// Benchmarks sampling the latest value.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSampleLatest()
    {
        var clock = new TestClock();
        var observer = new IntSignalObserver();
        using var source = new Signal<int>();
        using var subscription = source.Sample(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        source.OnNext(Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks timestamp projection.
    /// </summary>
    /// <returns>The number of timestamps observed.</returns>
    [Benchmark]
    public int PrimitivesTimestampRange()
    {
        var count = 0;
        using var subscription = Signal.Range(1, Count).Timestamp(Sequencer.Immediate).Subscribe(_ => count++);
        return count;
    }

    /// <summary>
    /// Benchmarks time-interval projection.
    /// </summary>
    /// <returns>The number of intervals observed.</returns>
    [Benchmark]
    public int PrimitivesTimeIntervalRange()
    {
        var count = 0;
        using var subscription = Signal.Range(1, Count).TimeInterval(Sequencer.Immediate).Subscribe(_ => count++);
        return count;
    }

    /// <summary>
    /// Benchmarks timeout error delivery.
    /// </summary>
    /// <returns>The number of timeout errors observed.</returns>
    [Benchmark]
    public int PrimitivesTimeoutNever()
    {
        var clock = new TestClock();
        var observer = new IntSignalObserver();
        using var subscription = Signal.Never<int>().Timeout(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.ErrorCount;
    }

    /// <summary>
    /// Benchmarks immediate observe-on dispatch.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesObserveOnImmediate()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).ObserveOn(Sequencer.Immediate).Subscribe(observer);
        return observer.Total;
    }
}
