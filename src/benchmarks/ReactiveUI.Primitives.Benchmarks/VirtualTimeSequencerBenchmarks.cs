// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using RxHistoricalScheduler = System.Reactive.Concurrency.HistoricalScheduler;
using RxVirtualTimeSchedulerExtensions = System.Reactive.Concurrency.VirtualTimeSchedulerExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures queueing relative and absolute actions on virtual-time schedulers and advancing through them.</summary>
[MemoryDiagnoser]
public class VirtualTimeSequencerBenchmarks
{
    /// <summary>The number of actions queued per case.</summary>
    private const int Count = 1000;

    /// <summary>Benchmarks a tick-based virtual-time sequencer with relative and absolute actions.</summary>
    /// <returns>The number of executed actions plus the final clock.</returns>
    [Benchmark(Baseline = true)]
    public long PrimitivesTickVirtualTimeSequencer()
    {
        var executed = 0;
        Action increment = () => executed++;
        VirtualTimeSequencer<long, long> sequencer = new(
            0L,
            Comparer<long>.Default,
            static (absolute, relative) => absolute + relative,
            static absolute => new DateTimeOffset(absolute, TimeSpan.Zero),
            static span => span.Ticks);
        for (var i = 1; i <= Count; i++)
        {
            _ = sequencer.ScheduleRelative((long)i, increment);
            _ = sequencer.ScheduleAbsolute((long)i, increment);
        }

        sequencer.AdvanceBy(Count);
        return executed + sequencer.Clock;
    }

    /// <summary>Benchmarks a tick-based virtual-time scheduler with relative and absolute actions using System.Reactive.</summary>
    /// <returns>The number of executed actions plus the final clock.</returns>
    [Benchmark]
    public long SystemReactiveTickVirtualTimeScheduler()
    {
        var executed = 0;
        Action increment = () => executed++;
        TickVirtualTimeScheduler scheduler = new();
        for (var i = 1; i <= Count; i++)
        {
            _ = RxVirtualTimeSchedulerExtensions.ScheduleRelative(scheduler, (long)i, increment);
            _ = RxVirtualTimeSchedulerExtensions.ScheduleAbsolute(scheduler, (long)i, increment);
        }

        scheduler.AdvanceBy(Count);
        return executed + scheduler.Clock;
    }

    /// <summary>Benchmarks the date-based virtual clock with relative and absolute actions.</summary>
    /// <returns>The number of executed actions.</returns>
    [Benchmark]
    public int PrimitivesVirtualClockActions()
    {
        var executed = 0;
        Action increment = () => executed++;
        VirtualClock clock = new();
        var start = clock.Clock;
        for (var i = 1; i <= Count; i++)
        {
            _ = clock.ScheduleRelative(TimeSpan.FromTicks(i), increment);
            _ = clock.ScheduleAbsolute(start.AddTicks(i), increment);
        }

        clock.AdvanceBy(TimeSpan.FromTicks(Count));
        return executed;
    }

    /// <summary>Benchmarks the date-based historical scheduler with relative and absolute actions using System.Reactive.</summary>
    /// <returns>The number of executed actions.</returns>
    [Benchmark]
    public int SystemReactiveHistoricalSchedulerActions()
    {
        var executed = 0;
        Action increment = () => executed++;
        RxHistoricalScheduler scheduler = new();
        var start = scheduler.Clock;
        for (var i = 1; i <= Count; i++)
        {
            _ = RxVirtualTimeSchedulerExtensions.ScheduleRelative(scheduler, TimeSpan.FromTicks(i), increment);
            _ = RxVirtualTimeSchedulerExtensions.ScheduleAbsolute(scheduler, start.AddTicks(i), increment);
        }

        scheduler.AdvanceBy(TimeSpan.FromTicks(Count));
        return executed;
    }

    /// <summary>A System.Reactive virtual-time scheduler measured in ticks.</summary>
    private sealed class TickVirtualTimeScheduler : System.Reactive.Concurrency.VirtualTimeScheduler<long, long>
    {
        /// <summary>Initializes a new instance of the <see cref="TickVirtualTimeScheduler"/> class.</summary>
        public TickVirtualTimeScheduler()
            : base(0L, Comparer<long>.Default)
        {
        }

        /// <inheritdoc/>
        protected override long Add(long absolute, long relative) => absolute + relative;

        /// <inheritdoc/>
        protected override DateTimeOffset ToDateTimeOffset(long absolute) => new(absolute, TimeSpan.Zero);

        /// <inheritdoc/>
        protected override long ToRelative(TimeSpan timeSpan) => timeSpan.Ticks;
    }
}
