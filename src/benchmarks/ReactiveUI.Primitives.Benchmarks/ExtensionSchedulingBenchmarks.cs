// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using RxBoolSubject = System.Reactive.Subjects.Subject<bool>;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the scheduling extension operators that move values onto a sequencer, delay them, or share a timer.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ExtensionSchedulingBenchmarks
{
    /// <summary>The number of time windows each case drives.</summary>
    private const int Count = 1000;

    /// <summary>The number of values pushed per window by the marshalling cases.</summary>
    private const int BurstSize = 8;

    /// <summary>The number of observers attached to the shared timer.</summary>
    private const int FanOut = 4;

    /// <summary>The number of inline scheduled callbacks run by the ScheduleSafe cases.</summary>
    private const int InlineCount = 64;

    /// <summary>The delay or tick period used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromTicks(1);

    /// <summary>The virtual clock the Primitives shared-timer case keys its cached timer on.</summary>
    private readonly VirtualClock _syncClock = new();

    /// <summary>The historical scheduler the ReactiveUI.Extensions shared-timer case keys its cached timer on.</summary>
    private readonly HistoricalScheduler _syncScheduler = new();

    /// <summary>Benchmarks ObserveOnSafe marshalling a burst of values per window onto a virtual clock.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ObserveOnSafe")]
    public int PrimitivesObserveOnSafe()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ObserveOnSafe(source, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ObserveOnSafe burst marshalling using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark]
    [BenchmarkCategory("ObserveOnSafe")]
    public int PackageObserveOnSafe()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ObserveOnSafe(source, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ObserveOnIf with a fixed true flag, marshalling every burst onto a virtual clock.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ObserveOnIfFlag")]
    public int PrimitivesObserveOnIfFlag()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ObserveOnIf(source, true, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ObserveOnIf with a fixed true flag using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark]
    [BenchmarkCategory("ObserveOnIfFlag")]
    public int PackageObserveOnIfFlag()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = PackageExtensions.ObserveOnIf(source, true, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ObserveOnIf driven by a condition stream that toggles between a virtual clock and inline delivery.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ObserveOnIfCondition")]
    public int PrimitivesObserveOnIfCondition()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using Signal<bool> condition = new();
        using var subscription = PrimitivesExtensions.ObserveOnIf(source, condition, clock, Sequencer.Immediate)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            condition.OnNext((i & 1) == 0);
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks ObserveOnIf driven by a toggling condition stream using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values.</returns>
    [Benchmark]
    [BenchmarkCategory("ObserveOnIfCondition")]
    public int PackageObserveOnIfCondition()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using RxBoolSubject condition = new();
        using var subscription = PackageExtensions.ObserveOnIf(
                source,
                condition,
                scheduler,
                ImmediateScheduler.Instance)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            condition.OnNext((i & 1) == 0);
            for (var j = 0; j < BurstSize; j++)
            {
                source.OnNext(j);
            }

            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks Schedule on a source with a relative delay and a transform, delivering one value per window.</summary>
    /// <returns>The sum of the transformed values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ScheduleSourceDelayed")]
    public int PrimitivesScheduleSourceDelayed()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        IObservable<int> stream = source;
        using var subscription = PrimitivesExtensions.Schedule(stream, Window, clock, static value => value + 1)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks Schedule on a source with a relative delay and a transform using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the transformed values.</returns>
    [Benchmark]
    [BenchmarkCategory("ScheduleSourceDelayed")]
    public int PackageScheduleSourceDelayed()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        IObservable<int> stream = source;
        using var subscription = PackageExtensions.Schedule(stream, Window, scheduler, static value => value + 1)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks Schedule on a source at an absolute due time with a side effect, building one pipeline per window.</summary>
    /// <returns>The sum of the delivered values plus the side-effect tally.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ScheduleSourceAbsolute")]
    public int PrimitivesScheduleSourceAbsolute()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        ValueTally tally = new();
        using Signal<int> source = new();
        IObservable<int> stream = source;
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PrimitivesExtensions.Schedule(stream, clock.Now + Window, clock, tally.Add)
                .Subscribe(observer);
            source.OnNext(i);
            clock.AdvanceBy(Window);
        }

        return observer.Total + tally.Total;
    }

    /// <summary>Benchmarks Schedule on a source at an absolute due time with a side effect using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values plus the side-effect tally.</returns>
    [Benchmark]
    [BenchmarkCategory("ScheduleSourceAbsolute")]
    public int PackageScheduleSourceAbsolute()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        ValueTally tally = new();
        using RxSubject source = new();
        IObservable<int> stream = source;
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PackageExtensions.Schedule(stream, scheduler.Now + Window, scheduler, tally.Add)
                .Subscribe(observer);
            source.OnNext(i);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total + tally.Total;
    }

    /// <summary>Benchmarks Schedule for single values: a delayed emission, a transformed emission and an absolute emission with a side effect.</summary>
    /// <returns>The sum of the delivered values plus the side-effect tally.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ScheduleValue")]
    public int PrimitivesScheduleValue()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        ValueTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            using var delayed = PrimitivesExtensions.Schedule(i, Window, clock).Subscribe(observer);
            using var transformed = PrimitivesExtensions.Schedule(i, clock, static value => value + 1)
                .Subscribe(observer);
            using var absolute = PrimitivesExtensions.Schedule(i, clock.Now + Window, clock, tally.Add)
                .Subscribe(observer);
            clock.AdvanceBy(Window);
        }

        return observer.Total + tally.Total;
    }

    /// <summary>Benchmarks Schedule for single values using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the delivered values plus the side-effect tally.</returns>
    [Benchmark]
    [BenchmarkCategory("ScheduleValue")]
    public int PackageScheduleValue()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        ValueTally tally = new();
        for (var i = 0; i < Count; i++)
        {
            using var delayed = PackageExtensions.Schedule(i, Window, scheduler).Subscribe(observer);
            using var transformed = PackageExtensions.Schedule(i, scheduler, static value => value + 1)
                .Subscribe(observer);
            using var absolute = PackageExtensions.Schedule(i, scheduler.Now + Window, scheduler, tally.Add)
                .Subscribe(observer);
            scheduler.AdvanceBy(Window);
        }

        return observer.Total + tally.Total;
    }

    /// <summary>Benchmarks SyncTimer fanning one shared periodic timer out to several observers, then releasing it.</summary>
    /// <returns>The number of ticks observed across every observer.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SyncTimer")]
    public int PrimitivesSyncTimer()
    {
        CountingSignalWitness<DateTime> observer = new();
        var subscriptions = new IDisposable[FanOut];
        for (var i = 0; i < FanOut; i++)
        {
            subscriptions[i] = PrimitivesExtensions.SyncTimer(Window, _syncClock).Subscribe(observer);
        }

        for (var i = 0; i < Count; i++)
        {
            _syncClock.AdvanceBy(Window);
        }

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }

        return observer.Count;
    }

    /// <summary>Benchmarks SyncTimer fan-out using ReactiveUI.Extensions.</summary>
    /// <returns>The number of ticks observed across every observer.</returns>
    [Benchmark]
    [BenchmarkCategory("SyncTimer")]
    public int PackageSyncTimer()
    {
        CountingSignalWitness<DateTime> observer = new();
        var subscriptions = new IDisposable[FanOut];
        for (var i = 0; i < FanOut; i++)
        {
            subscriptions[i] = PackageExtensions.SyncTimer(Window, _syncScheduler).Subscribe(observer);
        }

        for (var i = 0; i < Count; i++)
        {
            _syncScheduler.AdvanceBy(Window);
        }

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }

        return observer.Count;
    }

    /// <summary>Benchmarks ScheduleSafe without a sequencer, running each zero-delay callback inline.</summary>
    /// <returns>The side-effect tally.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ScheduleSafe")]
    public int PrimitivesScheduleSafeInline()
    {
        ValueTally tally = new();
        for (var i = 0; i < InlineCount; i++)
        {
            using var immediate = PrimitivesExtensions.ScheduleSafe(null, tally.Increment);
            using var delayed = PrimitivesExtensions.ScheduleSafe(null, TimeSpan.Zero, tally.Increment);
        }

        return tally.Total;
    }

    /// <summary>Benchmarks ScheduleSafe without a scheduler using ReactiveUI.Extensions.</summary>
    /// <returns>The side-effect tally.</returns>
    [Benchmark]
    [BenchmarkCategory("ScheduleSafe")]
    public int PackageScheduleSafeInline()
    {
        ValueTally tally = new();
        for (var i = 0; i < InlineCount; i++)
        {
            using var immediate = PackageExtensions.ScheduleSafe(null, tally.Increment);
            using var delayed = PackageExtensions.ScheduleSafe(null, TimeSpan.Zero, tally.Increment);
        }

        return tally.Total;
    }

    /// <summary>Accumulates the values seen by scheduled side effects.</summary>
    private sealed class ValueTally
    {
        /// <summary>Gets the accumulated total.</summary>
        internal int Total { get; private set; }

        /// <summary>Adds a value to the total.</summary>
        /// <param name="value">The value to add.</param>
        internal void Add(int value) => Total += value;

        /// <summary>Adds one to the total.</summary>
        internal void Increment() => Total++;
    }
}
