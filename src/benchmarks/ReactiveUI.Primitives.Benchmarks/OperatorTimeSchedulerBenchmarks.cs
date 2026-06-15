// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Time.Testing;
using R3;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for time and scheduler operators.</summary>
[MemoryDiagnoser]
public class OperatorTimeSchedulerBenchmarks
{
    /// <summary>The number of values produced by each benchmarked sequence.</summary>
    private const int Count = 16;

    /// <summary>Benchmarks delayed range delivery.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDelayRange()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(1, Count).Shift(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks delayed range delivery using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDelayRange()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(1, Count).Delay(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks delayed range delivery using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DelayRange()
    {
        FakeTimeProvider timeProvider = new();
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Delay(
                R3.Observable.Range(1, Count),
                TimeSpan.FromTicks(1),
                timeProvider)
            .Subscribe(observer);
        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks delayed subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesDelayStartRange()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(1, Count).DelayStart(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks delayed subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveDelayStartRange()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(1, Count)
            .DelaySubscription(TimeSpan.FromTicks(1), scheduler)
            .Subscribe(observer);
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks delayed subscription using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3DelayStartRange()
    {
        FakeTimeProvider timeProvider = new();
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.DelaySubscription(
                R3.Observable.Range(1, Count),
                TimeSpan.FromTicks(1),
                timeProvider)
            .Subscribe(observer);
        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks throttle over a burst.</summary>
    /// <returns>The last observed value.</returns>
    [Benchmark]
    public int PrimitivesThrottleBurst()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Calm(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.LastValue;
    }

    /// <summary>Benchmarks throttle over a burst using System.Reactive.</summary>
    /// <returns>The last observed value.</returns>
    [Benchmark]
    public int SystemReactiveThrottleBurst()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.Throttle(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.LastValue;
    }

    /// <summary>Benchmarks debounce over a burst using R3.</summary>
    /// <returns>The last observed value.</returns>
    [Benchmark]
    public int R3ThrottleBurst()
    {
        FakeTimeProvider timeProvider = new();
        IntR3Witness observer = new();
        using Subject<int> source = new();
        using var subscription = R3.ObservableExtensions.Debounce(source, TimeSpan.FromTicks(1), timeProvider)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.LastValue;
    }

    /// <summary>Benchmarks sampling the latest value.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesSampleLatest()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Probe(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        source.OnNext(Count);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks sampling the latest value using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveSampleLatest()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.Sample(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        source.OnNext(Count);
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks sampling the latest value using R3 throttle-last semantics.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3SampleLatest()
    {
        FakeTimeProvider timeProvider = new();
        IntR3Witness observer = new();
        using Subject<int> source = new();
        using var subscription = R3.ObservableExtensions.ThrottleLast(source, TimeSpan.FromTicks(1), timeProvider)
            .Subscribe(observer);
        source.OnNext(Count);
        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.Total;
    }

    /// <summary>Benchmarks timestamp projection.</summary>
    /// <returns>The number of timestamps observed.</returns>
    [Benchmark]
    public int PrimitivesTimestampRange()
    {
        CountingSignalWitness<Moment<int>> observer = new();
        using var subscription = Signal.Sequence(1, Count).Timestamp(Sequencer.Immediate).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks timestamp projection using System.Reactive.</summary>
    /// <returns>The number of timestamps observed.</returns>
    [Benchmark]
    public int SystemReactiveTimestampRange()
    {
        CountingSignalWitness<Timestamped<int>> observer = new();
        using var subscription = RxObservable.Range(1, Count).Timestamp(ImmediateScheduler.Instance).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks timestamp projection using R3.</summary>
    /// <returns>The number of timestamps observed.</returns>
    [Benchmark]
    public int R3TimestampRange()
    {
        CountingR3Witness<(long Timestamp, int Value)> observer = new();
        using var subscription = R3.ObservableExtensions.Timestamp(R3.Observable.Range(1, Count)).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks time-interval projection.</summary>
    /// <returns>The number of intervals observed.</returns>
    [Benchmark]
    public int PrimitivesTimeIntervalRange()
    {
        CountingSignalWitness<Core.TimeInterval<int>> observer = new();
        using var subscription = Signal.Sequence(1, Count).TimeInterval(Sequencer.Immediate).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks time-interval projection using System.Reactive.</summary>
    /// <returns>The number of intervals observed.</returns>
    [Benchmark]
    public int SystemReactiveTimeIntervalRange()
    {
        CountingSignalWitness<System.Reactive.TimeInterval<int>> observer = new();
        using var subscription = RxObservable.Range(1, Count)
            .TimeInterval(ImmediateScheduler.Instance)
            .Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks time-interval projection using R3.</summary>
    /// <returns>The number of intervals observed.</returns>
    [Benchmark]
    public int R3TimeIntervalRange()
    {
        CountingR3Witness<(TimeSpan Interval, int Value)> observer = new();
        using var subscription = R3.ObservableExtensions.TimeInterval(R3.Observable.Range(1, Count)).Subscribe(observer);
        return observer.Count;
    }

    /// <summary>Benchmarks timeout error delivery after a source becomes idle.</summary>
    /// <returns>The number of timeout errors observed.</returns>
    [Benchmark]
    public int PrimitivesTimeoutIdle()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Expire(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        source.OnNext(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.ErrorCount;
    }

    /// <summary>Benchmarks timeout error delivery after a source becomes idle using System.Reactive.</summary>
    /// <returns>The number of timeout errors observed.</returns>
    [Benchmark]
    public int SystemReactiveTimeoutIdle()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = source.Timeout(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        source.OnNext(0);
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.ErrorCount;
    }

    /// <summary>Benchmarks timeout error delivery after a source becomes idle using R3.</summary>
    /// <returns>The number of timeout errors observed.</returns>
    [Benchmark]
    public int R3TimeoutIdle()
    {
        FakeTimeProvider timeProvider = new();
        IntR3Witness observer = new();
        using Subject<int> source = new();
        using var subscription = R3.ObservableExtensions.Timeout(
                source,
                TimeSpan.FromTicks(1),
                timeProvider)
            .Subscribe(observer);
        source.OnNext(0);
        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.ErrorCount;
    }

    /// <summary>Benchmarks immediate observe-on dispatch.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesObserveOnImmediate()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(1, Count).ObserveOn(Sequencer.Immediate).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks immediate observe-on dispatch using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveObserveOnImmediate()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Range(1, Count).ObserveOn(ImmediateScheduler.Instance).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks immediate observe-on dispatch using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ObserveOnImmediate()
    {
        IntR3Witness observer = new();
        using ImmediateSynchronizationContext context = new();
        using var subscription = R3.ObservableExtensions.ObserveOn(R3.Observable.Range(1, Count), context).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>A synchronization context that invokes callbacks synchronously on the calling thread.</summary>
    private sealed class ImmediateSynchronizationContext : SynchronizationContext, IDisposable
    {
        /// <summary>Invokes the callback synchronously.</summary>
        /// <param name="d">The callback to invoke.</param>
        /// <param name="state">The state passed to the callback.</param>
        public override void Post(SendOrPostCallback d, object? state) => d(state);

        /// <summary>Invokes the callback synchronously.</summary>
        /// <param name="d">The callback to invoke.</param>
        /// <param name="state">The state passed to the callback.</param>
        public override void Send(SendOrPostCallback d, object? state) => d(state);

        /// <summary>Releases the resources used by the synchronization context.</summary>
        public void Dispose()
        {
        }
    }
}
