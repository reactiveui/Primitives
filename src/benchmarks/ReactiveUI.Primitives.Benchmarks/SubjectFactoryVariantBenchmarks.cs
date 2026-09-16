// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the specialised subject signals: scheduled, serialized, delayable and priority-semaphore.</summary>
[MemoryDiagnoser]
public class SubjectFactoryVariantBenchmarks
{
    /// <summary>The number of values pushed through each case.</summary>
    private const int Count = 64;

    /// <summary>The priority-semaphore capacity.</summary>
    private const int Capacity = 4;

    /// <summary>The raised priority-semaphore capacity.</summary>
    private const int RaisedCapacity = 8;

    /// <summary>The virtual time advanced to drain scheduled deliveries.</summary>
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    /// <summary>Pushes values through a scheduled signal and drains them on a virtual clock.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesScheduledSignal()
    {
        VirtualClock clock = new();
        IntSignalWitness observer = new();
        using var signal = Signal.Scheduled<int>(clock);
        using var subscription = signal.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i);
        }

        clock.AdvanceBy(Tick);
        return observer.Total;
    }

    /// <summary>Pushes values through a subject observed on a historical scheduler using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveObserveOnSubject()
    {
        HistoricalScheduler scheduler = new();
        IntSignalWitness observer = new();
        using RxSubject subject = new();
        using var subscription = RxObservable.ObserveOn(subject, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        scheduler.AdvanceBy(Tick);
        return observer.Total;
    }

    /// <summary>Hands a scheduled signal from its default observer to a subscriber and back.</summary>
    /// <returns>The combined total observed by the default observer and the subscriber.</returns>
    [Benchmark]
    public int PrimitivesScheduledSignalDefaultObserverHandOff()
    {
        VirtualClock clock = new();
        IntSignalWitness fallback = new();
        IntSignalWitness observer = new();
        using var signal = Signal.Scheduled(clock, fallback);
        for (var i = 0; i < Count; i++)
        {
            var subscription = signal.Subscribe(observer);
            signal.OnNext(i);
            clock.AdvanceBy(Tick);
            subscription.Dispose();
            signal.OnNext(i);
            clock.AdvanceBy(Tick);
        }

        return fallback.Total + observer.Total;
    }

    /// <summary>Pushes values through a serialized signal and a serialized wrapper.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesSerializedSignal()
    {
        IntSignalWitness observer = new();
        using var owned = Signal.Serialized<int>();
        using Signal<int> inner = new();
        using var wrapped = Signal.Serialized(inner);
        using var ownedSubscription = owned.Subscribe(observer);
        using var wrappedSubscription = wrapped.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            owned.OnNext(i);
            wrapped.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Pushes values through synchronized subjects using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveSynchronizedSubject()
    {
        IntSignalWitness observer = new();
        using RxSubject first = new();
        using RxSubject second = new();
        var owned = System.Reactive.Subjects.Subject.Synchronize(first);
        var wrapped = System.Reactive.Subjects.Subject.Synchronize(second);
        using var ownedSubscription = owned.Subscribe(observer);
        using var wrappedSubscription = wrapped.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            owned.OnNext(i);
            wrapped.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Buffers values while delayed and flushes a de-duplicated batch.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesDelayableFlush()
    {
        IntSignalWitness observer = new();
        var delayed = true;
        using var signal = Signal.Delayable<int>(() => delayed, DistinctBatch);
        using var subscription = signal.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i >> 1);
        }

        signal.Flush();
        delayed = false;
        signal.OnNext(Count);
        return observer.Total;
    }

    /// <summary>Queues values behind a priority semaphore and releases every slot.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesPrioritySemaphoreRelease()
    {
        IntSignalWitness observer = new();
        using PrioritySemaphoreSignal<int> signal = new(Capacity);
        using var subscription = signal.Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(Count - i);
        }

        for (var i = 0; i < Count; i++)
        {
            signal.Release();
        }

        signal.MaximumCount = RaisedCapacity;
        signal.OnNext(Count);
        signal.OnCompleted();
        return observer.Total;
    }

    /// <summary>De-duplicates a buffered batch, keeping first occurrences in order.</summary>
    /// <param name="batch">The buffered batch.</param>
    /// <returns>The distinct values.</returns>
    private static List<int> DistinctBatch(IList<int> batch)
    {
        HashSet<int> seen = [];
        List<int> distinct = [];
        for (var i = 0; i < batch.Count; i++)
        {
            if (seen.Add(batch[i]))
            {
                distinct.Add(batch[i]);
            }
        }

        return distinct;
    }
}
