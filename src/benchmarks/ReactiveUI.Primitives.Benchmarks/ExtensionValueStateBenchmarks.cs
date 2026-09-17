// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageObserverExtensions = ReactiveUI.Extensions.ObserverExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesObserverExtensions = ReactiveUI.Primitives.Extensions.ObserverExtensions;
using R3IntBehaviorSubject = R3.BehaviorSubject<int>;
using RxIntBehaviorSubject = System.Reactive.Subjects.BehaviorSubject<int>;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures current-value subjects, single-value sources, observer fan-out helpers, property bridges and timed state markers.</summary>
[MemoryDiagnoser]
public class ExtensionValueStateBenchmarks
{
    /// <summary>The number of values, subscriptions or ticks in each case.</summary>
    private const int Count = 256;

    /// <summary>The number of observers attached in the fan-out cases.</summary>
    private const int ObserverCount = 4;

    /// <summary>The number of time windows advanced between value bursts.</summary>
    private const int QuietEvery = 4;

    /// <summary>The period of the heartbeat and staleness windows.</summary>
    private static readonly TimeSpan Period = TimeSpan.FromTicks(1);

    /// <summary>The array payload used by the fan-out cases.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>The list payload used by the fan-out cases.</summary>
    private static readonly List<int> ValueList = [.. CreateValues()];

    /// <summary>Broadcasts values from a current-value subject to several subscribers, then detaches them.</summary>
    /// <returns>The sum observed across all subscribers plus the final value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCurrentValueSubjectBroadcast()
    {
        var observers = CreateObservers();
        using CurrentValueSubject<int> subject = new(0);
        var subscriptions = new IDisposable[ObserverCount];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        PushRange(subject);
        DisposeAll(subscriptions);
        return SumTotals(observers) + subject.Value;
    }

    /// <summary>Broadcasts values from a behavior subject to several subscribers, then detaches them, using System.Reactive.</summary>
    /// <returns>The sum observed across all subscribers plus the final value.</returns>
    [Benchmark]
    public int SystemReactiveCurrentValueSubjectBroadcast()
    {
        var observers = CreateObservers();
        using RxIntBehaviorSubject subject = new(0);
        var subscriptions = new IDisposable[ObserverCount];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        PushRange(subject);
        DisposeAll(subscriptions);
        return SumTotals(observers) + subject.Value;
    }

    /// <summary>Broadcasts values from a behavior subject to several subscribers, then detaches them, using R3.</summary>
    /// <returns>The sum observed across all subscribers plus the final value.</returns>
    [Benchmark]
    public int R3CurrentValueSubjectBroadcast()
    {
        var observers = new IntR3Witness[ObserverCount];
        using R3IntBehaviorSubject subject = new(0);
        var subscriptions = new IDisposable[ObserverCount];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            observers[i] = new();
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        DisposeAll(subscriptions);
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total + subject.Value;
    }

    /// <summary>Pushes values through the observer side of a read-only behavior pair.</summary>
    /// <returns>The sum observed by the subscriber.</returns>
    [Benchmark]
    public int PrimitivesReadOnlyBehavior()
    {
        IntSignalWitness observer = new();
        var (observable, sink) = PrimitivesExtensions.ToReadOnlyBehavior(0);
        using var subscription = observable.Subscribe(observer);
        PushRange(sink);
        return observer.Total;
    }

    /// <summary>Pushes values through the observer side of a read-only behavior pair using ReactiveUI.Extensions.</summary>
    /// <returns>The sum observed by the subscriber.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsReadOnlyBehavior()
    {
        IntSignalWitness observer = new();
        var (observable, sink) = PackageExtensions.ToReadOnlyBehavior(0);
        using var subscription = observable.Subscribe(observer);
        PushRange(sink);
        return observer.Total;
    }

    /// <summary>Creates and subscribes a single-value source once per value.</summary>
    /// <returns>The sum of the emitted values plus completions.</returns>
    [Benchmark]
    public int PrimitivesReturnSubscribe()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PrimitivesObservables.Return(i).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Creates and subscribes a single-value source once per value using System.Reactive.</summary>
    /// <returns>The sum of the emitted values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveReturnSubscribe()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = RxObservable.Return(i).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Creates and subscribes a single-value source once per value using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsReturnSubscribe()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PackageObservables.Return(i).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Emits an array and a list into an observer through the indexed fast path.</summary>
    /// <returns>The sum observed.</returns>
    [Benchmark]
    public int PrimitivesFastForEach()
    {
        IntSignalWitness observer = new();
        PrimitivesObserverExtensions.FastForEach(observer, Values);
        PrimitivesObserverExtensions.FastForEach(observer, ValueList);
        return observer.Total;
    }

    /// <summary>Emits an array and a list into an observer through the indexed fast path using ReactiveUI.Extensions.</summary>
    /// <returns>The sum observed.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsFastForEach()
    {
        IntSignalWitness observer = new();
        PackageObserverExtensions.FastForEach(observer, Values);
        PackageObserverExtensions.FastForEach(observer, ValueList);
        return observer.Total;
    }

    /// <summary>Broadcasts values to a swap-on-write observer array, then removes every observer.</summary>
    /// <returns>The sum observed across all observers plus the remaining observer count.</returns>
    [Benchmark]
    public int PrimitivesObserverArrayBroadcastAndRemove()
    {
        var witnesses = CreateObservers();
        IObserver<int>[] empty = [];
        IObserver<int>[] observers = [.. witnesses];
        for (var i = 0; i < Count; i++)
        {
            ObserverArrayHelpers.Broadcast(observers, i);
        }

        for (var i = witnesses.Length - 1; i >= 0; i--)
        {
            observers = ObserverArrayHelpers.RemoveOrNull(observers, witnesses[i], empty) ?? observers;
        }

        return SumTotals(witnesses) + observers.Length;
    }

    /// <summary>Bridges a property's change notifications into an observable and raises changes.</summary>
    /// <returns>The sum of the observed property values.</returns>
    [Benchmark]
    public int PrimitivesPropertyChanges()
    {
        IntSignalWitness observer = new();
        PropertySource source = new();
        using var subscription = PrimitivesExtensions.ToPropertyObservable(source, static item => item.Current).Subscribe(observer);
        RaiseChanges(source);
        return observer.Total;
    }

    /// <summary>Bridges a property's change notifications into an observable and raises changes using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the observed property values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsPropertyChanges()
    {
        IntSignalWitness observer = new();
        PropertySource source = new();
        using var subscription = PackageExtensions.ToPropertyObservable(source, static item => item.Current).Subscribe(observer);
        RaiseChanges(source);
        return observer.Total;
    }

    /// <summary>Injects heartbeats into quiet periods of a value stream in virtual time.</summary>
    /// <returns>The number of updates and heartbeats observed.</returns>
    [Benchmark]
    public int PrimitivesHeartbeat()
    {
        VirtualClock clock = new();
        CountingSignalWitness<Heartbeat<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.Heartbeat(source, Period, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            if (i % QuietEvery == 0)
            {
                clock.AdvanceBy(Period);
            }
        }

        return observer.Count;
    }

    /// <summary>Injects heartbeats into quiet periods of a value stream in virtual time using ReactiveUI.Extensions.</summary>
    /// <returns>The number of updates and heartbeats observed.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsHeartbeat()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Heartbeat<int>> observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.Heartbeat(source, Period, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            if (i % QuietEvery == 0)
            {
                scheduler.AdvanceBy(Period);
            }
        }

        return observer.Count;
    }

    /// <summary>Marks a value stream stale after quiet periods in virtual time.</summary>
    /// <returns>The number of updates and stale markers observed.</returns>
    [Benchmark]
    public int PrimitivesDetectStale()
    {
        VirtualClock clock = new();
        CountingSignalWitness<Stale<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.DetectStale(source, Period, clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            if (i % QuietEvery == 0)
            {
                clock.AdvanceBy(Period);
            }
        }

        return observer.Count;
    }

    /// <summary>Marks a value stream stale after quiet periods in virtual time using ReactiveUI.Extensions.</summary>
    /// <returns>The number of updates and stale markers observed.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsDetectStale()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Stale<int>> observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.DetectStale(source, Period, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            if (i % QuietEvery == 0)
            {
                scheduler.AdvanceBy(Period);
            }
        }

        return observer.Count;
    }

    /// <summary>Queues values and a completion into timer-sink state under a gate and flushes after each release.</summary>
    /// <returns>The sum delivered plus completions.</returns>
    [Benchmark]
    public int PrimitivesTimerSinkStateQueueAndFlush()
    {
        IntSignalWitness observer = new();
        TimerSinkState<int> state = new(observer);
        var gate = new object();
        for (var i = 0; i < Count; i++)
        {
            lock (gate)
            {
                _ = state.QueueLocked(i);
            }

            state.Flush();
        }

        lock (gate)
        {
            _ = state.QueueCompletedLocked();
        }

        state.Flush();
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Pushes the ascending range into the observer.</summary>
    /// <param name="sink">The observer to push into.</param>
    private static void PushRange(IObserver<int> sink)
    {
        for (var i = 0; i < Count; i++)
        {
            sink.OnNext(i);
        }
    }

    /// <summary>Raises a change for the watched property and an unrelated property on every step.</summary>
    /// <param name="source">The property source.</param>
    private static void RaiseChanges(PropertySource source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.Current = i;
            source.Other = i;
        }
    }

    /// <summary>Creates the fan-out observers.</summary>
    /// <returns>The observers.</returns>
    private static IntSignalWitness[] CreateObservers()
    {
        var observers = new IntSignalWitness[ObserverCount];
        for (var i = 0; i < observers.Length; i++)
        {
            observers[i] = new();
        }

        return observers;
    }

    /// <summary>Disposes every subscription in order.</summary>
    /// <param name="subscriptions">The subscriptions to dispose.</param>
    private static void DisposeAll(IDisposable[] subscriptions)
    {
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i].Dispose();
        }
    }

    /// <summary>Sums the totals observed by each observer.</summary>
    /// <param name="observers">The observers.</param>
    /// <returns>The combined total.</returns>
    private static int SumTotals(IntSignalWitness[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

    /// <summary>Builds the ascending value payload.</summary>
    /// <returns>The payload.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    /// <summary>Property holder that raises a change notification on every assignment.</summary>
    private sealed class PropertySource : INotifyPropertyChanged
    {
        /// <summary>Occurs when a property is assigned.</summary>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets or sets the watched value.</summary>
        public int Current
        {
            get;
            set
            {
                field = value;
                PropertyChanged?.Invoke(this, new(nameof(Current)));
            }
        }

        /// <summary>Gets or sets an unwatched value.</summary>
        public int Other
        {
            get;
            set
            {
                field = value;
                PropertyChanged?.Invoke(this, new(nameof(Other)));
            }
        }
    }
}
