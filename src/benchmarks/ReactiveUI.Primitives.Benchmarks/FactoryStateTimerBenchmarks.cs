// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Time.Testing;
using R3;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the previously-uncovered factories: the closure-free <c>CreateWithState</c> against
/// closure-based <c>Create</c>, the <c>Iterate</c> generator, the <c>Every</c>/<c>After</c> timer
/// factories under virtual time, and the <c>FromEventPattern</c> event bridge.
/// </summary>
[MemoryDiagnoser]
[System.Diagnostics.DebuggerDisplay("Limit = {_limit}")]
public class FactoryStateTimerBenchmarks
{
    /// <summary>The number of values generated or events raised per benchmark iteration.</summary>
    private const int Count = 16;

    /// <summary>The number of virtual-time ticks advanced for interval timer benchmarks.</summary>
    private const int Ticks = 4;

    /// <summary>The upper bound passed as explicit state to the create-with-state benchmarks.</summary>
    private readonly int _limit = Count;

    /// <summary>Benchmarks creating a sequence from explicit state without a per-subscription closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCreateWithState()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.CreateWithState<int, int>(_limit, static (limit, target) =>
        {
            for (var i = 1; i <= limit; i++)
            {
                target.OnNext(i);
            }

            target.OnCompleted();
            return EmptyDisposable.Instance;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks creating a sequence with a System.Reactive closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveCreateClosure()
    {
        var limit = _limit;
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Create<int>(target =>
        {
            for (var i = 1; i <= limit; i++)
            {
                target.OnNext(i);
            }

            target.OnCompleted();
            return System.Reactive.Disposables.Disposable.Empty;
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks creating a sequence with an R3 closure.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3CreateClosure()
    {
        var limit = _limit;
        IntR3Witness observer = new();
        using var subscription = Observable.Create<int>(target =>
        {
            for (var i = 1; i <= limit; i++)
            {
                target.OnNext(i);
            }

            target.OnCompleted(R3.Result.Success);
            return Disposable.Create(static () => { });
        }).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks the iterate generator (no R3 equivalent).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesIterate()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Iterate(1, static s => s <= Count, static s => s + 1, static s => s)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks the iterate generator using System.Reactive Generate.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveGenerate()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Generate(1, static s => s <= Count, static s => s + 1, static s => s)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks an interval timer under virtual time.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int PrimitivesEvery()
    {
        VirtualClock clock = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = Signal.Every(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(Ticks));
        return observer.Count;
    }

    /// <summary>Benchmarks an interval timer under virtual time using System.Reactive.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int SystemReactiveInterval()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = RxObservable.Interval(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        scheduler.AdvanceBy(TimeSpan.FromTicks(Ticks));
        return observer.Count;
    }

    /// <summary>Benchmarks an interval timer under virtual time using R3.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int R3Interval()
    {
        FakeTimeProvider timeProvider = new();
        CountingR3Witness<Unit> observer = new();
        using var subscription = Observable.Interval(TimeSpan.FromTicks(1), timeProvider).Subscribe(observer);
        timeProvider.Advance(TimeSpan.FromTicks(Ticks));
        return observer.Count;
    }

    /// <summary>Benchmarks a one-shot timer under virtual time.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int PrimitivesAfter()
    {
        VirtualClock clock = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = Signal.After(TimeSpan.FromTicks(1), clock).Subscribe(observer);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Count;
    }

    /// <summary>Benchmarks a one-shot timer under virtual time using System.Reactive.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int SystemReactiveTimer()
    {
        HistoricalScheduler scheduler = new();
        CountingSignalWitness<long> observer = new();
        using var subscription = RxObservable.Timer(TimeSpan.FromTicks(1), scheduler).Subscribe(observer);
        scheduler.AdvanceBy(TimeSpan.FromTicks(1));
        return observer.Count;
    }

    /// <summary>Benchmarks a one-shot timer under virtual time using R3.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public int R3Timer()
    {
        FakeTimeProvider timeProvider = new();
        CountingR3Witness<Unit> observer = new();
        using var subscription = Observable.Timer(TimeSpan.FromTicks(1), timeProvider).Subscribe(observer);
        timeProvider.Advance(TimeSpan.FromTicks(1));
        return observer.Count;
    }

    /// <summary>Benchmarks bridging a .NET event into an observable.</summary>
    /// <returns>The number of events observed.</returns>
    [Benchmark]
    public int PrimitivesFromEventPattern()
    {
        EventSource source = new();
        CountingSignalWitness<EventPattern<EventArgs>> observer = new();
        using var subscription =
            Signal.FromEventPattern(h => source.Tick += h, h => source.Tick -= h).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.Raise();
        }

        return observer.Count;
    }

    /// <summary>Benchmarks bridging a .NET event into an observable using System.Reactive (no direct R3 equivalent).</summary>
    /// <returns>The number of events observed.</returns>
    [Benchmark]
    public int SystemReactiveFromEventPattern()
    {
        EventSource source = new();
        CountingSignalWitness<System.Reactive.EventPattern<object>> observer = new();
        using var subscription = RxObservable.FromEventPattern(h => source.Tick += h, h => source.Tick -= h)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.Raise();
        }

        return observer.Count;
    }

    /// <summary>A minimal event publisher used to drive the event-bridge benchmarks.</summary>
    private sealed class EventSource
    {
        /// <summary>Occurs each time <see cref="Raise"/> is invoked.</summary>
        public event EventHandler? Tick;

        /// <summary>Raises the <see cref="Tick"/> event once.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Raise() => Tick?.Invoke(this, EventArgs.Empty);
    }
}
