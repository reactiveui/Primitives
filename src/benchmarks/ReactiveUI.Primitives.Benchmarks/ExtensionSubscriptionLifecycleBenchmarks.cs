// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObjectSubject = System.Reactive.Subjects.Subject<object>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures operators that act at subscribe, dispose, sampling and empty-completion boundaries.</summary>
[MemoryDiagnoser]
public class ExtensionSubscriptionLifecycleBenchmarks
{
    /// <summary>The number of values or subscription cycles in each case.</summary>
    private const int Count = 256;

    /// <summary>The seed or fallback value.</summary>
    private const int Seed = 42;

    /// <summary>The number of repeated values at the start of each distinct run.</summary>
    private const int RunLength = 4;

    /// <summary>The trigger payload used by sampling cases.</summary>
    private static readonly object Tick = new();

    /// <summary>Seeds a default value, then forwards only changed values.</summary>
    /// <returns>The sum of the forwarded values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesLatestOrDefault()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.LatestOrDefault(source, Seed).Subscribe(observer);
        PushRuns(source);
        return observer.Total;
    }

    /// <summary>Seeds a default value, then forwards only changed values using System.Reactive.</summary>
    /// <returns>The sum of the forwarded values.</returns>
    [Benchmark]
    public int SystemReactiveLatestOrDefault()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.DistinctUntilChanged(RxObservable.StartWith(source, Seed)).Subscribe(observer);
        PushRuns(source);
        return observer.Total;
    }

    /// <summary>Seeds a default value, then forwards only changed values using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the forwarded values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsLatestOrDefault()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.LatestOrDefault(source, Seed).Subscribe(observer);
        PushRuns(source);
        return observer.Total;
    }

    /// <summary>Emits the latest source value on every trigger.</summary>
    /// <returns>The sum of the sampled values.</returns>
    [Benchmark]
    public int PrimitivesSampleLatest()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using Signal<object> trigger = new();
        using var subscription = PrimitivesExtensions.SampleLatest(source, trigger).Subscribe(observer);
        PushSampled(source, trigger);
        return observer.Total;
    }

    /// <summary>Emits the latest source value on every trigger using System.Reactive.</summary>
    /// <returns>The sum of the sampled values.</returns>
    [Benchmark]
    public int SystemReactiveSampleLatest()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using RxObjectSubject trigger = new();
        using var subscription = RxObservable.WithLatestFrom(trigger, source, static (_, value) => value).Subscribe(observer);
        PushSampled(source, trigger);
        return observer.Total;
    }

    /// <summary>Emits the latest source value on every trigger using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the sampled values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsSampleLatest()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using RxObjectSubject trigger = new();
        using var subscription = PackageExtensions.SampleLatest(source, trigger).Subscribe(observer);
        PushSampled(source, trigger);
        return observer.Total;
    }

    /// <summary>Subscribes to an empty source that switches to a single-value fallback, once per cycle.</summary>
    /// <returns>The sum of the fallback values.</returns>
    [Benchmark]
    public int PrimitivesSwitchIfEmpty()
    {
        IntSignalWitness observer = new();
        var fallback = PrimitivesObservables.Return(Seed);
        var pipeline = PrimitivesExtensions.SwitchIfEmpty(Signal.None<int>(), fallback);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes to an empty source that switches to a single-value fallback using System.Reactive.</summary>
    /// <returns>The sum of the fallback values.</returns>
    [Benchmark]
    public int SystemReactiveSwitchIfEmpty()
    {
        IntSignalWitness observer = new();
        var pipeline = RxObservable.DefaultIfEmpty(RxObservable.Empty<int>(), Seed);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes to an empty source that switches to a single-value fallback using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the fallback values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsSwitchIfEmpty()
    {
        IntSignalWitness observer = new();
        var pipeline = PackageExtensions.SwitchIfEmpty(RxObservable.Empty<int>(), PackageObservables.Return(Seed));
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes repeatedly to a hot source that replays a fixed initial value to each subscriber.</summary>
    /// <returns>The sum of the replayed values.</returns>
    [Benchmark]
    public int PrimitivesReplayLastOnSubscribe()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        var pipeline = PrimitivesExtensions.ReplayLastOnSubscribe(source, Seed);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes repeatedly to a hot source seeded with a fixed initial value using System.Reactive.</summary>
    /// <returns>The sum of the replayed values.</returns>
    [Benchmark]
    public int SystemReactiveReplayLastOnSubscribe()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        var pipeline = RxObservable.StartWith(source, Seed);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Subscribes repeatedly to a hot source that replays a fixed initial value using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the replayed values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsReplayLastOnSubscribe()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        var pipeline = PackageExtensions.ReplayLastOnSubscribe(source, Seed);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Runs subscribe and dispose side effects over repeated subscription cycles on a silent source.</summary>
    /// <returns>The number of subscribe and dispose callbacks.</returns>
    [Benchmark]
    public int PrimitivesDoOnSubscribeAndDispose()
    {
        CallbackCounter counter = new();
        IntSignalWitness observer = new();
        var pipeline = PrimitivesExtensions.DoOnDispose(
            PrimitivesExtensions.DoOnSubscribe(Signal.Silent<int>(), counter.Increment),
            counter.Increment);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return counter.Value;
    }

    /// <summary>Runs subscribe and dispose side effects over repeated subscription cycles on a silent source using System.Reactive.</summary>
    /// <returns>The number of subscribe and dispose callbacks.</returns>
    [Benchmark]
    public int SystemReactiveDoOnSubscribeAndDispose()
    {
        CallbackCounter counter = new();
        IntSignalWitness observer = new();
        var pipeline = RxObservable.Finally(
            RxObservable.Defer(() =>
            {
                counter.Increment();
                return RxObservable.Never<int>();
            }),
            counter.Increment);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return counter.Value;
    }

    /// <summary>Runs subscribe and dispose side effects over repeated subscription cycles on a silent source using ReactiveUI.Extensions.</summary>
    /// <returns>The number of subscribe and dispose callbacks.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsDoOnSubscribeAndDispose()
    {
        CallbackCounter counter = new();
        IntSignalWitness observer = new();
        var pipeline = PackageExtensions.DoOnDispose(
            PackageExtensions.DoOnSubscribe(RxObservable.Never<int>(), counter.Increment),
            counter.Increment);
        for (var i = 0; i < Count; i++)
        {
            using var subscription = pipeline.Subscribe(observer);
        }

        return counter.Value;
    }

    /// <summary>Pushes runs of repeated values into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushRuns(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i / RunLength);
        }
    }

    /// <summary>Pushes a value and then a trigger for every step.</summary>
    /// <param name="source">The value subject.</param>
    /// <param name="trigger">The trigger subject.</param>
    private static void PushSampled(IObserver<int> source, IObserver<object> trigger)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
            trigger.OnNext(Tick);
        }
    }

    /// <summary>Counts side-effect callbacks.</summary>
    private sealed class CallbackCounter
    {
        /// <summary>Gets the number of callbacks recorded.</summary>
        public int Value { get; private set; }

        /// <summary>Records one callback.</summary>
        public void Increment() => Value++;
    }
}
