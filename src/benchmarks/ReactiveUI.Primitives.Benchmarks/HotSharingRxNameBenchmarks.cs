// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the System.Reactive-named hot-sharing operators: Publish, Share, RefCount and Replay.</summary>
[MemoryDiagnoser]
public class HotSharingRxNameBenchmarks
{
    /// <summary>The number of values pushed or replayed by each case.</summary>
    private const int Count = 64;

    /// <summary>The bounded replay size.</summary>
    private const int BufferSize = 16;

    /// <summary>The replay window used by the time-constrained replay case.</summary>
    private static readonly TimeSpan ReplayWindow = TimeSpan.FromMinutes(1);

    /// <summary>Publishes a live source to two observers through an explicit connection.</summary>
    /// <returns>The combined total observed by both observers.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPublishConnect()
    {
        IntSignalWitness first = new();
        IntSignalWitness second = new();
        using Signal<int> source = new();
        var published = source.Publish();
        using var firstSubscription = published.Subscribe(first);
        using var secondSubscription = published.Subscribe(second);
        using var connection = published.Connect();
        Push(source);
        return first.Total + second.Total;
    }

    /// <summary>Publishes a live source to two observers through an explicit connection using System.Reactive.</summary>
    /// <returns>The combined total observed by both observers.</returns>
    [Benchmark]
    public int SystemReactivePublishConnect()
    {
        IntSignalWitness first = new();
        IntSignalWitness second = new();
        using RxSubject source = new();
        var published = RxObservable.Publish(source);
        using var firstSubscription = published.Subscribe(first);
        using var secondSubscription = published.Subscribe(second);
        using var connection = published.Connect();
        Push(source);
        return first.Total + second.Total;
    }

    /// <summary>Shares a live source through a reference-counted connection.</summary>
    /// <returns>The combined total observed by both observers.</returns>
    [Benchmark]
    public int PrimitivesShareRefCount()
    {
        IntSignalWitness first = new();
        IntSignalWitness second = new();
        using Signal<int> source = new();
        var shared = source.Share().RefCount();
        using var firstSubscription = shared.Subscribe(first);
        using var secondSubscription = shared.Subscribe(second);
        Push(source);
        return first.Total + second.Total;
    }

    /// <summary>Shares a live source through a reference-counted connection using System.Reactive.</summary>
    /// <returns>The combined total observed by both observers.</returns>
    [Benchmark]
    public int SystemReactivePublishRefCount()
    {
        IntSignalWitness first = new();
        IntSignalWitness second = new();
        using RxSubject source = new();
        var shared = RxObservable.RefCount(RxObservable.Publish(source));
        using var firstSubscription = shared.Subscribe(first);
        using var secondSubscription = shared.Subscribe(second);
        Push(source);
        return first.Total + second.Total;
    }

    /// <summary>Multicasts a range through a selector that merges the shared stream with itself.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesPublishSelector()
    {
        IntSignalWitness observer = new();
        using var subscription = Signal.Range(1, Count)
            .Publish(static shared => Signal.Blend(shared, shared))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Multicasts a range through a selector that merges the shared stream with itself using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactivePublishSelector()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.Publish(
                RxObservable.Range(1, Count),
                static shared => RxObservable.Merge(shared, shared))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Replays a connected range to a late observer through unbounded, bounded and time-bounded hubs.</summary>
    /// <returns>The combined total replayed to the late observers.</returns>
    [Benchmark]
    public int PrimitivesReplayLateSubscribe()
    {
        IntSignalWitness observer = new();
        var source = Signal.Range(1, Count);
        var unbounded = source.Replay();
        var bounded = source.Replay(BufferSize);
        var windowed = source.Replay(BufferSize, ReplayWindow);
        using var unboundedConnection = unbounded.Connect();
        using var boundedConnection = bounded.Connect();
        using var windowedConnection = windowed.Connect();
        using var unboundedSubscription = unbounded.Subscribe(observer);
        using var boundedSubscription = bounded.Subscribe(observer);
        using var windowedSubscription = windowed.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Replays a connected range to a late observer through unbounded, bounded and time-bounded hubs using System.Reactive.</summary>
    /// <returns>The combined total replayed to the late observers.</returns>
    [Benchmark]
    public int SystemReactiveReplayLateSubscribe()
    {
        IntSignalWitness observer = new();
        var source = RxObservable.Range(1, Count, System.Reactive.Concurrency.ImmediateScheduler.Instance);
        var unbounded = RxObservable.Replay(source);
        var bounded = RxObservable.Replay(source, BufferSize);
        var windowed = RxObservable.Replay(source, BufferSize, ReplayWindow);
        using var unboundedConnection = unbounded.Connect();
        using var boundedConnection = bounded.Connect();
        using var windowedConnection = windowed.Connect();
        using var unboundedSubscription = unbounded.Subscribe(observer);
        using var boundedSubscription = bounded.Subscribe(observer);
        using var windowedSubscription = windowed.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Pushes the benchmark values into an observer.</summary>
    /// <param name="source">The observer receiving the values.</param>
    private static void Push(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }
    }
}
