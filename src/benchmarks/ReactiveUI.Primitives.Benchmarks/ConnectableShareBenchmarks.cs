// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for connectable and share APIs.</summary>
[MemoryDiagnoser]
public class ConnectableShareBenchmarks
{
    /// <summary>The inclusive start value of the range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The number of elements produced by the range used by each benchmark.</summary>
    private const int Count = 32;

    /// <summary>Benchmarks publish-live connection.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPublishLiveConnect()
    {
        var observer = new IntSignalWitness();
        var connectable = Signal.Sequence(Start, Count).ShareLive();
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>Benchmarks publish connection using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactivePublishLiveConnect()
    {
        var observer = new IntSignalWitness();
        var connectable = RxObservable.Publish(RxObservable.Range(Start, Count));
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>Benchmarks publish connection using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3PublishLiveConnect()
    {
        var observer = new IntR3Witness();
        var connectable = R3.ObservableExtensions.Publish(R3.Observable.Range(Start, Count));
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>Benchmarks share-live reference counting.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesShareLiveSubscribe()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(Start, Count).ShareLatest().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks share/reference counting using System.Reactive publish-refcount.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveShareLiveSubscribe()
    {
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.RefCount(RxObservable.Publish(RxObservable.Range(Start, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks share/reference counting using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ShareLiveSubscribe()
    {
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.Share(R3.Observable.Range(Start, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks replay-live late subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesReplayLiveLateSubscribe()
    {
        var observer = new IntSignalWitness();
        var connectable = Signal.Sequence(Start, Count).ReplayLive(Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks replay late subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveReplayLiveLateSubscribe()
    {
        var observer = new IntSignalWitness();
        var connectable = RxObservable.Replay(RxObservable.Range(Start, Count), Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks replay late subscription using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ReplayLiveLateSubscribe()
    {
        var observer = new IntR3Witness();
        var connectable = R3.ObservableExtensions.Replay(R3.Observable.Range(Start, Count), Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks ref-count subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRefCountSubscribe()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(Start, Count).ShareLive().AutoShare().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks ref-count subscription using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3RefCountSubscribe()
    {
        var observer = new IntR3Witness();
        using var subscription = R3.ObservableExtensions.RefCount(
                R3.ObservableExtensions.Publish(R3.Observable.Range(Start, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks auto-connect subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesAutoConnectSubscribe()
    {
        var observer = new IntSignalWitness();
        using var subscription = Signal.Sequence(Start, Count).ShareLive().AutoConnect().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks auto-connect subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveAutoConnectSubscribe()
    {
        var observer = new IntSignalWitness();
        using var subscription = RxObservable.AutoConnect(RxObservable.Publish(RxObservable.Range(Start, Count)))
            .Subscribe(observer);
        return observer.Total;
    }
}
