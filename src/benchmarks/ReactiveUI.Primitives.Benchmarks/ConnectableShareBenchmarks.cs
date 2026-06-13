// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using BenchmarkDotNet.Attributes;
using R3;
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
        IntSignalWitness observer = new();
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
        IntSignalWitness observer = new();
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
        IntR3Witness observer = new();
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
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(Start, Count).ShareLatest().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks share/reference counting using System.Reactive publish-refcount.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveShareLiveSubscribe()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.RefCount(RxObservable.Publish(RxObservable.Range(Start, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks share/reference counting using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ShareLiveSubscribe()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Share(R3.Observable.Range(Start, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks replay-live late subscription.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesReplayLiveLateSubscribe()
    {
        IntSignalWitness observer = new();
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
        IntSignalWitness observer = new();
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
        IntR3Witness observer = new();
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
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(Start, Count).ShareLive().AutoShare().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks ref-count subscription using R3.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3RefCountSubscribe()
    {
        IntR3Witness observer = new();
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
        IntSignalWitness observer = new();
        using var subscription = Signal.Sequence(Start, Count).ShareLive().AutoConnect().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks auto-connect subscription using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveAutoConnectSubscribe()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.AutoConnect(RxObservable.Publish(RxObservable.Range(Start, Count)))
            .Subscribe(observer);
        return observer.Total;
    }
}
