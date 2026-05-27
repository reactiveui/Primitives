// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for connectable and share APIs.
/// </summary>
[MemoryDiagnoser]
public class ConnectableShareBenchmarks
{
    private const int Count = 32;

    /// <summary>
    /// Benchmarks publish-live connection.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPublishLiveConnect()
    {
        var observer = new IntSignalObserver();
        var connectable = Signal.Range(1, Count).PublishLive();
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks publish connection using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactivePublishLiveConnect()
    {
        var observer = new IntSignalObserver();
        var connectable = RxObservable.Publish(RxObservable.Range(1, Count));
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks publish connection using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3PublishLiveConnect()
    {
        var observer = new IntR3Observer();
        var connectable = R3.ObservableExtensions.Publish(R3.Observable.Range(1, Count));
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks share-live reference counting.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesShareLiveSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).ShareLive().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks share/reference counting using System.Reactive publish-refcount.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveShareLiveSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.RefCount(RxObservable.Publish(RxObservable.Range(1, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks share/reference counting using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ShareLiveSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.Share(R3.Observable.Range(1, Count)).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks replay-live late subscription.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesReplayLiveLateSubscribe()
    {
        var observer = new IntSignalObserver();
        var connectable = Signal.Range(1, Count).ReplayLive(Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks replay late subscription using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveReplayLiveLateSubscribe()
    {
        var observer = new IntSignalObserver();
        var connectable = RxObservable.Replay(RxObservable.Range(1, Count), Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks replay late subscription using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3ReplayLiveLateSubscribe()
    {
        var observer = new IntR3Observer();
        var connectable = R3.ObservableExtensions.Replay(R3.Observable.Range(1, Count), Count);
        using var connection = connectable.Connect();
        using var subscription = connectable.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks ref-count subscription.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesRefCountSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).PublishLive().RefCount().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks ref-count subscription using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveRefCountSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.RefCount(RxObservable.Publish(RxObservable.Range(1, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks ref-count subscription using R3.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3RefCountSubscribe()
    {
        var observer = new IntR3Observer();
        using var subscription = R3.ObservableExtensions.RefCount(
                R3.ObservableExtensions.Publish(R3.Observable.Range(1, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks auto-connect subscription.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int PrimitivesAutoConnectSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Range(1, Count).PublishLive().AutoConnect().Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks auto-connect subscription using System.Reactive.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveAutoConnectSubscribe()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.AutoConnect(RxObservable.Publish(RxObservable.Range(1, Count)))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks auto-connect-equivalent subscription using R3 publish/connect.
    /// </summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int R3AutoConnectSubscribe()
    {
        var observer = new IntR3Observer();
        var connectable = R3.ObservableExtensions.Publish(R3.Observable.Range(1, Count));
        using var subscription = connectable.Subscribe(observer);
        using var connection = connectable.Connect();
        return observer.Total;
    }
}
