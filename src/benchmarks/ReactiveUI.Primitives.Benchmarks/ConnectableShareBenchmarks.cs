// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

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
}
