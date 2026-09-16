// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using AsyncRxObservable = System.Reactive.Linq.AsyncObservable;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the fixed-arity async latest-value combiners across every supported source count.</summary>
[MemoryDiagnoser]
public class AsyncSignalSyncLatestBenchmarks
{
    /// <summary>The number of values produced by each source.</summary>
    private const int Count = 16;

    /// <summary>Combines the latest values of two primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesSyncLatest2LastAsync() =>
        await Source()
            .SyncLatest(Source(), static (v1, v2) => v1 + v2)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of two primitive async sequences with the pair-named operator.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPairLatestLastAsync() =>
        await Source()
            .PairLatest(Source(), static (v1, v2) => v1 + v2)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of two ReactiveUI.Extensions async ranges.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCombineLatest2LastAsync() =>
        await ExtensionsAsyncObservable.LastAsync(
                ExtensionsAsyncObservable.CombineLatest(
                    ExtensionsSource(),
                    ExtensionsSource(),
                    static (v1, v2) => v1 + v2))
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of two System.Reactive.Async ranges.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> AsyncRxCombineLatest2LastAsync() =>
        await AsyncRxObservable.LastAsync(
            AsyncRxObservable.CombineLatest(
                AsyncRxObservable.Range(0, Count),
                AsyncRxObservable.Range(0, Count),
                static (v1, v2) => v1 + v2));

    /// <summary>Combines the latest values of three primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest3LastAsync() =>
        await Source()
            .SyncLatest(Source(), Source(), static (v1, v2, v3) => v1 + v2 + v3)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of four primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest4LastAsync() =>
        await Source()
            .SyncLatest(Source(), Source(), Source(), static (v1, v2, v3, v4) => v1 + v2 + v3 + v4)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of four ReactiveUI.Extensions async ranges.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCombineLatest4LastAsync() =>
        await ExtensionsAsyncObservable.LastAsync(
                ExtensionsAsyncObservable.CombineLatest(
                    ExtensionsSource(),
                    ExtensionsSource(),
                    ExtensionsSource(),
                    ExtensionsSource(),
                    static (v1, v2, v3, v4) => v1 + v2 + v3 + v4))
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of five primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest5LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5) => v1 + v2 + v3 + v4 + v5)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of six primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest6LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6) => v1 + v2 + v3 + v4 + v5 + v6)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of seven primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest7LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7) => v1 + v2 + v3 + v4 + v5 + v6 + v7)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of eight primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest8LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of nine primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest9LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9) => v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of ten primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest10LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of eleven primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest11LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of twelve primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest12LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of thirteen primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest13LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of fourteen primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest14LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of fifteen primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest15LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of sixteen primitive async sequences.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatest16LastAsync() =>
        await Source()
            .SyncLatest(
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                Source(),
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16) =>
                    v1 + v2 + v3 + v4 + v5 + v6 + v7 + v8 + v9 + v10 + v11 + v12 + v13 + v14 + v15 + v16)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Creates one primitive async source.</summary>
    /// <returns>A sequence of <see cref="Count"/> values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservableAsync<int> Source() => PrimitivesAsyncSignal.Sequence(0, Count);

    /// <summary>Creates one ReactiveUI.Extensions async source.</summary>
    /// <returns>A range of <see cref="Count"/> values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReactiveUI.Extensions.Async.IObservableAsync<int> ExtensionsSource() =>
        ExtensionsAsyncObservable.Range(0, Count);
}
