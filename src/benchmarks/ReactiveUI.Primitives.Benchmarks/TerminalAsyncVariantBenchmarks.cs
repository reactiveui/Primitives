// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the async terminal variants (CollectListAsync, FirstOrDefaultAsync,
/// LastOrDefaultAsync) against their System.Reactive and R3 task-returning equivalents.
/// </summary>
[MemoryDiagnoser]
public class TerminalAsyncVariantBenchmarks
{
    private const int Count = 16;

    /// <summary>
    /// Benchmarks collecting a range into a list asynchronously.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesCollectListAsync() =>
        (await Signal.Sequence(1, Count).CollectListAsync().ConfigureAwait(false)).Count;

    /// <summary>
    /// Benchmarks collecting a range into a list asynchronously using System.Reactive.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveCollectListAsync() =>
        (await RxObservable.Range(1, Count).ToList().ToTask().ConfigureAwait(false)).Count;

    /// <summary>
    /// Benchmarks collecting a range into a list asynchronously using R3.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> R3CollectListAsync() =>
        (await R3.ObservableExtensions.ToListAsync(R3.Observable.Range(1, Count), CancellationToken.None)
            .ConfigureAwait(false)).Count;

    /// <summary>
    /// Benchmarks the first-or-default async terminal.
    /// </summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> PrimitivesFirstOrDefaultAsync() =>
        Signal.Sequence(1, Count).FirstOrDefaultAsync();

    /// <summary>
    /// Benchmarks the first-or-default async terminal using System.Reactive.
    /// </summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> SystemReactiveFirstOrDefaultAsync() =>
        RxObservable.Range(1, Count).FirstOrDefaultAsync().ToTask();

    /// <summary>
    /// Benchmarks the first-or-default async terminal using R3.
    /// </summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> R3FirstOrDefaultAsync() =>
        R3.ObservableExtensions.FirstOrDefaultAsync(R3.Observable.Range(1, Count), cancellationToken: CancellationToken.None);

    /// <summary>
    /// Benchmarks the last-or-default async terminal.
    /// </summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> PrimitivesLastOrDefaultAsync() =>
        Signal.Sequence(1, Count).LastOrDefaultAsync();

    /// <summary>
    /// Benchmarks the last-or-default async terminal using System.Reactive.
    /// </summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> SystemReactiveLastOrDefaultAsync() =>
        RxObservable.Range(1, Count).LastOrDefaultAsync().ToTask();

    /// <summary>
    /// Benchmarks the last-or-default async terminal using R3.
    /// </summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> R3LastOrDefaultAsync() =>
        R3.ObservableExtensions.LastOrDefaultAsync(R3.Observable.Range(1, Count), cancellationToken: CancellationToken.None);
}
