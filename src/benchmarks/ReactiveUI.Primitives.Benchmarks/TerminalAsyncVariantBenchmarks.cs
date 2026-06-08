// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the async terminal variants (CollectListAsync, FirstOrDefaultAsync,
/// LastOrDefaultAsync) against their System.Reactive and R3 task-returning equivalents.
/// </summary>
[MemoryDiagnoser]
public class TerminalAsyncVariantBenchmarks
{
    /// <summary>The inclusive start value of the range used by each benchmark.</summary>
    private const int Start = 1;

    /// <summary>The number of elements produced by the range used by each benchmark.</summary>
    private const int Count = 16;

    /// <summary>Benchmarks collecting a range into a list asynchronously.</summary>
    /// <returns>The collected count.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesCollectListAsync() =>
        (await Signal.Sequence(Start, Count).CollectListAsync().ConfigureAwait(false)).Count;

    /// <summary>Benchmarks collecting a range into a list asynchronously using System.Reactive.</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveCollectListAsync() =>
        (await RxObservable.Range(Start, Count).ToListAsync().ConfigureAwait(false)).Count;

    /// <summary>Benchmarks collecting a range into a list asynchronously using R3.</summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> R3CollectListAsync() =>
        (await R3.ObservableExtensions.ToListAsync(R3.Observable.Range(Start, Count), CancellationToken.None)
            .ConfigureAwait(false)).Count;

    /// <summary>Benchmarks the first-or-default async terminal.</summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> PrimitivesFirstOrDefaultAsync() =>
        Signal.Sequence(Start, Count).FirstOrDefaultAsync();

    /// <summary>Benchmarks the first-or-default async terminal using System.Reactive.</summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> SystemReactiveFirstOrDefaultAsync() =>
        RxObservable.Range(Start, Count).FirstOrDefaultAsync().ToTask();

    /// <summary>Benchmarks the first-or-default async terminal using R3.</summary>
    /// <returns>The first value or default.</returns>
    [Benchmark]
    public Task<int> R3FirstOrDefaultAsync() =>
        R3.ObservableExtensions.FirstOrDefaultAsync(R3.Observable.Range(Start, Count), cancellationToken: CancellationToken.None);

    /// <summary>Benchmarks the last-or-default async terminal.</summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> PrimitivesLastOrDefaultAsync() =>
        Signal.Sequence(Start, Count).LastOrDefaultAsync();

    /// <summary>Benchmarks the last-or-default async terminal using System.Reactive.</summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> SystemReactiveLastOrDefaultAsync() =>
        RxObservable.Range(Start, Count).LastOrDefaultAsync().ToTask();

    /// <summary>Benchmarks the last-or-default async terminal using R3.</summary>
    /// <returns>The last value or default.</returns>
    [Benchmark]
    public Task<int> R3LastOrDefaultAsync() =>
        R3.ObservableExtensions.LastOrDefaultAsync(R3.Observable.Range(Start, Count), cancellationToken: CancellationToken.None);
}
