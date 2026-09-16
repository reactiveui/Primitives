// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using ShimAsyncExtensions = ReactiveUI.Primitives.Async.Reactive.SignalAsyncReactiveExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures async sources that emit System.Reactive's Unit: operation, task and action bridges, and value-to-unit projection.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class RxUnitAsyncSourceBenchmarks
{
    /// <summary>The number of values projected to unit.</summary>
    private const int Count = 32;

    /// <summary>An asynchronous operation that completes synchronously.</summary>
    private static readonly Func<CancellationToken, ValueTask> CompletedOperation = static _ => default;

    /// <summary>Drains a unit source that runs a synchronously completing asynchronous operation.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("FromAsyncOperation")]
    public async Task<int> PrimitivesFromAsyncOperationCountAsync() =>
        await ShimAsyncExtensions.FromAsync(CompletedOperation).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a unit source that runs a synchronously completing asynchronous operation in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("FromAsyncOperation")]
    public async Task<int> ExtensionsFromAsyncOperationCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.FromAsync(CompletedOperation))
            .ConfigureAwait(false);

    /// <summary>Drains a unit source bridged from a completed task.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("TaskBridge")]
    public async Task<int> PrimitivesTaskToAsyncSignalCountAsync() =>
        await ShimAsyncExtensions.ToAsyncSignal(Task.CompletedTask).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a unit source bridged from a completed task in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("TaskBridge")]
    public async Task<int> ExtensionsTaskToObservableAsyncCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.ToObservableAsync(Task.CompletedTask))
            .ConfigureAwait(false);

    /// <summary>Drains a unit source that runs an action during subscription.</summary>
    /// <returns>The number of values observed plus the number of action runs.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StartAction")]
    public async Task<int> PrimitivesStartActionCountAsync()
    {
        var runs = 0;
        var count = await ShimAsyncExtensions.Start(() => runs++).CountAsync().ConfigureAwait(false);
        return count + runs;
    }

    /// <summary>Drains a unit source that runs an action during subscription in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed plus the number of action runs.</returns>
    [Benchmark]
    [BenchmarkCategory("StartAction")]
    public async Task<int> ExtensionsStartActionCountAsync()
    {
        var runs = 0;
        var count = await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Start(() => runs++))
            .ConfigureAwait(false);
        return count + runs;
    }

    /// <summary>Projects every value of a sequence to unit.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AsUnit")]
    public async Task<int> PrimitivesAsSignalCountAsync() =>
        await ShimAsyncExtensions.AsSignal(PrimitivesAsyncSignal.Sequence(0, Count)).CountAsync().ConfigureAwait(false);

    /// <summary>Projects every value of a range to unit in ReactiveUI.Extensions.</summary>
    /// <returns>The number of unit values observed.</returns>
    [Benchmark]
    [BenchmarkCategory("AsUnit")]
    public async Task<int> ExtensionsAsSignalCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.AsSignal(ExtensionsAsyncObservable.Range(0, Count)))
            .ConfigureAwait(false);
}
