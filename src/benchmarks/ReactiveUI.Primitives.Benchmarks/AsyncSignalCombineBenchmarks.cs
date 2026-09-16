// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using AsyncRxObservable = System.Reactive.Linq.AsyncObservable;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsAsyncSubject = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using ExtensionsObservableAsync = ReactiveUI.Extensions.Async.IObservableAsync<int>;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async operators that merge, concatenate, flatten, pair or prefix sequences.</summary>
[MemoryDiagnoser]
public class AsyncSignalCombineBenchmarks
{
    /// <summary>The number of values produced by each inner or outer source.</summary>
    private const int Count = 16;

    /// <summary>The number of sources combined by the multi-source cases.</summary>
    private const int SourceCount = 4;

    /// <summary>The values placed ahead of a sequence by the prefix cases.</summary>
    private static readonly int[] Prefix = [-3, -2, -1];

    /// <summary>Merges four primitive async sequences and counts the values.</summary>
    /// <returns>The number of merged values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesMergeCountAsync() =>
        await PrimitivesSources().Merge().CountAsync().ConfigureAwait(false);

    /// <summary>Blends two primitive async sequences and counts the values.</summary>
    /// <returns>The number of blended values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesBlendPairCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Blend(PrimitivesAsyncSignal.Sequence(Count, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Merges four ReactiveUI.Extensions async ranges and counts the values.</summary>
    /// <returns>The number of merged values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsMergeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Merge(ExtensionsSources()))
            .ConfigureAwait(false);

    /// <summary>Merges four System.Reactive.Async ranges and counts the values.</summary>
    /// <returns>The number of merged values.</returns>
    [Benchmark]
    public async Task<int> AsyncRxMergeCountAsync() =>
        await AsyncRxObservable.Count(
            AsyncRxObservable.Merge(
                AsyncRxObservable.Select(
                    AsyncRxObservable.Range(0, SourceCount),
                    static index => AsyncRxObservable.Range(index * Count, Count))));

    /// <summary>Concatenates four primitive async sequences and counts the values.</summary>
    /// <returns>The number of concatenated values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesConcatCountAsync() =>
        await PrimitivesSources().Concat().CountAsync().ConfigureAwait(false);

    /// <summary>Chains two primitive async sequences and counts the values.</summary>
    /// <returns>The number of chained values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesChainPairCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Chain(PrimitivesAsyncSignal.Sequence(Count, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Concatenates the inner sequences emitted by a primitive async outer sequence.</summary>
    /// <returns>The number of concatenated values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesConcatInnerSignalsCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, SourceCount)
            .Map(static index => PrimitivesAsyncSignal.Sequence(index * Count, Count))
            .Chain()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Concatenates four ReactiveUI.Extensions async ranges and counts the values.</summary>
    /// <returns>The number of concatenated values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsConcatCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Concat(ExtensionsSources()))
            .ConfigureAwait(false);

    /// <summary>Concatenates two System.Reactive.Async ranges and counts the values.</summary>
    /// <returns>The number of concatenated values.</returns>
    [Benchmark]
    public async Task<int> AsyncRxConcatPairCountAsync() =>
        await AsyncRxObservable.Count(
            AsyncRxObservable.Concat(AsyncRxObservable.Range(0, Count), AsyncRxObservable.Range(Count, Count)));

    /// <summary>Projects each primitive async value to an inner sequence and merges the results.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFlatMapCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, SourceCount)
            .FlatMap(static index => PrimitivesAsyncSignal.Sequence(index, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Projects each primitive async value to an inner sequence with the Rx-named operator.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSelectManyCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, SourceCount)
            .SelectMany(static index => PrimitivesAsyncSignal.Sequence(index, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Projects each primitive async value to an inner sequence with the monadic-bind name.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesBindCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, SourceCount)
            .Bind(static index => PrimitivesAsyncSignal.Sequence(index, Count))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Projects each ReactiveUI.Extensions async value to an inner range and merges the results.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSelectManyCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.SelectMany(
                    ExtensionsAsyncObservable.Range(0, SourceCount),
                    static index => ExtensionsAsyncObservable.Range(index, Count)))
            .ConfigureAwait(false);

    /// <summary>Projects each System.Reactive.Async value to an inner range and merges the results.</summary>
    /// <returns>The number of flattened values.</returns>
    [Benchmark]
    public async Task<int> AsyncRxSelectManyCountAsync() =>
        await AsyncRxObservable.Count(
            AsyncRxObservable.SelectMany(
                AsyncRxObservable.Range(0, SourceCount),
                static index => AsyncRxObservable.Range(index, Count)));

    /// <summary>Switches a primitive async outer signal across four live inner signals.</summary>
    /// <returns>The sum of the values forwarded from the inner signals.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSwitchToSumAsync() =>
        DrivePrimitivesSwitchAsync(static outer => outer.SwitchTo());

    /// <summary>Switches a primitive async outer signal across four live inner signals with the Rx-named operator.</summary>
    /// <returns>The sum of the values forwarded from the inner signals.</returns>
    [Benchmark]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> PrimitivesSwitchSumAsync() =>
        DrivePrimitivesSwitchAsync(static outer => outer.Switch());

    /// <summary>Switches a ReactiveUI.Extensions async outer subject across four live inner subjects.</summary>
    /// <returns>The sum of the values forwarded from the inner subjects.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSwitchSumAsync()
    {
        var outer = ExtensionsAsyncSubject.Create<ExtensionsObservableAsync>();
        var sum = 0;
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Switch(outer),
                value => sum += value)
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < SourceCount; i++)
            {
                var inner = ExtensionsAsyncSubject.Create<int>();
                await outer.OnNextAsync(inner, CancellationToken.None).ConfigureAwait(false);
                for (var value = 0; value < Count; value++)
                {
                    await inner.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
                }

                await inner.OnCompletedAsync(ReactiveUI.Extensions.Async.Result.Success).ConfigureAwait(false);
                await inner.DisposeAsync().ConfigureAwait(false);
            }

            await outer.OnCompletedAsync(ReactiveUI.Extensions.Async.Result.Success).ConfigureAwait(false);
            return sum;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await outer.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Pairs two primitive async sequences element by element and sums the pairs.</summary>
    /// <returns>The sum of the paired values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPairSumAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Pair(PrimitivesAsyncSignal.Sequence(Count, Count), static (left, right) => left + right)
            .ReduceAsync(0, static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Zips two primitive async sequences element by element and sums the pairs.</summary>
    /// <returns>The sum of the zipped values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesZipSumAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Zip(PrimitivesAsyncSignal.Sequence(Count, Count), static (left, right) => left + right)
            .ReduceAsync(0, static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Zips two ReactiveUI.Extensions async ranges element by element and sums the pairs.</summary>
    /// <returns>The sum of the zipped values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsZipSumAsync() =>
        await ExtensionsAsyncObservable.AggregateAsync(
                ExtensionsAsyncObservable.Zip(
                    ExtensionsAsyncObservable.Range(0, Count),
                    ExtensionsAsyncObservable.Range(Count, Count),
                    static (left, right) => left + right),
                0,
                static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Prefixes a primitive async sequence with several values and counts the result.</summary>
    /// <returns>The number of values including the prefix.</returns>
    [Benchmark]
    public async Task<int> PrimitivesStartWithCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .StartWith(Prefix)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Prefixes a primitive async sequence with a single leading value.</summary>
    /// <returns>The number of values including the prefix.</returns>
    [Benchmark]
    public async Task<int> PrimitivesLeadCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Lead(-1)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Prefixes a ReactiveUI.Extensions async range with several values and counts the result.</summary>
    /// <returns>The number of values including the prefix.</returns>
    [Benchmark]
    public async Task<int> ExtensionsStartWithCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.StartWith(ExtensionsAsyncObservable.Range(0, Count), Prefix))
            .ConfigureAwait(false);

    /// <summary>Prefixes a System.Reactive.Async range with several values and counts the result.</summary>
    /// <returns>The number of values including the prefix.</returns>
    [Benchmark]
    public async Task<int> AsyncRxStartWithCountAsync() =>
        await AsyncRxObservable.Count(AsyncRxObservable.StartWith(AsyncRxObservable.Range(0, Count), Prefix));

    /// <summary>Combines the latest values of four primitive async sequences into lists.</summary>
    /// <returns>The number of combined snapshots.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCombineLatestListCountAsync() =>
        await PrimitivesSources().CombineLatest().CountAsync().ConfigureAwait(false);

    /// <summary>Combines the latest values of four primitive async sequences with the primitives-named operator.</summary>
    /// <returns>The last combined total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSyncLatestSelectorLastAsync() =>
        await PrimitivesSources()
            .SyncLatest(static values => values[0] + values[values.Count - 1])
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Combines the latest values of four ReactiveUI.Extensions async ranges into lists.</summary>
    /// <returns>The number of combined snapshots.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCombineLatestListCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.CombineLatest(ExtensionsSources()))
            .ConfigureAwait(false);

    /// <summary>Tracks the minimum latest value across four primitive async sequences.</summary>
    /// <returns>The last minimum.</returns>
    [Benchmark]
    public async Task<int> PrimitivesGetMinLastAsync()
    {
        var sources = PrimitivesSources();
        return await sources[0]
            .GetMin(sources[1], sources[2], sources[3])
            .LastAsync()
            .ConfigureAwait(false);
    }

    /// <summary>Tracks the minimum latest value across four ReactiveUI.Extensions async ranges.</summary>
    /// <returns>The last minimum.</returns>
    [Benchmark]
    public async Task<int> ExtensionsGetMinLastAsync()
    {
        var sources = ExtensionsSources();
        return await ExtensionsAsyncObservable.LastAsync(
                ExtensionsAsyncObservable.GetMin(sources[0], sources[1], sources[2], sources[3]))
            .ConfigureAwait(false);
    }

    /// <summary>Tracks whether every latest value across four primitive async boolean sequences is true.</summary>
    /// <returns>The number of aggregate states emitted.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCombineLatestAllTrueCountAsync()
    {
        var sources = new IObservableAsync<bool>[SourceCount];
        for (var i = 0; i < SourceCount; i++)
        {
            sources[i] = PrimitivesAsyncSignal.Sequence(i, Count).Map(static value => (value & 1) == 0);
        }

        return await sources.CombineLatestValuesAreAllTrue().CountAsync().ConfigureAwait(false);
    }

    /// <summary>Pushes four live inner signals through a primitive async switch, completing each before the next arrives.</summary>
    /// <param name="switcher">The switch operator applied to the outer signal.</param>
    /// <returns>The sum of the values forwarded from the inner signals.</returns>
    private static async Task<int> DrivePrimitivesSwitchAsync(
        Func<IObservableAsync<IObservableAsync<int>>, IObservableAsync<int>> switcher)
    {
        var outer = PrimitivesAsyncSignalFactory.Create<IObservableAsync<int>>();
        var sum = 0;
        var subscription = await switcher(outer).SubscribeAsync(value => sum += value).ConfigureAwait(false);
        try
        {
            for (var i = 0; i < SourceCount; i++)
            {
                var inner = PrimitivesAsyncSignalFactory.Create<int>();
                await outer.OnNextAsync(inner, CancellationToken.None).ConfigureAwait(false);
                for (var value = 0; value < Count; value++)
                {
                    await inner.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
                }

                await inner.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                await inner.DisposeAsync().ConfigureAwait(false);
            }

            await outer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return sum;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await outer.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Builds four primitive async sequences with distinct value ranges.</summary>
    /// <returns>The sources.</returns>
    private static IObservableAsync<int>[] PrimitivesSources()
    {
        var sources = new IObservableAsync<int>[SourceCount];
        for (var i = 0; i < SourceCount; i++)
        {
            sources[i] = PrimitivesAsyncSignal.Sequence(i * Count, Count);
        }

        return sources;
    }

    /// <summary>Builds four ReactiveUI.Extensions async ranges with distinct value ranges.</summary>
    /// <returns>The sources.</returns>
    private static ExtensionsObservableAsync[] ExtensionsSources()
    {
        var sources = new ExtensionsObservableAsync[SourceCount];
        for (var i = 0; i < SourceCount; i++)
        {
            sources[i] = ExtensionsAsyncObservable.Range(i * Count, Count);
        }

        return sources;
    }
}
