// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async accumulation, grouping and terminal element operators over a completed sequence.</summary>
[MemoryDiagnoser]
public class AsyncSignalAggregateBenchmarks
{
    /// <summary>The number of values produced by each source sequence.</summary>
    private const int Count = 64;

    /// <summary>The midpoint value used by the predicate-driven cases.</summary>
    private const int Half = Count / 2;

    /// <summary>The mask that assigns each value to one of four groups.</summary>
    private const int GroupMask = 3;

    /// <summary>Reduces a primitive async sequence to its sum.</summary>
    /// <returns>The sum of the sequence.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesReduceSumAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .ReduceAsync(0, static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Aggregates a primitive async sequence to its sum with the Rx-named operator.</summary>
    /// <returns>The sum of the sequence.</returns>
    [Benchmark]
    public async Task<int> PrimitivesAggregateSumAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .AggregateAsync(0, static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Aggregates a ReactiveUI.Extensions async range to its sum.</summary>
    /// <returns>The sum of the sequence.</returns>
    [Benchmark]
    public async Task<int> ExtensionsAggregateSumAsync() =>
        await ExtensionsAsyncObservable.AggregateAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                0,
                static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Emits every running total of a primitive async sequence and keeps the last one.</summary>
    /// <returns>The final running total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFoldLastAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Fold(0, static (sum, value) => sum + value)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Emits every running total of a primitive async sequence with the Rx-named operator.</summary>
    /// <returns>The final running total.</returns>
    [Benchmark]
    public async Task<int> PrimitivesScanLastAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Scan(0, static (sum, value) => sum + value)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Emits every running total of a ReactiveUI.Extensions async range and keeps the last one.</summary>
    /// <returns>The final running total.</returns>
    [Benchmark]
    public async Task<int> ExtensionsScanLastAsync() =>
        await ExtensionsAsyncObservable.LastAsync(
                ExtensionsAsyncObservable.Scan(
                    ExtensionsAsyncObservable.Range(0, Count),
                    0,
                    static (sum, value) => sum + value))
            .ConfigureAwait(false);

    /// <summary>Emits the seed and every running total of a primitive async sequence.</summary>
    /// <returns>The number of totals emitted.</returns>
    [Benchmark]
    public async Task<int> PrimitivesScanWithInitialCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .ScanWithInitial(0, static (sum, value) => sum + value)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Emits the seed and every running total of a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of totals emitted.</returns>
    [Benchmark]
    public async Task<int> ExtensionsScanWithInitialCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ScanWithInitial(
                    ExtensionsAsyncObservable.Range(0, Count),
                    0,
                    static (sum, value) => sum + value))
            .ConfigureAwait(false);

    /// <summary>Counts a primitive async sequence into a 64-bit total.</summary>
    /// <returns>The value count.</returns>
    [Benchmark]
    public async Task<long> PrimitivesLongCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .LongCountAsync()
            .ConfigureAwait(false);

    /// <summary>Counts a ReactiveUI.Extensions async range into a 64-bit total.</summary>
    /// <returns>The value count.</returns>
    [Benchmark]
    public async Task<long> ExtensionsLongCountAsync() =>
        await ExtensionsAsyncObservable.LongCountAsync(ExtensionsAsyncObservable.Range(0, Count))
            .ConfigureAwait(false);

    /// <summary>Reads the first value of a primitive async sequence matching a predicate, or a default.</summary>
    /// <returns>The first matching value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFirstOrDefaultAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .FirstOrDefaultAsync(static value => value > Half, -1)
            .ConfigureAwait(false);

    /// <summary>Reads the first value of a ReactiveUI.Extensions async range matching a predicate, or a default.</summary>
    /// <returns>The first matching value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsFirstOrDefaultAsync() =>
        await ExtensionsAsyncObservable.FirstOrDefaultAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                static value => value > Half,
                -1)
            .ConfigureAwait(false);

    /// <summary>Reads the last value of a primitive async sequence.</summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesLastAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .LastAsync()
            .ConfigureAwait(false);

    /// <summary>Reads the last value of a ReactiveUI.Extensions async range.</summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsLastAsync() =>
        await ExtensionsAsyncObservable.LastAsync(ExtensionsAsyncObservable.Range(0, Count))
            .ConfigureAwait(false);

    /// <summary>Reads the last value of a primitive async sequence matching a predicate, or a default.</summary>
    /// <returns>The last matching value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesLastOrDefaultAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .LastOrDefaultAsync(static value => value < Half, -1)
            .ConfigureAwait(false);

    /// <summary>Reads the last value of a ReactiveUI.Extensions async range matching a predicate, or a default.</summary>
    /// <returns>The last matching value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsLastOrDefaultAsync() =>
        await ExtensionsAsyncObservable.LastOrDefaultAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                static value => value < Half,
                -1)
            .ConfigureAwait(false);

    /// <summary>Reads the only value of a primitive async sequence matching a predicate.</summary>
    /// <returns>The single matching value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSingleAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .SingleAsync(static value => value == Half)
            .ConfigureAwait(false);

    /// <summary>Reads the only value of a ReactiveUI.Extensions async range matching a predicate.</summary>
    /// <returns>The single matching value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSingleAsync() =>
        await ExtensionsAsyncObservable.SingleAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                static value => value == Half)
            .ConfigureAwait(false);

    /// <summary>Reads the only value of a primitive async sequence matching a predicate, or a default.</summary>
    /// <returns>The single matching value, or the default.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSingleOrDefaultAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .SingleOrDefaultAsync(static value => value == Count, -1)
            .ConfigureAwait(false);

    /// <summary>Reads the only value of a ReactiveUI.Extensions async range matching a predicate, or a default.</summary>
    /// <returns>The single matching value, or the default.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSingleOrDefaultAsync() =>
        await ExtensionsAsyncObservable.SingleOrDefaultAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                static value => value == Count,
                -1)
            .ConfigureAwait(false);

    /// <summary>Waits for a primitive async sequence to complete while a tap sums its values.</summary>
    /// <returns>The sum observed before completion.</returns>
    [Benchmark]
    public async Task<int> PrimitivesWaitCompletionAsync()
    {
        var sum = 0;
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Tap(value => sum += value)
            .WaitCompletionAsync()
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Waits for a ReactiveUI.Extensions async range to complete while a tap sums its values.</summary>
    /// <returns>The sum observed before completion.</returns>
    [Benchmark]
    public async Task<int> ExtensionsWaitCompletionAsync()
    {
        var sum = 0;
        await ExtensionsAsyncObservable.WaitCompletionAsync(
                ExtensionsAsyncObservable.Do(
                    ExtensionsAsyncObservable.Range(0, Count),
                    (value, _) =>
                    {
                        sum += value;
                        return default;
                    }))
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Sums a primitive async sequence with a per-value callback.</summary>
    /// <returns>The sum of the sequence.</returns>
    [Benchmark]
    public async Task<int> PrimitivesForEachSumAsync()
    {
        var sum = 0;
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .ForEachAsync(value => sum += value)
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Sums a ReactiveUI.Extensions async range with a per-value callback.</summary>
    /// <returns>The sum of the sequence.</returns>
    [Benchmark]
    public async Task<int> ExtensionsForEachSumAsync()
    {
        var sum = 0;
        await ExtensionsAsyncObservable.ForEachAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                value => sum += value)
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Splits a primitive async sequence into four keyed groups and counts the merged group values.</summary>
    /// <returns>The number of values observed across all groups.</returns>
    [Benchmark]
    public async Task<int> PrimitivesGroupByFlatMapCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .GroupBy(static value => value & GroupMask)
            .FlatMap(static IObservableAsync<int> (group) => group)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Splits a ReactiveUI.Extensions async range into four keyed groups and counts the merged group values.</summary>
    /// <returns>The number of values observed across all groups.</returns>
    [Benchmark]
    public async Task<int> ExtensionsGroupBySelectManyCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.SelectMany(
                    ExtensionsAsyncObservable.GroupBy(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => value & GroupMask),
                    static group => group))
            .ConfigureAwait(false);

    /// <summary>Reads a primitive async sequence through an unbounded channel with async iteration.</summary>
    /// <returns>The sum of the values read.</returns>
    [Benchmark]
    public async Task<int> PrimitivesToAsyncEnumerableSumAsync()
    {
        var sum = 0;
        await foreach (var value in PrimitivesAsyncSignal.Sequence(0, Count)
                           .ToAsyncEnumerable(static () => Channel.CreateUnbounded<int>())
                           .ConfigureAwait(false))
        {
            sum += value;
        }

        return sum;
    }

    /// <summary>Reads a ReactiveUI.Extensions async range through an unbounded channel with async iteration.</summary>
    /// <returns>The sum of the values read.</returns>
    [Benchmark]
    public async Task<int> ExtensionsToAsyncEnumerableSumAsync()
    {
        var sum = 0;
        await foreach (var value in ExtensionsAsyncObservable.ToAsyncEnumerable(
                               ExtensionsAsyncObservable.Range(0, Count),
                               static () => Channel.CreateUnbounded<int>())
                           .ConfigureAwait(false))
        {
            sum += value;
        }

        return sum;
    }
}
