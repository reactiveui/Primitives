// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using AsyncRxObservable = System.Reactive.Linq.AsyncObservable;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async filtering operators that drop, keep or narrow values from a completed sequence.</summary>
[MemoryDiagnoser]
public class AsyncSignalFilterBenchmarks
{
    /// <summary>The number of values produced by each source sequence.</summary>
    private const int Count = 64;

    /// <summary>The number of leading values dropped by skip and take cases.</summary>
    private const int Half = Count / 2;

    /// <summary>Skips the first half of a primitive async sequence and counts the rest.</summary>
    /// <returns>The number of values that passed the skip.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesSkipCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Skip(Half)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Skips the first half of a ReactiveUI.Extensions async range and counts the rest.</summary>
    /// <returns>The number of values that passed the skip.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSkipCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Skip(ExtensionsAsyncObservable.Range(0, Count), Half))
            .ConfigureAwait(false);

    /// <summary>Skips the first half of a System.Reactive.Async range and counts the rest.</summary>
    /// <returns>The number of values that passed the skip.</returns>
    [Benchmark]
    public async Task<int> AsyncRxSkipCountAsync() =>
        await AsyncRxObservable.Count(AsyncRxObservable.Skip(AsyncRxObservable.Range(0, Count), Half));

    /// <summary>Skips values of a primitive async sequence while they stay below the midpoint.</summary>
    /// <returns>The number of values after the predicate first fails.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSkipWhileCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .SkipWhile(static value => value < Half)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Skips values of a ReactiveUI.Extensions async range while they stay below the midpoint.</summary>
    /// <returns>The number of values after the predicate first fails.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSkipWhileCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.SkipWhile(
                    ExtensionsAsyncObservable.Range(0, Count),
                    static value => value < Half))
            .ConfigureAwait(false);

    /// <summary>Takes the first half of a primitive async sequence and counts it.</summary>
    /// <returns>The number of values taken.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Take(Half)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Takes the first half of a ReactiveUI.Extensions async range and counts it.</summary>
    /// <returns>The number of values taken.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Take(ExtensionsAsyncObservable.Range(0, Count), Half))
            .ConfigureAwait(false);

    /// <summary>Takes values of a primitive async sequence while they stay below the midpoint.</summary>
    /// <returns>The number of values taken.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeWhileCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .TakeWhile(static value => value < Half)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Takes values of a ReactiveUI.Extensions async range while they stay below the midpoint.</summary>
    /// <returns>The number of values taken.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeWhileCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.TakeWhile(
                    ExtensionsAsyncObservable.Range(0, Count),
                    static value => value < Half))
            .ConfigureAwait(false);

    /// <summary>Removes repeated values from a primitive async sequence with the Rx-named operator.</summary>
    /// <returns>The number of distinct values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDistinctCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => value & 0xF)
            .Distinct()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Removes repeated values from a primitive async sequence with the primitives-named operator.</summary>
    /// <returns>The number of distinct values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesUniqueCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => value & 0xF)
            .Unique()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Removes repeated values from a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of distinct values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDistinctCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Distinct(
                    ExtensionsAsyncObservable.Select(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => value & 0xF)))
            .ConfigureAwait(false);

    /// <summary>Removes adjacent duplicates from a primitive async sequence.</summary>
    /// <returns>The number of values that differ from their predecessor.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDistinctUntilChangedCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => value >> 2)
            .DistinctUntilChanged()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Removes adjacent duplicates from a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of values that differ from their predecessor.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDistinctUntilChangedCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.DistinctUntilChanged(
                    ExtensionsAsyncObservable.Select(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => value >> 2)))
            .ConfigureAwait(false);

    /// <summary>Keeps the string values of a mixed primitive async sequence.</summary>
    /// <returns>The number of string values kept.</returns>
    [Benchmark]
    public async Task<int> PrimitivesOfTypeCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => (value & 1) == 0 ? (object?)"even" : value)
            .KeepType<string>()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Keeps the string values of a mixed ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of string values kept.</returns>
    [Benchmark]
    public async Task<int> ExtensionsOfTypeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.OfType<object, string>(
                    ExtensionsAsyncObservable.Select(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => (value & 1) == 0 ? (object)"even" : value)))
            .ConfigureAwait(false);

    /// <summary>Casts boxed values of a primitive async sequence back to integers and sums them.</summary>
    /// <returns>The sum of the cast values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCastSumAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => (object?)value)
            .CastTo<int>()
            .ReduceAsync(0, static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Casts boxed values of a ReactiveUI.Extensions async range back to integers and sums them.</summary>
    /// <returns>The sum of the cast values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCastSumAsync() =>
        await ExtensionsAsyncObservable.AggregateAsync(
                ExtensionsAsyncObservable.Cast<object, int>(
                    ExtensionsAsyncObservable.Select(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => (object)value)),
                0,
                static (sum, value) => sum + value)
            .ConfigureAwait(false);

    /// <summary>Negates and keeps the true values of a primitive async boolean sequence.</summary>
    /// <returns>The number of values that were false before negation.</returns>
    [Benchmark]
    public async Task<int> PrimitivesNotWhereTrueCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => (value & 1) == 0)
            .Not()
            .WhereTrue()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Negates and keeps the true values of a ReactiveUI.Extensions async boolean range.</summary>
    /// <returns>The number of values that were false before negation.</returns>
    [Benchmark]
    public async Task<int> ExtensionsNotWhereTrueCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.WhereTrue(
                    ExtensionsAsyncObservable.Not(
                        ExtensionsAsyncObservable.Select(
                            ExtensionsAsyncObservable.Range(0, Count),
                            static value => (value & 1) == 0))))
            .ConfigureAwait(false);

    /// <summary>Pairs each value of a primitive async sequence with its predecessor.</summary>
    /// <returns>The number of adjacent pairs.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPairwiseCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Pairwise()
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Pairs each value of a ReactiveUI.Extensions async range with its predecessor.</summary>
    /// <returns>The number of adjacent pairs.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPairwiseCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Pairwise(ExtensionsAsyncObservable.Range(0, Count)))
            .ConfigureAwait(false);
}
