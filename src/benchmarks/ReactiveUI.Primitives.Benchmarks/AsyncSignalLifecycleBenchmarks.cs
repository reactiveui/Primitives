// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async side-effect, sharing, resource and early-termination operators.</summary>
[MemoryDiagnoser]
public class AsyncSignalLifecycleBenchmarks
{
    /// <summary>The number of values produced by each source.</summary>
    private const int Count = 64;

    /// <summary>The value at which predicate-driven termination stops.</summary>
    private const int Half = Count / 2;

    /// <summary>Taps each value of a primitive async sequence to accumulate a sum.</summary>
    /// <returns>The sum observed by the tap.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesTapSumAsync()
    {
        var sum = 0;
        _ = await PrimitivesAsyncSignal.Sequence(0, Count)
            .Tap(value => sum += value)
            .CountAsync()
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Taps each value of a primitive async sequence with the Rx-named operator.</summary>
    /// <returns>The sum observed by the tap.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDoSumAsync()
    {
        var sum = 0;
        _ = await PrimitivesAsyncSignal.Sequence(0, Count)
            .Do(value => sum += value, null, null)
            .CountAsync()
            .ConfigureAwait(false);
        return sum;
    }

    /// <summary>Taps each value of a ReactiveUI.Extensions async range to accumulate a sum.</summary>
    /// <returns>The sum observed by the tap.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDoSumAsync()
    {
        var sum = 0;
        _ = await ExtensionsAsyncObservable.CountAsync(
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

    /// <summary>Runs a subscribe and a dispose side effect around a primitive async sequence.</summary>
    /// <returns>The number of values plus the two side effects.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDoOnSubscribeOnDisposeCountAsync()
    {
        var effects = 0;
        var count = await PrimitivesAsyncSignal.Sequence(0, Count)
            .DoOnSubscribe(() => effects++)
            .OnDispose(() => effects++)
            .CountAsync()
            .ConfigureAwait(false);
        return count + effects;
    }

    /// <summary>Runs a subscribe and a dispose side effect around a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of values plus the two side effects.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDoOnSubscribeOnDisposeCountAsync()
    {
        var effects = 0;
        var count = await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.OnDispose(
                    ExtensionsAsyncObservable.DoOnSubscribe(
                        ExtensionsAsyncObservable.Range(0, Count),
                        () => effects++),
                    () => effects++))
            .ConfigureAwait(false);
        return count + effects;
    }

    /// <summary>Creates, uses and disposes a per-subscription resource around a primitive async sequence.</summary>
    /// <returns>The number of values plus one when the resource was disposed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesUsingResourceCountAsync()
    {
        CountingResource resource = new();
        var count = await PrimitivesAsyncSignal.Using(
                _ => new ValueTask<CountingResource>(resource),
                static _ => PrimitivesAsyncSignal.Sequence(0, Count))
            .CountAsync()
            .ConfigureAwait(false);
        return count + resource.Disposals;
    }

    /// <summary>Creates, uses and disposes a per-subscription resource around a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of values plus one when the resource was disposed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsUsingResourceCountAsync()
    {
        CountingResource resource = new();
        var count = await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Using(
                    _ => new ValueTask<CountingResource>(resource),
                    static _ => ExtensionsAsyncObservable.Range(0, Count)))
            .ConfigureAwait(false);
        return count + resource.Disposals;
    }

    /// <summary>Subscribes a callback to a primitive async sequence and disposes the subscription.</summary>
    /// <returns>The sum observed by the callback.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSubscribeCallbackSumAsync()
    {
        var sum = 0;
        var subscription = await PrimitivesAsyncSignal.Sequence(0, Count)
            .SubscribeAsync(value => sum += value)
            .ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return sum;
    }

    /// <summary>Subscribes a callback to a ReactiveUI.Extensions async range and disposes the subscription.</summary>
    /// <returns>The sum observed by the callback.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSubscribeCallbackSumAsync()
    {
        var sum = 0;
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                value => sum += value)
            .ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return sum;
    }

    /// <summary>Publishes a primitive async sequence to two subscribers and connects it once.</summary>
    /// <returns>The sum observed by both subscribers.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPublishConnectSumAsync()
    {
        using var connectable = PrimitivesAsyncSignal.Sequence(0, Count).Publish();
        var sum = 0;
        var first = await connectable.SubscribeAsync(value => sum += value).ConfigureAwait(false);
        var second = await connectable.SubscribeAsync(value => sum += value).ConfigureAwait(false);
        var connection = await connectable.ConnectAsync(CancellationToken.None).ConfigureAwait(false);
        await connection.DisposeAsync().ConfigureAwait(false);
        await second.DisposeAsync().ConfigureAwait(false);
        await first.DisposeAsync().ConfigureAwait(false);
        return sum;
    }

    /// <summary>Publishes a ReactiveUI.Extensions async range to two subscribers and connects it once.</summary>
    /// <returns>The sum observed by both subscribers.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPublishConnectSumAsync()
    {
        var connectable = ExtensionsAsyncObservable.Publish(ExtensionsAsyncObservable.Range(0, Count));
        var sum = 0;
        var first = await ExtensionsAsyncObservable.SubscribeAsync(connectable, value => sum += value)
            .ConfigureAwait(false);
        var second = await ExtensionsAsyncObservable.SubscribeAsync(connectable, value => sum += value)
            .ConfigureAwait(false);
        var connection = await connectable.ConnectAsync(CancellationToken.None).ConfigureAwait(false);
        await connection.DisposeAsync().ConfigureAwait(false);
        await second.DisposeAsync().ConfigureAwait(false);
        await first.DisposeAsync().ConfigureAwait(false);
        return sum;
    }

    /// <summary>Shares a primitive async sequence through a reference-counted connection.</summary>
    /// <returns>The number of shared values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesRefCountCountAsync()
    {
        using var connectable = PrimitivesAsyncSignal.Sequence(0, Count).Publish();
        return await connectable.RefCount().CountAsync().ConfigureAwait(false);
    }

    /// <summary>Shares a ReactiveUI.Extensions async range through a reference-counted connection.</summary>
    /// <returns>The number of shared values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsRefCountCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.RefCount(
                    ExtensionsAsyncObservable.Publish(ExtensionsAsyncObservable.Range(0, Count))))
            .ConfigureAwait(false);

    /// <summary>Runs a function through a primitive async start sequence and reads its result.</summary>
    /// <returns>The function result.</returns>
    [Benchmark]
    public async Task<int> PrimitivesStartFirstAsync() =>
        await PrimitivesAsyncSignal.Start(static () => Count)
            .FirstAsync()
            .ConfigureAwait(false);

    /// <summary>Runs a function through a ReactiveUI.Extensions async start sequence and reads its result.</summary>
    /// <returns>The function result.</returns>
    [Benchmark]
    public async Task<int> ExtensionsStartFirstAsync() =>
        await ExtensionsAsyncObservable.FirstAsync(ExtensionsAsyncObservable.Start(static () => Count))
            .ConfigureAwait(false);

    /// <summary>Partitions a primitive async sequence into even and odd branches and sums both.</summary>
    /// <returns>The sum observed across both branches.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPartitionSumAsync()
    {
        var (evens, odds) = PrimitivesAsyncSignal.Sequence(0, Count).Partition(static value => (value & 1) == 0);
        var evenSum = await evens.ReduceAsync(0, static (sum, value) => sum + value).ConfigureAwait(false);
        var oddSum = await odds.ReduceAsync(0, static (sum, value) => sum + value).ConfigureAwait(false);
        return evenSum + oddSum;
    }

    /// <summary>Partitions a ReactiveUI.Extensions async range into even and odd branches and sums both.</summary>
    /// <returns>The sum observed across both branches.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPartitionSumAsync()
    {
        var (evens, odds) = ExtensionsAsyncObservable.Partition(
            ExtensionsAsyncObservable.Range(0, Count),
            static value => (value & 1) == 0);
        var evenSum = await ExtensionsAsyncObservable.AggregateAsync(evens, 0, static (sum, value) => sum + value)
            .ConfigureAwait(false);
        var oddSum = await ExtensionsAsyncObservable.AggregateAsync(odds, 0, static (sum, value) => sum + value)
            .ConfigureAwait(false);
        return evenSum + oddSum;
    }

    /// <summary>Stops a primitive async sequence once a value satisfies a predicate.</summary>
    /// <returns>The number of values before the stop.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilPredicateCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .TakeUntil(static value => value >= Half)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Stops a primitive async sequence once an asynchronous predicate is satisfied.</summary>
    /// <returns>The number of values before the stop.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilAsyncPredicateCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .TakeUntil(static (value, _) => new ValueTask<bool>(value >= Half))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Mirrors a primitive async sequence until a stop sequence that never fires.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilOtherCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .TakeUntil(PrimitivesAsyncSignal.Never<int>(), new TakeUntilOptions { SourceFailsWhenOtherFails = true })
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Mirrors a ReactiveUI.Extensions async range until a stop sequence that never fires.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeUntilOtherCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.TakeUntil(
                    ExtensionsAsyncObservable.Range(0, Count),
                    ExtensionsAsyncObservable.Never<int>()))
            .ConfigureAwait(false);

    /// <summary>Mirrors a primitive async sequence until a pending task or a live cancellation token stops it.</summary>
    /// <returns>The number of values forwarded through both stop conditions.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilTaskAndTokenCountAsync()
    {
        TaskCompletionSource stop = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource cancellation = new();
        var count = await PrimitivesAsyncSignal.Sequence(0, Count)
            .TakeUntil(stop.Task)
            .TakeUntil(cancellation.Token)
            .CountAsync()
            .ConfigureAwait(false);
        _ = stop.TrySetResult();
        return count;
    }

    /// <summary>Emits the first primitive async value that satisfies a condition.</summary>
    /// <returns>The first matching value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesWaitUntilFirstAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .WaitUntil(static value => value >= Half)
            .FirstAsync()
            .ConfigureAwait(false);

    /// <summary>Emits the first ReactiveUI.Extensions async value that satisfies a condition.</summary>
    /// <returns>The first matching value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsWaitUntilFirstAsync() =>
        await ExtensionsAsyncObservable.FirstAsync(
                ExtensionsAsyncObservable.WaitUntil(
                    ExtensionsAsyncObservable.Range(0, Count),
                    static value => value >= Half))
            .ConfigureAwait(false);

    /// <summary>An async resource that records how often it is disposed.</summary>
    private sealed class CountingResource : IAsyncDisposable
    {
        /// <summary>Gets the number of disposals.</summary>
        public int Disposals { get; private set; }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }
}
