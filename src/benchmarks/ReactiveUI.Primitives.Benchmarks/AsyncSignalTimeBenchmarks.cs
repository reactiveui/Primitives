// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsAsyncSubject = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks async time-based operators driven by a fake time provider.</summary>
[MemoryDiagnoser]
public class AsyncSignalTimeBenchmarks
{
    /// <summary>The number of values pushed through each operator.</summary>
    private const int Count = 32;

    /// <summary>The quiet period, delay and deadline used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromMilliseconds(10);

    /// <summary>The deadline that a synchronous sequence never misses.</summary>
    private static readonly TimeSpan GenerousDeadline = TimeSpan.FromMinutes(1);

    /// <summary>Pushes a burst into a primitive async debounce and advances time until the last value is forwarded.</summary>
    /// <returns>The value forwarded after the quiet period.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesThrottleBurstAsync()
    {
        FakeTimeProvider time = new();
        var source = PrimitivesAsyncSignalFactory.Create<int>();
        TaskCompletionSource<int> forwarded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await source.Throttle(Window, time)
            .SubscribeAsync(value => forwarded.TrySetResult(value))
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                await source.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            time.Advance(Window);
            return await forwarded.Task.ConfigureAwait(false);
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Pushes a burst into a ReactiveUI.Extensions async throttle and advances time until the last value is forwarded.</summary>
    /// <returns>The value forwarded after the quiet period.</returns>
    [Benchmark]
    public async Task<int> ExtensionsThrottleBurstAsync()
    {
        FakeTimeProvider time = new();
        var source = ExtensionsAsyncSubject.Create<int>();
        TaskCompletionSource<int> forwarded = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Throttle(source, Window, time),
                value => forwarded.TrySetResult(value))
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                await source.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            time.Advance(Window);
            return await forwarded.Task.ConfigureAwait(false);
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Delays each value of a primitive async signal, advancing time once per pending push.</summary>
    /// <returns>The sum of the delayed values.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDelayPerValueAsync()
    {
        FakeTimeProvider time = new();
        var source = PrimitivesAsyncSignalFactory.Create<int>();
        var sum = 0;
        var subscription = await source.Delay(Window, time)
            .SubscribeAsync(value => sum += value)
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                var pending = source.OnNextAsync(i, CancellationToken.None);
                time.Advance(Window);
                await pending.ConfigureAwait(false);
            }

            return sum;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Delays each value of a ReactiveUI.Extensions async subject, advancing time once per pending push.</summary>
    /// <returns>The sum of the delayed values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDelayPerValueAsync()
    {
        FakeTimeProvider time = new();
        var source = ExtensionsAsyncSubject.Create<int>();
        var sum = 0;
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Delay(source, Window, time),
                value => sum += value)
            .ConfigureAwait(false);
        try
        {
            for (var i = 0; i < Count; i++)
            {
                var pending = source.OnNextAsync(i, CancellationToken.None);
                time.Advance(Window);
                await pending.ConfigureAwait(false);
            }

            return sum;
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Rearms a primitive async deadline for every value of a sequence that never misses it.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTimeoutRearmCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .Timeout(GenerousDeadline, new FakeTimeProvider())
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Rearms a ReactiveUI.Extensions async deadline for every value of a range that never misses it.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTimeoutRearmCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Timeout(
                    ExtensionsAsyncObservable.Range(0, Count),
                    GenerousDeadline,
                    new FakeTimeProvider()))
            .ConfigureAwait(false);

    /// <summary>Switches a silent primitive async source to its fallback once the deadline elapses.</summary>
    /// <returns>The number of fallback values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTimeoutFallbackCountAsync()
    {
        FakeTimeProvider time = new();
        var counting = PrimitivesAsyncSignal.Never<int>()
            .Timeout(Window, PrimitivesAsyncSignal.Sequence(0, Count), time)
            .CountAsync();
        time.Advance(Window);
        return await counting.ConfigureAwait(false);
    }

    /// <summary>Switches a silent ReactiveUI.Extensions async source to its fallback once the deadline elapses.</summary>
    /// <returns>The number of fallback values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTimeoutFallbackCountAsync()
    {
        FakeTimeProvider time = new();
        var counting = ExtensionsAsyncObservable.CountAsync(
            ExtensionsAsyncObservable.Timeout(
                ExtensionsAsyncObservable.Never<int>(),
                Window,
                ExtensionsAsyncObservable.Range(0, Count),
                time));
        time.Advance(Window);
        return await counting.ConfigureAwait(false);
    }
}
