// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;
using ExtensionsSubjectAsync = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures async timers, intervals and debounce driven by a clock that fires only when the benchmark steps it.</summary>
[MemoryDiagnoser]
public class AsyncSignalSteppedTimerBenchmarks
{
    /// <summary>The number of ticks or debounce windows each case drives.</summary>
    private const int TickCount = 16;

    /// <summary>The due time and period used by every case.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromMilliseconds(10);

    /// <summary>Fires a one-shot timer and waits for its completion.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesOneShotTimerAsync()
    {
        SteppedTimeProvider clock = new();
        AsyncTallyWitness<long> witness = new();
        var subscription = await SignalAsync.Timer(Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await clock.WhenTimerArmedAsync().ConfigureAwait(false);
        _ = clock.Step();
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Fires a one-shot timer in ReactiveUI.Extensions and waits for its completion.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsOneShotTimerAsync()
    {
        SteppedTimeProvider clock = new();
        ExtensionsAsyncTallyWitness<long> witness = new();
        var subscription = await ExtensionsAsyncObservable.Timer(Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await clock.WhenTimerArmedAsync().ConfigureAwait(false);
        _ = clock.Step();
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Steps a periodic timer through a run of ticks.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesPeriodicTimerTicksAsync()
    {
        SteppedTimeProvider clock = new();
        AsyncTallyWitness<long> witness = new();
        var subscription = await SignalAsync.Timer(Window, Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < TickCount; i++)
        {
            var tick = witness.NextValue;
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await tick.ConfigureAwait(false);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }

    /// <summary>Steps a periodic timer in ReactiveUI.Extensions through a run of ticks.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPeriodicTimerTicksAsync()
    {
        SteppedTimeProvider clock = new();
        ExtensionsAsyncTallyWitness<long> witness = new();
        var subscription = await ExtensionsAsyncObservable.Timer(Window, Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < TickCount; i++)
        {
            var tick = witness.NextValue;
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await tick.ConfigureAwait(false);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }

    /// <summary>Steps an interval through a run of ticks.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesIntervalTicksAsync()
    {
        SteppedTimeProvider clock = new();
        AsyncTallyWitness<long> witness = new();
        var subscription = await SignalAsync.Interval(Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < TickCount; i++)
        {
            var tick = witness.NextValue;
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await tick.ConfigureAwait(false);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }

    /// <summary>Steps an interval in ReactiveUI.Extensions through a run of ticks.</summary>
    /// <returns>The number of ticks observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsIntervalTicksAsync()
    {
        SteppedTimeProvider clock = new();
        ExtensionsAsyncTallyWitness<long> witness = new();
        var subscription = await ExtensionsAsyncObservable.Interval(Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < TickCount; i++)
        {
            var tick = witness.NextValue;
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await tick.ConfigureAwait(false);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }

    /// <summary>Debounces two values per window, so each window supersedes one pending delay and forwards the other.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesThrottleSupersedeAsync()
    {
        SteppedTimeProvider clock = new();
        AsyncTallyWitness<int> witness = new();
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        var subscription = await signal.Throttle(Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < TickCount; i++)
        {
            var forwarded = witness.NextValue;
            await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            await signal.OnNextAsync(i + 1, CancellationToken.None).ConfigureAwait(false);
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await forwarded.ConfigureAwait(false);
        }

        await signal.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }

    /// <summary>Debounces two values per window in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsThrottleSupersedeAsync()
    {
        SteppedTimeProvider clock = new();
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subject = ExtensionsSubjectAsync.Create<int>();
        var subscription = await ExtensionsAsyncObservable.Throttle(subject.Values, Window, clock)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var window = 0; window < TickCount; window++)
        {
            var forwarded = witness.NextValue;
            await subject.OnNextAsync(window, CancellationToken.None).ConfigureAwait(false);
            await subject.OnNextAsync(window + 1, CancellationToken.None).ConfigureAwait(false);
            await clock.WhenTimerArmedAsync().ConfigureAwait(false);
            _ = clock.Step();
            await forwarded.ConfigureAwait(false);
        }

        await subject.OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await subject.DisposeAsync().ConfigureAwait(false);
        return witness.Count;
    }
}
