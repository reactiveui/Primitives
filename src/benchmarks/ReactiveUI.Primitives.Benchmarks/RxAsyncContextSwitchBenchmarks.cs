// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncContext = ReactiveUI.Extensions.Async.AsyncContext;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using ShimAsyncContext = ReactiveUI.Primitives.Async.Reactive.AsyncContext;
using ShimAsyncExtensions = ReactiveUI.Primitives.Async.Reactive.SignalAsyncReactiveExtensions;
using ShimContextExtensions = ReactiveUI.Primitives.Async.Reactive.AsyncContextExtensions;
using ShimContextSwitchSignal = ReactiveUI.Primitives.Async.Reactive.ContextSwitchSignalAsync<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures moving async notifications onto System.Reactive schedulers, synchronization contexts and task schedulers.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class RxAsyncContextSwitchBenchmarks
{
    /// <summary>The number of values pushed through each context switch.</summary>
    private const int Count = 32;

    /// <summary>A synchronization context that runs every posted continuation on the posting thread.</summary>
    private static readonly InlineSynchronizationContext InlineContext = new();

    /// <summary>A task scheduler that runs every queued task on the queuing thread.</summary>
    private static readonly InlineTaskScheduler InlineScheduler = new();

    /// <summary>Delivers every value through an immediate System.Reactive scheduler.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WitnessOnScheduler")]
    public async Task<int> PrimitivesWitnessOnImmediateSchedulerCountAsync() =>
        await ShimAsyncExtensions.WitnessOn(PrimitivesAsyncSignal.Sequence(0, Count), ImmediateScheduler.Instance)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Delivers every value through an immediate System.Reactive scheduler in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("WitnessOnScheduler")]
    public async Task<int> ExtensionsObserveOnImmediateSchedulerCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ObserveOn(ExtensionsAsyncObservable.Range(0, Count), ImmediateScheduler.Instance))
            .ConfigureAwait(false);

    /// <summary>Posts every value to a synchronization context, yielding even when already on it.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WitnessOnSynchronizationContext")]
    public async Task<int> PrimitivesWitnessOnForcedSynchronizationContextCountAsync() =>
        await ShimAsyncExtensions.WitnessOn(PrimitivesAsyncSignal.Sequence(0, Count), InlineContext, true)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Posts every value to a synchronization context, yielding even when already on it, in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("WitnessOnSynchronizationContext")]
    public async Task<int> ExtensionsObserveOnForcedSynchronizationContextCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ObserveOn(ExtensionsAsyncObservable.Range(0, Count), InlineContext, true))
            .ConfigureAwait(false);

    /// <summary>Yields every value back to the synchronization context that was current at subscription.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("YieldToCurrentContext")]
    public async Task<int> PrimitivesYieldToCurrentContextCountAsync()
    {
        ValueTask<int> counting;
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(InlineContext);
        try
        {
            counting = ShimAsyncExtensions.Yield(PrimitivesAsyncSignal.Sequence(0, Count)).CountAsync();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        return await counting.ConfigureAwait(false);
    }

    /// <summary>Forces every value back onto the synchronization context that was current at subscription in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("YieldToCurrentContext")]
    public async Task<int> ExtensionsForcedObserveOnCurrentContextCountAsync()
    {
        Task<int> counting;
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(InlineContext);
        try
        {
            counting = ExtensionsAsyncObservable.CountAsync(
                    ExtensionsAsyncObservable.ObserveOn(
                        ExtensionsAsyncObservable.Range(0, Count),
                        ExtensionsAsyncContext.GetCurrent(),
                        true))
                .AsTask();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        return await counting.ConfigureAwait(false);
    }

    /// <summary>Delivers every value through the context-switch signal bound to an immediate scheduler context.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ContextSwitchSignal")]
    public async Task<int> PrimitivesContextSwitchSignalCountAsync()
    {
        ShimContextSwitchSignal switched = new(
            PrimitivesAsyncSignal.Sequence(0, Count),
            ShimAsyncContext.From(ImmediateScheduler.Instance),
            false);
        return await switched.CountAsync().ConfigureAwait(false);
    }

    /// <summary>Delivers every value through an immediate scheduler context in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("ContextSwitchSignal")]
    public async Task<int> ExtensionsObserveOnImmediateContextCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ObserveOn(
                    ExtensionsAsyncObservable.Range(0, Count),
                    ExtensionsAsyncContext.From(ImmediateScheduler.Instance),
                    false))
            .ConfigureAwait(false);

    /// <summary>Delivers every value through a task scheduler selected by a true condition.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ConditionalObserveOn")]
    public async Task<int> PrimitivesObserveOnIfTaskSchedulerCountAsync() =>
        await ShimAsyncExtensions.ObserveOnIf(PrimitivesAsyncSignal.Sequence(0, Count), true, InlineScheduler)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Delivers every value through a task scheduler selected by a true condition in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("ConditionalObserveOn")]
    public async Task<int> ExtensionsObserveOnIfTaskSchedulerCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ObserveOnIf(ExtensionsAsyncObservable.Range(0, Count), true, InlineScheduler))
            .ConfigureAwait(false);

    /// <summary>Delivers every value through an optional scheduler context that is supplied, forcing a yield per value.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("OptionalObserveOn")]
    public async Task<int> PrimitivesObserveOnSafeContextCountAsync() =>
        await ShimAsyncExtensions.ObserveOnSafe(
                PrimitivesAsyncSignal.Sequence(0, Count),
                ShimAsyncContext.From(ImmediateScheduler.Instance),
                true)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Delivers every value through an optional scheduler context that is supplied, forcing a yield per value, in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values delivered.</returns>
    [Benchmark]
    [BenchmarkCategory("OptionalObserveOn")]
    public async Task<int> ExtensionsObserveOnSafeContextCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.ObserveOnSafe(
                    ExtensionsAsyncObservable.Range(0, Count),
                    ExtensionsAsyncContext.From(ImmediateScheduler.Instance),
                    true))
            .ConfigureAwait(false);

    /// <summary>Awaits a switch to the default context from the default context, which continues inline.</summary>
    /// <returns>The number of switches that landed on the requested context.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SwitchToCurrentContext")]
    public async Task<int> PrimitivesSwitchToCurrentContextAsync()
    {
        var context = ShimAsyncContext.Default;
        var matched = 0;
        for (var i = 0; i < Count; i++)
        {
            await context.SwitchContextAsync(false, CancellationToken.None);
            if (ShimContextExtensions.IsSameAsCurrentAsyncContext(context))
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>Awaits a switch to the default context from the default context in ReactiveUI.Extensions.</summary>
    /// <returns>The number of switches that landed on the requested context.</returns>
    [Benchmark]
    [BenchmarkCategory("SwitchToCurrentContext")]
    public async Task<int> ExtensionsSwitchToCurrentContextAsync()
    {
        var context = ExtensionsAsyncContext.Default;
        var matched = 0;
        for (var i = 0; i < Count; i++)
        {
            await context.SwitchContextAsync(false, CancellationToken.None);
            if (ReactiveUI.Extensions.Async.AsyncContextMixins.IsSameAsCurrentAsyncContext(context))
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>A synchronization context that invokes posted callbacks on the posting thread.</summary>
    private sealed class InlineSynchronizationContext : SynchronizationContext
    {
        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => d(state);

        /// <inheritdoc/>
        public override SynchronizationContext CreateCopy() => this;
    }

    /// <summary>A task scheduler that executes queued tasks on the queuing thread.</summary>
    private sealed class InlineTaskScheduler : TaskScheduler
    {
        /// <inheritdoc/>
        protected override IEnumerable<Task>? GetScheduledTasks() => null;

        /// <inheritdoc/>
        protected override void QueueTask(Task task) => _ = TryExecuteTask(task);

        /// <inheritdoc/>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => TryExecuteTask(task);
    }
}
