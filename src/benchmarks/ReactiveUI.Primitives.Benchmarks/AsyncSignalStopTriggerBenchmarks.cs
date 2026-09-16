// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsSubjectAsync = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures forwarding a live async signal until a stop trigger fires: a task, another signal or a cancellation token.</summary>
[MemoryDiagnoser]
public class AsyncSignalStopTriggerBenchmarks
{
    /// <summary>The number of values forwarded before the stop trigger fires.</summary>
    private const int Count = 32;

    /// <summary>Forwards values until a task completes.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesTakeUntilTaskAsync()
    {
        TaskCompletionSource stop = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        AsyncTallyWitness<int> witness = new();
        var subscription = await signal.TakeUntil(stop.Task)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < Count; i++)
        {
            await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        stop.SetResult();
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Forwards values until a task completes in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeUntilTaskAsync()
    {
        TaskCompletionSource stop = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var subject = ExtensionsSubjectAsync.Create<int>();
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subscription = await ExtensionsAsyncObservable.TakeUntil(subject.Values, stop.Task)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var value = 0; value < Count; value++)
        {
            await subject.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        stop.SetResult();
        var observed = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await subject.DisposeAsync().ConfigureAwait(false);
        return observed;
    }

    /// <summary>Forwards values until another signal emits.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilSignalAsync()
    {
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        var stop = PrimitivesAsyncSignalFactory.Create<bool>();
        AsyncTallyWitness<int> witness = new();
        var subscription = await signal.TakeUntil(stop)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < Count; i++)
        {
            await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await stop.OnNextAsync(true, CancellationToken.None).ConfigureAwait(false);
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await stop.DisposeAsync().ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Forwards values until another subject emits in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeUntilSignalAsync()
    {
        var subject = ExtensionsSubjectAsync.Create<int>();
        var stop = ExtensionsSubjectAsync.Create<bool>();
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subscription = await ExtensionsAsyncObservable.TakeUntil(subject.Values, stop.Values)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var value = 0; value < Count; value++)
        {
            await subject.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        await stop.OnNextAsync(true, CancellationToken.None).ConfigureAwait(false);
        var observed = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await stop.DisposeAsync().ConfigureAwait(false);
        await subject.DisposeAsync().ConfigureAwait(false);
        return observed;
    }

    /// <summary>Forwards values until a cancellation token is cancelled.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTakeUntilCancellationAsync()
    {
        using CancellationTokenSource stop = new();
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        AsyncTallyWitness<int> witness = new();
        var subscription = await signal.TakeUntil(stop.Token)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var i = 0; i < Count; i++)
        {
            await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await stop.CancelAsync().ConfigureAwait(false);
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await signal.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Forwards values until a cancellation token is cancelled in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values forwarded.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTakeUntilCancellationAsync()
    {
        using CancellationTokenSource stop = new();
        var subject = ExtensionsSubjectAsync.Create<int>();
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subscription = await ExtensionsAsyncObservable.TakeUntil(subject.Values, stop.Token)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        for (var value = 0; value < Count; value++)
        {
            await subject.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        await stop.CancelAsync().ConfigureAwait(false);
        var observed = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await subject.DisposeAsync().ConfigureAwait(false);
        return observed;
    }
}
