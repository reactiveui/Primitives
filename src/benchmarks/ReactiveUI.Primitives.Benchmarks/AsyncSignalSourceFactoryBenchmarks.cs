// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsDisposableAsync = ReactiveUI.Extensions.Async.Disposables.DisposableAsync;
using ExtensionsObserver = ReactiveUI.Extensions.Async.IObserverAsync<int>;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures building and draining the async source factories: constant, deferred, callback, job and bridge sources.</summary>
[MemoryDiagnoser]
public class AsyncSignalSourceFactoryBenchmarks
{
    /// <summary>The number of values produced by multi-value sources.</summary>
    private const int Count = 32;

    /// <summary>The value produced by single-value sources.</summary>
    private const int Value = 42;

    /// <summary>The failure carried by failing sources.</summary>
    private static readonly InvalidOperationException Fault = new("source failure");

    /// <summary>A pre-completed task used by the task bridge.</summary>
    private static readonly Task<int> CompletedTask = Task.FromResult(Value);

    /// <summary>The values used by the enumerable bridge.</summary>
    private static readonly int[] Values = [.. Enumerable.Range(0, Count)];

    /// <summary>Drains a constant single-value source.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesReturnCountAsync() =>
        await SignalAsync.Return(Value).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a constant single-value source in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsReturnCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Return(Value)).ConfigureAwait(false);

    /// <summary>Drains an empty source.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesEmptyCountAsync() =>
        await SignalAsync.Empty<int>().CountAsync().ConfigureAwait(false);

    /// <summary>Drains an empty source in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsEmptyCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Empty<int>()).ConfigureAwait(false);

    /// <summary>Subscribes to a failing source and waits for its failure completion.</summary>
    /// <returns>One when the failure was observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesThrowCompletionAsync()
    {
        AsyncTallyWitness<int> witness = new();
        var subscription = await SignalAsync.Throw<int>(Fault)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Failed ? 1 : 0;
    }

    /// <summary>Subscribes to a failing source in ReactiveUI.Extensions and waits for its failure completion.</summary>
    /// <returns>One when the failure was observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsThrowCompletionAsync()
    {
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subscription = await ExtensionsAsyncObservable.Throw<int>(Fault)
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Failed ? 1 : 0;
    }

    /// <summary>Subscribes to and disposes a source that never notifies.</summary>
    /// <returns>One when no terminal notification arrived.</returns>
    [Benchmark]
    public async Task<int> PrimitivesNeverSubscribeDisposeAsync()
    {
        AsyncTallyWitness<int> witness = new();
        var subscription = await SignalAsync.Never<int>()
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Completion.IsCompleted ? 0 : 1;
    }

    /// <summary>Subscribes to and disposes a source that never notifies in ReactiveUI.Extensions.</summary>
    /// <returns>One when no terminal notification arrived.</returns>
    [Benchmark]
    public async Task<int> ExtensionsNeverSubscribeDisposeAsync()
    {
        ExtensionsAsyncTallyWitness<int> witness = new();
        var subscription = await ExtensionsAsyncObservable.Never<int>()
            .SubscribeAsync(witness, CancellationToken.None)
            .ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return witness.Completion.IsCompleted ? 0 : 1;
    }

    /// <summary>Drains a source built per subscriber by a synchronous factory.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDeferCountAsync() =>
        await SignalAsync.Defer(static () => SignalAsync.Return(Value)).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a source built per subscriber by a synchronous factory in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDeferCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Defer(static () => ExtensionsAsyncObservable.Return(Value)))
            .ConfigureAwait(false);

    /// <summary>Drains a source built per subscriber by an asynchronous factory.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDeferAsyncFactoryCountAsync() =>
        await SignalAsync.Defer(static _ => new ValueTask<IObservableAsync<int>>(SignalAsync.Return(Value)))
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Drains a source built per subscriber by an asynchronous factory in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDeferAsyncFactoryCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Defer(static _ =>
                    new ValueTask<ReactiveUI.Extensions.Async.IObservableAsync<int>>(ExtensionsAsyncObservable.Return(Value))))
            .ConfigureAwait(false);

    /// <summary>Drains a callback source that pushes a run of values.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCreateCountAsync() =>
        await SignalAsync.Create<int>(static async (observer, cancellationToken) =>
            {
                await PushValuesAsync(observer, cancellationToken).ConfigureAwait(false);
                return DisposableAsync.Empty;
            })
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Drains a callback source that pushes a run of values in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCreateCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Create<int>(static async (observer, cancellationToken) =>
                {
                    await PushExtensionsValuesAsync(observer, cancellationToken).ConfigureAwait(false);
                    return ExtensionsDisposableAsync.Create(static () => default);
                }))
            .ConfigureAwait(false);

    /// <summary>Drains a job source that starts on the subscribing thread.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesBackgroundJobCountAsync() =>
        await SignalAsync.CreateAsBackgroundJob<int>(PushValuesAsync, true).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a job source that starts on the subscribing thread in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsBackgroundJobCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.CreateAsBackgroundJob<int>(PushExtensionsValuesAsync, true))
            .ConfigureAwait(false);

    /// <summary>Drains a source whose value comes from an asynchronous factory.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFromAsyncCountAsync() =>
        await SignalAsync.FromAsync(static _ => new ValueTask<int>(Value)).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a source whose value comes from an asynchronous factory in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsFromAsyncCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.FromAsync(static _ => new ValueTask<int>(Value)))
            .ConfigureAwait(false);

    /// <summary>Drains a source that runs a synchronous function per subscriber.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesStartCountAsync() =>
        await SignalAsync.Start(static () => Value).CountAsync().ConfigureAwait(false);

    /// <summary>Drains a source that runs a synchronous function per subscriber in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsStartCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.Start(static () => Value))
            .ConfigureAwait(false);

    /// <summary>Drains a completed task bridged to an async signal.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesTaskBridgeCountAsync() =>
        await CompletedTask.ToAsyncSignal().CountAsync().ConfigureAwait(false);

    /// <summary>Drains a completed task bridged to an async observable in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsTaskBridgeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.ToObservableAsync(CompletedTask))
            .ConfigureAwait(false);

    /// <summary>Drains an array bridged to an async signal.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesEnumerableBridgeCountAsync() =>
        await Values.ToAsyncSignal().CountAsync().ConfigureAwait(false);

    /// <summary>Drains an array bridged to an async observable in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsEnumerableBridgeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.ToObservableAsync(Values))
            .ConfigureAwait(false);

    /// <summary>Drains an async iterator bridged to an async signal.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesAsyncEnumerableBridgeCountAsync() =>
        await ProduceValuesAsync().ToAsyncSignal().CountAsync().ConfigureAwait(false);

    /// <summary>Drains an async iterator bridged to an async observable in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values observed.</returns>
    [Benchmark]
    public async Task<int> ExtensionsAsyncEnumerableBridgeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(ExtensionsAsyncObservable.ToObservableAsync(ProduceValuesAsync()))
            .ConfigureAwait(false);

    /// <summary>Reads the only value of a single-value source.</summary>
    /// <returns>The single value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSingleAsync() =>
        await SignalAsync.Return(Value).SingleAsync().ConfigureAwait(false);

    /// <summary>Reads the only value of a single-value source in ReactiveUI.Extensions.</summary>
    /// <returns>The single value.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSingleAsync() =>
        await ExtensionsAsyncObservable.SingleAsync(ExtensionsAsyncObservable.Return(Value)).ConfigureAwait(false);

    /// <summary>Pushes a run of values and completes a primitives async observer.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <param name="cancellationToken">The notification cancellation token.</param>
    /// <returns>A task that completes once every notification was delivered.</returns>
    private static async ValueTask PushValuesAsync(IObserverAsync<int> observer, CancellationToken cancellationToken)
    {
        for (var i = 0; i < Count; i++)
        {
            await observer.OnNextAsync(i, cancellationToken).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }

    /// <summary>Pushes a run of values and completes a ReactiveUI.Extensions async observer.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <param name="cancellationToken">The notification cancellation token.</param>
    /// <returns>A task that completes once every notification was delivered.</returns>
    private static async ValueTask PushExtensionsValuesAsync(ExtensionsObserver observer, CancellationToken cancellationToken)
    {
        for (var value = 0; value < Count; value++)
        {
            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
    }

    /// <summary>Yields a run of values asynchronously.</summary>
    /// <returns>The value sequence.</returns>
    private static async IAsyncEnumerable<int> ProduceValuesAsync()
    {
        await ValueTask.CompletedTask.ConfigureAwait(false);
        for (var i = 0; i < Count; i++)
        {
            yield return i;
        }
    }
}
