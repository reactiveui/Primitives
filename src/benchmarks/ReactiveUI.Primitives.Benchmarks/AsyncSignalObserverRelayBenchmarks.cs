// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsResult = ReactiveUI.Extensions.Async.Result;
using ExtensionsUnhandledExceptionHandler = ReactiveUI.Extensions.Async.UnhandledExceptionHandler;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures delivering notifications to caller-supplied observers: relayed observers, delegate subscribers and a faulting completion handler.</summary>
[MemoryDiagnoser]
public class AsyncSignalObserverRelayBenchmarks
{
    /// <summary>The number of values delivered by each case.</summary>
    private const int Count = 32;

    /// <summary>The failure thrown by the faulting completion handler.</summary>
    private static readonly InvalidOperationException Fault = new("completion handler failure");

    /// <summary>Completes when the primitives unhandled exception handler receives a failure.</summary>
    private TaskCompletionSource<Exception> _primitivesFault = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes when the ReactiveUI.Extensions unhandled exception handler receives a failure.</summary>
    private TaskCompletionSource<Exception> _extensionsFault = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Routes both libraries' unhandled exceptions to this benchmark.</summary>
    [GlobalSetup]
    public void Setup()
    {
        UnhandledExceptionHandler.Register(error => Volatile.Read(ref _primitivesFault).TrySetResult(error));
        ExtensionsUnhandledExceptionHandler.Register(error => Volatile.Read(ref _extensionsFault).TrySetResult(error));
    }

    /// <summary>Delivers a run of values and completion through a relaying wrapper.</summary>
    /// <returns>The number of values the wrapped observer received.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesWrappedObserverRelayAsync()
    {
        AsyncTallyWitness<int> witness = new();
        var wrapped = witness.Wrap();
        for (var i = 0; i < Count; i++)
        {
            await wrapped.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await wrapped.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        var count = await witness.Completion.ConfigureAwait(false);
        await wrapped.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Delivers a run of values and completion through a relaying wrapper in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values the wrapped observer received.</returns>
    [Benchmark]
    public async Task<int> ExtensionsWrappedObserverRelayAsync()
    {
        ExtensionsAsyncTallyWitness<int> witness = new();
        var wrapped = ExtensionsAsyncObservable.Wrap(witness);
        for (var value = 0; value < Count; value++)
        {
            await wrapped.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
        }

        await wrapped.OnCompletedAsync(ExtensionsResult.Success).ConfigureAwait(false);
        var observed = await witness.Completion.ConfigureAwait(false);
        await wrapped.DisposeAsync().ConfigureAwait(false);
        return observed;
    }

    /// <summary>Drains a range into delegate callbacks.</summary>
    /// <returns>The number of values the callbacks received.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDelegateSubscriberAsync()
    {
        var count = 0;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await SignalAsync.Range(0, Count)
            .SubscribeAsync(_ => count++, null, _ => completed.TrySetResult(), CancellationToken.None)
            .ConfigureAwait(false);
        await completed.Task.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Drains a range into delegate callbacks in ReactiveUI.Extensions.</summary>
    /// <returns>The number of values the callbacks received.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDelegateSubscriberAsync()
    {
        var observed = 0;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Range(0, Count),
                _ => observed++,
                null,
                _ => completed.TrySetResult(),
                CancellationToken.None)
            .ConfigureAwait(false);
        await completed.Task.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return observed;
    }

    /// <summary>Drains a range into an asynchronous value callback.</summary>
    /// <returns>The number of values the callback received.</returns>
    [Benchmark]
    public async Task<int> PrimitivesAsyncDelegateSubscriberAsync()
    {
        var count = 0;
        var subscription = await SignalAsync.Range(0, Count)
            .SubscribeAsync((_, _) =>
            {
                count++;
                return default;
            })
            .ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Routes a throwing completion callback to the registered unhandled exception handler.</summary>
    /// <returns>One when the handler received the failure.</returns>
    [Benchmark]
    public async Task<int> PrimitivesFaultingCompletionHandlerAsync()
    {
        TaskCompletionSource<Exception> reported = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _primitivesFault, reported);
        var subscription = await SignalAsync.Return(Count)
            .SubscribeAsync(static _ => { }, null, static _ => throw Fault, CancellationToken.None)
            .ConfigureAwait(false);
        var error = await reported.Task.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return ReferenceEquals(error, Fault) ? 1 : 0;
    }

    /// <summary>Routes a throwing completion callback to the registered unhandled exception handler in ReactiveUI.Extensions.</summary>
    /// <returns>One when the handler received the failure.</returns>
    [Benchmark]
    public async Task<int> ExtensionsFaultingCompletionHandlerAsync()
    {
        TaskCompletionSource<Exception> reported = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _extensionsFault, reported);
        var subscription = await ExtensionsAsyncObservable.SubscribeAsync(
                ExtensionsAsyncObservable.Return(Count),
                static _ => { },
                null,
                static _ => throw Fault,
                CancellationToken.None)
            .ConfigureAwait(false);
        var failure = await reported.Task.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        return ReferenceEquals(failure, Fault) ? 1 : 0;
    }
}
