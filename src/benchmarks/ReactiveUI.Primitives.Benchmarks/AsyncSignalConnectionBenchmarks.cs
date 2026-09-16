// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures sharing one async source among several subscribers through a connectable signal.</summary>
[MemoryDiagnoser]
public class AsyncSignalConnectionBenchmarks
{
    /// <summary>The number of values produced by the shared source.</summary>
    private const int Count = 32;

    /// <summary>The number of subscribers attached before connecting.</summary>
    private const int SubscriberCount = 4;

    /// <summary>Publishes a sequence, attaches subscribers, connects and waits for every subscriber to complete.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark(Baseline = true)]
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public Task<int>PrimitivesPublishConnectAsync() =>
        ConnectAsync(SignalAsync.Range(0, Count).Publish());

    /// <summary>Publishes a replay-latest view of a sequence, attaches subscribers, connects and waits for completion.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public Task<int>PrimitivesReplayLatestPublishConnectAsync() =>
        ConnectAsync(SignalAsync.Range(0, Count).ReplayLatestPublish());

    /// <summary>Publishes a sequence in ReactiveUI.Extensions, attaches subscribers, connects and waits for completion.</summary>
    /// <returns>The total number of values observed by all subscribers.</returns>
    [Benchmark]
    public async Task<int> ExtensionsPublishConnectAsync()
    {
        var connectable = ExtensionsAsyncObservable.Publish(ExtensionsAsyncObservable.Range(0, Count));
        var observers = new ExtensionsAsyncTallyWitness<int>[SubscriberCount];
        var handles = new IAsyncDisposable[SubscriberCount];
        for (var index = 0; index < SubscriberCount; index++)
        {
            observers[index] = new();
            handles[index] = await connectable.SubscribeAsync(observers[index], CancellationToken.None).ConfigureAwait(false);
        }

        var connection = await connectable.ConnectAsync(CancellationToken.None).ConfigureAwait(false);
        var observed = 0;
        foreach (var observer in observers)
        {
            observed += await observer.Completion.ConfigureAwait(false);
        }

        await connection.DisposeAsync().ConfigureAwait(false);
        foreach (var handle in handles)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }

        return observed;
    }

    /// <summary>Attaches subscribers to a connectable signal, connects it and waits for every subscriber to complete.</summary>
    /// <param name="connectable">The connectable signal to drive.</param>
    /// <returns>The total number of values observed by all subscribers.</returns>
    private static async Task<int> ConnectAsync(ConnectableSignalAsync<int> connectable)
    {
        using (connectable)
        {
            IObservableAsync<int> shared = connectable;
            var witnesses = new AsyncTallyWitness<int>[SubscriberCount];
            var subscriptions = new IAsyncDisposable[SubscriberCount];
            for (var i = 0; i < SubscriberCount; i++)
            {
                witnesses[i] = new();
                subscriptions[i] = await shared.SubscribeAsync(witnesses[i], CancellationToken.None).ConfigureAwait(false);
            }

            var connection = await connectable.ConnectAsync(CancellationToken.None).ConfigureAwait(false);
            var total = 0;
            for (var i = 0; i < witnesses.Length; i++)
            {
                total += await witnesses[i].Completion.ConfigureAwait(false);
            }

            await connection.DisposeAsync().ConfigureAwait(false);
            for (var i = 0; i < subscriptions.Length; i++)
            {
                await subscriptions[i].DisposeAsync().ConfigureAwait(false);
            }

            return total;
        }
    }
}
