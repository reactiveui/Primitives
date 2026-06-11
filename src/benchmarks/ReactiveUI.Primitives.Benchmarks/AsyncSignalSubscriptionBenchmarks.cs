// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;

using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks subscription churn for asynchronous signal implementations.</summary>
[MemoryDiagnoser]
public class AsyncSignalSubscriptionBenchmarks
{
    /// <summary>The number of observers subscribed and disposed by each benchmark operation.</summary>
    private const int SubscriberCount = 8;

    /// <summary>The value replayed to each late subscriber.</summary>
    private const int ReplayValue = 42;

    /// <summary>Subscribes and disposes multiple observers against an async replay-latest signal.</summary>
    /// <returns>The total value observed during subscription replay.</returns>
    [Benchmark]
    public async Task<int> PrimitivesReplayLatestSubscribeDisposeAsync()
    {
        var signal = PrimitivesAsyncSignalFactory.CreateReplayLatest<int>();
        var observers = new CountingWitness[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];

        try
        {
            await signal.OnNextAsync(ReplayValue, CancellationToken.None).ConfigureAwait(false);

            for (var i = 0; i < SubscriberCount; i++)
            {
                var observer = new CountingWitness();
                observers[i] = observer;
                subscriptions[i] = await signal.SubscribeAsync(observer, CancellationToken.None).ConfigureAwait(false);
            }

            return Sum(observers);
        }
        finally
        {
            await DisposeAllAsync(subscriptions).ConfigureAwait(false);
            await signal.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Sums the totals recorded by the async observers.</summary>
    /// <param name="observers">The observers to sum.</param>
    /// <returns>The combined observed total.</returns>
    private static int Sum(CountingWitness[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

    /// <summary>Disposes every non-null async subscription in order.</summary>
    /// <param name="subscriptions">The subscriptions to dispose.</param>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    private static async ValueTask DisposeAllAsync(IAsyncDisposable[] subscriptions)
    {
        for (var i = 0; i < subscriptions.Length; i++)
        {
            if (subscriptions[i] is not null)
            {
                await subscriptions[i].DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Observer that accumulates replayed async signal values.</summary>
    private sealed class CountingWitness : Async.WitnessAsync<int>
    {
        /// <summary>Gets the accumulated value total.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Async.Result result) => default;

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Total += value;
            return default;
        }
    }
}
