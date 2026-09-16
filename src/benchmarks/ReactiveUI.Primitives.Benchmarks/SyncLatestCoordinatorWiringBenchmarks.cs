// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Advanced;
using ReactiveUI.Primitives.Async.Signals;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures combining the latest values of two sources through a coordinator wired by the slot extensions, against the built-in operator.</summary>
[MemoryDiagnoser]
public class SyncLatestCoordinatorWiringBenchmarks
{
    /// <summary>The number of values pushed into the first source once both sources hold one.</summary>
    private const int PushCount = 32;

    /// <summary>Combines two sources through a coordinator wired by the slot extensions.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesWiredCoordinatorAsync()
    {
        var first = PrimitivesAsyncSignalFactory.CreateBehavior(0);
        var second = PrimitivesAsyncSignalFactory.CreateBehavior(0);
        AsyncTallyWitness<int> witness = new();
        SumCoordinator coordinator = new(witness, first, second);
        await ((ISyncLatestCoordinator<int>)coordinator).SubscribeSourcesAsync(CancellationToken.None)
            .ConfigureAwait(false);
        return await DriveAsync(coordinator, witness, first, second).ConfigureAwait(false);
    }

    /// <summary>Combines two sources through the built-in typed operator.</summary>
    /// <returns>The number of combined values observed.</returns>
    [Benchmark]
    public async Task<int> PrimitivesBuiltInCombineLatestAsync()
    {
        var first = PrimitivesAsyncSignalFactory.CreateBehavior(0);
        var second = PrimitivesAsyncSignalFactory.CreateBehavior(0);
        AsyncTallyWitness<int> witness = new();
        var combined = first.CombineLatest(second, static (a, b) => a + b);
        var subscription = await combined.SubscribeAsync(witness, CancellationToken.None).ConfigureAwait(false);
        for (var i = 0; i < PushCount; i++)
        {
            await first.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await first.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await second.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        var count = await witness.Completion.ConfigureAwait(false);
        await subscription.DisposeAsync().ConfigureAwait(false);
        await first.DisposeAsync().ConfigureAwait(false);
        await second.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>Pushes values into the first source, completes both, and waits for the combined completion.</summary>
    /// <param name="coordinator">The coordinator under measurement.</param>
    /// <param name="witness">The witness counting combined values.</param>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <returns>The number of combined values observed.</returns>
    private static async Task<int> DriveAsync(
        ISyncLatestCoordinator<int> coordinator,
        AsyncTallyWitness<int> witness,
        ISignalAsync<int> first,
        ISignalAsync<int> second)
    {
        for (var i = 0; i < PushCount; i++)
        {
            await first.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
        }

        await first.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await second.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        var count = await witness.Completion.ConfigureAwait(false);
        await coordinator.DisposeAsync().ConfigureAwait(false);
        await first.DisposeAsync().ConfigureAwait(false);
        await second.DisposeAsync().ConfigureAwait(false);
        return count;
    }

    /// <summary>A coordinator that sums the latest value of two sources, wired through the slot extensions.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    private sealed class SumCoordinator(
        IObserverAsync<int> observer,
        IObservableAsync<int> first,
        IObservableAsync<int> second) : ISyncLatestCoordinator<int>
    {
        /// <summary>The number of combined sources.</summary>
        private const int SourceCount = 2;

        /// <summary>The latest value of the first source.</summary>
        private int _firstValue;

        /// <summary>The latest value of the second source.</summary>
        private int _secondValue;

        /// <inheritdoc/>
        public SyncLatestLifecycle<int> Lifecycle { get; } = new(observer, SourceCount);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => Lifecycle.DisposeAsync();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask EmitLatestAsync() => Lifecycle.EmitDownstreamAsync(_firstValue + _secondValue);

        /// <inheritdoc/>
        public ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>
            index == 0
                ? first.SubscribeToSlotAsync(this, index, value => _firstValue = value, cancellationToken)
                : second.SubscribeToSlotAsync(this, index, value => _secondValue = value, cancellationToken);
    }
}
