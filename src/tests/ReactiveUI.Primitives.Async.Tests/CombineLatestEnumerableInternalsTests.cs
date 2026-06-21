// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Direct unit tests for the internal types inside
/// <c>SyncLatestEnumerableSignal{TSource,TResult}</c> that the public API path doesn't
/// fully exercise — specifically the contractual <see cref="IAsyncDisposable.DisposeAsync"/>
/// stub on <c>IndexedWitness</c>.</summary>
public class CombineLatestEnumerableInternalsTests
{
    /// <summary>Verifies the per-source <c>IndexedWitness</c>'s no-op <c>DisposeAsync</c> —
    /// required by the <see cref="IObserverAsync{T}"/> contract but never invoked by the
    /// pipeline, so coverage of the line otherwise relies on a direct call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIndexedObserverDisposed_ThenNoOp()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1)];
        NoOpWitness downstream = new();
        SyncLatestEnumerableCoordinator<int, int> subscription =
            new(
                sources,
                downstream,
                static s => s[0]);
        SyncLatestEnumerableWitness<int, int> indexed = new(subscription, 0);

        await indexed.DisposeAsync();

        await Assert.That(indexed).IsNotNull();
    }

    /// <summary>No-op downstream observer.</summary>
    private sealed class NoOpWitness : IObserverAsync<int>
    {
        /// <inheritdoc/>
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
