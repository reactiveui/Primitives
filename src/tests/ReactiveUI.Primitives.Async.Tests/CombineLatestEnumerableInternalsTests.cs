// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests disposal of per-source sync-latest observers.</summary>
public class CombineLatestEnumerableInternalsTests
{
    /// <summary>Verifies that disposing a per-source observer is a no-op.</summary>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
