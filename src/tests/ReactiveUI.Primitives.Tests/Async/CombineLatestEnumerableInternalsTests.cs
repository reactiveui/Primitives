// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Direct unit tests for the internal types inside
/// <c>CombineLatestEnumerableSignal{TSource,TResult}</c> that the public API path doesn't
/// fully exercise — specifically the contractual <see cref="IAsyncDisposable.DisposeAsync"/>
/// stub on <c>IndexedObserver</c>.</summary>
public class CombineLatestEnumerableInternalsTests
{
    /// <summary>Verifies the per-source <c>IndexedObserver</c>'s no-op <c>DisposeAsync</c> —
    /// required by the <see cref="IObserverAsync{T}"/> contract but never invoked by the
    /// pipeline, so coverage of the line otherwise relies on a direct call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIndexedObserverDisposed_ThenNoOp()
    {
        var sources = new[] { SignalAsync.Return(1) };
        var downstream = new NoOpObserver();
        var subscription = new SignalAsync.CombineLatestEnumerableSignal<int, int>.Subscription(
            sources,
            downstream,
            static s => s[0]);
        var indexed = new SignalAsync.CombineLatestEnumerableSignal<int, int>.IndexedObserver(subscription, 0);

        await indexed.DisposeAsync();

        await Assert.That(indexed).IsNotNull();
    }

    /// <summary>No-op downstream observer.</summary>
    private sealed class NoOpObserver : IObserverAsync<int>
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
