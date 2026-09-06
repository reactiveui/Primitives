// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits values from an asynchronous enumerable for each subscription.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsyncEnumerableSignal: Source = {Source}")]
public sealed class AsyncEnumerableSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncEnumerableSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    public AsyncEnumerableSignal(IAsyncEnumerable<T> source) => Source = source;

    /// <summary>Gets the source sequence.</summary>
    private IAsyncEnumerable<T> Source { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        AsyncEnumerableSubscription<T> subscription = new(observer, Source);
        subscription.Start();
        return new(subscription);
    }
}
