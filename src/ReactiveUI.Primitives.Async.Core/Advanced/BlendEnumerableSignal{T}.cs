// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that merges items from an enumerable collection of observables into a single stream.</summary>
/// <typeparam name="T">The type of the elements emitted by the observable sequences.</typeparam>
/// <param name="sources">The collection of source observables to merge.</param>
[System.Diagnostics.DebuggerDisplay("BlendEnumerableSignal<{typeof(T).Name,nq}>")]
public sealed class BlendEnumerableSignal<T>(IEnumerable<IObservableAsync<T>> sources) : IObservableAsync<T>
{
    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        BlendSequenceCoordinator<T> subscription = new(observer, sources);
        subscription.LinkExternalCancellation(cancellationToken);
        subscription.BeginSubscribing();
        return new(subscription);
    }
}
