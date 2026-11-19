// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that merges items from an observable of observables into a single stream.</summary>
/// <typeparam name="T">The type of the elements emitted by the inner observable sequences.</typeparam>
/// <param name="sources">The source observable whose inner observable sequences will be merged.</param>
[System.Diagnostics.DebuggerDisplay("BlendSignalSourcesSignal<{typeof(T).Name,nq}>")]
public sealed class BlendSignalSourcesSignal<T>(IObservableAsync<IObservableAsync<T>> sources) : IObservableAsync<T>
{
    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        BlendCoordinator<T> subscription = new(observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(sources, cancellationToken));
    }
}
