// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that emits items from the source until the specified cancellation token is canceled.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="cancellationToken">The cancellation token that triggers completion.</param>
[System.Diagnostics.DebuggerDisplay("CancellationStopSignal: Source = {Source}")]
public sealed class CancellationStopSignal<T>(IObservableAsync<T> source, CancellationToken cancellationToken) : IObservableAsync<T>
{
    /// <summary>Gets the source observable sequence.</summary>
    internal IObservableAsync<T> Source { get; } = source;

    /// <summary>Gets the cancellation token that triggers completion.</summary>
    internal CancellationToken CancellationToken { get; } = cancellationToken;

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        CancellationStopCoordinator<T> subscription = new(this, observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
