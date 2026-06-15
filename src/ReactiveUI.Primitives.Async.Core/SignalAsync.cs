// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents an asynchronous observable sequence that allows observers to subscribe and receive notifications
/// asynchronously.
/// </summary>
/// <remarks>Implementations of this class enable the observer pattern for asynchronous event streams, allowing
/// observers to receive notifications as data becomes available. Observers should dispose the returned subscription to
/// stop receiving notifications and release resources. This class is intended to be used as a base for custom
/// asynchronous observable implementations.</remarks>
/// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
public abstract class SignalAsync<T> : IObservableAsync<T>
{
    /// <summary>Asynchronously subscribes the specified asynchronous observer to receive notifications from the observable sequence.</summary>
    /// <remarks>The returned <see cref="IAsyncDisposable"/> should be disposed when the observer no longer
    /// wishes to receive notifications. Multiple calls to this method with the same observer will result in multiple
    /// independent subscriptions.</remarks>
    /// <param name="observer">The asynchronous observer that will receive notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A task that represents the asynchronous operation. The result contains an <see cref="IAsyncDisposable"/> that
    /// can be disposed to unsubscribe the observer.</returns>
    public ValueTask<IAsyncDisposable>
        SubscribeAsync(IObserverAsync<T> observer, CancellationToken cancellationToken) =>
        SubscribeAsyncCore(observer, cancellationToken);

    /// <summary>Subscribes the specified asynchronous observer to receive notifications from the observable sequence.</summary>
    /// <remarks>The returned disposable should be disposed when the observer no longer wishes to receive
    /// notifications. Multiple calls to this method may result in multiple independent subscriptions.</remarks>
    /// <param name="observer">The observer that will receive asynchronous notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation. The result contains an object that can be
    /// disposed to unsubscribe the observer.</returns>
    protected abstract ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken);
}
