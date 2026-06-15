// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents a provider for asynchronous push-based notifications that supports asynchronous subscription and disposal.</summary>
/// <remarks>Use this interface to implement observable sequences that allow observers to subscribe asynchronously
/// and receive notifications in an asynchronous manner. This is useful for scenarios where subscription or
/// unsubscription may involve asynchronous operations, such as network or I/O-bound tasks. Implementations should
/// ensure that notifications are delivered according to the observer's contract and that resources are released when
/// the subscription is disposed.</remarks>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
public interface IObservableAsync<T>
{
    /// <summary>Subscribes the specified asynchronous observer to receive notifications from the observable sequence.</summary>
    /// <remarks>The returned <see cref="IAsyncDisposable"/> should be disposed when the observer no longer
    /// wishes to receive notifications. Multiple observers may be subscribed concurrently.</remarks>
    /// <param name="observer">The observer that will receive asynchronous notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation. The result contains an <see
    /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe the observer.</returns>
    ValueTask<IAsyncDisposable> SubscribeAsync(IObserverAsync<T> observer, CancellationToken cancellationToken);
}
