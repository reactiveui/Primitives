// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents a provider for asynchronous push-based notifications that supports asynchronous subscription and disposal.</summary>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
/// <remarks>Subscription and unsubscription are themselves awaitable, so a sequence backed by network or I/O work can
/// complete its setup and teardown before the caller proceeds.</remarks>
public interface IObservableAsync<T>
{
    /// <summary>Subscribes the specified asynchronous observer to receive notifications from the observable sequence.</summary>
    /// <param name="observer">The observer that will receive asynchronous notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A handle whose disposal unsubscribes the observer.</returns>
    /// <remarks>Implementations must tolerate several observers subscribed at once, and must release the
    /// subscription's resources when the returned handle is disposed.</remarks>
    ValueTask<IAsyncDisposable> SubscribeAsync(IObserverAsync<T> observer, CancellationToken cancellationToken);
}
