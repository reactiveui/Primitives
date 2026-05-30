// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Provides factory methods for creating and starting cancelable task-based subscriptions.
/// </summary>
internal static class CancelableTaskSubscription
{
    /// <summary>
    /// Creates and immediately starts a new cancelable task subscription.
    /// </summary>
    /// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
    /// <param name="runAsyncCore">The asynchronous function that defines the subscription logic.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <returns>A running <see cref="CancelableTaskSubscription{T}"/> instance.</returns>
    public static CancelableTaskSubscription<T> CreateAndStart<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> runAsyncCore,
        IObserverAsync<T> observer)
    {
        var ret = new AnonymousCancelableTaskSubscription<T>(runAsyncCore, observer);
        ret.Run();
        return ret;
    }

    /// <summary>
    /// A cancelable task subscription that delegates its core logic to a user-supplied function.
    /// </summary>
    /// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
    /// <param name="runAsyncCore">The asynchronous function that defines the subscription logic.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    internal sealed class AnonymousCancelableTaskSubscription<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> runAsyncCore,
        IObserverAsync<T> observer) : CancelableTaskSubscription<T>(observer)
    {
        /// <inheritdoc/>
        protected override ValueTask RunAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken) =>
            runAsyncCore(observer, cancellationToken);
    }
}
