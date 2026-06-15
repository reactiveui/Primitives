// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Provides factory methods for creating and starting cancelable task-based subscriptions.</summary>
public static class TaskSignalSubscription
{
    /// <summary>Creates and immediately starts a new cancelable task subscription.</summary>
    /// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
    /// <param name="executeAsyncCore">The asynchronous function that defines the subscription logic.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <returns>A running <see cref="TaskSignalSubscription{T}"/> instance.</returns>
    public static TaskSignalSubscription<T> StartNew<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> executeAsyncCore,
        IObserverAsync<T> observer)
    {
        AnonymousTaskSignalSubscription<T> ret = new(executeAsyncCore, observer);
        ret.Start();
        return ret;
    }

    /// <summary>A cancelable task subscription that delegates its core logic to a user-supplied function.</summary>
    /// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
    /// <param name="executeAsyncCore">The asynchronous function that defines the subscription logic.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    internal sealed class AnonymousTaskSignalSubscription<T>(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> executeAsyncCore,
        IObserverAsync<T> observer) : TaskSignalSubscription<T>(observer)
    {
        /// <inheritdoc/>
        protected override ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken) =>
            executeAsyncCore(observer, cancellationToken);
    }
}
