// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Wraps an <see cref="IObserverAsync{T}"/> to provide base observer behavior while delegating all notifications.
/// </summary>
/// <typeparam name="T">The type of elements received by the observer.</typeparam>
/// <param name="observer">The inner observer to delegate notifications to.</param>
internal sealed class ForwardingAsyncWitness<T>(IObserverAsync<T> observer) : ObserverAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        observer.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);
}
