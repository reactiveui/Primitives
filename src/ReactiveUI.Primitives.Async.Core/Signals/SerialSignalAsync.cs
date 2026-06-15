// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that notifies observers in a serial manner, ensuring each observer is notified
/// one at a time.
/// </summary>
/// <remarks>SerialSignalAsync{T} is designed for scenarios where observers must be notified sequentially rather
/// than concurrently. This can be useful when observer operations are not thread-safe or when order of notification is
/// important. Notifications to observers are performed asynchronously and in sequence.</remarks>
/// <typeparam name="T">The type of the elements processed and observed by the Signal.</typeparam>
public sealed class SerialSignalAsync<T> : BaseSignalAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        SerialBroadcastHelpers.BroadcastOnNextAsync(observers, value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken) =>
        SerialBroadcastHelpers.BroadcastOnErrorResumeAsync(observers, error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result) =>
        SerialBroadcastHelpers.BroadcastOnCompletedAsync(observers, result);
}
