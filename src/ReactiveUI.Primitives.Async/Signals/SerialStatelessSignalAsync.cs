// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents a stateless asynchronous Signal that notifies observers of events in a serial, sequential manner.
/// </summary>
/// <remarks>Observers are notified one at a time in the order they are registered. Each observer receives the
/// event only after the previous observer has completed processing. This class is suitable for scenarios where event
/// delivery order and sequential processing are required. Thread safety and ordering are managed internally.</remarks>
/// <typeparam name="T">The type of the elements processed and observed by the Signal.</typeparam>
public sealed class SerialStatelessSignalAsync<T> : BaseStatelessSignalAsync<T>
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
