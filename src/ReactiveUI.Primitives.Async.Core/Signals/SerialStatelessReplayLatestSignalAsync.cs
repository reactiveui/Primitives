// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents a serial, stateless asynchronous Signal that replays only the last value to new observers and supports
/// asynchronous notification delivery.
/// </summary>
/// <remarks>This Signal delivers notifications to observers one at a time in the order they are received. It
/// does not maintain any state beyond the most recent value, and only the last value (if any) is replayed to new
/// subscribers. All observer notifications are dispatched asynchronously and serially, ensuring that each observer
/// receives notifications in the correct order.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be replayed to new observers before any values are published. If not specified, no
/// value is replayed until the first value is received.</param>
public sealed class SerialStatelessReplayLatestSignalAsync<T>(Optional<T> startValue)
    : BaseStatelessReplayLatestSignalAsync<T>(startValue)
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        SerialBroadcastHelpers.BroadcastOnNextAsyncMulti(observers, value, cancellationToken);

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
