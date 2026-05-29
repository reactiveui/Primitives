// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents an asynchronous subject that replays only the latest value to new subscribers and ensures that
/// notifications are delivered to observers in a serial, thread-safe manner.
/// </summary>
/// <remarks>This subject is designed for scenarios where only the most recent value is relevant to subscribers.
/// When a new observer subscribes, it immediately receives the latest value (if any) and then all subsequent
/// notifications. All observer notifications are performed asynchronously and in a serial order, ensuring thread
/// safety. This type is suitable for use cases where replaying only the latest value is desired, such as event streams
/// or state broadcasts.</remarks>
/// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
/// <param name="startValue">An optional initial value to be emitted to new subscribers before any other values are published.</param>
public sealed class SerialReplayLatestSubjectAsync<T>(Optional<T> startValue)
    : BaseReplayLatestSubjectAsync<T>(startValue)
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
