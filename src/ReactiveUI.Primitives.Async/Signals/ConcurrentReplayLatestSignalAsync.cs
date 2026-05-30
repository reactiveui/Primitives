// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that replays only the latest value to new observers and supports concurrent
/// notification of observers.
/// </summary>
/// <remarks>This Signal notifies all observers concurrently, which can improve throughput in scenarios with
/// multiple observers. The order in which observers receive notifications is not guaranteed. This type is thread-safe
/// and suitable for use in asynchronous and concurrent environments.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be emitted to observers upon subscription if no other value has been published.</param>
public sealed class ConcurrentReplayLatestSignalAsync<T>(Optional<T> startValue)
    : BaseReplayLatestSignalAsync<T>(startValue)
{
    /// <summary>
    /// Asynchronously notifies all observers in the collection with the specified value.
    /// </summary>
    /// <param name="observers">The collection of observers to be notified. Cannot be null.</param>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected override ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        Concurrent.ForwardOnNextConcurrently(observers, value, cancellationToken);

    /// <summary>
    /// Asynchronously notifies all observers of an error and resumes processing, if possible.
    /// </summary>
    /// <param name="observers">The collection of observers to be notified of the error. Cannot be null.</param>
    /// <param name="error">The exception that occurred. Cannot be null.</param>
    /// <param name="cancellationToken">A token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected override ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken) =>
        Concurrent.ForwardOnErrorResumeConcurrently(observers, error, cancellationToken);

    /// <summary>
    /// Notifies all observers that the asynchronous operation has completed, forwarding the specified result to each
    /// observer.
    /// </summary>
    /// <remarks>Observers are notified concurrently. The method does not guarantee the order in which
    /// observers are notified.</remarks>
    /// <param name="observers">A read-only list of observers to be notified of the completion event. Cannot be null.</param>
    /// <param name="result">The result to forward to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected override ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result) =>
        Concurrent.ForwardOnCompletedConcurrently(observers, result);
}
