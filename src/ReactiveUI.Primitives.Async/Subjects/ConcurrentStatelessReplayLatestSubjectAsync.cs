// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents an asynchronous subject that replays the latest value to new observers and forwards notifications to all
/// observers concurrently without maintaining internal state.
/// </summary>
/// <remarks>This subject is designed for concurrent scenarios where notifications to observers should be
/// delivered in parallel. It does not buffer or store a sequence of values, but only replays the most recent value (if
/// any) to new subscribers. Thread safety is ensured for concurrent observer notifications. If a notification operation
/// is canceled, not all observers may receive the notification.</remarks>
/// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
/// <param name="startValue">An optional initial value to be replayed to new observers. If not specified, no value is replayed until the first
/// value is published.</param>
public sealed class ConcurrentStatelessReplayLatestSubjectAsync<T>(Optional<T> startValue)
    : BaseStatelessReplayLastSubjectAsync<T>(startValue)
{
    /// <summary>
    /// Asynchronously notifies all observers in the collection with the specified value.
    /// </summary>
    /// <remarks>Notifications are forwarded to all observers concurrently. If the operation is canceled, not
    /// all observers may receive the notification.</remarks>
    /// <param name="observers">A read-only list of observers to be notified. Cannot be null.</param>
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
    /// <param name="observers">A read-only list of observers to be notified of the error.</param>
    /// <param name="error">The exception that occurred and is to be forwarded to the observers. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected override ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken) =>
        Concurrent.ForwardOnErrorResumeConcurrently(observers, error, cancellationToken);

    /// <summary>
    /// Asynchronously notifies all observers that the sequence has completed, forwarding the completion signal to each
    /// observer.
    /// </summary>
    /// <remarks>Observers are notified concurrently. If any observer throws an exception during notification,
    /// the exception may be propagated according to the implementation of the forwarding mechanism.</remarks>
    /// <param name="observers">A read-only list of observers to be notified of the completion event. Cannot be null.</param>
    /// <param name="result">The result information to be passed to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation. The task completes when all observers have
    /// been notified.</returns>
    protected override ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result) =>
        Concurrent.ForwardOnCompletedConcurrently(observers, result);
}
