// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Provides an asynchronous subject that forwards notifications to observers concurrently.
/// </summary>
/// <remarks>Observers are notified in parallel for each event. This class is suitable for scenarios where high
/// throughput and concurrent notification of multiple observers are required. Thread safety is ensured for observer
/// notification operations. Cancellation tokens can be used to cancel ongoing notification tasks.</remarks>
/// <typeparam name="T">The type of value observed and forwarded to observers.</typeparam>
public sealed class ConcurrentSubjectAsync<T> : BaseSubjectAsync<T>
{
    /// <summary>
    /// Forwards the specified value to all observers asynchronously.
    /// </summary>
    /// <remarks>Observers are notified concurrently. If cancellation is requested, the operation may
    /// terminate before all observers are notified.</remarks>
    /// <param name="observers">A read-only list of observers that will receive the value. Cannot be null.</param>
    /// <param name="value">The value to be sent to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the forwarding operation.</param>
    /// <returns>A ValueTask that represents the asynchronous forwarding operation.</returns>
    protected override ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        Concurrent.ForwardOnNextConcurrently(observers, value, cancellationToken);

    /// <summary>
    /// Handles an error by resuming asynchronous observation for each observer in the collection.
    /// </summary>
    /// <remarks>This method processes all observers concurrently. If the operation is canceled via the
    /// provided cancellation token, the task will complete in a canceled state.</remarks>
    /// <param name="observers">A read-only list of observers to which the error handling and resumption logic will be applied.</param>
    /// <param name="error">The exception that triggered the error handling process.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask representing the asynchronous operation of forwarding the error and resuming observation for all
    /// observers.</returns>
    protected override ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken) =>
        Concurrent.ForwardOnErrorResumeConcurrently(observers, error, cancellationToken);

    /// <summary>
    /// Notifies all observers of the completion event asynchronously.
    /// </summary>
    /// <remarks>Observers are notified concurrently. If any observer throws an exception during notification,
    /// the exception may be aggregated and surfaced to the caller.</remarks>
    /// <param name="observers">A read-only list of observers to be notified of the completion event.</param>
    /// <param name="result">The result information to be provided to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation. The task completes when all observers have
    /// been notified.</returns>
    protected override ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result) =>
        Concurrent.ForwardOnCompletedConcurrently(observers, result);
}
