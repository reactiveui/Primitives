// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents a stateless asynchronous subject that forwards notifications to observers concurrently.
/// </summary>
/// <remarks>This subject distributes notifications to all subscribed observers in parallel, allowing for improved
/// throughput in scenarios where observer processing can occur independently. Use this type when observer notification
/// order is not important and concurrent delivery is desired. Thread safety is ensured for concurrent observer
/// notifications.</remarks>
/// <typeparam name="T">The type of the elements processed by the subject.</typeparam>
public sealed class ConcurrentStatelessSubjectAsync<T> : BaseStatelessSubjectAsync<T>
{
    /// <summary>
    /// Asynchronously notifies all observers in the collection with the specified value.
    /// </summary>
    /// <remarks>Observers are notified concurrently. The operation completes when all observers have been
    /// notified or when the operation is canceled.</remarks>
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
    /// Notifies all specified observers of an error and resumes processing asynchronously.
    /// </summary>
    /// <param name="observers">The collection of observers to notify of the error. Cannot be null.</param>
    /// <param name="error">The exception that occurred. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
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
    /// <remarks>Observers are notified concurrently. The method completes when all observers have been
    /// notified.</remarks>
    /// <param name="observers">A read-only list of observers to be notified of the completion event. Cannot be null.</param>
    /// <param name="result">The result to forward to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected override ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result) =>
        Concurrent.ForwardOnCompletedConcurrently(observers, result);
}
