// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Defines an asynchronous observer that receives notifications about a sequence of values, completion, or errors, and supports asynchronous resource cleanup.</summary>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
/// <remarks>Every notification is awaitable and cancellable, so a producer that awaits them gets backpressure for
/// free: the observer's handler has to finish before the next value is pushed.</remarks>
public interface IObserverAsync<in T> : IAsyncDisposable
{
    /// <summary>Signals that the sequence has terminated, successfully or with a failure.</summary>
    /// <param name="result">The terminal outcome, carrying the failure when the sequence faulted.</param>
    /// <returns>A task that completes when the observer has handled the termination.</returns>
    ValueTask OnCompletedAsync(Result result);

    /// <summary>Reports a non-terminal error, leaving the sequence free to carry on.</summary>
    /// <param name="error">The exception to report.</param>
    /// <param name="cancellationToken">A token that cancels the observer's handling of the error.</param>
    /// <returns>A task that completes when the observer has handled the error.</returns>
    /// <remarks>Unlike a faulted <see cref="OnCompletedAsync"/>, this does not end the sequence; an implementation
    /// chooses whether to swallow the error or tear itself down.</remarks>
    ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken);

    /// <summary>Delivers the next value in the sequence.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A token that cancels the observer's handling of the value.</param>
    /// <returns>A task that completes when the observer has consumed the value.</returns>
    ValueTask OnNextAsync(T value, CancellationToken cancellationToken);
}
