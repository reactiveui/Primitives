// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Defines an asynchronous observer that receives notifications about a sequence of values, completion, or errors, and
/// supports asynchronous resource cleanup.
/// </summary>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
/// <remarks>Implementations of this interface allow for non-blocking, asynchronous handling of data streams,
/// including support for cancellation and proper disposal of resources. This is useful in scenarios where observers
/// need to process events or data asynchronously, such as in reactive or event-driven programming models.</remarks>
public interface IObserverAsync<in T> : IAsyncDisposable
{
    /// <summary>Performs asynchronous completion logic in response to the specified result.</summary>
    /// <param name="result">The result object that provides information about the completed operation. Cannot be null.</param>
    /// <returns>A ValueTask that represents the asynchronous completion operation.</returns>
    ValueTask OnCompletedAsync(Result result);

    /// <summary>Handles the specified error and resumes asynchronous processing, if possible.</summary>
    /// <param name="error">The exception that caused the error. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the error handling operation.</param>
    /// <returns>A ValueTask that represents the asynchronous error handling operation.</returns>
    /// <remarks>Implementations may choose to suppress the error and continue processing, or perform cleanup
    /// and terminate gracefully. The behavior depends on the specific implementation.</remarks>
    ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken);

    /// <summary>Asynchronously processes the next value in the sequence.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    ValueTask OnNextAsync(T value, CancellationToken cancellationToken);
}
