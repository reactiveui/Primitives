// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>An asynchronous witness whose notification gate, cancellation link and disposal run through <see cref="WitnessAsync"/>, so an implementation only supplies the hooks.</summary>
/// <typeparam name="T">The type of the elements received by the witness.</typeparam>
/// <remarks>
/// Implement the notification members by forwarding to the matching <see cref="WitnessAsync"/> methods, and implement the
/// hooks explicitly. Implement <see cref="IAsyncDisposable.DisposeAsync"/> by releasing what the witness owns and then
/// returning <see cref="WitnessAsync.DisposeStateAsync"/>.
/// </remarks>
public interface IWitnessAsync<T> : IObserverAsync<T>, IWitnessState
{
    /// <summary>Delivers the value; a failure is routed to error-resume handling and cancellation is swallowed.</summary>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">The effective token for this notification.</param>
    /// <returns>A task that completes when the value has been handled.</returns>
    ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken);

    /// <summary>Handles a non-terminal error; a failure reaches the unhandled exception handler.</summary>
    /// <param name="error">The exception that triggered the error handling logic.</param>
    /// <param name="cancellationToken">The effective token for this notification.</param>
    /// <returns>A task that completes when the error has been handled.</returns>
    ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken);

    /// <summary>Handles the terminal result at most once, before disposal.</summary>
    /// <param name="result">The result of the operation.</param>
    /// <returns>A task that completes when the result has been handled.</returns>
    ValueTask OnCompletedAsyncCore(Result result);
}
