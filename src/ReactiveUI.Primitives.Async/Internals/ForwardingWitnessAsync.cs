// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>Base observer that forwards every notification to a downstream observer.</summary>
/// <typeparam name="T">The observed element type.</typeparam>
/// <param name="downstream">The observer that receives forwarded notifications.</param>
internal abstract class ForwardingWitnessAsync<T>(IObserverAsync<T> downstream) : ObserverAsync<T>
{
    /// <summary>Gets the downstream observer.</summary>
    protected IObserverAsync<T> Downstream { get; } = downstream;

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        Downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        Downstream.OnCompletedAsync(result);
}
