// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Base observer that forwards every notification unchanged to a downstream observer. A derived type
/// overrides only the notifications it needs to intercept and inherits pass-through behaviour for the rest.
/// </summary>
/// <typeparam name="T">The observed element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ForwardingWitnessAsync: Downstream = {Downstream}")]
public class ForwardingWitnessAsync<T> : WitnessAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="ForwardingWitnessAsync{T}"/> class.</summary>
    /// <param name="downstream">The observer that receives forwarded notifications.</param>
    protected ForwardingWitnessAsync(IObserverAsync<T> downstream) => Downstream = downstream;

    /// <summary>Gets the downstream observer.</summary>
    protected IObserverAsync<T> Downstream { get; }

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
