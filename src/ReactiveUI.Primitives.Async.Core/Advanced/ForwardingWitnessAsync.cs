// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Base observer that forwards every notification to a downstream observer. Every notification is already
/// implemented here, so nothing is left for a derived type to supply: this is a base class, not a contract.
/// The protected constructor, rather than <c>abstract</c>, is what keeps it from being used on its own.
/// </summary>
/// <typeparam name="T">The observed element type.</typeparam>
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
