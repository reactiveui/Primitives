// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer for a single retry attempt.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Subscription = {Subscription}")]
public sealed class ReattemptWitness<T> : WitnessAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="ReattemptWitness{T}"/> class.</summary>
    /// <param name="subscription">The retry coordinator.</param>
    public ReattemptWitness(ReattemptSubscription<T> subscription) => Subscription = subscription;

    /// <summary>Gets the retry coordinator.</summary>
    private ReattemptSubscription<T> Subscription { get; }

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Subscription.RelayNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        Subscription.RelayErrorAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        Subscription.CompleteAttemptAsync(result);
}
