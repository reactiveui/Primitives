// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary><see cref="WitnessAsync{T}"/> that forwards every source notification straight into a <see cref="TakeUntilLifecycle{T}"/>, which gates it on its way downstream.</summary>
/// <typeparam name="T">The downstream element type.</typeparam>
/// <param name="lifecycle">The shared lifecycle owning the gate and forwarding logic.</param>
public sealed class TakeUntilSourceWitness<T>(TakeUntilLifecycle<T> lifecycle) : WitnessAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return lifecycle.RelayNextAsync(value);
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return lifecycle.RelayErrorAsync(error);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        lifecycle.RelayCompletionAsync(result);
}
