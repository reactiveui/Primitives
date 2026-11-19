// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer that counts emissions and signals completion on the final one.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="budget">The take budget, decremented per emission.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("TakeWitness<T>: {_witness}")]
public sealed class TakeWitness<T>(
    IObserverAsync<T> downstream,
    int budget,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Remaining take budget; decremented per forwarded value.</summary>
    private int _remaining = budget;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness = new(subscribeToken);

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (_remaining == 0)
        {
            return default;
        }

        _remaining--;
        return _remaining == 0
            ? ForwardThenFinishAsync(this, downstream, value, cancellationToken)
            : downstream.OnNextAsync(value, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);

    /// <summary>Forwards the final value, signals downstream completion, and tears down upstream.</summary>
    /// <param name="self">The take witness to dispose from notification.</param>
    /// <param name="target">The downstream observer receiving the final value and the completion.</param>
    /// <param name="value">The final value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes after both the value and the completion are forwarded.</returns>
    private static async ValueTask ForwardThenFinishAsync(
        TakeWitness<T> self,
        IObserverAsync<T> target,
        T value,
        CancellationToken cancellationToken)
    {
        await target.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        await target.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await WitnessAsync.DisposeFromNotificationAsync(self).ConfigureAwait(false);
    }
}
