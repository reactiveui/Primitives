// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription accumulator observer.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TAccumulate">The accumulator type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="seed">The seed accumulator value emitted during subscription.</param>
/// <param name="accumulator">The synchronous accumulator.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("ScanWithInitialWitness<TSource, TAccumulate>: {_witness}")]
public sealed class ScanWithInitialWitness<TSource, TAccumulate>(
    IObserverAsync<TAccumulate> downstream,
    TAccumulate seed,
    Func<TAccumulate, TSource, TAccumulate> accumulator,
    CancellationToken subscribeToken) : IWitnessAsync<TSource>
{
    /// <summary>Running accumulator state; seeded with the initial value.</summary>
    private TAccumulate _accumulator = seed;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness = new(subscribeToken);

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
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
    ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
    {
        _accumulator = accumulator(_accumulator, value);
        return downstream.OnNextAsync(_accumulator, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);
}
