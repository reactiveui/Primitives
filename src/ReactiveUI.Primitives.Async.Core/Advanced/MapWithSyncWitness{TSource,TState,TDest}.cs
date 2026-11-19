// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness applying the stateful selector inline.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TState">The caller-supplied state type.</typeparam>
/// <typeparam name="TDest">The projected element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="state">The caller-supplied state.</param>
/// <param name="selector">The stateful projection.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("MapWithSyncWitness<TSource, TState, TDest>: {_witness}")]
public sealed class MapWithSyncWitness<TSource, TState, TDest>(
    IObserverAsync<TDest> downstream,
    TState state,
    Func<TState, TSource, TDest> selector,
    CancellationToken subscribeToken) : IWitnessAsync<TSource>
{
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken) =>
        downstream.OnNextAsync(selector(state, value), cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);
}
