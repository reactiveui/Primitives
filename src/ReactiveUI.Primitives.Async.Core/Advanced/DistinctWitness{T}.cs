// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness that tracks seen values in a <see cref="HashSet{T}"/>.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream witness.</param>
/// <param name="comparer">The equality comparer.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("DistinctWitness<T>: {_witness}")]
public sealed class DistinctWitness<T>(
    IObserverAsync<T> downstream,
    IEqualityComparer<T> comparer,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>The values forwarded so far; <see cref="HashSet{T}.Add"/> returns <see langword="false"/> for a duplicate.</summary>
    private readonly HashSet<T> _seen = [with(comparer)];

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
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        _seen.Add(value) ? downstream.OnNextAsync(value, cancellationToken) : default;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);
}
