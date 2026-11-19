// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer that maintains the running accumulator and forwards each result.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <typeparam name="TAcc">The accumulator type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="seed">The initial accumulator value used to prime <see cref="_acc"/>.</param>
/// <param name="accumulator">The synchronous accumulator.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
[System.Diagnostics.DebuggerDisplay("FoldSyncWitness<T, TAcc>: {_witness}")]
public sealed class FoldSyncWitness<T, TAcc>(
    IObserverAsync<TAcc> downstream,
    TAcc seed,
    Func<TAcc, T, TAcc> accumulator,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>The accumulator state protected by the observer's notification gate.</summary>
    private TAcc _acc = seed;

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
        _acc = accumulator(_acc, value);
        return downstream.OnNextAsync(_acc, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);
}
