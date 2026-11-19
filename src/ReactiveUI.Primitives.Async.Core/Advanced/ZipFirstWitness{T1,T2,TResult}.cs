// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer for the first source sequence that delegates to the shared <see cref="ZipState{T1, T2, TResult}"/>.</summary>
/// <typeparam name="T1">The type of the elements in the first source sequence.</typeparam>
/// <typeparam name="T2">The type of the elements in the second source sequence.</typeparam>
/// <typeparam name="TResult">The type of the elements in the resulting sequence produced by the selector function.</typeparam>
/// <param name="state">The shared zip state.</param>
[System.Diagnostics.DebuggerDisplay("ZipFirstWitness: {_witness}")]
public sealed class ZipFirstWitness<T1, T2, TResult>(ZipState<T1, T2, TResult> state) : IWitnessAsync<T1>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T1 value, CancellationToken cancellationToken) =>
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

    /// <summary>Forwards an element from the first source to the zip state for pairing.</summary>
    /// <param name="value">The element from the first source.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T1>.OnNextAsyncCore(T1 value, CancellationToken cancellationToken) =>
        state.OnNext1Async(value, cancellationToken);

    /// <summary>Forwards a non-fatal error to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T1>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        state.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Handles the first source completing.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T1>.OnCompletedAsyncCore(Result result) =>
        state.OnCompleted1Async(result);
}
