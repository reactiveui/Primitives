// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Witness wrapper that forwards all notifications and decrements the parent's reference count on disposal, disconnecting from the source when the count reaches zero.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="parent">The parent ref-count observable.</param>
/// <param name="observer">The downstream witness to forward notifications to.</param>
[System.Diagnostics.DebuggerDisplay("RefCountWitness<T>: {_witness}")]
public sealed class RefCountWitness<T>(RefCountSignal<T> parent, IObserverAsync<T> observer) : IWitnessAsync<T>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

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

    /// <summary>Forwards an element to the downstream witness.</summary>
    /// <param name="value">The element to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        observer.OnNextAsync(value, cancellationToken);

    /// <summary>Forwards a non-fatal error to the downstream witness.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Forwards completion to the downstream observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

    /// <summary>Decrements the parent's reference count and disconnects from the source when it reaches zero.</summary>
    /// <returns>A task representing the asynchronous disposal operation.</returns>
    [DebuggerStepThrough]
    public async ValueTask DisposeAsync()
    {
        await parent.ReleaseSubscriberAsync().ConfigureAwait(false);
        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
    }
}
