// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that invokes asynchronous side-effect callbacks before forwarding notifications.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="observer">The downstream observer to forward notifications to.</param>
/// <param name="onNext">An asynchronous callback to invoke for each element, or null to take no action on elements.</param>
/// <param name="onErrorResume">An asynchronous callback to invoke on error, or null to take no action on errors.</param>
/// <param name="onCompleted">An asynchronous callback to invoke on completion, or null to take no action on completion.</param>
[System.Diagnostics.DebuggerDisplay("AsyncSideEffectWitness<T>: {_witness}")]
public sealed class AsyncSideEffectWitness<T>(
    IObserverAsync<T> observer,
    Func<T, CancellationToken, ValueTask>? onNext,
    Func<Exception, CancellationToken, ValueTask>? onErrorResume,
    Func<Result, ValueTask>? onCompleted) : IWitnessAsync<T>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Gets the observer that receives forwarded notifications.</summary>
    private IObserverAsync<T> Downstream { get; } = observer;

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
    async ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (onNext is not null)
        {
            await onNext(value, cancellationToken).ConfigureAwait(false);
        }

        await Downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    async ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(
        Exception error,
        CancellationToken cancellationToken)
    {
        if (onErrorResume is not null)
        {
            await onErrorResume(error, cancellationToken).ConfigureAwait(false);
        }

        await Downstream.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    async ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        if (onCompleted is not null)
        {
            await onCompleted(result).ConfigureAwait(false);
        }

        await Downstream.OnCompletedAsync(result).ConfigureAwait(false);
    }
}
