// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that runs a callback when disposed.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="observer">The downstream observer to forward notifications to.</param>
/// <param name="finallySync">The synchronous action to invoke when disposed; <see langword="null"/> when the callback is asynchronous.</param>
/// <param name="finallyAsync">The asynchronous callback to invoke when disposed; <see langword="null"/> when the callback is synchronous.</param>
[System.Diagnostics.DebuggerDisplay("OnDisposeWitness: {_witness}")]
public sealed class OnDisposeWitness<T>(
    IObserverAsync<T> observer,
    Action? finallySync,
    Func<ValueTask>? finallyAsync) : IWitnessAsync<T>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Latches to 1 once the disposal callback has run so completion plus explicit disposal invokes it once.</summary>
    private int _invoked;

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
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        Downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => Downstream.OnCompletedAsync(result);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        ExceptionDispatchInfo? failure = null;
        if (Interlocked.Exchange(ref _invoked, 1) == 0)
        {
            try
            {
                if (finallySync is not null)
                {
                    finallySync();
                }
                else if (finallyAsync is not null)
                {
                    await finallyAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                failure = ExceptionDispatchInfo.Capture(e);
            }
        }

        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
        failure?.Throw();
    }
}
