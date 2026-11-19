// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer that forwards while the predicate holds.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="predicate">The take-while predicate.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("TakeWhileSyncWitness<T>: {_witness}")]
public sealed class TakeWhileSyncWitness<T>(
    IObserverAsync<T> downstream,
    Func<T, bool> predicate,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Latches to <c>1</c> once the predicate has returned <see langword="false"/>.</summary>
    private int _terminated;

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
        if (Volatile.Read(ref _terminated) != 0)
        {
            return default;
        }

        if (predicate(value))
        {
            return downstream.OnNextAsync(value, cancellationToken);
        }

        Volatile.Write(ref _terminated, 1);
        return CompleteAndDisposeAsync();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);

    /// <summary>Signals downstream completion and tears down the upstream subscription.</summary>
    /// <returns>A task representing the completion and disposal operation.</returns>
    private async ValueTask CompleteAndDisposeAsync()
    {
        await downstream.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false);
    }
}
