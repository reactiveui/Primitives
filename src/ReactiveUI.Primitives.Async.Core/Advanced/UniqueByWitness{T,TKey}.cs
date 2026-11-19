// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness that compares extracted keys against the most-recently-forwarded one.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
/// <param name="downstream">The downstream witness.</param>
/// <param name="keySelector">The key selector.</param>
/// <param name="comparer">The key equality comparer.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("UniqueByWitness<T, TKey>: {_witness}")]
public sealed class UniqueByWitness<T, TKey>(
    IObserverAsync<T> downstream,
    Func<T, TKey> keySelector,
    IEqualityComparer<TKey> comparer,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>The most-recently-forwarded key; valid only when <see cref="_hasPrevious"/> is set.</summary>
    private TKey? _previousKey;

    /// <summary>Latches to <see langword="true"/> after the first emission has been forwarded.</summary>
    private bool _hasPrevious;

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
        var key = keySelector(value);
        if (_hasPrevious && comparer.Equals(_previousKey!, key))
        {
            return default;
        }

        _previousKey = key;
        _hasPrevious = true;
        return downstream.OnNextAsync(value, cancellationToken);
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
