// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer with a latched gate; once the gate opens, the predicate is not invoked again.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="predicate">The async skip-while predicate.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("SkipWhileAsyncWitness<T>: {_witness}")]
public sealed class SkipWhileAsyncWitness<T>(
    IObserverAsync<T> downstream,
    Func<T, CancellationToken, ValueTask<bool>> predicate,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Latches to <see langword="false"/> after the predicate first returns <see langword="false"/>.</summary>
    private bool _skipping = true;

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
        if (!_skipping)
        {
            return downstream.OnNextAsync(value, cancellationToken);
        }

        var pending = predicate(value, cancellationToken);
        if (pending.IsCompletedSuccessfully)
        {
            if (pending.Result)
            {
                return default;
            }

            _skipping = false;
            return downstream.OnNextAsync(value, cancellationToken);
        }

        return EvaluateAndForwardAsync(pending, value, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);

    /// <summary>Slow path when the async predicate does not complete synchronously.</summary>
    /// <param name="pending">The pending predicate evaluation.</param>
    /// <param name="value">The candidate value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes after the predicate resolves and (if not skipped) the downstream forward completes.</returns>
    private async ValueTask EvaluateAndForwardAsync(
        ValueTask<bool> pending,
        T value,
        CancellationToken cancellationToken)
    {
        if (await pending.ConfigureAwait(false))
        {
            return;
        }

        _skipping = false;
        await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
    }
}
