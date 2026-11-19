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
/// <param name="accumulator">The asynchronous accumulator.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
[System.Diagnostics.DebuggerDisplay("FoldAsyncWitness<T, TAcc>: {_witness}")]
public sealed class FoldAsyncWitness<T, TAcc>(
    IObserverAsync<TAcc> downstream,
    TAcc seed,
    Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
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
        var pending = accumulator(_acc, value, cancellationToken);
        if (pending.IsCompletedSuccessfully)
        {
            _acc = pending.Result;
            return downstream.OnNextAsync(_acc, cancellationToken);
        }

        return AwaitAndForwardAsync(pending, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);

    /// <summary>Slow path for asynchronously-completing accumulators; awaits and then forwards.</summary>
    /// <param name="pending">The pending accumulator <see cref="ValueTask{TResult}"/>.</param>
    /// <param name="cancellationToken">The cancellation token to pass downstream.</param>
    /// <returns>A task that completes after the accumulator resolves and the downstream emission completes.</returns>
    private async ValueTask AwaitAndForwardAsync(ValueTask<TAcc> pending, CancellationToken cancellationToken)
    {
        _acc = await pending.ConfigureAwait(false);
        await downstream.OnNextAsync(_acc, cancellationToken).ConfigureAwait(false);
    }
}
