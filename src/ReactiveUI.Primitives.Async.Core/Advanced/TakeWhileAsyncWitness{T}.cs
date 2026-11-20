// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer that skips the await when the predicate completes synchronously.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="predicate">The async take-while predicate.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("TakeWhileAsyncWitness<T>: {_witness}")]
public sealed class TakeWhileAsyncWitness<T>(
    IObserverAsync<T> downstream,
    Func<T, CancellationToken, ValueTask<bool>> predicate,
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

        var pending = predicate(value, cancellationToken);
        if (pending.IsCompletedSuccessfully)
        {
            if (pending.Result)
            {
                return downstream.OnNextAsync(value, cancellationToken);
            }

            Volatile.Write(ref _terminated, 1);
            return CompleteAndDisposeAsync();
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

    /// <summary>Signals downstream completion and tears down the upstream subscription.</summary>
    /// <returns>A task representing the completion and disposal operation.</returns>
    private async ValueTask CompleteAndDisposeAsync()
    {
        await downstream.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false);
    }

    /// <summary>Slow path when the async predicate does not complete synchronously.</summary>
    /// <param name="pending">The pending predicate evaluation.</param>
    /// <param name="value">The candidate value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes after the predicate resolves and the downstream emission / completion runs.</returns>
    private async ValueTask EvaluateAndForwardAsync(
        ValueTask<bool> pending,
        T value,
        CancellationToken cancellationToken)
    {
        if (await pending.ConfigureAwait(false))
        {
            await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (Interlocked.Exchange(ref _terminated, 1) == 0)
        {
            await CompleteAndDisposeAsync().ConfigureAwait(false);
        }
    }
}
