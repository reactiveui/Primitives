// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness that flattens each upstream enumerable inline.</summary>
/// <typeparam name="T">The flattened element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("ForEachEnumerableWitness<T>: {_witness}")]
public sealed class ForEachEnumerableWitness<T>(
    IObserverAsync<T> downstream,
    CancellationToken subscribeToken) : IWitnessAsync<IEnumerable<T>>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness = new(subscribeToken);

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(IEnumerable<T> value, CancellationToken cancellationToken) =>
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
    async ValueTask IWitnessAsync<IEnumerable<T>>.OnNextAsyncCore(
        IEnumerable<T> value,
        CancellationToken cancellationToken)
    {
        if (value is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                await downstream.OnNextAsync(array[i], cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        if (value is IReadOnlyList<T> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                await downstream.OnNextAsync(list[i], cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        foreach (var item in value)
        {
            await downstream.OnNextAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<IEnumerable<T>>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<IEnumerable<T>>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);
}
