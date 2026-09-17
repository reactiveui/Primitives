// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-source observer used by arity-specific <c>SyncLatest</c> coordinators to record typed source values and forward source terminal notifications to the shared lifecycle.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The downstream element type.</typeparam>
/// <param name="parent">The parent coordinator.</param>
/// <param name="sourceBit">The completion bit owned by the source.</param>
/// <param name="recordValue">Stores the latest source value in the parent coordinator.</param>
[DebuggerDisplay("SyncLatestWitness: {_witness}")]
public sealed class SyncLatestWitness<TSource, TResult>(
    ISyncLatestCoordinator<TResult> parent,
    int sourceBit,
    Action<TSource> recordValue) : IWitnessAsync<TSource>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
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
    async ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        lock (parent.Lifecycle.ValuesLock)
        {
            recordValue(value);
        }

        await parent.EmitLatestAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return parent.Lifecycle.OnErrorResumeAsync(error);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
        parent.Lifecycle.OnSourceCompletedAsync(result, sourceBit);
}
