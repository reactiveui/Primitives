// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Inner observer that relays projected values to a flat-map coordinator.</summary>
/// <typeparam name="TResult">The result element type.</typeparam>
[DebuggerDisplay("FlatMapWitness: Coordinator = {Coordinator}")]
public sealed class FlatMapWitness<TResult> : IWitnessAsync<TResult>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Initializes a new instance of the <see cref="FlatMapWitness{TResult}"/> class.</summary>
    /// <param name="coordinator">The flat-map coordinator.</param>
    public FlatMapWitness(FlatMapCoordinator<TResult> coordinator) => Coordinator = coordinator;

    /// <summary>Gets the flat-map coordinator.</summary>
    private FlatMapCoordinator<TResult> Coordinator { get; }

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TResult value, CancellationToken cancellationToken) =>
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
    ValueTask IWitnessAsync<TResult>.OnNextAsyncCore(TResult value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayNextAsync(value);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<TResult>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayErrorAsync(error);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TResult>.OnCompletedAsyncCore(Result result) =>
        Coordinator.CompleteInnerAsync(result);
}
