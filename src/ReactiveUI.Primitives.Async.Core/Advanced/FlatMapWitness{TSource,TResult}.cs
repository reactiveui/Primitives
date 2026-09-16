// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Outer observer that projects source values to inner observables.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The result element type.</typeparam>
[DebuggerDisplay("FlatMapWitness: Coordinator = {Coordinator}, SyncSelector = {SyncSelector}, AsyncSelector = {AsyncSelector}")]
public sealed class FlatMapWitness<TSource, TResult> : IWitnessAsync<TSource>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Initializes a new instance of the <see cref="FlatMapWitness{TSource,TResult}"/> class.</summary>
    /// <param name="coordinator">The flat-map coordinator.</param>
    /// <param name="syncSelector">The synchronous selector.</param>
    /// <param name="asyncSelector">The asynchronous selector.</param>
    public FlatMapWitness(
        FlatMapCoordinator<TResult> coordinator,
        Func<TSource, IObservableAsync<TResult>>? syncSelector,
        Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>>? asyncSelector)
    {
        Coordinator = coordinator;
        SyncSelector = syncSelector;
        AsyncSelector = asyncSelector;
    }

    /// <summary>Gets the flat-map coordinator.</summary>
    private FlatMapCoordinator<TResult> Coordinator { get; }

    /// <summary>Gets the synchronous selector.</summary>
    private Func<TSource, IObservableAsync<TResult>>? SyncSelector { get; }

    /// <summary>Gets the asynchronous selector.</summary>
    private Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>>? AsyncSelector { get; }

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
        var inner = SyncSelector is not null
            ? SyncSelector(value)
            : await AsyncSelector!(value, cancellationToken).ConfigureAwait(false);

        await Coordinator.SubscribeInnerAsync(inner).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayErrorAsync(error);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
        Coordinator.CompleteOuterAsync(result);
}
