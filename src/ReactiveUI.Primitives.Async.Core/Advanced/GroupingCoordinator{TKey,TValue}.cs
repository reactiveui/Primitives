// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer subscription that tracks groups by key, creating new grouped observables as new keys are encountered.</summary>
/// <typeparam name="TKey">The type of the grouping key.</typeparam>
/// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
/// <param name="parent">The parent GroupBy observable that provides the key selector and signal factory.</param>
/// <param name="observer">The downstream observer to receive grouped observables.</param>
[System.Diagnostics.DebuggerDisplay("GroupingCoordinator<TKey, TValue>: {_witness}")]
public sealed class GroupingCoordinator<TKey, TValue>(
    GroupByAsyncSignal<TKey, TValue> parent,
    IObserverAsync<GroupedAsyncSignal<TKey, TValue>> observer) : IWitnessAsync<TValue>
    where TKey : notnull
{
    /// <summary>The composite disposable that tracks all group subscription disposables.</summary>
    private readonly MultipleDisposableAsync _disposables = new();

    /// <summary>A dictionary mapping each encountered key to its corresponding group signal.</summary>
    private Dictionary<TKey, ISignalAsync<TValue>> _signalsByKey = [];

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TValue value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <summary>Disposes all tracked group subscriptions.</summary>
    /// <returns>A task representing the asynchronous disposal operation.</returns>
    public async ValueTask DisposeAsync()
    {
        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
        await _disposables.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Subscribes this observer to the parent's source sequence.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable representing the source subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(parent.Source, this, observer, cancellationToken);

    /// <summary>Routes the element to the appropriate group signal, creating a new group if the key is new.</summary>
    /// <param name="value">The element to route.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    async ValueTask IWitnessAsync<TValue>.OnNextAsyncCore(TValue value, CancellationToken cancellationToken)
    {
        var key = parent.KeySelector(value);
        if (!_signalsByKey.TryGetValue(key, out var signal))
        {
            signal = parent.GroupSignalSelector(key);
            _signalsByKey.Add(key, signal);

            await observer.OnNextAsync(
                new(
                    key,
                    signal.Values,
                    _disposables,
                    this.InternalDisposedToken),
                cancellationToken).ConfigureAwait(false);
        }

        await signal.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Forwards a non-fatal error to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TValue>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Completes all group signals and then completes the downstream observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    async ValueTask IWitnessAsync<TValue>.OnCompletedAsyncCore(Result result)
    {
        var signals = _signalsByKey.Values;
        _signalsByKey = null!;
        foreach (var signal in signals)
        {
            await signal.OnCompletedAsync(result).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(result).ConfigureAwait(false);
    }
}
