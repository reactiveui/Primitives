// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Applies a synchronous selector to each source value, forwarding without an await state machine.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <typeparam name="TDest">The projected element type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="firstSelector">The synchronous selector.</param>
[System.Diagnostics.DebuggerDisplay("MapSyncSignal<{typeof(T).Name,nq}>")]
public sealed class MapSyncSignal<T, TDest>(
    IObservableAsync<T> source,
    Func<T, TDest> firstSelector) : IObservableAsync<TDest>, IMapFusableAsync<TDest>, IKeepFusableAsync<TDest>
{
    /// <summary>Builds or fuses a synchronous projection operator over <paramref name="source"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="selector">The synchronous projection.</param>
    /// <returns>An observable sequence of projected values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<TDest> Create(IObservableAsync<T> source, Func<T, TDest> selector)
    {
        if (selector is not null)
        {
            if (source is SignalAsync.EmptySignalAsync<T>)
            {
                return SignalAsync.EmptySignalAsync<TDest>.Instance;
            }

            if (source is SignalAsync.NeverSignalAsync<T>)
            {
                return SignalAsync.NeverSignalAsync<TDest>.Instance;
            }

            if (source is IMapFusableAsync<T> fusable && fusable.TryFuseMap(selector) is { } fused)
            {
                return fused;
            }
        }

        return new MapSyncSignal<T, TDest>(source, selector!);
    }

    /// <inheritdoc/>
    IObservableAsync<TNext>? IMapFusableAsync<TDest>.TryFuseMap<TNext>(Func<TDest, TNext> selector) =>
        source is not null && firstSelector is not null
            ? new MapMapSyncSignal<T, TDest, TNext>(source, firstSelector, selector)
            : null;

    /// <inheritdoc/>
    IObservableAsync<TDest>? IKeepFusableAsync<TDest>.TryFuseKeep(Func<TDest, bool> predicate) =>
        source is not null && firstSelector is not null
            ? new MapKeepSyncSignal<T, TDest>(source, firstSelector, predicate)
            : null;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
        IObserverAsync<TDest> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new MapSyncWitness<T, TDest>(observer, firstSelector, cancellationToken),
            observer,
            cancellationToken);
}
